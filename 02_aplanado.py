"""
02_aplanado.py: de raw a silver.

Lee una versión del dump JSON de Wikidata (bz2 o texto plano), la parsea con un
esquema mínimo y escribe Parquet particionado por versión:

  silver/entidades/version=V/                 id, tipo, modificado
  silver/etiquetas/version=V/idioma=es|en/    id, etiqueta, descripcion
  silver/p31/version=V/                       id, p31
  silver/catalogo_versiones/version=V/        una fila: archivo, md5, conteos, duración

El dump se lee una sola vez: primero se guarda un Parquet intermedio, luego se
revisan los controles de calidad y solo si pasan se publican las tablas silver.

Uso en Dataproc:
  gcloud dataproc jobs submit pyspark 02_aplanado.py --cluster=CLUSTER --region=us-central1 -- \
      --entrada=gs://BUCKET/raw/wikidata/version=20261005/wikidata-20261005-all.json.bz2 \
      --version=20261005 --salida=gs://BUCKET/silver

Con --solo-medir no escribe nada: procesa dos pedazos del archivo y estima
cuántas horas-núcleo tomaría procesarlo completo.
"""
import argparse
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T

# Esquema mínimo: solo lo que usamos de cada entidad. Spark se salta el resto del JSON.
VALOR = T.StructType([T.StructField("value", T.StringType())])
ES_EN = T.StructType([T.StructField("es", VALOR), T.StructField("en", VALOR)])
DECLARACION = T.StructType([
    T.StructField("mainsnak", T.StructType([
        T.StructField("datavalue", T.StructType([
            T.StructField("value", T.StructType([T.StructField("id", T.StringType())])),
        ])),
    ])),
    T.StructField("rank", T.StringType()),
])
ESQUEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("modified", T.StringType()),
    T.StructField("labels", ES_EN),
    T.StructField("descriptions", ES_EN),
    T.StructField("claims", T.StructType([T.StructField("P31", T.ArrayType(DECLARACION))])),
])

ID_VALIDO = r"^[A-Z][1-9][0-9]*$"  # una letra y un número, como Q31 o P31

# Valores de P31 sin vacíos, sin rango deprecated y sin repetidos.
P31 = (
    "array_distinct(filter(transform("
    "filter(e.claims.P31, c -> c.rank IS NULL OR c.rank <> 'deprecated'), "
    "c -> c.mainsnak.datavalue.value.id), x -> x IS NOT NULL))"
)


def log(msg):
    print(f"[aplanado {datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def leer_y_parsear(spark, entrada):
    """Una fila por línea con entidad. El archivo es un arreglo JSON con una entidad
    por línea: se descartan las líneas '[' y ']' y se quita la coma final."""
    lineas = spark.read.text(entrada).where(
        ~F.col("value").isin("[", "]") & (F.length("value") > 0))
    entidad = F.from_json(F.regexp_replace("value", ",$", ""), ESQUEMA)
    return (
        lineas
        .select(entidad.alias("e"), "value", F.expr("octet_length(value)").alias("bytes_linea"))
        .select(
            F.col("e.id").alias("id"),
            F.col("e.type").alias("tipo"),
            F.expr("try_cast(e.modified AS TIMESTAMP)").alias("modificado"),
            F.col("e.labels.es.value").alias("etiqueta_es"),
            F.col("e.descriptions.es.value").alias("descripcion_es"),
            F.col("e.labels.en.value").alias("etiqueta_en"),
            F.col("e.descriptions.en.value").alias("descripcion_en"),
            F.expr(P31).alias("p31"),
            F.col("e.id").isNotNull().alias("ok"),
            F.when(F.col("e.id").isNull(), F.substring("value", 1, 300)).alias("linea_con_error"),
            "bytes_linea",
        )
    )


# ---------- utilidades de archivos (Hadoop FS, funciona igual con gs:// y local) ----------

def _fs(spark, ruta):
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(ruta)
    return p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), p


def existe(spark, ruta):
    fs, p = _fs(spark, ruta)
    return fs.exists(p)


def borrar(spark, ruta):
    fs, p = _fs(spark, ruta)
    if fs.exists(p):
        fs.delete(p, True)


def borrar_si_vacia(spark, ruta):
    fs, p = _fs(spark, ruta)
    if fs.exists(p) and len(fs.listStatus(p)) == 0:
        fs.delete(p, True)


def tamano(spark, ruta):
    try:
        fs, p = _fs(spark, ruta)
        return int(fs.getContentSummary(p).getLength())
    except Exception:
        return None


def md5_publicado(spark, entrada, version):
    """Busca el md5 del archivo en el wikidata-V-md5sums.txt que está junto al dump."""
    partes = entrada.rstrip("/").rsplit("/", 1)
    carpeta, nombre = partes if len(partes) == 2 else (".", partes[0])
    ruta = f"{carpeta}/wikidata-{version}-md5sums.txt"
    if not existe(spark, ruta):
        return None
    for fila in spark.read.text(ruta).collect():
        partes = fila.value.split()
        if len(partes) == 2 and partes[1] == nombre:
            return partes[0]
    return None


# ---------- modo --solo-medir ----------

def _contar(filas):
    t0 = time.time()
    n = malas = nbytes = 0
    for ok, b in filas:
        n += 1
        malas += 1 - ok
        nbytes += b
    return [(n, malas, nbytes, time.time() - t0)]


def medir(spark, entrada):
    df = leer_y_parsear(spark, entrada).select(F.col("ok").cast("int"), "bytes_linea")
    rdd = df.rdd
    pedazos = rdd.getNumPartitions()
    elegidos = sorted({0, pedazos // 2})
    log(f"El archivo se lee en {pedazos} pedazos; midiendo los pedazos {elegidos}...")
    res = spark.sparkContext.runJob(rdd, _contar, partitions=elegidos)
    for i, (n, malas, nbytes, seg) in zip(elegidos, res):
        log(f"Pedazo {i}: {n:,} entidades, {nbytes / 1e6:,.0f} MB de JSON, {malas} con error, "
            f"{seg:,.0f} s -> {nbytes / 1e6 / seg:,.1f} MB/s por núcleo")
    seg_prom = sum(r[3] for r in res) / len(res)
    json_prom = sum(r[2] for r in res) / len(res)
    log(f"Estimado para el archivo completo: ~{json_prom * pedazos / 1e12:,.2f} TB de JSON y "
        f"~{seg_prom * pedazos / 3600:,.1f} horas-núcleo de lectura")


# ---------- proceso completo ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrada", required=True, help="dump .json.bz2 o .json")
    ap.add_argument("--version", required=True, help="fecha del dump, YYYYMMDD")
    ap.add_argument("--salida", help="carpeta base de silver, p. ej. gs://BUCKET/silver")
    ap.add_argument("--max-error", type=float, default=0.001,
                    help="fracción máxima de líneas que no se pueden parsear (0.001 = 0.1%%)")
    ap.add_argument("--solo-medir", action="store_true")
    args = ap.parse_args()
    if not args.solo_medir and not args.salida:
        ap.error("falta --salida")

    v = args.version
    datetime.strptime(v, "%Y%m%d")  # falla temprano si la versión no es una fecha
    spark = (SparkSession.builder.appName(f"aplanado-wikidata-{v}")
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    if args.solo_medir:
        medir(spark, args.entrada)
        return

    t0 = time.time()
    salida = args.salida.rstrip("/")
    tmp = f"{salida}/_tmp_aplanado/version={v}"

    # Paso 1, la única lectura del dump: JSON -> Parquet intermedio.
    log(f"Leyendo {args.entrada}")
    leer_y_parsear(spark, args.entrada).write.mode("overwrite").parquet(tmp)
    seg_lectura = time.time() - t0
    log(f"Parquet intermedio listo en {seg_lectura:,.0f} s")

    # Paso 2: controles de calidad sobre el Parquet intermedio.
    inter = spark.read.parquet(tmp)
    ok = F.col("ok")

    def cuenta(cond):
        return F.sum(F.when(cond, 1).otherwise(0))

    r = inter.agg(
        F.count(F.lit(1)).alias("n_lineas"),
        cuenta(~ok).alias("n_error_parseo"),
        F.sum("bytes_linea").alias("bytes_lineas"),
        cuenta(ok).alias("n_ok"),
        F.countDistinct("id").alias("n_ids_distintos"),
        cuenta(ok & ~F.col("id").rlike(ID_VALIDO)).alias("n_ids_invalidos"),
    ).first().asDict()
    r = {k: (val or 0) for k, val in r.items()}

    pct_error = r["n_error_parseo"] / r["n_lineas"] if r["n_lineas"] else 1.0
    log(f"{r['n_lineas']:,} líneas, {r['n_error_parseo']:,} sin parsear ({pct_error:.4%})")
    if r["n_lineas"] == 0 or pct_error > args.max_error:
        ejemplos = [x.linea_con_error for x in
                    inter.where(~ok).select("linea_con_error").limit(3).collect()]
        raise RuntimeError(
            f"Control de esquema: {pct_error:.4%} de líneas sin parsear supera el máximo de "
            f"{args.max_error:.4%}. La versión {v} NO se publica en silver. "
            f"El intermedio queda en {tmp}. Ejemplos: {ejemplos}")

    n_dup = r["n_ok"] - r["n_ids_distintos"]
    buenas = inter.where(ok)
    if n_dup:
        log(f"AVISO: {n_dup:,} ids repetidos; se conserva una fila por id")
        buenas = buenas.dropDuplicates(["id"])
    if r["n_ids_invalidos"]:
        log(f"AVISO: {r['n_ids_invalidos']:,} ids con formato inválido")

    # Paso 3: publicar las tablas silver de esta versión.
    buenas.select("id", "tipo", "modificado").write.mode("overwrite") \
        .parquet(f"{salida}/entidades/version={v}")

    def idioma(cod):
        return buenas.select("id", F.lit(cod).alias("idioma"),
                             F.col(f"etiqueta_{cod}").alias("etiqueta"),
                             F.col(f"descripcion_{cod}").alias("descripcion"))

    (idioma("es").unionByName(idioma("en"))
     .where(F.col("etiqueta").isNotNull() | F.col("descripcion").isNotNull())
     .write.mode("overwrite").partitionBy("idioma")
     .parquet(f"{salida}/etiquetas/version={v}"))

    buenas.select("id", F.explode("p31").alias("p31")).write.mode("overwrite") \
        .parquet(f"{salida}/p31/version={v}")
    log("Tablas silver publicadas")

    # Paso 4: conteos sobre lo que quedó publicado (así el catálogo cuadra con las tablas).
    ent = spark.read.parquet(f"{salida}/entidades/version={v}")
    por_tipo = {x["tipo"]: x["count"] for x in ent.groupBy("tipo").count().collect()}
    et = spark.read.parquet(f"{salida}/etiquetas/version={v}")
    por_idioma = {x["idioma"]: x for x in et.groupBy("idioma").agg(
        F.count(F.lit(1)).alias("filas"), F.count("etiqueta").alias("etiquetas"),
        F.count("descripcion").alias("descripciones")).collect()}

    def de_idioma(cod, campo):
        return por_idioma[cod][campo] if cod in por_idioma else 0

    # Control referencial: clases de P31 que no existen como entidad en esta versión.
    p31 = spark.read.parquet(f"{salida}/p31/version={v}")
    clases = p31.select(F.col("p31").alias("clase")).distinct()
    n_clases = clases.count()
    n_existen = (ent.select("id").join(F.broadcast(clases), F.col("id") == F.col("clase"))
                 .select("clase").distinct().count())
    n_p31 = p31.count()

    seg_total = time.time() - t0
    fila = {
        "fecha_dump": datetime.strptime(v, "%Y%m%d").date(),
        "archivo": args.entrada,
        "bytes_archivo": tamano(spark, args.entrada),
        "md5_publicado": md5_publicado(spark, args.entrada, v),
        "n_lineas": r["n_lineas"],
        "n_error_parseo": r["n_error_parseo"],
        "pct_error_parseo": pct_error,
        "bytes_json": r["bytes_lineas"] + r["n_lineas"],  # más un salto de línea por línea
        "n_entidades": sum(por_tipo.values()),
        "n_items": por_tipo.get("item", 0),
        "n_propiedades": por_tipo.get("property", 0),
        "n_ids_duplicados": n_dup,
        "n_ids_invalidos": r["n_ids_invalidos"],
        "n_etiquetas_es": de_idioma("es", "etiquetas"),
        "n_descripciones_es": de_idioma("es", "descripciones"),
        "n_etiquetas_en": de_idioma("en", "etiquetas"),
        "n_descripciones_en": de_idioma("en", "descripciones"),
        "n_filas_etiquetas": de_idioma("es", "filas") + de_idioma("en", "filas"),
        "n_filas_p31": n_p31,
        "n_clases_p31": n_clases,
        "n_clases_p31_huerfanas": n_clases - n_existen,
        "segundos_lectura": round(seg_lectura, 1),
        "segundos_total": round(seg_total, 1),
        "spark_version": spark.version,
        "procesado_en": datetime.now(timezone.utc).replace(tzinfo=None),
    }
    tipos = {
        "fecha_dump": T.DateType(), "archivo": T.StringType(), "md5_publicado": T.StringType(),
        "pct_error_parseo": T.DoubleType(), "segundos_lectura": T.DoubleType(),
        "segundos_total": T.DoubleType(), "spark_version": T.StringType(),
        "procesado_en": T.TimestampType(),
    }
    esquema = T.StructType([T.StructField(k, tipos.get(k, T.LongType())) for k in fila])
    spark.createDataFrame([tuple(fila.values())], esquema).coalesce(1) \
        .write.mode("overwrite").parquet(f"{salida}/catalogo_versiones/version={v}")

    borrar(spark, tmp)
    borrar_si_vacia(spark, f"{salida}/_tmp_aplanado")
    log("Versión registrada en catalogo_versiones:")
    for k, val in fila.items():
        print(f"  {k:24} {val:,}" if isinstance(val, int) else f"  {k:24} {val}", flush=True)


if __name__ == "__main__":
    main()
