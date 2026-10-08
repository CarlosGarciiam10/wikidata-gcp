"""
03_dataset_clasificador.py: de silver a gold, el dataset del clasificador.

Toma las entidades con descripción en español y sus valores de P31, define las clases
(los K valores de P31 más frecuentes entre esas entidades, más "otro"), colapsa las
descripciones repetidas y separa entrenamiento y prueba. También guarda las entidades
con descripción en español pero sin P31: a esas el modelo les va a proponer un tipo.

  gold/clasificador/version=V/clases     clase, nombre_clase, rango, entidades, descripciones_unicas, en_dataset
  gold/clasificador/version=V/dataset    texto, descripcion, clase, n_entidades, pureza, split
  gold/clasificador/version=V/sin_tipo   id, etiqueta, descripcion, texto
  gold/clasificador/version=V/resumen    una fila con los conteos de cada paso

Las dos uniones por id (entidades con P31 y sin P31) se hacen sobre tablas con
bucketing por id, así que ninguna necesita shuffle.

Uso en Dataproc:
  gcloud dataproc jobs submit pyspark 03_dataset_clasificador.py --cluster=CLUSTER --region=us-central1 -- \
      --silver=gs://BUCKET/silver --gold=gs://BUCKET/gold --version=20261005
"""
import argparse
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window, functions as F


def log(msg):
    print(f"[dataset {datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def normalizar(col):
    """Minúsculas, números a 0 y espacios colapsados. Así 'publicado en 2019' y
    'publicado en 2020' cuentan como la misma descripción."""
    return F.trim(F.regexp_replace(F.regexp_replace(F.lower(col), r"[0-9]+", "0"), r"\s+", " "))


def borrar(spark, ruta):
    jvm = spark.sparkContext._jvm
    p = jvm.org.apache.hadoop.fs.Path(ruta)
    fs = p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if fs.exists(p):
        fs.delete(p, True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", required=True, help="carpeta base de silver, p. ej. gs://BUCKET/silver")
    ap.add_argument("--gold", required=True, help="carpeta base de gold, p. ej. gs://BUCKET/gold")
    ap.add_argument("--version", required=True, help="fecha del dump, YYYYMMDD")
    ap.add_argument("--clases", type=int, default=20, help="cuántos valores de P31 son clase (más 'otro')")
    ap.add_argument("--max-por-clase", type=int, default=30000,
                    help="tope de descripciones únicas por clase en el dataset")
    ap.add_argument("--prueba", type=float, default=0.2, help="fracción para prueba")
    ap.add_argument("--buckets", type=int, default=64)
    ap.add_argument("--semilla", type=int, default=42)
    args = ap.parse_args()

    v = args.version
    datetime.strptime(v, "%Y%m%d")
    silver = args.silver.rstrip("/")
    gold = f"{args.gold.rstrip('/')}/clasificador/version={v}"
    spark = SparkSession.builder.appName(f"dataset-clasificador-{v}").getOrCreate()
    t0 = time.time()

    etiquetas = spark.read.parquet(f"{silver}/etiquetas/version={v}")
    desc = (etiquetas.where((F.col("idioma") == "es") & F.col("descripcion").isNotNull())
            .select("id", "etiqueta", "descripcion"))
    p31 = spark.read.parquet(f"{silver}/p31/version={v}").select("id", "p31")

    # 1. Bucketing por id: las dos tablas quedan repartidas igual, así que unirlas no requiere shuffle.
    tablas = {"desc": f"wd_desc_es_{v}", "p31": f"wd_p31_{v}"}
    for df, clave in [(desc, "desc"), (p31, "p31")]:
        (df.write.mode("overwrite").bucketBy(args.buckets, "id").sortBy("id")
         .option("path", f"{gold}/_buckets/{clave}").saveAsTable(tablas[clave]))
    desc_b, p31_b = spark.table(tablas["desc"]), spark.table(tablas["p31"])
    con_tipo = desc_b.join(p31_b, "id")
    plan = con_tipo._jdf.queryExecution().executedPlan().toString()
    log("Unión por id " + ("sin shuffle (bucketing)" if "Exchange hashpartitioning" not in plan
                           else "CON shuffle: revisar bucketing"))

    # 2. Clases: los K valores de P31 más frecuentes entre las entidades con descripción en español.
    top = (con_tipo.groupBy("p31").count()
           .orderBy(F.desc("count"), "p31").limit(args.clases).collect())
    rango = {r["p31"]: i + 1 for i, r in enumerate(top)}
    ids_clase = list(rango)
    nombres = {r["id"]: r["nombre"] for r in (
        etiquetas.where(F.col("id").isin(ids_clase))
        .groupBy("id").agg(F.coalesce(
            F.max(F.when(F.col("idioma") == "es", F.col("etiqueta"))),
            F.max(F.when(F.col("idioma") == "en", F.col("etiqueta")))).alias("nombre"))
        .collect())}
    mapa = spark.createDataFrame([(c, r) for c, r in rango.items()], "p31 string, rango int")

    # 3. Una clase por entidad: si tiene varios P31, la más frecuente de las K; si ninguna está, "otro".
    etiquetado = (con_tipo.join(F.broadcast(mapa), "p31", "left")
                  .groupBy("id").agg(F.first("descripcion").alias("descripcion"),
                                     F.min("rango").alias("rango"))
                  .join(F.broadcast(mapa.withColumnRenamed("p31", "clase")), "rango", "left")
                  .withColumn("clase", F.coalesce("clase", F.lit("otro")))
                  .withColumn("texto", normalizar(F.col("descripcion")))
                  .where(F.length("texto") > 0))

    # 4. Desduplicar: una fila por descripción normalizada, con la clase mayoritaria.
    por_texto = etiquetado.groupBy("texto", "clase").agg(
        F.count(F.lit(1)).alias("n"), F.first("descripcion").alias("descripcion"))
    unicas = (por_texto
              .withColumn("n_entidades", F.sum("n").over(Window.partitionBy("texto")))
              .withColumn("orden", F.row_number().over(
                  Window.partitionBy("texto").orderBy(F.desc("n"), "clase")))
              .where("orden = 1")
              .withColumn("pureza", F.col("n") / F.col("n_entidades"))
              .select("texto", "descripcion", "clase", "n_entidades", "pureza")
              .cache())

    # 5. Tope por clase y separación estratificada: cada clase manda la misma fracción a prueba.
    #    Las clases con menos de 1/prueba descripciones distintas (frases de plantilla) se quedan
    #    completas en entrenamiento. Como cada texto aparece una sola vez, ninguna descripción
    #    queda en los dos lados.
    unicas_por_clase = {r["clase"]: r["count"] for r in unicas.groupBy("clase").count().collect()}
    fracciones = {c: min(1.0, args.max_por_clase / n) for c, n in unicas_por_clase.items()}
    por_clase = Window.partitionBy("clase")
    dataset = (unicas.sampleBy("clase", fracciones, seed=args.semilla)
               .withColumn("_n", F.count(F.lit(1)).over(por_clase))
               .withColumn("_i", F.row_number().over(por_clase.orderBy(F.rand(args.semilla))))
               .withColumn("split", F.when(F.col("_i") <= F.floor(F.col("_n") * args.prueba), "test")
                           .otherwise("train"))
               .drop("_n", "_i"))
    dataset.repartition(16).write.mode("overwrite").parquet(f"{gold}/dataset")
    dataset = spark.read.parquet(f"{gold}/dataset")

    # 6. Entidades con descripción en español y sin P31: las candidatas a propuesta.
    sin_tipo = desc_b.join(p31_b, "id", "left_anti").withColumn("texto", normalizar(F.col("descripcion")))
    sin_tipo.write.mode("overwrite").parquet(f"{gold}/sin_tipo")

    # 7. Tabla de clases y resumen.
    entidades_por_clase = {r["clase"]: r["count"] for r in etiquetado.groupBy("clase").count().collect()}
    en_dataset = {r["clase"]: r["count"] for r in dataset.groupBy("clase").count().collect()}
    filas = [(c, nombres.get(c, c), r, entidades_por_clase.get(c, 0), unicas_por_clase.get(c, 0),
              en_dataset.get(c, 0)) for c, r in rango.items()]
    filas.append(("otro", "otro", len(rango) + 1, entidades_por_clase.get("otro", 0),
                  unicas_por_clase.get("otro", 0), en_dataset.get("otro", 0)))
    spark.createDataFrame(filas, "clase string, nombre_clase string, rango int, entidades long, "
                                 "descripciones_unicas long, en_dataset long") \
        .coalesce(1).write.mode("overwrite").parquet(f"{gold}/clases")

    split = {r["split"]: r["count"] for r in dataset.groupBy("split").count().collect()}
    n_desc = desc_b.count()
    n_con_tipo = sum(entidades_por_clase.values())
    n_sin_tipo = spark.read.parquet(f"{gold}/sin_tipo").count()
    resumen = {
        "entidades_con_descripcion_es": n_desc,
        "con_p31": n_con_tipo,
        "sin_p31": n_sin_tipo,
        "descripciones_unicas": sum(unicas_por_clase.values()),
        "en_dataset": sum(en_dataset.values()),
        "entrenamiento": split.get("train", 0),
        "prueba": split.get("test", 0),
        "clases": len(rango) + 1,
        "segundos": int(time.time() - t0),
    }
    spark.createDataFrame([tuple(resumen.values())], ", ".join(f"{k} long" for k in resumen)) \
        .coalesce(1).write.mode("overwrite").parquet(f"{gold}/resumen")

    for t in tablas.values():
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    borrar(spark, f"{gold}/_buckets")

    log("Clases (entidades con descripción en español, descripciones únicas, en el dataset):")
    for c, nombre, r, ent, uni, ds in filas:
        print(f"  {r:>2}. {c:<12} {nombre[:30]:<30} {ent:>12,} {uni:>12,} {ds:>9,}", flush=True)
    log("Resumen:")
    for k, val in resumen.items():
        print(f"  {k:30} {val:,}", flush=True)


if __name__ == "__main__":
    main()
