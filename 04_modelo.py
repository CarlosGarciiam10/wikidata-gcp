"""
04_modelo.py: entrena y evalúa el clasificador de P31 y propone tipo a las entidades sin P31.

Pipeline de Spark ML: RegexTokenizer -> StopWordsRemover (español) -> HashingTF -> IDF ->
regresión logística multinomial. Línea base: Naive Bayes multinomial sobre las mismas palabras.

  gold/clasificador/version=V/metricas            modelo, accuracy, f1_macro, n_train, n_test, segundos
  gold/clasificador/version=V/metricas_por_clase  modelo, clase, nombre_clase, precision, recall, f1, soporte
  gold/clasificador/version=V/confusion           modelo, clase_real, clase_predicha, n
  gold/clasificador/version=V/errores             errores de prueba con más confianza, por clase
  gold/clasificador/version=V/propuestas          id, etiqueta, descripcion, clase_propuesta, nombre_clase, probabilidad
  modelos/clasificador_p31/version=V/             el pipeline entrenado (regresión logística)

Uso en Dataproc:
  gcloud dataproc jobs submit pyspark 04_modelo.py --cluster=CLUSTER --region=us-central1 -- \
      --gold=gs://BUCKET/gold --modelos=gs://BUCKET/modelos --version=20261005
"""
import argparse
import time
from datetime import datetime, timezone

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import HashingTF, IDF, IndexToString, RegexTokenizer, StopWordsRemover, StringIndexer
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, Window, functions as F


def log(msg):
    print(f"[modelo {datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def evaluar(pred, modelo):
    """Accuracy, F1 macro y métricas por clase a partir de la matriz de confusión."""
    conf = [(r["clase"], r["clase_predicha"], r["count"])
            for r in pred.groupBy("clase", "clase_predicha").count().collect()]
    total = sum(n for _, _, n in conf)
    clases = sorted({c for c, _, _ in conf} | {p for _, p, _ in conf})
    por_clase = []
    for c in clases:
        tp = sum(n for r, p, n in conf if r == c and p == c)
        soporte = sum(n for r, _, n in conf if r == c)
        predichos = sum(n for _, p, n in conf if p == c)
        precision = tp / predichos if predichos else 0.0
        recall = tp / soporte if soporte else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        por_clase.append((modelo, c, precision, recall, f1, soporte))
    con_soporte = [x for x in por_clase if x[5] > 0]
    accuracy = sum(n for r, p, n in conf if r == p) / total if total else 0.0
    f1_macro = sum(x[4] for x in con_soporte) / len(con_soporte) if con_soporte else 0.0
    return accuracy, f1_macro, por_clase, [(modelo, r, p, n) for r, p, n in conf]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", required=True, help="carpeta base de gold, p. ej. gs://BUCKET/gold")
    ap.add_argument("--modelos", required=True, help="carpeta para guardar el modelo, p. ej. gs://BUCKET/modelos")
    ap.add_argument("--version", required=True, help="fecha del dump, YYYYMMDD")
    ap.add_argument("--umbral", type=float, default=0.8,
                    help="probabilidad mínima para proponer un P31")
    ap.add_argument("--reg", type=float, default=0.001, help="regularización L2 de la regresión logística")
    ap.add_argument("--iter", type=int, default=100)
    args = ap.parse_args()

    v = args.version
    datetime.strptime(v, "%Y%m%d")
    gold = f"{args.gold.rstrip('/')}/clasificador/version={v}"
    spark = SparkSession.builder.appName(f"modelo-p31-{v}").getOrCreate()

    ds = spark.read.parquet(f"{gold}/dataset")
    train, test = ds.where("split = 'train'"), ds.where("split = 'test'")
    nombres = {r["clase"]: r["nombre_clase"] for r in spark.read.parquet(f"{gold}/clases").collect()}

    # Texto -> palabras -> TF-IDF. Se ajusta solo con entrenamiento.
    indexador = StringIndexer(inputCol="clase", outputCol="label", handleInvalid="skip").fit(train)
    etiquetas = indexador.labelsArray[0]
    texto = Pipeline(stages=[
        RegexTokenizer(inputCol="texto", outputCol="tokens", pattern=r"[^\p{L}\p{N}]+", minTokenLength=2),
        StopWordsRemover(inputCol="tokens", outputCol="palabras",
                         stopWords=StopWordsRemover.loadDefaultStopWords("spanish")),
        HashingTF(inputCol="palabras", outputCol="tf", numFeatures=1 << 18),
        IDF(inputCol="tf", outputCol="features", minDocFreq=2),
    ]).fit(indexador.transform(train))
    tr = texto.transform(indexador.transform(train)).cache()
    te = texto.transform(indexador.transform(test)).cache()
    n_train, n_test = tr.count(), te.count()
    log(f"{n_train:,} descripciones de entrenamiento, {n_test:,} de prueba, {len(etiquetas)} clases")

    a_clase = IndexToString(inputCol="prediction", outputCol="clase_predicha", labels=etiquetas)
    candidatos = {
        "regresion_logistica": LogisticRegression(family="multinomial", maxIter=args.iter, regParam=args.reg),
        "naive_bayes": NaiveBayes(featuresCol="tf", modelType="multinomial", smoothing=1.0),
    }
    metricas, por_clase, confusion, ajustados, predicciones = [], [], [], {}, {}
    for nombre, estimador in candidatos.items():
        t = time.time()
        ajustados[nombre] = estimador.fit(tr)
        seg = time.time() - t
        pred = a_clase.transform(ajustados[nombre].transform(te)) \
            .withColumn("probabilidad", F.array_max(vector_to_array("probability")))
        predicciones[nombre] = pred
        acc, f1, pc, cf = evaluar(pred, nombre)
        metricas.append((nombre, acc, f1, n_train, n_test, len(etiquetas), round(seg, 1)))
        por_clase += [(m, c, nombres.get(c, c), p, r, f, s) for m, c, p, r, f, s in pc]
        confusion += cf
        log(f"{nombre}: accuracy {acc:.3f}, F1 macro {f1:.3f}, entrenado en {seg:,.0f} s")

    spark.createDataFrame(metricas, "modelo string, accuracy double, f1_macro double, n_train long, "
                                    "n_test long, n_clases int, segundos double") \
        .coalesce(1).write.mode("overwrite").parquet(f"{gold}/metricas")
    spark.createDataFrame(por_clase, "modelo string, clase string, nombre_clase string, precision double, "
                                     "recall double, f1 double, soporte long") \
        .coalesce(1).write.mode("overwrite").parquet(f"{gold}/metricas_por_clase")
    spark.createDataFrame(confusion, "modelo string, clase_real string, clase_predicha string, n long") \
        .coalesce(1).write.mode("overwrite").parquet(f"{gold}/confusion")

    # Errores con más confianza de la regresión logística: los más útiles para revisar a mano.
    w = Window.partitionBy("clase").orderBy(F.desc("probabilidad"))
    (predicciones["regresion_logistica"].where("clase <> clase_predicha")
     .withColumn("orden", F.row_number().over(w)).where("orden <= 20")
     .select("descripcion", F.col("clase").alias("clase_real"), "clase_predicha", "probabilidad",
             "n_entidades")
     .coalesce(1).write.mode("overwrite").parquet(f"{gold}/errores"))

    # Modelo final (regresión logística) como un solo pipeline: texto -> clase propuesta.
    final = PipelineModel(stages=[*texto.stages, ajustados["regresion_logistica"], a_clase])
    final.write().overwrite().save(f"{args.modelos.rstrip('/')}/clasificador_p31/version={v}")

    # Propuestas de P31 para entidades sin tipo, solo con probabilidad alta y clase distinta de "otro".
    mapa = spark.createDataFrame(list(nombres.items()), "clase_propuesta string, nombre_clase string")
    propuestas = (final.transform(spark.read.parquet(f"{gold}/sin_tipo"))
                  .withColumn("probabilidad", F.array_max(vector_to_array("probability")))
                  .where((F.col("probabilidad") >= args.umbral) & (F.col("clase_predicha") != "otro"))
                  .select("id", "etiqueta", "descripcion",
                          F.col("clase_predicha").alias("clase_propuesta"), "probabilidad")
                  .join(F.broadcast(mapa), "clase_propuesta", "left"))
    propuestas.write.mode("overwrite").parquet(f"{gold}/propuestas")
    n_prop = spark.read.parquet(f"{gold}/propuestas").count()
    n_sin = spark.read.parquet(f"{gold}/sin_tipo").count()
    log(f"Propuestas de P31: {n_prop:,} de {n_sin:,} entidades sin tipo (probabilidad >= {args.umbral})")


if __name__ == "__main__":
    main()
