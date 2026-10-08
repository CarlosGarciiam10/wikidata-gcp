# Wikidata versionado en GCP

Proyecto final de Grandes Datos (Maestría, Universidad Panamericana).

Producto analítico versionado sobre el dump JSON completo de Wikidata en Google Cloud, con un caso de
procesamiento de lenguaje natural: un clasificador que propone la clase (P31, «instancia de») de las
entidades que no la tienen, a partir de su descripción en español.

## Arquitectura

```
dumps.wikimedia.org ──> raw/ (bz2 + md5) ──Spark──> silver/ (Parquet) ──Spark──> gold/ (dataset, métricas, propuestas)
                                                        │                              │
                                                        └──── BigQuery (tablas externas y vistas) ────> Data Studio
```

- **Cloud Storage** (`gs://bigdata-507701-wikidata`): zonas `raw/`, `silver/` y `gold/`, más `modelos/`,
  `sample/`, `pruebas/`, `codigo/` y `logs/`. Cada versión vive en su propia partición `version=YYYYMMDD`.
- **Dataproc** (Spark 3.5): una plantilla de flujo de trabajo crea un clúster administrado
  (1 maestro + 5 trabajadores e2-standard-4), corre los pasos y lo borra al terminar.
- **BigQuery** (dataset `wikidata`, us-central1): tablas externas sobre los Parquet, sin copiar los datos,
  y vistas con los campos que usa el tablero.
- **Data Studio**: tablero con dos páginas, la versión y su cobertura, y el clasificador.

## Versión procesada: 2026-10-05

| Indicador | Valor |
|---|---|
| Entidades | 121,860,983 |
| Archivo bz2 | 103.3 GB |
| JSON sin comprimir | 1.84 TB |
| Errores de parseo | 0 |
| Ids repetidos | 0 |
| Horas de proceso | 4.18 |
| Accuracy / F1 macro (regresión logística) | 0.956 / 0.655 |
| Accuracy / F1 macro (Naive Bayes, línea base) | 0.936 / 0.406 |

## Archivos

| Archivo | Qué hace |
|---|---|
| `01_ingesta.sh` | Descarga el dump de una versión, valida su md5 y lo sube a `raw/`. Se corre en una VM temporal. |
| `02_aplanado.py` | Spark, de raw a silver: parsea el JSON con un esquema mínimo y escribe `entidades`, `etiquetas` (es/en), `p31` y `catalogo_versiones` en Parquet. Solo publica si pasan los controles de calidad. |
| `03_dataset_clasificador.py` | Spark, de silver a gold: define las 20 clases más frecuentes más «otro», desduplica descripciones y separa entrenamiento y prueba. |
| `04_modelo.py` | Spark ML: TF-IDF con regresión logística multinomial y Naive Bayes como línea base; guarda métricas, errores, el modelo y las propuestas de P31. |
| `pipeline.yaml` | Plantilla de flujo de trabajo de Dataproc: prueba 03 y 04 con la muestra y después corre el aplanado, el dataset y el modelo. |
| `05_publica.sh` | Publica la versión en BigQuery como tablas externas y crea las vistas del tablero. |
| `sql/tablero_vistas.sql` | Vistas que lee el tablero. |

## Procesar una versión nueva

```bash
# 1. Ingesta, en una VM temporal de la misma región que el bucket
bash 01_ingesta.sh YYYYMMDD gs://bigdata-507701-wikidata

# 2. Código al bucket y flujo de trabajo en Dataproc (cambiar la versión en pipeline.yaml)
gcloud storage cp 02_aplanado.py 03_dataset_clasificador.py 04_modelo.py gs://bigdata-507701-wikidata/codigo/
gcloud dataproc workflow-templates instantiate-from-file --file=pipeline.yaml --region=us-central1 --async

# 3. Publicación en BigQuery, desde Cloud Shell
bash 05_publica.sh YYYYMMDD
```

Las tablas silver de BigQuery leen todas las versiones por partición, así que una versión nueva aparece
sin recrearlas; las del clasificador apuntan a la versión que se pasa a `05_publica.sh`.
