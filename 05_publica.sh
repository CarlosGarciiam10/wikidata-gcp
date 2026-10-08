#!/usr/bin/env bash
# 05_publica.sh: publica una versión en BigQuery como tablas externas sobre los Parquet de GCS.
#
# Las tablas silver se leen con particiones estilo Hive (version=..., idioma=...), así que ven las
# versiones nuevas sin recrearse. Las del clasificador apuntan a la versión que se pasa como parámetro.
# Uso, desde Cloud Shell:  bash 05_publica.sh 20261005
set -euo pipefail

V="${1:?uso: 05_publica.sh YYYYMMDD}"
P=bigdata-507701
DS=wikidata
B=gs://bigdata-507701-wikidata

# Dataset en la misma región que el bucket.
bq --location=us-central1 show "$P:$DS" >/dev/null 2>&1 || bq --location=us-central1 mk -d "$P:$DS"

# Silver: una tabla externa por tabla, particionada por versión (y etiquetas también por idioma).
for T in entidades etiquetas p31 catalogo_versiones; do
  bq mkdef --source_format=PARQUET --hive_partitioning_mode=AUTO \
    --hive_partitioning_source_uri_prefix="$B/silver/$T" "$B/silver/$T/*.parquet" > "/tmp/def_$T.json"
  bq rm -f -t "$P:$DS.$T" >/dev/null 2>&1 || true
  bq mk --external_table_definition="/tmp/def_$T.json" "$P:$DS.$T"
done

# Gold: resultados del clasificador de la versión V.
for T in clases metricas metricas_por_clase propuestas resumen confusion errores; do
  bq rm -f -t "$P:$DS.clasificador_$T" >/dev/null 2>&1 || true
  bq mk --external_table_definition="PARQUET=$B/gold/clasificador/version=$V/$T/*.parquet" \
    "$P:$DS.clasificador_$T"
done

# Vistas del tablero (Data Studio), con nombres de columna para mostrarse.
bq query --use_legacy_sql=false < "$(dirname "$0")/sql/tablero_vistas.sql"

bq ls "$P:$DS"
