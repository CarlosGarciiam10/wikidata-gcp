#!/usr/bin/env bash
# 01_ingesta.sh: baja una versión del dump JSON de Wikidata, valida el md5 y la sube a la zona raw.
#
# Se corre dentro de una VM temporal en la misma región que el bucket. Así la usamos con 20261005:
#   gcloud compute instances create ingesta-wikidata --zone=us-central1-a --machine-type=e2-small \
#       --boot-disk-size=200GB --boot-disk-type=pd-standard --scopes=cloud-platform
#   gcloud compute ssh ingesta-wikidata --zone=us-central1-a
#   nohup ./01_ingesta.sh 20261005 gs://bigdata-507701-wikidata > ingesta.log 2>&1 &
# Al terminar se borra la VM (y su disco):
#   gcloud compute instances delete ingesta-wikidata --zone=us-central1-a
set -euo pipefail

V="${1:?uso: 01_ingesta.sh YYYYMMDD gs://BUCKET}"
BUCKET="${2:?uso: 01_ingesta.sh YYYYMMDD gs://BUCKET}"
URL="https://dumps.wikimedia.org/wikidatawiki/entities/$V"
F="wikidata-$V-all.json.bz2"
MD5="wikidata-$V-md5sums.txt"

log() { echo "[$(date -u +%H:%M:%S)] $*"; }

# La imagen de Debian no trae bzip2; solo hace falta para sacar la muestra.
command -v bzip2 >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq bzip2; }

log "Descargando $F"
curl -sS -L -C - -o "$F" "$URL/$F"        # -C - reanuda la descarga si se corta
curl -sS -L -o "$MD5" "$URL/$MD5"

log "Validando md5"
grep " $F\$" "$MD5" | md5sum -c - | tee md5.log   # si no coincide, el script se detiene aquí

log "Sacando la muestra: las primeras 5,000 líneas de los primeros 50 MB"
head -c 50000000 "$F" | bzip2 -dc 2>/dev/null | head -n 5000 > muestra.json || true

log "Subiendo a $BUCKET"
gcloud storage cp "$F" "$MD5" "$BUCKET/raw/wikidata/version=$V/"
gcloud storage cp muestra.json "$BUCKET/sample/"
gcloud storage cp md5.log "$BUCKET/logs/ingesta-$V/"
log "Listo"
