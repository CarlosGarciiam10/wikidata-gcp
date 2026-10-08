-- Vistas del tablero en Data Studio, sobre las tablas externas que registra 05_publica.sh.
-- Los nombres de columna van sin espacios ni acentos porque Data Studio no acepta los nombres
-- flexibles de BigQuery; los nombres largos ("TB de JSON", etc.) se ponen en la fuente de datos del tablero.

CREATE OR REPLACE VIEW `bigdata-507701.wikidata.tablero_version` AS
SELECT
  PARSE_DATE('%Y%m%d', CAST(version AS STRING)) AS Version,
  n_entidades AS Entidades,
  ROUND(bytes_archivo / 1e9, 1) AS gb_bz2,
  ROUND(bytes_json / 1e12, 2) AS tb_json,
  n_error_parseo AS errores_parseo,
  n_ids_duplicados AS ids_repetidos,
  n_clases_p31 AS clases_p31,
  n_clases_p31_huerfanas AS clases_sin_entidad,
  ROUND(segundos_total / 3600, 2) AS horas_proceso,
  md5_publicado AS md5
FROM `bigdata-507701.wikidata.catalogo_versiones`;

CREATE OR REPLACE VIEW `bigdata-507701.wikidata.tablero_cobertura` AS
SELECT
  PARSE_DATE('%Y%m%d', CAST(c.version AS STRING)) AS Version,
  x.campo AS Campo,
  x.n AS Entidades,
  ROUND(100 * x.n / c.n_entidades, 1) AS Porcentaje
FROM `bigdata-507701.wikidata.catalogo_versiones` AS c,
UNNEST([STRUCT('Descripción en inglés' AS campo, c.n_descripciones_en AS n),
        STRUCT('Etiqueta en inglés', c.n_etiquetas_en),
        STRUCT('Descripción en español', c.n_descripciones_es),
        STRUCT('Etiqueta en español', c.n_etiquetas_es)]) AS x;

CREATE OR REPLACE VIEW `bigdata-507701.wikidata.tablero_clases` AS
SELECT
  k.nombre_clase AS Clase,
  k.clase AS Id,
  k.entidades AS Entidades,
  k.descripciones_unicas AS Descripciones,
  k.en_dataset AS en_dataset,
  ROUND(m.f1, 3) AS F1,
  m.soporte AS Soporte
FROM `bigdata-507701.wikidata.clasificador_clases` AS k
LEFT JOIN `bigdata-507701.wikidata.clasificador_metricas_por_clase` AS m
  ON m.clase = k.clase AND m.modelo = 'regresion_logistica';

CREATE OR REPLACE VIEW `bigdata-507701.wikidata.tablero_modelos` AS
SELECT
  IF(modelo = 'regresion_logistica', 'Regresión logística', 'Naive Bayes') AS Modelo,
  ROUND(accuracy, 3) AS Accuracy,
  ROUND(f1_macro, 3) AS f1_macro,
  n_train AS Entrenamiento,
  n_test AS Prueba,
  segundos AS Segundos
FROM `bigdata-507701.wikidata.clasificador_metricas`;

CREATE OR REPLACE VIEW `bigdata-507701.wikidata.tablero_propuestas` AS
SELECT
  id AS Entidad,
  etiqueta AS Etiqueta,
  descripcion AS Descripcion,
  nombre_clase AS Propuesta,
  ROUND(probabilidad, 3) AS Probabilidad
FROM `bigdata-507701.wikidata.clasificador_propuestas`;
