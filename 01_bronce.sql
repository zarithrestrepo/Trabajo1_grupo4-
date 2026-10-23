-- =====================================================================
-- PARTE A - 1. BRONCE
-- Antes: subir el CSV (sin modificarlo) a
--   s3://apgd-trabajo1-zarith-262/bronce/consumo/consumo_sintetico_subestaciones.csv
-- Bucket del trabajo: apgd-trabajo1-zarith-262 (us-east-2).
-- =====================================================================

CREATE DATABASE IF NOT EXISTS trabajo1;

-- Tabla externa sobre el archivo tal cual llega: TODAS las columnas como texto.
-- No se salta el encabezado a propósito: bronce refleja el archivo crudo,
-- y la fila de encabezado aparece como un hallazgo de calidad.
CREATE EXTERNAL TABLE IF NOT EXISTS trabajo1.bronce_consumo (
  medidor      string,
  fecha        string,
  hora         string,
  energia_kwh  string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
STORED AS TEXTFILE
LOCATION 's3://apgd-trabajo1-zarith-262/bronce/consumo/';

-- Verificación
SELECT * FROM trabajo1.bronce_consumo LIMIT 10;

SELECT count(*) AS filas_bronce FROM trabajo1.bronce_consumo;   -- esperado: 1.138.801 (1.138.800 + encabezado)
