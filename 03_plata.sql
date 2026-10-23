-- =====================================================================
-- PARTE A - 3. PLATA: reglas de calidad + tablas Iceberg (válidas y rechazadas)
-- Ejecutar las sentencias UNA POR UNA en el editor de Athena.
--
-- Reglas (se aplican en orden; una fila rechazada lleva el PRIMER motivo que incumple):
--   R1 medidor vacío o fuera del catálogo SUB-NN   -> hallazgos 1 y 2 (encabezado con BOM)
--   R2 fecha vacía o inválida                       -> hallazgo 3
--   R3 hora vacía o fuera de 0-23                   -> hallazgo 5
--   R4 energía vacía o no numérica                  -> hallazgo 6
--   R5 energía negativa                             -> hallazgo 7 (preventiva)
--   R6 energía en cero                              -> hallazgo 8 (1.258 filas): una subestación
--      energizada no consume 0 kWh en una hora; son bloques de corte o falla de medición
--      que sesgarían promedios, picos y factor de carga en oro.
--   R7 duplicado de (medidor, fecha, hora)          -> hallazgo 9 (preventiva, se conserva la 1.ª)
-- =====================================================================

CREATE OR REPLACE VIEW trabajo1.plata_clasificacion AS
WITH base AS (
  SELECT medidor, fecha, hora, energia_kwh,
         try_cast(fecha AS date)          AS f,
         try_cast(hora AS integer)        AS h,
         try_cast(energia_kwh AS double)  AS e,
         row_number() OVER (PARTITION BY medidor, fecha, hora ORDER BY energia_kwh) AS rn
  FROM trabajo1.bronce_consumo
)
SELECT medidor, fecha, hora, energia_kwh, f, h, e,
  CASE
    WHEN NOT regexp_like(coalesce(medidor, ''), '^SUB-[0-9]{2}$') THEN 'R1: medidor vacío o fuera del catálogo SUB-NN (incluye encabezado)'
    WHEN f IS NULL                                               THEN 'R2: fecha vacía o inválida'
    WHEN h IS NULL OR h NOT BETWEEN 0 AND 23                     THEN 'R3: hora vacía o fuera de 0-23'
    WHEN e IS NULL                                               THEN 'R4: energía vacía o no numérica'
    WHEN e < 0                                                   THEN 'R5: energía negativa'
    WHEN e = 0                                                   THEN 'R6: lectura en cero (corte o falla de medición)'
    WHEN rn > 1                                                  THEN 'R7: registro duplicado (medidor, fecha, hora)'
  END AS motivo
FROM base;

-- Tabla Iceberg con las filas válidas (tipadas, Parquet, particionada por año)
CREATE TABLE trabajo1.plata_consumo_valido
WITH (
  table_type   = 'ICEBERG',
  is_external  = false,
  location     = 's3://apgd-trabajo1-zarith-262/plata/consumo_valido/',
  format       = 'PARQUET',
  partitioning = ARRAY['year(fecha)']
) AS
SELECT medidor,
       f AS fecha,
       h AS hora,
       e AS energia_kwh
FROM trabajo1.plata_clasificacion
WHERE motivo IS NULL;

-- Tabla Iceberg con las filas rechazadas, tal cual venían, y su motivo
CREATE TABLE trabajo1.plata_consumo_rechazado
WITH (
  table_type  = 'ICEBERG',
  is_external = false,
  location    = 's3://apgd-trabajo1-zarith-262/plata/consumo_rechazado/',
  format      = 'PARQUET'
) AS
SELECT medidor, fecha, hora, energia_kwh, motivo
FROM trabajo1.plata_clasificacion
WHERE motivo IS NOT NULL;

-- ---------------------------------------------------------------------
-- Conteos: bronce = válidas + rechazadas
-- Esperado: 1.138.801 = 1.137.542 + 1.259
-- ---------------------------------------------------------------------
SELECT b.filas                             AS filas_bronce,
       v.filas                             AS filas_validas,
       r.filas                             AS filas_rechazadas,
       v.filas + r.filas                   AS validas_mas_rechazadas,
       b.filas = v.filas + r.filas         AS cuadra
FROM (SELECT count(*) AS filas FROM trabajo1.bronce_consumo)          b,
     (SELECT count(*) AS filas FROM trabajo1.plata_consumo_valido)    v,
     (SELECT count(*) AS filas FROM trabajo1.plata_consumo_rechazado) r;

-- Rechazos por motivo
SELECT motivo, count(*) AS filas
FROM trabajo1.plata_consumo_rechazado
GROUP BY motivo
ORDER BY filas DESC;
