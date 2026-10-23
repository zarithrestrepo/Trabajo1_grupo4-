-- =====================================================================
-- PARTE A - 2. HALLAZGOS DE CALIDAD (tabla de hallazgos cuantificada con SQL)
-- Una sola consulta que devuelve una fila por defecto revisado.
-- =====================================================================

WITH b AS (
  SELECT medidor, fecha, hora, energia_kwh,
         try_cast(fecha AS date)          AS f,
         try_cast(hora AS integer)        AS h,
         try_cast(energia_kwh AS double)  AS e
  FROM trabajo1.bronce_consumo
),
dup AS (
  SELECT coalesce(sum(c - 1), 0) AS extra
  FROM (SELECT medidor, fecha, hora, count(*) AS c
        FROM b GROUP BY medidor, fecha, hora HAVING count(*) > 1)
),
claves AS (
  SELECT DISTINCT medidor, f, h
  FROM b
  WHERE f IS NOT NULL AND h BETWEEN 0 AND 23 AND regexp_like(medidor, '^SUB-[0-9]{2}$')
),
serie AS (  -- horas esperadas (medidores x días x 24) vs horas presentes con clave válida
  SELECT count(DISTINCT medidor) * (date_diff('day', min(f), max(f)) + 1) * 24 AS esperadas,
         count(*)                                                             AS presentes
  FROM claves
)
SELECT * FROM (
            SELECT 0 AS n, 'Filas totales en bronce' AS hallazgo, count(*) AS filas FROM b
  UNION ALL SELECT 1, 'Fila de encabezado leída como dato (con BOM UTF-8)', count(*) FROM b WHERE lower(fecha) = 'fecha'
  UNION ALL SELECT 2, 'Medidor vacío o fuera del patrón SUB-NN',            count(*) FROM b WHERE NOT regexp_like(coalesce(medidor, ''), '^SUB-[0-9]{2}$')
  UNION ALL SELECT 3, 'Fecha vacía o inválida',                             count(*) FROM b WHERE f IS NULL
  UNION ALL SELECT 4, 'Fecha futura (posterior a hoy)',                     count(*) FROM b WHERE f > current_date
  UNION ALL SELECT 5, 'Hora vacía, no entera o fuera de 0-23',              count(*) FROM b WHERE h IS NULL OR h NOT BETWEEN 0 AND 23
  UNION ALL SELECT 6, 'Energía vacía o no numérica',                        count(*) FROM b WHERE e IS NULL
  UNION ALL SELECT 7, 'Energía negativa',                                   count(*) FROM b WHERE e < 0
  UNION ALL SELECT 8, 'Energía en cero (interrupción o falla de medición)', count(*) FROM b WHERE e = 0
  UNION ALL SELECT 9, 'Registros duplicados (medidor, fecha, hora)',        extra FROM dup
  UNION ALL SELECT 10, 'Horas faltantes en la serie horaria',               esperadas - presentes FROM serie
)
ORDER BY n;

-- Detalle del hallazgo 8: los ceros vienen en bloques diurnos de 4 a 14 horas
-- seguidas en 7 subestaciones (no son lecturas sueltas), típico de cortes o mantenimientos.
SELECT medidor, count(*) AS horas_en_cero, min(fecha) AS primera, max(fecha) AS ultima
FROM trabajo1.bronce_consumo
WHERE try_cast(energia_kwh AS double) = 0
GROUP BY medidor
ORDER BY horas_en_cero DESC;

SELECT try_cast(hora AS integer) AS hora, count(*) AS horas_en_cero
FROM trabajo1.bronce_consumo
WHERE try_cast(energia_kwh AS double) = 0
GROUP BY 1 ORDER BY 1;
