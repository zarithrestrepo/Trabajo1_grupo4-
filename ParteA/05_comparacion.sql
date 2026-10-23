-- =====================================================================
-- PARTE A - 5. COMPARACIÓN: la misma pregunta en las tres capas
-- "Top 5 subestaciones por energía consumida en 2025 (MWh)"
-- Las tres devuelven el mismo resultado (los ceros no cambian la suma).
--
-- Ejecutar cada una 3 veces y anotar de la pestaña "Query stats" de Athena:
--   Data scanned  y  Run time   -> usar la mediana en la tabla del README.
-- =====================================================================

-- BRONCE (CSV de texto: lee el archivo completo y convierte tipos en cada consulta)
SELECT medidor, round(sum(try_cast(energia_kwh AS double)) / 1000, 1) AS energia_mwh_2025
FROM trabajo1.bronce_consumo
WHERE fecha LIKE '2025-%'
GROUP BY medidor
ORDER BY energia_mwh_2025 DESC
LIMIT 5;

-- PLATA (Iceberg/Parquet columnar, particionado por año: lee solo 2025 y 2 columnas)
SELECT medidor, round(sum(energia_kwh) / 1000, 1) AS energia_mwh_2025
FROM trabajo1.plata_consumo_valido
WHERE fecha BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
GROUP BY medidor
ORDER BY energia_mwh_2025 DESC
LIMIT 5;

-- ORO (agregado mensual: ~1.560 filas en total)
SELECT medidor, round(sum(energia_kwh_total) / 1000, 1) AS energia_mwh_2025
FROM trabajo1.oro_consumo_mensual
WHERE anio = 2025
GROUP BY medidor
ORDER BY energia_mwh_2025 DESC
LIMIT 5;
