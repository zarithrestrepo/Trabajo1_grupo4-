-- =====================================================================
-- PARTE A - 4. ORO
-- Pregunta de negocio:
--   ¿Cuánta energía consume cada subestación por mes, cuál es su pico horario
--   y su factor de carga? -> permite priorizar ampliaciones de capacidad
--   (subestaciones con picos altos y crecientes) y detectar subutilización
--   (factor de carga bajo).
--   factor de carga = demanda media / demanda pico (1 = carga plana).
-- =====================================================================

CREATE TABLE trabajo1.oro_consumo_mensual
WITH (
  table_type  = 'ICEBERG',
  is_external = false,
  location    = 's3://apgd-trabajo1-zarith-262/oro/consumo_mensual/',
  format      = 'PARQUET'
) AS
SELECT medidor,
       year(fecha)                                   AS anio,
       month(fecha)                                  AS mes,
       sum(energia_kwh)                              AS energia_kwh_total,
       round(sum(energia_kwh) / 1000, 2)             AS energia_mwh,
       round(avg(energia_kwh), 2)                    AS demanda_media_kwh,
       max(energia_kwh)                              AS demanda_pico_kwh,
       round(avg(energia_kwh) / max(energia_kwh), 3) AS factor_carga,
       count(*)                                      AS horas_validas
FROM trabajo1.plata_consumo_valido
GROUP BY medidor, year(fecha), month(fecha);

-- Respuesta a la pregunta: top 10 subestaciones por pico en el último año completo,
-- con su energía anual y factor de carga medio
SELECT medidor,
       round(sum(energia_mwh), 1)      AS energia_mwh_2025,
       max(demanda_pico_kwh)           AS pico_kwh_2025,
       round(avg(factor_carga), 3)     AS factor_carga_medio
FROM trabajo1.oro_consumo_mensual
WHERE anio = 2025
GROUP BY medidor
ORDER BY pico_kwh_2025 DESC
LIMIT 10;

-- Crecimiento del pico anual por subestación (para ver tendencia)
SELECT medidor, anio, max(demanda_pico_kwh) AS pico_anual_kwh, round(sum(energia_mwh), 1) AS energia_mwh
FROM trabajo1.oro_consumo_mensual
GROUP BY medidor, anio
ORDER BY medidor, anio;
