# Trabajo 1 – (APGD 2026-2)

Integrantes: Zarith Isabella Restrepo, Isabela Botero y Marcela Londoño · Video: https://www.youtube.com/watch?v=2yGMaICuUWQ

---

## PARTE A – Medallion en Athena

### A1. Descripción del conjunto de datos

| Aspecto | Valor |
|---|---|
| Qué es | Consumo horario de energía activa de 26 subestaciones eléctricas (SUB-01 … SUB-26) |
| Origen | Proyecto integrador de _[integrante]_: serie sintética generada a partir de los perfiles de consumo del proyecto _(ajustar)_ |
| Periodo | 2021-09-01 a 2026-08-30 (5 años, 1.825 días × 24 h) |
| Filas | **1.138.800** + 1 encabezado (supera el mínimo de 1.000.000, **no requiere aumento de datos**) |
| Peso | 30,9 MB (29,4 MiB), CSV UTF-8 con BOM |
| Columnas | `Medidor` (texto), `Fecha` (fecha AAAA-MM-DD), `Hora del día` (entero 0-23), `Energía activa (kWh)` (decimal) |

### A2. Bronce
1. **S3 → Create bucket** (p. ej. `apgd-trabajo1-<iniciales>`), us-east-2, opciones por defecto.
2. Crear carpetas y subir el CSV **sin modificar** a `bronce/consumo/`.
3. **Athena → Settings → Manage**: ubicación de resultados `s3://<bucket>/athena-results/`.
4. En todos los `.sql` reemplazar `TU-BUCKET` por el nombre del bucket (Ctrl+Shift+H en VS Code).
5. Ejecutar `01_bronce.sql` (una sentencia a la vez). Todas las columnas son `string`.

### A3. Hallazgos (`02_hallazgos.sql`)
Valores que obtuvimos al perfilar el archivo (deben coincidir con Athena):

| # | Hallazgo | Filas |
|---|---|---|
| 0 | Filas totales en bronce | 1.138.801 |
| 1 | Encabezado leído como dato (con BOM UTF-8) | 1 |
| 2 | Medidor vacío o fuera del patrón SUB-NN | 1 (el encabezado) |
| 3 | Fecha vacía o inválida | 1 (el encabezado) |
| 4 | Fecha futura | 0 |
| 5 | Hora vacía o fuera de 0-23 | 1 (el encabezado) |
| 6 | Energía vacía o no numérica | 1 (el encabezado) |
| 7 | Energía negativa | 0 |
| 8 | **Energía en cero** | **1.258** |
| 9 | Duplicados (medidor, fecha, hora) | 0 |
| 10 | Horas faltantes en la serie | 0 |

**Interpretación:** el conjunto es de buena calidad estructural. El defecto real son **1.258 horas
en 0 kWh** en 7 subestaciones (SUB-07, 15, 22: 195 c/u; SUB-06: 190; SUB-12, 18: 184; SUB-02: 115).
No son lecturas sueltas: vienen en **bloques diurnos de 4 a 14 horas seguidas** (7:00-20:00), lo
que corresponde a cortes o mantenimientos programados, o a fallas del medidor. El encabezado (con
BOM) aparece como fila porque bronce lee el archivo tal cual.

### A4. Plata (`03_plata.sql`)
Reglas, en orden de aplicación (cada fila rechazada lleva el primer motivo):

| Regla | Justificación (hallazgo) | Rechaza |
|---|---|---|
| R1 medidor ∈ catálogo `SUB-NN` | H1, H2: encabezado con BOM | 1 |
| R2 fecha válida | H3 | 0 (ya cayó en R1) |
| R3 hora entera 0-23 | H5 | 0 |
| R4 energía numérica | H6 | 0 |
| R5 energía ≥ 0 | H7: preventiva (medidores bidireccionales) | 0 |
| **R6 energía ≠ 0** | **H8: 1.258 ceros en bloques de corte; sesgan medias, picos y factor de carga** | **1.258** |
| R7 sin duplicados de clave | H9: preventiva, conserva el primero | 0 |

Resultado esperado: **1.138.801 = 1.137.542 válidas + 1.259 rechazadas** ✅

Las tablas `plata_consumo_valido` (tipada, Parquet, particionada por año) y
`plata_consumo_rechazado` (valores originales + `motivo`) son Iceberg.

### A5. Oro (`04_oro.sql`)
**Pregunta de negocio:** *¿Cuánta energía consume cada subestación por mes, cuál es su pico horario y
su factor de carga?* Sirve para priorizar ampliaciones de capacidad (picos altos y crecientes) y
detectar subestaciones subutilizadas (factor de carga bajo). Tabla Iceberg `oro_consumo_mensual`
(26 subestaciones × 60 meses ≈ 1.560 filas).

### A6. Comparación (`05_comparacion.sql`)
Misma pregunta, *Top 5 subestaciones por MWh en 2025*, en las tres capas. Ejecutar cada consulta 3
veces y anotar **Data scanned** y **Run time** desde *Query stats*:

| Capa | Formato | Bytes escaneados | Tiempo (mediana) |
|---|---|---|---|
| Bronce | CSV texto | 30.871.595 B (30,9 MB) | 1.302 ms (motor 1.140 ms) |
| Plata | Iceberg / Parquet, partición por año | 386.431 B (386 KB), **80× menos** | 738 ms (motor 585 ms) |
| Oro | Iceberg agregado mensual | 6.750 B (6,7 KB), **4.574× menos** | 710 ms (motor 531 ms) |

Resultado idéntico en las tres capas: SUB-01 9.485,1 · SUB-18 9.226,7 · SUB-06 8.636,8 · SUB-14 8.299,9 · SUB-25 5.381,6 MWh.
*(Ejecutado el 2026-10-23, 3 corridas por capa, mediana del tiempo total.)*

**Interpretación:** bronce siempre lee el archivo completo (CSV no es columnar, no tiene
particiones y además convierte texto a número en cada consulta). Plata lee solo las columnas
`medidor`, `fecha` y `energia_kwh` de la partición 2025, comprimidas en Parquet, y usa
estadísticas min/max para saltarse bloques, así que lee una fracción de los bytes. Oro ya trae la
agregación hecha (≈1.560 filas), por lo que lee unos pocos KB. En Athena se paga por TB escaneado,
así que esa reducción baja el costo en la misma proporción. A este tamaño los tiempos se parecen
porque el arranque de la consulta pesa más que la lectura; la diferencia en tiempo crece con el
volumen.

### Pregunta final A: ¿y si los datos fueran 1.000 veces mayores? (~1.100 M filas, ~30 GB CSV)
- No aceptar un único CSV en bronce: pedir la ingesta en archivos comprimidos (gzip/Parquet) de 128 MB a 1 GB, particionados en S3 por `anio/mes`, y usar *partition projection*.
- Generar plata/oro de forma **incremental** (`INSERT INTO` / `MERGE` de Iceberg por partición nueva) en vez de recrearlas con CTAS completos, y correr `OPTIMIZE`/`VACUUM` para compactar archivos pequeños.
- Particionar plata por `mes(fecha)` y ordenar o hacer *bucketing* por `medidor`.
- Mover las transformaciones pesadas a **AWS Glue (Spark) o EMR** y dejar Athena para consultar; orquestar con Step Functions o Glue Workflows.
- Controlar el costo con *workgroups* con límite de bytes por consulta y hacer que los usuarios consulten oro, no bronce.
- La regla de duplicados (ventana `row_number` global) se vuelve costosa: deduplicar dentro de cada partición.

---

## PARTE B – Streaming: IoT Core → SQS → Lambda → DynamoDB

Eventos: lecturas reales tomadas al azar del CSV del proyecto. El 3 % se daña a propósito
(falta energía, energía no numérica o negativa, medidor inválido, hora 25, fecha 30-feb) para
probar los descartes.

```json
{"event_id":"uuid","medidor":"SUB-07","fecha":"2025-03-14","hora":7,"energia_kwh":133.25,"sent_at_ms":1761234567890}
```

### Creación en la consola (us-east-2), en este orden

**1. DynamoDB → Create table** (las dos en modo *On-demand*)
- `subestaciones_lecturas`: Partition key `medidor` (String), Sort key `lectura` (String).
- `subestaciones_descartes`: Partition key `event_id` (String).

> **Justificación de la llave:** el patrón de acceso principal es *“lecturas de una subestación en
> un rango de tiempo”*. `medidor` como partición agrupa la serie de cada subestación, y `lectura =
> fecha#hora#event_id` como clave de orden permite `Query medidor = 'SUB-07' AND lectura BETWEEN
> '2025-03-01' AND '2025-03-31~'` sin escanear la tabla. El `event_id` al final hace la clave
> única y **determinística**: SQS entrega *al menos una vez*, y si un mensaje se reentrega se
> sobrescribe el mismo ítem en vez de duplicarlo, así los conteos cuadran. Límite: solo hay 26
> particiones lógicas; con un volumen muy alto convendría agregar un sufijo de *sharding*.

**2. SQS → Create queue**: tipo **Standard**, nombre `subestaciones-lecturas`, visibility timeout 60 s.

**3. Lambda → Create function**: `procesar-lecturas`, Python 3.12.
- Rol: *Create a new role with basic Lambda permissions*. Luego, en **IAM → ese rol → Add permissions**: adjuntar `AWSLambdaSQSQueueExecutionRole` y una política inline con `dynamodb:PutItem` sobre las dos tablas. *(En AWS Academy usar `LabRole`.)*
- Pegar `parteB/lambda_function.py` en el editor → **Deploy**.
- *Configuration → General*: timeout 30 s. *Environment variables* (opcional): `TABLA_LECTURAS`, `TABLA_DESCARTES`.
- **Add trigger → SQS** → cola `subestaciones-lecturas`, batch size 10.

**4. IoT Core → Message routing → Rules → Create rule**: `lecturas_a_sqs`
- SQL: `SELECT *, timestamp() AS iot_ts_ms FROM 'subestaciones/lecturas'`
- Acción: **Simple Queue Service** → cola `subestaciones-lecturas`, *Use Base64* **desactivado**, *Create new role* (`iot-a-sqs`).
- Prueba rápida: *MQTT test client → Publish* en `subestaciones/lecturas` con el JSON de ejemplo → revisar que aparezca en DynamoDB.

### Publicar los 10.000 eventos (terminal de VS Code en Windows)
```powershell
cd C:\Users\Zarith\Downloads\trabajo1\parteB
pip install boto3
python publicar_eventos.py --csv "C:\Users\Zarith\Downloads\consumo_sintetico_subestaciones.csv"
```
Imprime publicados, cuántos se dañaron a propósito, la duración y deja `publicados.csv`.

### Cuadrar los conteos y medir el retraso
Esperar a que **SQS → Messages available / in flight = 0** y ejecutar:
```powershell
python conteos_y_retraso.py
```
Muestra `publicados = guardados + descartados`, los descartes por motivo, la verificación cruzada
con los eventos dañados y el retraso (promedio, p50, p95 y **máximo**) en dos medidas:
- **envío (PC) → DynamoDB:** de extremo a extremo; depende de que el reloj del PC esté sincronizado (*Configuración → Hora → Sincronizar ahora*).
- **IoT Core → DynamoDB:** usa solo relojes de AWS, así que no le afecta el desfase del PC.

| Publicados | Guardados | Descartados | ¿Cuadra? | Retraso prom. | Retraso máx. |
|---|---|---|---|---|---|
| 10.000 | 9.692 | 308 | ✅ SÍ | 96 ms (IoT→DDB 89 ms) | 2.435 ms (IoT→DDB 2.430 ms) |

Descartes por motivo: energía negativa 60 · fecha inválida 59 · falta energia_kwh 57 · medidor fuera de catálogo 50 ·
energía no numérica 43 · hora fuera de 0-23 39. Los 308 coinciden 1 a 1 con los eventos dañados a propósito.
p50 = 60 ms, p95 = 149 ms. El máximo de unos 2,4 s es un caso aislado; probablemente es el arranque en frío de una instancia nueva de Lambda.
Publicación: 73,5 s (136 eventos/s, 16 hilos).

### Pregunta final B: ¿a partir de qué volumen dejaríamos este flujo?
Este flujo procesa cada evento por separado: un mensaje SQS, una invocación de Lambda por lote de
10 y un `PutItem`. Funciona y es barato hasta **unos cientos o pocos miles de eventos por segundo**
(del orden de decenas de millones al día). Por encima de eso, o si se necesita **orden por
subestación, reproceso o varios consumidores**, lo cambiaríamos por **IoT Core → Amazon Kinesis
Data Streams** (o MSK/Kafka) con Lambda o **Managed Service for Apache Flink** procesando por
shard. Ofrece orden por clave de partición, retención para reprocesar, lotes de hasta 10.000
registros, varios consumidores y costo por shard en vez de por mensaje. Para dejar los datos
analíticos en el lago, usaríamos **Data Firehose → S3 (Parquet/Iceberg)** en lugar de DynamoDB. En
SQS Standard, además, el costo por mensaje y la cantidad de invocaciones de Lambda crecen en
línea con el volumen, y la cola no garantiza orden.
