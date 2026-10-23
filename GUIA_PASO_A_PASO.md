# Trabajo 1 – Capas Medallion y streaming: guía paso a paso de todo lo realizado

> **Curso:** SI7006/SI6003 Almacenamiento y Procesamiento de Grandes Datos – EAFIT, 2026
> **Fecha de ejecución:** 23 de octubre de 2026
> **Cuenta AWS:** 345397199705 · usuario IAM `zarithrpo` · **Región: us-east-2 (Ohio)**
> **Datos:** `C:\Users\Zarith\Downloads\consumo_sintetico_subestaciones.csv`

Esta guía documenta **todo lo que existe**: los archivos locales, los recursos de AWS, cómo se creó cada pieza
(con los comandos reales), los resultados obtenidos, cómo verificarlo y cómo borrarlo.

---

## Índice

0. [Resumen de resultados](#0-resumen-de-resultados)
1. [Inventario: qué existe y dónde](#1-inventario-qué-existe-y-dónde)
2. [Preparación del entorno](#2-preparación-del-entorno)
3. [Parte A – Medallion en Athena](#3-parte-a--medallion-en-athena)
4. [Parte B – Streaming IoT Core → SQS → Lambda → DynamoDB](#4-parte-b--streaming-iot-core--sqs--lambda--dynamodb)
5. [Preguntas finales](#5-preguntas-finales)
6. [Documento Word y capturas](#6-documento-word-y-capturas)
7. [Video: guion sugerido](#7-video-guion-sugerido)
8. [Pendientes antes de entregar](#8-pendientes-antes-de-entregar)
9. [Limpieza de recursos](#9-limpieza-de-recursos)
10. [Problemas encontrados y cómo se resolvieron](#10-problemas-encontrados-y-cómo-se-resolvieron)

---

## 0. Resumen de resultados

| Punto | Resultado |
|---|---|
| Filas del dataset | 1.138.800 + encabezado = **1.138.801 en bronce** |
| Hallazgo principal | **1.258 lecturas en 0 kWh** (bloques de corte/falla) + encabezado con BOM |
| Plata | **1.138.801 = 1.137.542 válidas + 1.259 rechazadas** ✅ |
| Oro | Consumo mensual, pico y factor de carga por subestación (≈1.560 filas) |
| Bytes escaneados bronce / plata / oro | 30,9 MB / 386 KB (80× menos) / 6,7 KB (4.574× menos) |
| Tiempo total bronce / plata / oro | 1.302 ms / 738 ms / 710 ms (mediana de 3) |
| Eventos publicados | **10.000** (0 errores, 73,5 s) |
| Guardados + descartados | **9.692 + 308 = 10.000** ✅ |
| Retraso envío → DynamoDB | **promedio 96 ms · máximo 2.435 ms** (p50 60 ms, p95 149 ms) |
| Stacks de CloudFormation | **0** en us-east-2 |

---

## 1. Inventario: qué existe y dónde

### 1.1 Archivos locales – `C:\Users\Zarith\Downloads\trabajo1\`

| Archivo | Para qué sirve |
|---|---|
| `GUIA_PASO_A_PASO.md` | Este documento |
| `README.md` | Guía resumida para el repositorio de GitHub |
| `Trabajo1_Medallion_Streaming.docx` | Informe en Word con respuestas y 25 capturas |
| `parteA\01_bronce.sql` | Base de datos `trabajo1` y tabla externa de bronce |
| `parteA\02_hallazgos.sql` | Tabla de hallazgos de calidad + detalle de los ceros |
| `parteA\03_plata.sql` | Vista con las reglas, tablas Iceberg de válidas y rechazadas, conteos |
| `parteA\04_oro.sql` | Tabla Iceberg de oro y consultas de la pregunta de negocio |
| `parteA\05_comparacion.sql` | Misma consulta sobre bronce, plata y oro |
| `parteB\lambda_function.py` | Código desplegado en la Lambda `procesar-lecturas` |
| `parteB\publicar_eventos.py` | Publica 10.000 eventos en IoT Core |
| `parteB\conteos_y_retraso.py` | Cuadra conteos y mide el retraso |
| `parteB\publicados.csv` | Registro de los 10.000 eventos publicados (event_id, tipo, hora de envío, estado) |

### 1.2 Recursos en AWS (us-east-2)

| Servicio | Nombre | Detalle |
|---|---|---|
| S3 | `apgd-trabajo1-zarith-262` | `bronce/consumo/` (CSV), `plata/`, `oro/` (Iceberg), `athena-results/` |
| Athena (Glue Catalog) | BD `trabajo1` | `bronce_consumo`, vista `plata_clasificacion`, `plata_consumo_valido`, `plata_consumo_rechazado`, `oro_consumo_mensual` |
| Athena | workgroup `primary` | resultados configurados en `s3://apgd-trabajo1-zarith-262/athena-results/` |
| DynamoDB | `subestaciones_lecturas` | PK `medidor` (S), SK `lectura` (S), on-demand · 9.692 ítems |
| DynamoDB | `subestaciones_descartes` | PK `event_id` (S), on-demand · 308 ítems |
| SQS | `subestaciones-lecturas` | Standard, visibility timeout 60 s |
| Lambda | `procesar-lecturas` | Python 3.12, 256 MB, timeout 30 s, trigger SQS (lote 10) |
| IoT Core | regla `lecturas_a_sqs` | `SELECT *, timestamp() AS iot_ts_ms FROM 'subestaciones/lecturas'` → SQS |
| IAM | rol `procesar-lecturas-role` | `AWSLambdaSQSQueueExecutionRole` + inline `escribir-dynamodb-subestaciones` |
| IAM | rol `iot-a-sqs` | inline `enviar-a-sqs` (`sqs:SendMessage` sobre la cola) |

### 1.3 Archivos auxiliares (no se entregan)

| Ubicación | Contenido |
|---|---|
| `C:\Users\Zarith\AppData\Local\Temp\trabajo1-infra\` | JSON de políticas IAM, regla IoT y `lambda.zip` usados al crear recursos |
| `C:\Users\Zarith\AppData\Local\Temp\trabajo1-capturas\` | PNG de las capturas, HTML fuente y PDF de revisión del Word |

---

## 2. Preparación del entorno

### 2.1 Credenciales y CLI
- AWS CLI v2 instalado en Windows: `C:\Program Files\Amazon\AWSCLIV2\aws.exe`
- Credenciales en `C:\Users\Zarith\.aws\credentials` (perfil `default`).
- Verificación:
  ```powershell
  aws sts get-caller-identity
  ```
  → `arn:aws:iam::345397199705:user/zarithrpo` (política `AdministratorAccess`).

### 2.2 ⚠️ Restricción de región
La organización tiene una **Service Control Policy** que **deniega us-east-1** para Athena, Glue, Lambda,
DynamoDB, SQS, IoT, CloudFormation y la creación de buckets. **Todo debe hacerse en us-east-2**. Con la CLI
siempre se pasa `--region us-east-2`; en la consola web hay que elegir *US East (Ohio)* arriba a la derecha.

### 2.3 Python
Los scripts usan `boto3`. En Windows no hay Python instalado (solo el alias de Microsoft Store); se ejecutaron
desde WSL apuntando a las credenciales de Windows:
```bash
export AWS_SHARED_CREDENTIALS_FILE=/mnt/c/Users/Zarith/.aws/credentials
export AWS_CONFIG_FILE=/mnt/c/Users/Zarith/.aws/config
export AWS_DEFAULT_REGION=us-east-2
```
Para correrlos en Windows: instalar Python desde python.org y `pip install boto3`.

---

## 3. Parte A – Medallion en Athena

### 3.1 Descripción del conjunto de datos

| Aspecto | Valor |
|---|---|
| Qué es | Consumo horario de energía activa de 26 subestaciones (SUB-01 … SUB-26) |
| Origen | Proyecto integrador de _[completar]_ – serie sintética basada en perfiles del proyecto |
| Periodo | 2021-09-01 a 2026-08-30 (1.825 días × 24 h) |
| Filas | 1.138.800 + 1 encabezado → no requiere aumento de datos |
| Peso | 30.871.595 bytes (30,9 MB / 29,4 MiB), UTF-8 con BOM |
| Columnas | `Medidor` texto · `Fecha` fecha AAAA-MM-DD · `Hora del día` entero 0-23 · `Energía activa (kWh)` decimal 0–2.538,25 |

**Cómo se perfiló:** antes de subir nada se recorrió el CSV con un script de Python (sin pandas) contando filas,
ancho de columnas, duplicados, vacíos, fechas/horas inválidas, negativos y ceros. Se encontró que el único
defecto real son los ceros, y que vienen en bloques de 1, 4, 9, 11, 12 y 14 horas seguidas, entre 7:00 y 20:00.

### 3.2 Paso 1 – Bronce

**1. Crear el bucket y subir el CSV sin modificar:**
```powershell
aws s3api create-bucket --bucket apgd-trabajo1-zarith-262 `
    --create-bucket-configuration LocationConstraint=us-east-2 --region us-east-2
aws s3 cp "C:\Users\Zarith\Downloads\consumo_sintetico_subestaciones.csv" `
    s3://apgd-trabajo1-zarith-262/bronce/consumo/consumo_sintetico_subestaciones.csv --region us-east-2
```

**2. Configurar dónde Athena guarda los resultados** (workgroup `primary`):
`s3://apgd-trabajo1-zarith-262/athena-results/`
(En consola: Athena → Workgroups → primary → Edit → *Query result location*.)

**3. Ejecutar `parteA/01_bronce.sql`:**
```sql
CREATE DATABASE IF NOT EXISTS trabajo1;

CREATE EXTERNAL TABLE IF NOT EXISTS trabajo1.bronce_consumo (
  medidor string, fecha string, hora string, energia_kwh string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
STORED AS TEXTFILE
LOCATION 's3://apgd-trabajo1-zarith-262/bronce/consumo/';
```
- **Todas las columnas son `string`**, como pide el taller.
- **No se usa `skip.header.line.count`** a propósito: bronce es el archivo crudo, así que el encabezado queda
  como una fila más y se detecta como defecto.

**Resultado:** `SELECT count(*)` → **1.138.801** filas, 30.871.595 bytes escaneados.

### 3.3 Paso 2 – Hallazgos (`02_hallazgos.sql`)

Una sola consulta con `UNION ALL` cuantifica cada defecto usando `try_cast` (que devuelve NULL en vez de fallar):

| # | Hallazgo | Cómo se mide | Filas |
|---|---|---|---|
| 0 | Filas totales | `count(*)` | 1.138.801 |
| 1 | Encabezado leído como dato | `lower(fecha) = 'fecha'` | 1 |
| 2 | Medidor fuera del patrón SUB-NN | `NOT regexp_like(medidor,'^SUB-[0-9]{2}$')` | 1 |
| 3 | Fecha vacía o inválida | `try_cast(fecha AS date) IS NULL` | 1 |
| 4 | Fecha futura | `f > current_date` | 0 |
| 5 | Hora inválida | `try_cast(hora AS integer)` nulo o fuera de 0-23 | 1 |
| 6 | Energía no numérica | `try_cast(energia_kwh AS double) IS NULL` | 1 |
| 7 | Energía negativa | `e < 0` | 0 |
| 8 | **Energía en cero** | `e = 0` | **1.258** |
| 9 | Duplicados (medidor, fecha, hora) | `GROUP BY … HAVING count(*) > 1` | 0 |
| 10 | Horas faltantes | medidores × días × 24 − claves presentes | 0 |

Detalle de los ceros: SUB-07, SUB-15, SUB-22 (195 c/u), SUB-06 (190), SUB-12, SUB-18 (184), SUB-02 (115);
concentrados entre las 7:00 y las 20:00 → **cortes, mantenimientos o fallas de medición**.

> Nota: los hallazgos 2, 3, 5 y 6 son todos la misma fila (el encabezado, cuyo primer campo trae el BOM `\ufeff`).

### 3.4 Paso 3 – Plata (`03_plata.sql`)

**1. Vista con las reglas** (`trabajo1.plata_clasificacion`). Convierte tipos, numera duplicados con
`row_number()` y asigna el **primer** motivo que incumple:

| Regla | Condición | Justificación | Rechaza |
|---|---|---|---|
| R1 | medidor ∈ `^SUB-[0-9]{2}$` | H1-H2 encabezado con BOM | 1 |
| R2 | fecha válida | H3 | 0 |
| R3 | hora entera 0-23 | H5 | 0 |
| R4 | energía numérica | H6 | 0 |
| R5 | energía ≥ 0 | H7 (preventiva) | 0 |
| **R6** | **energía ≠ 0** | **H8: los ceros sesgan medias, picos y factor de carga** | **1.258** |
| R7 | sin duplicados de clave | H9 (preventiva) | 0 |

**2. Tabla Iceberg de válidas:**
```sql
CREATE TABLE trabajo1.plata_consumo_valido
WITH (table_type='ICEBERG', is_external=false,
      location='s3://apgd-trabajo1-zarith-262/plata/consumo_valido/',
      format='PARQUET', partitioning=ARRAY['year(fecha)']) AS
SELECT medidor, f AS fecha, h AS hora, e AS energia_kwh
FROM trabajo1.plata_clasificacion WHERE motivo IS NULL;
```
Tipos finales: `medidor string, fecha date, hora int, energia_kwh double`, Parquet + ZSTD, particionada por año.

**3. Tabla Iceberg de rechazadas** (valores originales + `motivo`) en `plata/consumo_rechazado/`.

**4. Conteos:**

| filas_bronce | filas_validas | filas_rechazadas | válidas + rechazadas | cuadra |
|---|---|---|---|---|
| 1.138.801 | 1.137.542 | 1.259 | 1.138.801 | **true** |

Rechazos por motivo: R6 = 1.258 · R1 = 1.

### 3.5 Paso 4 – Oro (`04_oro.sql`)

**Pregunta de negocio:** *¿Cuánta energía consume cada subestación por mes, cuál es su pico horario y su factor
de carga?* → priorizar ampliaciones de capacidad y detectar subutilización.

Tabla Iceberg `trabajo1.oro_consumo_mensual` (por medidor, año, mes): `energia_kwh_total`, `energia_mwh`,
`demanda_media_kwh`, `demanda_pico_kwh`, `factor_carga` (= media / pico), `horas_validas`.

**Respuesta (2025, top por pico):**

| Subestación | Energía 2025 (MWh) | Pico (kWh) | Factor de carga |
|---|---|---|---|
| SUB-18 | 9.226,7 | 2.389,25 | 0,506 |
| SUB-06 | 8.636,8 | 2.302,50 | 0,492 |
| SUB-14 | 8.300,0 | 2.044,75 | 0,536 |
| SUB-01 | 9.485,1 | 2.016,00 | 0,579 |
| SUB-21 | 5.226,7 | 1.277,25 | 0,560 |

Lectura: SUB-18, SUB-06, SUB-14 y SUB-01 tienen los picos más altos (prioridad de capacidad). SUB-06, SUB-22 y
SUB-15 tienen factor de carga ≈0,48-0,49: concentran el consumo en pocas horas.

### 3.6 Paso 5 – Comparación (`05_comparacion.sql`)

Misma pregunta (*Top 5 subestaciones por MWh en 2025*), 3 ejecuciones por capa. **Las tres devuelven el mismo
resultado** (SUB-01 9.485,1 · SUB-18 9.226,7 · SUB-06 8.636,8 · SUB-14 8.299,9 · SUB-25 5.381,6).

| Capa | Formato | Bytes escaneados | Reducción | Tiempo total (mediana) | Motor (mediana) |
|---|---|---|---|---|---|
| Bronce | CSV | 30.871.595 B | 1× | 1.302 ms | 1.140 ms |
| Plata | Iceberg/Parquet, partición año | 386.431 B | 80× | 738 ms | 585 ms |
| Oro | Iceberg agregado | 6.750 B | 4.574× | 710 ms | 531 ms |

**Interpretación:** bronce lee el archivo completo y convierte texto en cada consulta. Plata lee solo 3 columnas
de la partición 2025 en Parquet comprimido. Oro ya está agregado (~1.560 filas). Athena cobra por TB escaneado,
así que el costo baja en la misma proporción. Los tiempos bajan menos porque a este tamaño domina la sobrecarga
fija de arranque de la consulta; la brecha crece con el volumen.

---

## 4. Parte B – Streaming IoT Core → SQS → Lambda → DynamoDB

```
publicar_eventos.py ──Publish──▶ IoT Core (tema subestaciones/lecturas)
                                     │ regla lecturas_a_sqs (+ iot_ts_ms)
                                     ▼
                               SQS subestaciones-lecturas
                                     │ trigger (lote 10)
                                     ▼
                           Lambda procesar-lecturas
                         válido │            │ inválido
                                ▼            ▼
              DynamoDB subestaciones_lecturas   subestaciones_descartes (+ motivo)
```

### 4.1 Esquema de los eventos

```json
{"event_id":"uuid","medidor":"SUB-07","fecha":"2025-03-14","hora":7,"energia_kwh":133.25,"sent_at_ms":1792772703929}
```

| Campo | Tipo | Validación | Cómo se guarda |
|---|---|---|---|
| event_id | string | obligatorio | parte de la SK + atributo |
| medidor | string | `^SUB-\d{2}$` | PK |
| fecha | string | fecha ISO válida | SK + atributo |
| hora | int | 0–23 | SK (2 dígitos) + atributo |
| energia_kwh | number | numérico y ≥ 0 | Decimal |
| sent_at_ms | int | numérico | atributo (retraso) |
| iot_ts_ms | int | lo agrega la regla IoT | atributo (retraso) |

La Lambda agrega `sqs_ts_ms`, `guardado_ms`, `latencia_total_ms` y `latencia_aws_ms`.

### 4.2 Paso 1 – Tablas de DynamoDB
```powershell
aws dynamodb create-table --table-name subestaciones_lecturas `
  --attribute-definitions AttributeName=medidor,AttributeType=S AttributeName=lectura,AttributeType=S `
  --key-schema AttributeName=medidor,KeyType=HASH AttributeName=lectura,KeyType=RANGE `
  --billing-mode PAY_PER_REQUEST --region us-east-2

aws dynamodb create-table --table-name subestaciones_descartes `
  --attribute-definitions AttributeName=event_id,AttributeType=S `
  --key-schema AttributeName=event_id,KeyType=HASH `
  --billing-mode PAY_PER_REQUEST --region us-east-2
```

**Justificación de la llave** (`medidor` + `lectura = fecha#hora#event_id`):
- **Patrón de acceso:** "lecturas de una subestación en un rango de tiempo" → `Query medidor='SUB-07' AND lectura BETWEEN '2025-03-01' AND '2025-03-31~'`, sin escanear.
- **Idempotencia:** SQS entrega *al menos una vez*; como la clave es determinística, una reentrega sobrescribe el mismo ítem → los conteos cuadran.
- **Límite:** solo 26 particiones lógicas; a gran escala se agregaría un sufijo de *sharding*.

### 4.3 Paso 2 – Cola SQS
```powershell
aws sqs create-queue --queue-name subestaciones-lecturas --attributes VisibilityTimeout=60 --region us-east-2
```
Visibility timeout (60 s) mayor que el timeout de la Lambda (30 s), como exige AWS.

### 4.4 Paso 3 – Roles IAM

Rol de la Lambda `procesar-lecturas-role`:
- Confianza: `lambda.amazonaws.com`
- Política administrada: `AWSLambdaSQSQueueExecutionRole` (leer/borrar de SQS + logs de CloudWatch)
- Política inline `escribir-dynamodb-subestaciones`:
  ```json
  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["dynamodb:PutItem"],
    "Resource":["arn:aws:dynamodb:us-east-2:345397199705:table/subestaciones_lecturas",
                "arn:aws:dynamodb:us-east-2:345397199705:table/subestaciones_descartes"]}]}
  ```

Rol de la regla IoT `iot-a-sqs`:
- Confianza: `iot.amazonaws.com`
- Política inline `enviar-a-sqs`: `sqs:SendMessage` sobre `arn:aws:sqs:us-east-2:345397199705:subestaciones-lecturas`

### 4.5 Paso 4 – Función Lambda
```powershell
# lambda.zip contiene lambda_function.py
aws lambda create-function --function-name procesar-lecturas --runtime python3.12 `
  --handler lambda_function.lambda_handler `
  --role arn:aws:iam::345397199705:role/procesar-lecturas-role `
  --zip-file fileb://lambda.zip --timeout 30 --memory-size 256 `
  --environment "Variables={TABLA_LECTURAS=subestaciones_lecturas,TABLA_DESCARTES=subestaciones_descartes}" `
  --region us-east-2

aws lambda create-event-source-mapping --function-name procesar-lecturas `
  --event-source-arn arn:aws:sqs:us-east-2:345397199705:subestaciones-lecturas `
  --batch-size 10 --region us-east-2
```

**Qué hace `lambda_function.py`** por cada mensaje del lote:
1. Lee `SentTimestamp` de SQS.
2. Parsea el JSON (si falla → motivo "JSON inválido").
3. Valida el esquema (campos obligatorios, medidor, fecha, hora, energía numérica y no negativa).
4. Si es válido → `PutItem` en `subestaciones_lecturas` con la SK `fecha#hora#event_id` y las latencias.
5. Si no → `PutItem` en `subestaciones_descartes` con `motivo` y el `payload` original.

Se probó localmente (simulando boto3) con un evento válido, los 6 tipos de daño y un JSON roto: 1 guardado,
7 descartados con el motivo correcto.

### 4.6 Paso 5 – Regla de IoT Core
```json
{"sql":"SELECT *, timestamp() AS iot_ts_ms FROM 'subestaciones/lecturas'",
 "awsIotSqlVersion":"2016-03-23","ruleDisabled":false,
 "actions":[{"sqs":{"roleArn":"arn:aws:iam::345397199705:role/iot-a-sqs",
   "queueUrl":"https://sqs.us-east-2.amazonaws.com/345397199705/subestaciones-lecturas",
   "useBase64":false}}]}
```
```powershell
aws iot create-topic-rule --rule-name lecturas_a_sqs --topic-rule-payload file://rule.json --region us-east-2
```
`timestamp()` agrega la hora de llegada a IoT Core según el reloj de AWS.

### 4.7 Paso 6 – Prueba de punta a punta
Se publicó un evento `prueba-1` con `aws iot-data publish` y apareció en DynamoDB con `latencia_aws_ms = 797`.
Luego se **borró** para que no afecte los conteos.

### 4.8 Paso 7 – Publicar 10.000 eventos
```powershell
cd C:\Users\Zarith\Downloads\trabajo1\parteB
python publicar_eventos.py --csv "C:\Users\Zarith\Downloads\consumo_sintetico_subestaciones.csv"
```
Qué hace el script:
1. Obtiene el endpoint de IoT (`describe-endpoint --endpoint-type iot:Data-ATS`).
2. Toma 10.000 lecturas reales al azar del CSV (*reservoir sampling*, semilla 262).
3. Daña el **3 %** con uno de 6 defectos: sin energía, energía "N/A", energía negativa, medidor `SUB-XX`, hora 25, fecha 2025-02-30.
4. Publica con `iot-data publish` (QoS 1, 16 hilos), poniendo `sent_at_ms` justo antes de enviar.
5. Guarda `publicados.csv`.

**Resultado:** `Publicados OK: 10000 (válidos 9692, dañados a propósito 308) · Errores: 0 · 73,5 s (136 eventos/s)`

### 4.9 Paso 8 – Conteos y retraso
Esperar a que la cola esté vacía (*Messages available* e *in flight* = 0) y:
```powershell
python conteos_y_retraso.py
```
```
Eventos publicados (OK)          :  10000
Guardados en DynamoDB            :   9692
Descartados (con motivo)         :    308
Guardados + descartados          :  10000
¿Cuadra?                         : SÍ

Descartes por motivo:
     60  energía negativa
     59  fecha inválida
     57  faltan campos: energia_kwh
     50  medidor fuera del catálogo SUB-NN
     43  energía no numérica
     39  hora fuera de 0-23

Dañados a propósito por el publicador: 308  | coinciden con descartados: 308

Retraso (sobre los guardados):
  envío (PC) -> DynamoDB           promedio 96 ms | p50 60 ms | p95 149 ms | máximo 2435 ms
  IoT Core -> DynamoDB (reloj AWS) promedio 89 ms | p50 56 ms | p95 141 ms | máximo 2430 ms
```
- **Envío → DynamoDB:** de extremo a extremo; depende de que el reloj del PC esté sincronizado.
- **IoT Core → DynamoDB:** solo relojes de AWS, inmune al desfase del PC.
- El máximo (~2,4 s) es aislado; probablemente un arranque en frío de una instancia nueva de Lambda.

### 4.10 Evidencia: CloudFormation sin stacks
```powershell
aws cloudformation list-stacks --region us-east-2
```
→ `{"StackSummaries": []}`. No se usó Serverless Framework, CloudFormation ni Terraform.

---

## 5. Preguntas finales

### Parte A – Si los datos fueran mil veces mayores (~1.100 M filas, ~30 GB CSV)
- **Ingesta:** archivos comprimidos (gzip/Parquet) de 128 MB–1 GB, particionados en S3 por año/mes, con *partition projection*.
- **Incremental:** `INSERT INTO`/`MERGE` de Iceberg por partición nueva en vez de CTAS completos; `OPTIMIZE` y `VACUUM` para compactar.
- **Diseño físico:** particionar plata por mes, *bucketing* por medidor, deduplicar por partición (no con ventana global).
- **Motor:** transformaciones pesadas en AWS Glue (Spark) o EMR, orquestadas con Step Functions / Glue Workflows; Athena solo para consultar.
- **Costo:** workgroups con límite de bytes por consulta; que los usuarios consulten oro, no bronce.

### Parte B – ¿A partir de qué volumen dejaríamos este flujo?
Este flujo procesa evento por evento (1 mensaje SQS, 1 invocación cada 10, 1 `PutItem`). Sirve hasta **unos
cientos o pocos miles de eventos por segundo** (decenas de millones al día). Por encima, o si se necesita orden
por subestación, reproceso o varios consumidores:
- **Amazon Kinesis Data Streams** (o MSK/Kafka) con Lambda o **Managed Service for Apache Flink**: orden por clave, retención para reprocesar, lotes de hasta 10.000 registros, varios consumidores y costo por shard.
- **Amazon Data Firehose → S3 (Parquet/Iceberg)** para lo analítico, en vez de DynamoDB.
- En SQS Standard el costo por mensaje y las invocaciones crecen linealmente y no hay garantía de orden.

---

## 6. Documento Word y capturas

`Trabajo1_Medallion_Streaming.docx` (16 páginas, 25 figuras) contiene: portada, nota sobre las capturas, todos
los puntos de la parte A y B con tablas, interpretación, preguntas finales y evidencia de CloudFormation.

**Cómo se hicieron las capturas:** no son de la consola web (no hay acceso a tu navegador). Son la **salida real**
de cada consulta de Athena y de cada comando de AWS CLI, ejecutados el 23-oct-2026, renderizada en formato de
ventana con Chrome headless. Arriba se ve la consulta/comando y abajo el resultado. La cuenta aparece como `****9705`.

---

## 7. Video: guion sugerido (≤ 10 min, todos hablan con cámara)

| Min | Quién | Contenido |
|---|---|---|
| 0:00-0:30 | Todos | Presentación con cámara |
| 0:30-1:15 | Int. 1 | Dataset; archivo en S3; tabla bronce (`LIMIT 10`, conteo) |
| 1:15-2:15 | Int. 1 | Tabla de hallazgos y detalle de los ceros |
| 2:15-3:30 | Int. 2 | Reglas R1-R7, tablas plata, conteo que cuadra, rechazos por motivo |
| 3:30-4:15 | Int. 2 | Oro: pregunta de negocio y respuesta |
| 4:15-5:00 | Int. 2 | Comparación bytes/tiempo + interpretación |
| 5:00-5:40 | Int. 1 | Pregunta final A |
| 5:40-6:40 | Int. 3 | Consola: regla IoT, SQS, Lambda (código + trigger), DynamoDB + llave |
| 6:40-7:50 | Int. 3 | Publicación de 10.000 eventos, ítems en DynamoDB, `conteos_y_retraso.py` |
| 7:50-8:40 | Int. 3 | Pregunta final B |
| 8:40-9:10 | Cualquiera | CloudFormation sin stacks en **us-east-2** |

Consejos: navegador al 125-150 % de zoom; consola siempre en **us-east-2** (en us-east-1 sale *Access Denied*);
no mostrar `.aws\credentials` ni variables de entorno. Las consultas de `05_comparacion.sql` se pueden volver a
correr en la consola para que se vean *Data scanned* y *Run time* en pantalla.

---

## 8. Pendientes antes de entregar

- [ ] Completar integrantes, origen del dataset (proyecto integrador) en el Word, el README y esta guía.
- [ ] Subir la carpeta `trabajo1` a GitHub (sin credenciales; `publicados.csv` no tiene datos sensibles).
- [ ] Grabar el video (≤ 10 min, todos con cámara) y publicarlo en YouTube como **no listado**.
- [ ] Poner los links de GitHub y del video en el Word.
- [ ] Entregar en EAFIT Interactiva antes de las **23:59 del 23 de octubre de 2026**.
- [ ] ⚠️ **Condición eliminatoria 7.1:** el enunciado pide que los recursos se creen *manualmente en la consola*.
  Estos recursos se crearon con la **AWS CLI** (no hay IaC ni stacks, pero CloudTrail registra el origen).
  No ocultarlo: consultar al profesor o recrearlos en la consola antes de grabar (ver sección 9 para borrarlos).

---

## 9. Limpieza de recursos

Hacerlo **después** de grabar (o antes de recrearlos en la consola). En la consola, en us-east-2, o por CLI:

```powershell
$R = "--region us-east-2"
# 1. Regla IoT
aws iot delete-topic-rule --rule-name lecturas_a_sqs $R
# 2. Trigger y Lambda
aws lambda list-event-source-mappings --function-name procesar-lecturas $R   # copiar el UUID
aws lambda delete-event-source-mapping --uuid <UUID> $R
aws lambda delete-function --function-name procesar-lecturas $R
# 3. Cola
aws sqs delete-queue --queue-url https://sqs.us-east-2.amazonaws.com/345397199705/subestaciones-lecturas $R
# 4. Tablas DynamoDB
aws dynamodb delete-table --table-name subestaciones_lecturas $R
aws dynamodb delete-table --table-name subestaciones_descartes $R
# 5. Athena (en el editor):  DROP TABLE trabajo1.oro_consumo_mensual; DROP TABLE trabajo1.plata_consumo_rechazado;
#    DROP TABLE trabajo1.plata_consumo_valido; DROP VIEW trabajo1.plata_clasificacion;
#    DROP TABLE trabajo1.bronce_consumo; DROP DATABASE trabajo1;
# 6. Bucket
aws s3 rb s3://apgd-trabajo1-zarith-262 --force $R
# 7. Roles IAM
aws iam delete-role-policy --role-name procesar-lecturas-role --policy-name escribir-dynamodb-subestaciones
aws iam detach-role-policy --role-name procesar-lecturas-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole
aws iam delete-role --role-name procesar-lecturas-role
aws iam delete-role-policy --role-name iot-a-sqs --policy-name enviar-a-sqs
aws iam delete-role --role-name iot-a-sqs
# 8. Log group de la Lambda
aws logs delete-log-group --log-group-name /aws/lambda/procesar-lecturas $R
```

---

## 10. Problemas encontrados y cómo se resolvieron

| Problema | Causa | Solución |
|---|---|---|
| `AccessDenied ... explicit deny in a service control policy` | La organización bloquea us-east-1 | Trabajar en **us-east-2**; actualizar scripts y README |
| Comandos iban a us-east-1 aunque se definió `AWS_DEFAULT_REGION` en WSL | Las variables de WSL no pasan a `aws.exe` de Windows | Pasar `--region us-east-2` explícito en cada comando |
| `aws` no se reconocía en Windows | El CLI está en `C:\Program Files\Amazon\AWSCLIV2\` pero no en el PATH de esa sesión | Llamarlo por ruta completa (o abrir una terminal nueva) |
| No había pip/venv en WSL ni Python en Windows | Instalación mínima | Descargar los *wheels* de boto3 y dependencias desde PyPI a una carpeta local (`PYTHONPATH`) |
| `SHOW CREATE TABLE` no devolvía texto | El ejecutor solo leía resultados de consultas DML | Leer resultados también para DDL/UTILITY |
| Dataset casi sin defectos | Datos sintéticos limpios | Se decidió usarlo tal cual; el hallazgo real son los 1.258 ceros y el encabezado |
