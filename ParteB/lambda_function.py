"""
Lambda: consume la cola SQS 'subestaciones-lecturas' y guarda en DynamoDB.

Flujo: IoT Core (tema subestaciones/lecturas) -> regla IoT -> SQS -> esta Lambda -> DynamoDB

Esquema esperado del evento (JSON):
  event_id     str   id único del evento (lo genera el publicador)
  medidor      str   SUB-01 ... SUB-26
  fecha        str   AAAA-MM-DD
  hora         int   0..23
  energia_kwh  num   >= 0
  sent_at_ms   int   epoch ms en que el publicador envió el evento
  iot_ts_ms    int   epoch ms en que IoT Core recibió el evento (lo agrega la regla con timestamp())

Válidos    -> tabla subestaciones_lecturas   PK medidor, SK lectura (= fecha#hora#event_id)
Descartados-> tabla subestaciones_descartes  PK event_id, con el motivo
"""
import json
import os
import re
import time
from datetime import date
from decimal import Decimal

import boto3

ddb = boto3.resource("dynamodb")
tabla_lecturas = ddb.Table(os.environ.get("TABLA_LECTURAS", "subestaciones_lecturas"))
tabla_descartes = ddb.Table(os.environ.get("TABLA_DESCARTES", "subestaciones_descartes"))

MEDIDOR_RE = re.compile(r"^SUB-\d{2}$")
CAMPOS = ("event_id", "medidor", "fecha", "hora", "energia_kwh", "sent_at_ms")


def es_numero(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validar(ev):
    """Devuelve None si el evento cumple el esquema, o el motivo del descarte."""
    if not isinstance(ev, dict):
        return "payload no es un objeto JSON"
    faltan = [c for c in CAMPOS if ev.get(c) in (None, "")]
    if faltan:
        return "faltan campos: " + ",".join(faltan)
    if not isinstance(ev["medidor"], str) or not MEDIDOR_RE.match(ev["medidor"]):
        return "medidor fuera del catálogo SUB-NN"
    try:
        date.fromisoformat(str(ev["fecha"]))
    except ValueError:
        return "fecha inválida"
    if not isinstance(ev["hora"], int) or isinstance(ev["hora"], bool) or not 0 <= ev["hora"] <= 23:
        return "hora fuera de 0-23"
    if not es_numero(ev["energia_kwh"]):
        return "energía no numérica"
    if ev["energia_kwh"] < 0:
        return "energía negativa"
    if not es_numero(ev["sent_at_ms"]):
        return "sent_at_ms no numérico"
    return None


def lambda_handler(event, context):
    guardados = descartados = 0
    for rec in event["Records"]:
        sqs_ts_ms = int(rec["attributes"]["SentTimestamp"])  # llegada a SQS
        try:
            ev = json.loads(rec["body"])
        except json.JSONDecodeError:
            ev, motivo = None, "JSON inválido"
        else:
            motivo = validar(ev)

        event_id = ev.get("event_id") if isinstance(ev, dict) and ev.get("event_id") else "sin-id-" + rec["messageId"]
        guardado_ms = int(time.time() * 1000)

        if motivo is None:
            iot_ts_ms = int(ev.get("iot_ts_ms") or sqs_ts_ms)
            # Clave determinística: si SQS reentrega el mensaje, se sobrescribe el mismo ítem (idempotente).
            tabla_lecturas.put_item(Item={
                "medidor": ev["medidor"],
                "lectura": f'{ev["fecha"]}#{ev["hora"]:02d}#{event_id}',
                "event_id": event_id,
                "fecha": ev["fecha"],
                "hora": ev["hora"],
                "energia_kwh": Decimal(str(ev["energia_kwh"])),
                "sent_at_ms": int(ev["sent_at_ms"]),
                "iot_ts_ms": iot_ts_ms,
                "sqs_ts_ms": sqs_ts_ms,
                "guardado_ms": guardado_ms,
                "latencia_total_ms": guardado_ms - int(ev["sent_at_ms"]),  # envío -> DynamoDB (reloj PC vs AWS)
                "latencia_aws_ms": guardado_ms - iot_ts_ms,                # IoT Core -> DynamoDB (mismo reloj AWS)
            })
            guardados += 1
        else:
            tabla_descartes.put_item(Item={
                "event_id": event_id,
                "motivo": motivo,
                "payload": rec["body"][:1000],
                "sqs_ts_ms": sqs_ts_ms,
                "guardado_ms": guardado_ms,
            })
            descartados += 1

    print(json.dumps({"lote": len(event["Records"]), "guardados": guardados, "descartados": descartados}))
    return {"guardados": guardados, "descartados": descartados}
