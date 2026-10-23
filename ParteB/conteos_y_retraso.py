"""
Cuadra los conteos y mide el retraso:
  publicados = guardados en DynamoDB + descartados (con motivo)
  retraso envío -> DynamoDB: promedio, p50, p95 y máximo

Uso (después de que la cola SQS quede vacía):
  python conteos_y_retraso.py
"""
import argparse
import collections
import csv
import statistics

import boto3


def escanear(tabla):
    items, kw = [], {}
    while True:
        r = tabla.scan(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            return items
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def resumen(nombre, valores):
    v = sorted(float(x) for x in valores)
    if not v:
        print(f"  {nombre}: sin datos")
        return
    print(f"  {nombre:<32} promedio {statistics.mean(v):8.0f} ms | p50 {v[len(v) // 2]:7.0f} ms"
          f" | p95 {v[int(len(v) * 0.95)]:7.0f} ms | máximo {v[-1]:7.0f} ms")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--region", default="us-east-2")
    p.add_argument("--publicados", default="publicados.csv")
    p.add_argument("--tabla-lecturas", default="subestaciones_lecturas")
    p.add_argument("--tabla-descartes", default="subestaciones_descartes")
    a = p.parse_args()

    with open(a.publicados, encoding="utf-8") as fh:
        pub = {r["event_id"]: r for r in csv.DictReader(fh) if r["estado"] == "ok"}

    ddb = boto3.resource("dynamodb", region_name=a.region)
    guardados = escanear(ddb.Table(a.tabla_lecturas))
    descartes = escanear(ddb.Table(a.tabla_descartes))

    ids_g = {i["event_id"] for i in guardados}
    ids_d = {i["event_id"] for i in descartes}
    faltan = set(pub) - ids_g - ids_d

    print("=" * 60)
    print(f"Eventos publicados (OK)          : {len(pub):>6}")
    print(f"Guardados en DynamoDB            : {len(ids_g):>6}")
    print(f"Descartados (con motivo)         : {len(ids_d):>6}")
    print(f"Guardados + descartados          : {len(ids_g) + len(ids_d):>6}")
    print(f"¿Cuadra?                         : {'SÍ' if len(pub) == len(ids_g) + len(ids_d) and not faltan else 'NO'}")
    if faltan:
        print(f"Sin llegar todavía / perdidos    : {len(faltan)}  (esperar a que la cola SQS quede vacía y repetir)")

    print("\nDescartes por motivo:")
    for motivo, c in collections.Counter(i["motivo"] for i in descartes).most_common():
        print(f"  {c:>5}  {motivo}")

    # Verificación cruzada: lo que el publicador dañó a propósito debe coincidir con lo descartado
    danados = {k for k, r in pub.items() if r["tipo"] != "valido"}
    print(f"\nDañados a propósito por el publicador: {len(danados)}  | coinciden con descartados: {len(danados & ids_d)}")

    print("\nRetraso (sobre los guardados):")
    resumen("envío (PC) -> DynamoDB", [i["latencia_total_ms"] for i in guardados])
    resumen("IoT Core -> DynamoDB (reloj AWS)", [i["latencia_aws_ms"] for i in guardados])
    print("=" * 60)


if __name__ == "__main__":
    main()
