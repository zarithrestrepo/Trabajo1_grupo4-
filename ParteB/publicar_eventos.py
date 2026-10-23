"""
Publica N eventos en IoT Core (API iot-data Publish) en el tema subestaciones/lecturas.

Los eventos se construyen con lecturas reales tomadas al azar del CSV del proyecto.
Un porcentaje (por defecto 3 %) se daña a propósito para probar los descartes de la Lambda.
Deja un registro en publicados.csv (event_id, tipo, sent_at_ms, estado) que usa conteos_y_retraso.py.

Uso (terminal de VS Code en Windows, con las credenciales de AWS ya configuradas):
  pip install boto3
  python publicar_eventos.py --csv "C:\\Users\\Zarith\\Downloads\\consumo_sintetico_subestaciones.csv"
"""
import argparse
import csv
import json
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3

DANOS = {
    "sin_energia":        lambda ev: ev.pop("energia_kwh"),
    "energia_texto":      lambda ev: ev.update(energia_kwh="N/A"),
    "energia_negativa":   lambda ev: ev.update(energia_kwh=-abs(ev["energia_kwh"]) - 1),
    "medidor_invalido":   lambda ev: ev.update(medidor="SUB-XX"),
    "hora_fuera_rango":   lambda ev: ev.update(hora=25),
    "fecha_invalida":     lambda ev: ev.update(fecha="2025-02-30"),
}


def muestrear(csv_path, n, rng):
    """Reservoir sampling de n filas del CSV."""
    muestra = []
    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        r = csv.reader(fh)
        next(r)
        for i, fila in enumerate(r):
            if i < n:
                muestra.append(fila)
            else:
                j = rng.randint(0, i)
                if j < n:
                    muestra[j] = fila
    return muestra


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--n", type=int, default=10000)
    p.add_argument("--topic", default="subestaciones/lecturas")
    p.add_argument("--region", default="us-east-2")
    p.add_argument("--pct-malos", type=float, default=0.03)
    p.add_argument("--hilos", type=int, default=16)
    p.add_argument("--semilla", type=int, default=262)
    p.add_argument("--salida", default="publicados.csv")
    a = p.parse_args()

    rng = random.Random(a.semilla)
    endpoint = boto3.client("iot", region_name=a.region).describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    iot = boto3.client("iot-data", region_name=a.region, endpoint_url=f"https://{endpoint}")

    print(f"Muestreando {a.n} lecturas de {a.csv} ...")
    eventos = []
    for medidor, fecha, hora, energia in muestrear(a.csv, a.n, rng):
        ev = {"event_id": str(uuid.uuid4()), "medidor": medidor, "fecha": fecha,
              "hora": int(hora), "energia_kwh": float(energia)}
        tipo = "valido"
        if rng.random() < a.pct_malos:
            tipo = rng.choice(list(DANOS))
            DANOS[tipo](ev)
        eventos.append((tipo, ev))

    registro, lock, hechos = [], threading.Lock(), [0]

    def publicar(item):
        tipo, ev = item
        ev["sent_at_ms"] = int(time.time() * 1000)
        try:
            iot.publish(topic=a.topic, qos=1, payload=json.dumps(ev))
            estado = "ok"
        except Exception as e:  # se registra y no cuenta como publicado
            estado = f"error: {e}"
        with lock:
            registro.append((ev["event_id"], tipo, ev["sent_at_ms"], estado))
            hechos[0] += 1
            if hechos[0] % 1000 == 0:
                print(f"  {hechos[0]} / {len(eventos)}")

    t0 = time.time()
    with ThreadPoolExecutor(a.hilos) as pool:
        list(pool.map(publicar, eventos))
    dur = time.time() - t0

    with open(a.salida, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["event_id", "tipo", "sent_at_ms", "estado"])
        w.writerows(registro)

    ok = sum(1 for r in registro if r[3] == "ok")
    malos = sum(1 for r in registro if r[3] == "ok" and r[1] != "valido")
    print(f"\nPublicados OK: {ok}  (válidos {ok - malos}, dañados a propósito {malos})")
    print(f"Errores de publicación: {len(registro) - ok}")
    print(f"Duración: {dur:.1f} s  ({ok / dur:.0f} eventos/s)  -> registro en {a.salida}")


if __name__ == "__main__":
    main()
