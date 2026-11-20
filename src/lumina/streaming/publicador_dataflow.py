"""Prepara y alimenta el pipeline de Dataflow.

    python -m lumina.streaming.publicador_dataflow --preparar          Crea suscripciones y sube los perfiles a GCS.
    python -m lumina.streaming.publicador_dataflow --tx 3000 --tasa 50 Publica GPS, ATM, clics y transacciones.

Los eventos salen de los mismos Parquet sintéticos que usa el consumidor local, con ráfagas y
reintentos duplicados. Cada corrida agrega un sufijo al id de transacción para que la deduplicación
de Redis solo actúe sobre los reintentos de esa misma corrida.
"""

import argparse
import json
import os
import sys
import time
import uuid

import numpy as np
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1, storage

from lumina.config import BUCKET_BRONZE, PROJECT_ID, REPORTES_DIR, TOPICS
from lumina.streaming.consumidor_local import preparar_eventos

sys.stdout.reconfigure(encoding="utf-8")

RUTA_PERFILES = "dataflow/perfiles_muestra.jsonl"


def nativo(v):
    if isinstance(v, np.floating):
        return float(v)
    if isinstance(v, np.integer):
        return int(v)
    return v


def preparar(n_tx):
    subscriber = pubsub_v1.SubscriberClient()
    for topico in TOPICS.values():
        path = subscriber.subscription_path(PROJECT_ID, f"{topico}-sub-dataflow")
        try:
            subscriber.create_subscription(request={
                "name": path, "topic": f"projects/{PROJECT_ID}/topics/{topico}", "ack_deadline_seconds": 60,
            })
            print(f"  [CREADA] {topico}-sub-dataflow")
        except AlreadyExists:
            print(f"  [YA EXISTE] {topico}-sub-dataflow")
    subscriber.close()

    datos = preparar_eventos(n_tx)
    lineas = [json.dumps({k: nativo(v) for k, v in p.items()}, ensure_ascii=False) for p in datos["perfiles"].values()]
    blob = storage.Client(project=PROJECT_ID).bucket(BUCKET_BRONZE).blob(RUTA_PERFILES)
    blob.upload_from_string("\n".join(lineas) + "\n", content_type="application/x-ndjson")
    print(f"  {len(lineas)} perfiles en gs://{BUCKET_BRONZE}/{RUTA_PERFILES}")


def publicar(n_tx, tasa):
    datos = preparar_eventos(n_tx)
    corrida = uuid.uuid4().hex[:6]
    publisher = pubsub_v1.PublisherClient()
    topic = {k: publisher.topic_path(PROJECT_ID, v) for k, v in TOPICS.items()}

    def enviar(clave, ev):
        return publisher.publish(topic[clave], json.dumps({k: nativo(v) for k, v in ev.items()}).encode())

    # GPS primero: el cruce de flujos necesita la última posición del cliente.
    futuros = [enviar(c, ev) for c in ("geo", "atm", "clicks") for ev in datos[c]]
    for f in futuros:
        f.result(timeout=60)
    print(f"  {len(futuros)} eventos GPS/ATM/clics publicados. Espera de 30 s para que lleguen a Redis.")
    time.sleep(30)

    t_ini = time.time()
    futuros = []
    for i, ev in enumerate(datos["tx"]):
        espera = t_ini + i / tasa - time.time()
        if espera > 0:
            time.sleep(espera)
        msg = dict(ev)
        msg["id_transaccion"] = f"{ev['id_transaccion']}:{corrida}"
        msg["seq"] = i
        msg["epoch_ms"] = int(time.time() * 1000)
        futuros.append(enviar("tx", msg))
    for f in futuros:
        f.result(timeout=60)
    segundos = round(time.time() - t_ini, 2)

    resumen = {"corrida": corrida, "tx_publicadas": len(futuros), "segundos": segundos, "tasa_real_tx_s": round(len(futuros) / segundos, 1),
               "rafagas": datos["n_rafagas"], "reintentos_duplicados": datos["n_reintentos"]}
    print(json.dumps(resumen, ensure_ascii=False))
    with open(os.path.join(REPORTES_DIR, "reporte_publicador_dataflow.json"), "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--preparar", action="store_true")
    ap.add_argument("--tx", type=int, default=3000)
    ap.add_argument("--tasa", type=int, default=50)
    a = ap.parse_args()
    if a.preparar:
        preparar(a.tx)
    else:
        publicar(a.tx, a.tasa)
