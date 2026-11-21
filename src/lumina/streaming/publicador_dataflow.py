"""Prepara y alimenta con tráfico de carga el pipeline de Dataflow.

Uso:
    python -m lumina.streaming.publicador_dataflow --preparar           Crea suscripciones y sube los perfiles a GCS.
    python -m lumina.streaming.publicador_dataflow --tx 3000 --tasa 50  Publica GPS, ATM, clics y transacciones.

Los eventos salen de los Parquet del proyecto, con ráfagas de un mismo cliente y reintentos duplicados.
Cada corrida agrega un sufijo al id de transacción para que la deduplicación de Redis solo actúe sobre
los reintentos de esa misma corrida. Las transacciones individuales de la demo se envían desde la API.
"""

import argparse
import json
import os
import random
import sys
import time
import uuid

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1, storage

from lumina.config import BUCKET_BRONZE, DATA_GENERATE_DIR, DATAFLOW_SUBSCRIPTIONS, PROJECT_ID, REPORTES_DIR, TOPICS

sys.stdout.reconfigure(encoding="utf-8")

RUTA_PERFILES = "dataflow/perfiles_muestra.jsonl"


def preparar_eventos(n_tx, semilla=2026):
    """Arma el tráfico de carga a partir de los Parquet del proyecto.

    Toma fraudes etiquetados y transacciones normales, los límites diarios de esos clientes desde el
    core, su distrito registrado como posición GPS, lecturas de cajeros y clics. Agrega 20 ráfagas de
    tres transacciones del mismo cliente y un 2 % de reintentos con el mismo id.

    Args:
        n_tx: Transacciones base.
        semilla: Semilla para que el tráfico sea reproducible.

    Returns:
        Diccionario con las listas tx, geo, atm y clicks, los perfiles por cliente y los conteos de
        ráfagas, reintentos y fraudes de la muestra.
    """
    rnd = random.Random(semilla)
    pf = pq.ParquetFile(os.path.join(DATA_GENERATE_DIR, "transacciones_paysim_enriquecidas.parquet"))
    cols = ["id_transaccion", "id_cliente", "id_entidad", "id_distrito", "id_atm", "canal", "type", "amount", "isFraud"]
    lote = pf.read_row_group(0, columns=cols).to_pandas()

    fraudes = lote[lote["isFraud"] == 1].head(max(30, n_tx // 50))
    normales = lote[lote["isFraud"] == 0].sample(n=n_tx - len(fraudes), random_state=semilla)
    tx = pd.concat([fraudes, normales]).sample(frac=1.0, random_state=semilla).to_dict("records")

    ids = pa.array(sorted({t["id_cliente"] for t in tx}))
    pfc = pq.ParquetFile(os.path.join(DATA_GENERATE_DIR, "core_clientes_12_entidades.parquet"))
    perfiles = {}
    for batch in pfc.iter_batches(batch_size=1_000_000, columns=["id_cliente", "id_distrito", "limite_diario_pen", "segmento"]):
        tabla = pa.Table.from_batches([batch])
        for r in tabla.filter(pc.is_in(tabla["id_cliente"], value_set=ids)).to_pylist():
            perfiles[r["id_cliente"]] = r

    geo = [
        {"geo_id": f"GEO-{i:06d}", "id_cliente": c, "id_distrito_actual": perfiles[c]["id_distrito"]}
        for i, c in enumerate(sorted(perfiles))
    ]

    rafagas = []
    for t in rnd.sample(tx, 20):
        for j in range(3):
            copia = dict(t)
            copia["id_transaccion"] = f"{t['id_transaccion']}-R{j + 1}"
            rafagas.append(copia)
    posiciones = rnd.sample(range(len(tx)), len(rafagas))
    for pos, ev in zip(sorted(posiciones, reverse=True), rafagas, strict=True):
        tx.insert(pos, ev)
    reintentos = rnd.sample(tx, max(10, len(tx) // 50))
    for ev in reintentos:
        tx.insert(rnd.randrange(len(tx)), dict(ev))

    pfa = pq.ParquetFile(os.path.join(DATA_GENERATE_DIR, "telemetria_atms_iot.parquet"))
    atm_df = pfa.read_row_group(0, columns=["lectura_id", "id_atm", "id_distrito", "efectivo_disponible_pen", "temperatura_c"]).to_pandas().head(400)
    pf_cat = pq.read_table(os.path.join(DATA_GENERATE_DIR, "dim_atm_catalogo.parquet"), columns=["id_atm", "capacidad_max_pen"]).to_pandas()
    atm = atm_df.merge(pf_cat, on="id_atm", how="left")
    atm["pct_efectivo"] = (atm["efectivo_disponible_pen"] / atm["capacidad_max_pen"] * 100).round(1)
    atm_ev = atm[["lectura_id", "id_atm", "id_distrito", "pct_efectivo", "temperatura_c"]].to_dict("records")

    pfk = pq.ParquetFile(os.path.join(DATA_GENERATE_DIR, "clickstream_app_web.parquet"))
    clk = pfk.read_row_group(0, columns=["evento_click_id", "id_cliente", "pantalla_visitada", "requiere_asesor_humano"]).to_pandas().head(300)
    clk["requiere_asesor_humano"] = clk["requiere_asesor_humano"].astype(int)
    return {"tx": tx, "geo": geo, "atm": atm_ev, "clicks": clk.to_dict("records"), "perfiles": perfiles,
            "n_rafagas": len(rafagas), "n_reintentos": len(reintentos), "n_fraudes_muestra": int(len(fraudes))}


def nativo(v):
    """Convierte escalares de NumPy a tipos de Python para serializarlos en JSON."""
    if isinstance(v, np.floating):
        return float(v)
    if isinstance(v, np.integer):
        return int(v)
    return v


def preparar(n_tx):
    """Crea las suscripciones *-sub-dataflow y sube a Cloud Storage los perfiles que lee Dataflow.

    Args:
        n_tx: Transacciones de la muestra; define qué clientes tienen perfil.
    """
    subscriber = pubsub_v1.SubscriberClient()
    for clave, sub_id in DATAFLOW_SUBSCRIPTIONS.items():
        path = subscriber.subscription_path(PROJECT_ID, sub_id)
        try:
            subscriber.create_subscription(request={
                "name": path, "topic": f"projects/{PROJECT_ID}/topics/{TOPICS[clave]}", "ack_deadline_seconds": 60,
            })
            print(f"  [CREADA] {sub_id}")
        except AlreadyExists:
            print(f"  [YA EXISTE] {sub_id}")
    subscriber.close()

    datos = preparar_eventos(n_tx)
    lineas = [json.dumps({k: nativo(v) for k, v in p.items()}, ensure_ascii=False) for p in datos["perfiles"].values()]
    blob = storage.Client(project=PROJECT_ID).bucket(BUCKET_BRONZE).blob(RUTA_PERFILES)
    blob.upload_from_string("\n".join(lineas) + "\n", content_type="application/x-ndjson")
    print(f"  {len(lineas)} perfiles en gs://{BUCKET_BRONZE}/{RUTA_PERFILES}")


def publicar(n_tx, tasa):
    """Publica GPS, ATM y clics, espera 30 s y luego publica las transacciones a una tasa fija.

    El GPS va primero porque la regla de salto de distrito necesita la última posición del cliente en
    Redis. Guarda el resumen en reporte_publicador_dataflow.json.

    Args:
        n_tx: Transacciones base, sin contar ráfagas ni reintentos.
        tasa: Transacciones por segundo.
    """
    datos = preparar_eventos(n_tx)
    corrida = uuid.uuid4().hex[:6]
    publisher = pubsub_v1.PublisherClient()
    topic = {k: publisher.topic_path(PROJECT_ID, v) for k, v in TOPICS.items()}

    def enviar(clave, ev):
        """Publica un evento en el tópico de la clave y devuelve el future de Pub/Sub."""
        return publisher.publish(topic[clave], json.dumps({k: nativo(v) for k, v in ev.items()}).encode())

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
