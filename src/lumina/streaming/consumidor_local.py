"""
Pilar 2 y 3 (Entrega P2): procesamiento en tiempo real con Pub/Sub real y estado en memoria.

Qué es real y qué no:
  REAL      Cloud Pub/Sub (4 tópicos, suscripciones con entrega ordenada, StreamingPull continuo).
  REAL      Latencia medida: desde que el evento se publica hasta que queda decidido.
  REAL      Las decisiones se escriben en BigQuery (silver_banco.stream_decisiones) por microlotes.
  LOCAL     El consumidor corre en la máquina del equipo, no en Dataflow ni en Cloud Run.
  SIMULADO  BigtableLocalCollection y RedisLocalTTLStore son estructuras en memoria de Python
            que reproducen el DISEÑO de claves. No son Cloud Bigtable ni Memorystore y sus
            tiempos de lectura no representan a esos servicios.

Los eventos son una reproducción de filas de los Parquet del proyecto, más un tráfico de prueba
controlado: ráfagas de un mismo cliente y reintentos duplicados.

Uso:
    python -m lumina.streaming.consumidor_local --tx 3000 --tasa 50 --streams 8
"""

import argparse
import io
import json
import os
import random
import sys
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import AlreadyExists
from google.cloud import bigquery, pubsub_v1

from lumina.config import (
    DATA_DIR,
    DATA_GENERATE_DIR,
    PROJECT_ID,
    REGION,
    REPORTES_DIR,
    STREAM_SUBSCRIPTIONS,
    TOPICS,
)

sys.stdout.reconfigure(encoding="utf-8")

NOSQL_DIR = os.path.join(DATA_DIR, "nosql_state")
OUT_REPORTE = os.path.join(REPORTES_DIR, "reporte_streaming_p2.json")
TABLA_SINK = f"{PROJECT_ID}.silver_banco.stream_decisiones"
ENDPOINT = f"{REGION}-pubsub.googleapis.com:443"

UMBRAL_PCT_EFECTIVO = 20.0
UMBRAL_TEMPERATURA_C = 35.0
UMBRAL_RIESGO_REVISION = 0.65
VENTANA_1M_MS = 60_000
VENTANA_5M_MS = 300_000


class BigtableLocalCollection:
    """Simulación en memoria del diseño de Cloud Bigtable: RowKey id_cliente#reverse_ts."""

    def __init__(self):
        self.rows = {}
        self.index_by_client = defaultdict(list)

    def put_event(self, id_cliente, epoch_ms, cf_tx, cf_riesgo, cf_geo):
        row_key = f"{id_cliente}#{9999999999999 - int(epoch_ms):013d}"
        self.rows[row_key] = {"row_key": row_key, "id_cliente": id_cliente, "cf_tx": cf_tx, "cf_riesgo": cf_riesgo, "cf_geo": cf_geo}
        self.index_by_client[id_cliente].append(row_key)
        self.index_by_client[id_cliente].sort()
        return row_key

    def persist_snapshot(self):
        os.makedirs(NOSQL_DIR, exist_ok=True)
        ruta = os.path.join(NOSQL_DIR, "bigtable_perfil_cliente.json")
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump([self.rows[k] for k in sorted(self.rows)], f, ensure_ascii=False, indent=2)
        return ruta


class RedisLocalTTLStore:
    """Simulación en memoria del diseño de Memorystore: hashes con expiración por clave."""

    def __init__(self):
        self.store = {}
        self.expires_at = {}

    def hset_with_ttl(self, key, mapping, ttl_seconds=86400):
        self.store[key] = dict(mapping)
        self.expires_at[key] = time.time() + ttl_seconds

    def persist_snapshot(self):
        os.makedirs(NOSQL_DIR, exist_ok=True)
        ruta = os.path.join(NOSQL_DIR, "redis_cache_snapshot.json")
        datos = {k: {"fields": v, "ttl_restante_seg": max(0, int(self.expires_at[k] - time.time()))} for k, v in self.store.items()}
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(datos, f, ensure_ascii=False, indent=2)
        return ruta


def asegurar_suscripciones():
    """Crea las suscripciones del flujo streaming con entrega ordenada (no se puede activar después)."""
    subscriber = pubsub_v1.SubscriberClient()
    for clave, sub_id in STREAM_SUBSCRIPTIONS.items():
        topic = f"projects/{PROJECT_ID}/topics/{TOPICS[clave]}"
        path = subscriber.subscription_path(PROJECT_ID, sub_id)
        try:
            subscriber.create_subscription(
                request={"name": path, "topic": topic, "enable_message_ordering": True, "ack_deadline_seconds": 30}
            )
            print(f"  [CREADA] {sub_id} (ordenada)")
        except AlreadyExists:
            pass
        info = subscriber.get_subscription(request={"subscription": path})
        if not info.enable_message_ordering:
            raise SystemExit(f"La suscripción {sub_id} existe sin entrega ordenada.")
        # Descarta backlog de corridas anteriores: sus marcas de tiempo alterarían la latencia medida.
        subscriber.seek(request={"subscription": path, "time": datetime.now(timezone.utc)})
    subscriber.close()


def preparar_eventos(n_tx, semilla=2026):
    """Toma filas reales de los Parquet: fraudes etiquetados, normales, GPS, ATM y clics."""
    rnd = random.Random(semilla)
    pf = pq.ParquetFile(os.path.join(DATA_GENERATE_DIR, "transacciones_paysim_enriquecidas.parquet"))
    cols = ["id_transaccion", "id_cliente", "id_entidad", "id_distrito", "id_atm", "canal", "type", "amount", "isFraud"]
    lote = pf.read_row_group(0, columns=cols).to_pandas()

    fraudes = lote[lote["isFraud"] == 1].head(max(30, n_tx // 50))
    normales = lote[lote["isFraud"] == 0].sample(n=n_tx - len(fraudes), random_state=semilla)
    tx = pd.concat([fraudes, normales]).sample(frac=1.0, random_state=semilla).to_dict("records")

    # Límites reales de cada cliente (dim core), leídos por lotes de fila con filtro.
    ids = pa.array(sorted({t["id_cliente"] for t in tx}))
    pfc = pq.ParquetFile(os.path.join(DATA_GENERATE_DIR, "core_clientes_12_entidades.parquet"))
    perfiles = {}
    for batch in pfc.iter_batches(batch_size=1_000_000, columns=["id_cliente", "id_distrito", "limite_diario_pen", "segmento"]):
        tabla = pa.Table.from_batches([batch])
        filtrado = tabla.filter(pc.is_in(tabla["id_cliente"], value_set=ids))
        for r in filtrado.to_pylist():
            perfiles[r["id_cliente"]] = r

    geo = [
        {"geo_id": f"GEO-{i:06d}", "id_cliente": c, "id_distrito_actual": perfiles[c]["id_distrito"]}
        for i, c in enumerate(sorted(perfiles))
    ]

    # Tráfico de prueba controlado: ráfagas (mismo cliente) y reintentos (mismo id de evento).
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


class ConsumidorEstado:
    """Lógica con estado compartida por los callbacks de StreamingPull."""

    def __init__(self, perfiles, run_id):
        self.lock = threading.Lock()
        self.perfiles = perfiles
        self.run_id = run_id
        self.vistos = set()
        self.gps = {}
        self.w1 = defaultdict(deque)
        self.w5 = defaultdict(deque)
        self.acumulado = defaultdict(float)
        self.bt = BigtableLocalCollection()
        self.redis = RedisLocalTTLStore()
        self.pendientes = []
        self.lat_e2e = []
        self.lat_proc = []
        self.conteos = Counter()
        self.alertas_atm = []
        self.ultimo_evento = time.time()
        self.fuera_de_orden = 0
        self.ultimo_seq = {}

    def tx(self, m):
        t0 = time.perf_counter()
        recibido = int(time.time() * 1000)
        ev = json.loads(m.data.decode("utf-8"))
        with self.lock:
            self.ultimo_evento = time.time()
            self.conteos["tx_recibidas"] += 1
            tid = ev["id_transaccion"]
            if tid in self.vistos:
                self.conteos["duplicados_filtrados"] += 1
                m.ack()
                return
            self.vistos.add(tid)

            cid = ev["id_cliente"]
            seq = ev["seq"]
            if seq < self.ultimo_seq.get(cid, -1):
                self.fuera_de_orden += 1
            self.ultimo_seq[cid] = seq

            ems = ev["epoch_ms"]
            for win, ancho in ((self.w1[cid], VENTANA_1M_MS), (self.w5[cid], VENTANA_5M_MS)):
                win.append(ems)
                while win and ems - win[0] > ancho:
                    win.popleft()
            c1, c5 = len(self.w1[cid]), len(self.w5[cid])

            monto = float(ev["amount"])
            self.acumulado[cid] += monto
            perfil = self.perfiles.get(cid, {})
            limite = float(perfil.get("limite_diario_pen", 0.0))
            gps = self.gps.get(cid)
            salto = 1 if gps is not None and gps != ev["id_distrito"] else 0
            riesgo = round(min(0.99, 0.08 * c1 + (0.45 if salto else 0.0) + (monto / 25000.0) * 0.4), 3)

            if riesgo >= UMBRAL_RIESGO_REVISION:
                decision = "REVISION_FRAUDE"
            elif limite and self.acumulado[cid] > limite:
                decision = "RECHAZADA_LIMITE"
            else:
                decision = "APROBADA"
            self.conteos[decision] += 1
            if salto:
                self.conteos["saltos_gps"] += 1
            if c1 >= 3:
                self.conteos["rafagas_vel_1m_3_o_mas"] += 1

            rk = self.bt.put_event(
                cid, ems,
                {"id_transaccion": tid, "amount": monto, "canal": ev["canal"], "type": ev["type"]},
                {"tx_count_1m": c1, "tx_count_5m": c5, "alerta_salto_geo": salto, "risk_score": riesgo},
                {"distrito_tx": ev["id_distrito"], "distrito_gps": gps},
            )
            self.redis.hset_with_ttl(f"limites:{cid}", {
                "limite_diario_pen": limite, "acumulado_dia_pen": round(self.acumulado[cid], 2),
                "vel_1m": c1, "vel_5m": c5, "ultimo_rowkey_bigtable": rk,
            })

            proc_ms = (time.perf_counter() - t0) * 1000.0
            e2e_ms = int(time.time() * 1000) - ems
            self.lat_proc.append(proc_ms)
            self.lat_e2e.append(e2e_ms)
            self.pendientes.append({
                "run_id": self.run_id, "id_transaccion": tid, "id_cliente": cid, "id_distrito": ev["id_distrito"],
                "canal": ev["canal"], "tipo_operacion": ev["type"], "monto_pen": monto,
                "vel_1m": c1, "vel_5m": c5, "salto_gps": salto, "riesgo": riesgo, "decision": decision,
                "es_fraude_etiqueta": int(ev["isFraud"]), "latencia_e2e_ms": e2e_ms, "latencia_proceso_ms": round(proc_ms, 3),
                "evento_ts": datetime.fromtimestamp(ems / 1000, tz=timezone.utc),
                "decidido_ts": datetime.fromtimestamp(recibido / 1000, tz=timezone.utc),
            })
        m.ack()

    def geo(self, m):
        ev = json.loads(m.data.decode("utf-8"))
        with self.lock:
            self.ultimo_evento = time.time()
            self.conteos["geo_recibidos"] += 1
            self.gps[ev["id_cliente"]] = ev["id_distrito_actual"]
        m.ack()

    def atm(self, m):
        ev = json.loads(m.data.decode("utf-8"))
        with self.lock:
            self.ultimo_evento = time.time()
            self.conteos["atm_recibidos"] += 1
            self.redis.hset_with_ttl(f"atm_health:{ev['id_atm']}", ev, ttl_seconds=3600)
            if (ev.get("pct_efectivo") or 100) < UMBRAL_PCT_EFECTIVO or (ev.get("temperatura_c") or 0) > UMBRAL_TEMPERATURA_C:
                self.alertas_atm.append(ev["id_atm"])
        m.ack()

    def clicks(self, m):
        ev = json.loads(m.data.decode("utf-8"))
        with self.lock:
            self.ultimo_evento = time.time()
            self.conteos["clicks_recibidos"] += 1
            self.conteos["clicks_requieren_asesor"] += int(ev.get("requiere_asesor_humano", 0))
        m.ack()


def escribir_microlote(bq, filas):
    """Carga por lote (load job) a BigQuery: sin costo de ingesta."""
    if not filas:
        return 0
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(filas), buf, compression="zstd")
    buf.seek(0)
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="evento_ts"),
        clustering_fields=["id_distrito", "decision"],
    )
    bq.load_table_from_file(buf, TABLA_SINK, job_config=cfg).result()
    return len(filas)


def percentiles(v):
    a = np.array(v, dtype=float)
    return {
        "n": int(a.size), "min": round(float(a.min()), 2), "p50": round(float(np.percentile(a, 50)), 2),
        "p95": round(float(np.percentile(a, 95)), 2), "p99": round(float(np.percentile(a, 99)), 2), "max": round(float(a.max()), 2),
    }


def ejecutar_pilar2_y_3_streaming_nosql(n_tx=3000, tasa=50, streams=8):
    print("  Preparando eventos desde los Parquet...")
    asegurar_suscripciones()
    datos = preparar_eventos(n_tx)
    run_id = uuid.uuid4().hex[:12]
    estado = ConsumidorEstado(datos["perfiles"], run_id)
    bq = bigquery.Client(project=PROJECT_ID, location=REGION)

    # Suscriptor con endpoint global: con el endpoint regional, medido en este proyecto desde Lima,
    # StreamingPull tardó ~41 s (p50) frente a ~1.9 s con el global. El publicador sí queda regional
    # porque el orden por clave solo se garantiza si se publica en una misma región.
    # Cada stream entregó ~22 msg/s en las pruebas; se abren varios en paralelo para tx y geo.
    # Pub/Sub mantiene la afinidad por ordering_key entre streams, así que el orden se conserva.
    control = pubsub_v1.types.FlowControl(max_messages=2000)
    futuros, clientes = [], []
    plan = (("tx", estado.tx, streams), ("geo", estado.geo, streams), ("atm", estado.atm, 1), ("clicks", estado.clicks, 1))
    for clave, cb, n in plan:
        for _ in range(n):
            cliente = pubsub_v1.SubscriberClient()
            path = cliente.subscription_path(PROJECT_ID, STREAM_SUBSCRIPTIONS[clave])
            futuros.append(cliente.subscribe(path, callback=cb, flow_control=control))
            clientes.append(cliente)
    print(f"  StreamingPull activo: {len(futuros)} streams en 4 suscripciones (run_id={run_id})")
    time.sleep(10)

    filas_sink = {"n": 0, "microlotes": 0}
    detener = threading.Event()

    def sink():
        while not detener.is_set() or estado.pendientes:
            time.sleep(5)
            with estado.lock:
                lote, estado.pendientes = estado.pendientes, []
            if lote:
                filas_sink["n"] += escribir_microlote(bq, lote)
                filas_sink["microlotes"] += 1

    hilo_sink = threading.Thread(target=sink, daemon=True)
    hilo_sink.start()

    publisher = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True),
        client_options=ClientOptions(api_endpoint=ENDPOINT),
    )
    topic = {k: publisher.topic_path(PROJECT_ID, v) for k, v in TOPICS.items()}
    pub_futs = []

    # GPS primero: el cruce de flujos necesita la última posición del cliente.
    for ev in datos["geo"]:
        pub_futs.append(publisher.publish(topic["geo"], json.dumps(ev).encode(), ordering_key=ev["id_cliente"]))
    for ev in datos["atm"]:
        pub_futs.append(publisher.publish(topic["atm"], json.dumps(ev, default=float).encode(), ordering_key=ev["id_atm"]))
    for ev in datos["clicks"]:
        pub_futs.append(publisher.publish(topic["clicks"], json.dumps(ev).encode(), ordering_key=ev["id_cliente"]))
    for f in pub_futs:
        f.result(timeout=60)
    # Espera a que el estado GPS esté cargado antes de iniciar las transacciones.
    t_geo = time.time()
    while estado.conteos["geo_recibidos"] < len(datos["geo"]) and time.time() - t_geo < 120:
        time.sleep(1)
    print(f"  Publicados {len(pub_futs)} eventos GPS/ATM/clics; GPS en estado: {estado.conteos['geo_recibidos']}. Inicia flujo a {tasa} tx/s")

    t_ini = time.time()
    intervalo = 1.0 / tasa
    tx_futs = []
    for i, ev in enumerate(datos["tx"]):
        objetivo = t_ini + i * intervalo
        espera = objetivo - time.time()
        if espera > 0:
            time.sleep(espera)
        msg = {k: (float(v) if isinstance(v, (np.floating,)) else int(v) if isinstance(v, (np.integer,)) else v) for k, v in ev.items()}
        msg["seq"] = i
        msg["epoch_ms"] = int(time.time() * 1000)
        tx_futs.append(publisher.publish(topic["tx"], json.dumps(msg).encode(), ordering_key=msg["id_cliente"]))
    for f in tx_futs:
        f.result(timeout=60)
    seg_pub = round(time.time() - t_ini, 2)
    print(f"  {len(tx_futs)} transacciones publicadas en {seg_pub} s")

    # Espera el drenaje: termina cuando llegó todo o tras 30 s sin eventos (máximo 5 min).
    while estado.conteos["tx_recibidas"] < len(tx_futs) and time.time() - estado.ultimo_evento < 30:
        time.sleep(1)
        if time.time() - t_ini > seg_pub + 300:
            break

    for fut in futuros:
        fut.cancel()
    for fut in futuros:
        try:
            fut.result(timeout=15)
        except Exception:
            pass
    for cliente in clientes:
        cliente.close()
    detener.set()
    hilo_sink.join(timeout=60)

    ruta_bt = estado.bt.persist_snapshot()
    ruta_rd = estado.redis.persist_snapshot()

    c = estado.conteos
    reporte = {
        "ejecutado_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "suscripciones": STREAM_SUBSCRIPTIONS,
        "entrega_ordenada_activa": True,
        "tasa_objetivo_tx_s": tasa,
        "streams_paralelos_tx": streams,
        "tx_publicadas": len(tx_futs),
        "segundos_publicacion": seg_pub,
        "tasa_real_tx_s": round(len(tx_futs) / seg_pub, 1),
        "trafico_prueba": {"rafagas": datos["n_rafagas"], "reintentos_duplicados": datos["n_reintentos"], "fraudes_etiquetados_en_muestra": datos["n_fraudes_muestra"]},
        "conteos": dict(c),
        "tx_unicas_decididas": len(estado.lat_e2e),
        "fuera_de_orden_por_cliente": estado.fuera_de_orden,
        "alertas_atm": len(set(estado.alertas_atm)),
        "latencia_e2e_ms": percentiles(estado.lat_e2e),
        "latencia_proceso_ms": percentiles(estado.lat_proc),
        "sink_bigquery": {"tabla": TABLA_SINK, "filas": filas_sink["n"], "microlotes": filas_sink["microlotes"]},
        "nosql_simulado": {"filas_bigtable_local": len(estado.bt.rows), "claves_redis_local": len(estado.redis.store), "archivos": [ruta_bt, ruta_rd]},
        "nota_latencia": "E2E = reloj local al publicar hasta decisión tomada; incluye ida y vuelta Lima-us-central1 por internet. El consumidor es local.",
    }
    with open(OUT_REPORTE, "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps({k: reporte[k] for k in ("tx_publicadas", "tx_unicas_decididas", "conteos", "latencia_e2e_ms", "latencia_proceso_ms", "sink_bigquery", "fuera_de_orden_por_cliente", "alertas_atm")}, ensure_ascii=False, indent=1))
    return reporte


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tx", type=int, default=3000)
    ap.add_argument("--tasa", type=int, default=50)
    ap.add_argument("--streams", type=int, default=8)
    a = ap.parse_args()
    ejecutar_pilar2_y_3_streaming_nosql(a.tx, a.tasa, a.streams)
