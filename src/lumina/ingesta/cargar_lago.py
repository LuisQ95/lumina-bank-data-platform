"""Ingesta hacia Google Cloud Storage (Landing/Bronze en formato Apache Parquet) y Cloud Pub/Sub (4 Tópicos Streaming)
Caso: DataBank Metropolitan -> Lumina Bank (Escala Real: 9M Clientes + 6.36M PaySim)
"""

import json
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import pubsub_v1, storage

from lumina.config import (
    BUCKET_BRONZE,
    BUCKET_LANDING,
    COORDS_DISTRITOS,
    DATA_DOWNLOAD_DIR,
    DATA_GENERATE_DIR,
    DISTRITOS,
    ENTIDADES,
    PROJECT_ID,
    TOPICS,
)

sys.stdout.reconfigure(encoding="utf-8")


def asegurar_dimensiones_maestras_parquet():
    """Genera los catálogos maestros dim_entidad (12) y dim_distrito (8) en Parquet ZSTD si no existen."""
    os.makedirs(DATA_GENERATE_DIR, exist_ok=True)
    ruta_ent = os.path.join(DATA_GENERATE_DIR, "dim_entidad_12.parquet")
    ruta_dis = os.path.join(DATA_GENERATE_DIR, "dim_distrito_8.parquet")

    tipos_core = ["Core_COBOL_DB2", "Core_Oracle_Flexcube", "Core_Temenos_T24", "Core_Cloud_Nativo"]
    segmentos = ["Banca Universal", "Banca Retail", "Banca Corporativa", "Microfinanzas Digital"]
    ent_rows = []
    for idx, ent in enumerate(ENTIDADES, start=1):
        ent_rows.append({
            "id_entidad": ent,
            "nombre_comercial": f"Lumina Federación {ent.replace('_', ' ')}",
            "sistema_core_origen": tipos_core[(idx - 1) % len(tipos_core)],
            "segmento_principal": segmentos[(idx - 1) % len(segmentos)],
            "clientes_activos_estimados": 750000,
            "sla_autorizacion_ms": 100,
            "moneda_base": "PEN",
            "estado_federacion": "ACTIVO_CDC"
        })
    pq.write_table(pa.Table.from_pylist(ent_rows), ruta_ent, compression="zstd")

    zonas = {
        "San Isidro": ("Zona Financiera Empresarial", "NSE_A", 65),
        "Miraflores": ("Zona Turística y Comercial", "NSE_A", 60),
        "San Borja": ("Zona Residencial Corporativa", "NSE_A_B", 48),
        "Santiago de Surco": ("Zona Residencial Extensa", "NSE_A_B", 58),
        "La Molina": ("Zona Residencial Este", "NSE_A_B", 42),
        "San Miguel": ("Zona Comercial Oeste", "NSE_B", 46),
        "Magdalena del Mar": ("Zona Costera Residencial", "NSE_B", 40),
        "Lince": ("Zona Céntrica Conectada", "NSE_B", 41),
    }
    dis_rows = []
    for d in DISTRITOS:
        lat, lon = COORDS_DISTRITOS[d]
        z_desc, nse, n_atms = zonas[d]
        dis_rows.append({
            "id_distrito": d,
            "ciudad": "Lima Metropolitana",
            "perfil_urbano": z_desc,
            "nivel_socioeconomico": nse,
            "latitud_centro": lat,
            "longitud_centro": lon,
            "radio_meta_cobertura_km": 8.0,
            "atms_instalados_meta": n_atms
        })
    pq.write_table(pa.Table.from_pylist(dis_rows), ruta_dis, compression="zstd")
    return ruta_ent, ruta_dis


def subir_archivos_a_cloud_storage():
    print("======================================================================")
    print(" 1. CARGANDO ARCHIVOS PARQUET A CLOUD STORAGE (LANDING & BRONZE)")
    print("======================================================================")
    asegurar_dimensiones_maestras_parquet()
    storage_client = storage.Client(project=PROJECT_ID)
    b_landing = storage_client.bucket(BUCKET_LANDING)
    b_bronze = storage_client.bucket(BUCKET_BRONZE)

    # Limpiar objetos CSV antiguos para no duplicar espacio
    for bucket in [b_landing, b_bronze]:
        for blob in storage_client.list_blobs(bucket):
            if blob.name.endswith(".csv"):
                blob.delete()

    archivos_landing = [
        (os.path.join(DATA_GENERATE_DIR, "cierres_contables_12_entidades.parquet"), "cierres/cierres_contables_12_entidades.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "cierres_contables_12_entidades_MANIFEST.json"), "cierres/_MANIFEST.json"),
        (os.path.join(DATA_GENERATE_DIR, "calendario_eventos_festivos_2026.parquet"), "calendario/calendario_eventos_festivos_2026.parquet"),
    ]
    for ruta_local, ruta_gcs in archivos_landing:
        if os.path.exists(ruta_local):
            blob = b_landing.blob(ruta_gcs)
            blob.upload_from_filename(ruta_local, timeout=300)
            sz_mb = os.path.getsize(ruta_local) / (1024**2)
            print(f"  [GCS Landing] Subido -> gs://{BUCKET_LANDING}/{ruta_gcs} ({sz_mb:.2f} MB)")

    archivos_bronze = [
        (os.path.join(DATA_DOWNLOAD_DIR, "paysim_kaggle_dataset.parquet"), "paysim/anio=2026/paysim_kaggle_dataset.parquet"),
        (os.path.join(DATA_DOWNLOAD_DIR, "sec_quarterly_financials_download.parquet"), "externos/sec/sec_quarterly_financials.parquet"),
        (os.path.join(DATA_DOWNLOAD_DIR, "google_trends_top_terms_download.parquet"), "externos/google_trends/google_trends_top_terms.parquet"),
        (os.path.join(DATA_DOWNLOAD_DIR, "edi_market_reference_download.parquet"), "externos/edi/edi_market_reference.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "core_clientes_12_entidades.parquet"), "cdc_cores/core_clientes_12_entidades.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "clickstream_app_web.parquet"), "streaming_sink/clicks/clickstream_app_web.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "telemetria_atms_iot.parquet"), "streaming_sink/atm/telemetria_atms_iot.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "dim_atm_catalogo.parquet"), "streaming_sink/atm/dim_atm_catalogo.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "geolocalizacion_clientes_gps.parquet"), "streaming_sink/geo/geolocalizacion_clientes_gps.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "transacciones_paysim_enriquecidas.parquet"), "streaming_sink/tx/transacciones_paysim_enriquecidas.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "dim_entidad_12.parquet"), "dimensiones/dim_entidad_12.parquet"),
        (os.path.join(DATA_GENERATE_DIR, "dim_distrito_8.parquet"), "dimensiones/dim_distrito_8.parquet"),
    ]
    for ruta_local, ruta_gcs in archivos_bronze:
        if os.path.exists(ruta_local):
            blob = b_bronze.blob(ruta_gcs)
            blob.upload_from_filename(ruta_local, timeout=600)
            sz_mb = os.path.getsize(ruta_local) / (1024**2)
            print(f"  [GCS Bronze]  Subido -> gs://{BUCKET_BRONZE}/{ruta_gcs} ({sz_mb:.2f} MB)")


def publicar_streaming_4_topicos(limite_eventos=50):
    print("\n======================================================================")
    print(" 2. PUBLICANDO EVENTOS EN VIVO A LOS 4 TÓPICOS DE CLOUD PUB/SUB")
    print("======================================================================")

    tx_file = os.path.join(DATA_GENERATE_DIR, "streaming_sample_tx.json")
    clicks_file = os.path.join(DATA_GENERATE_DIR, "streaming_sample_clicks.json")
    atm_file = os.path.join(DATA_GENERATE_DIR, "streaming_sample_atm.json")
    geo_file = os.path.join(DATA_GENERATE_DIR, "streaming_sample_geo.json")

    with open(tx_file, encoding="utf-8") as f:
        txs = json.load(f)
    with open(clicks_file, encoding="utf-8") as f:
        clicks = json.load(f)
    with open(atm_file, encoding="utf-8") as f:
        atms = json.load(f)
    with open(geo_file, encoding="utf-8") as f:
        geos = json.load(f)

    publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)

    t_tx = publisher.topic_path(PROJECT_ID, TOPICS["tx"])
    t_clicks = publisher.topic_path(PROJECT_ID, TOPICS["clicks"])
    t_atm = publisher.topic_path(PROJECT_ID, TOPICS["atm"])
    t_geo = publisher.topic_path(PROJECT_ID, TOPICS["geo"])

    futures = []
    start_t = time.time()

    for tx in txs[:limite_eventos]:
        futures.append(publisher.publish(
            t_tx, json.dumps(tx).encode("utf-8"),
            ordering_key=tx["id_cliente"], distrito=tx["id_distrito"], canal=tx["canal"]
        ))
    print(f"  [Pub/Sub '{TOPICS['tx']}'] Publicados {limite_eventos} eventos de pago (ordering_key=id_cliente)")

    for ck in clicks[:limite_eventos]:
        futures.append(publisher.publish(
            t_clicks, json.dumps(ck).encode("utf-8"),
            ordering_key=ck["id_cliente"], pantalla=ck["pantalla_visitada"]
        ))
    print(f"  [Pub/Sub '{TOPICS['clicks']}']        Publicados {limite_eventos} eventos de Clickstream App/Web")

    for atm in atms[:min(40, len(atms))]:
        futures.append(publisher.publish(
            t_atm, json.dumps(atm).encode("utf-8"),
            ordering_key=atm["id_atm"], estado_salud=atm["estado_salud"]
        ))
    print(f"  [Pub/Sub '{TOPICS['atm']}']           Publicados {min(40, len(atms))} eventos de Telemetría IoT ATM")

    for g in geos[:limite_eventos]:
        futures.append(publisher.publish(
            t_geo, json.dumps(g).encode("utf-8"),
            ordering_key=g["id_cliente"], distrito=g["id_distrito_actual"]
        ))
    print(f"  [Pub/Sub '{TOPICS['geo']}']           Publicados {limite_eventos} eventos de Geolocalización GPS")

    for fut in futures:
        fut.result(timeout=15)

    elapsed = round(time.time() - start_t, 2)
    print(f"\n[ÉXITO] {len(futures)} mensajes confirmados en Cloud Pub/Sub en {elapsed}s (Costo: $0.00).")


if __name__ == "__main__":
    subir_archivos_a_cloud_storage()
    publicar_streaming_4_topicos(limite_eventos=50)
