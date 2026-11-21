"""Descarga y Generación Masiva a Escala Real de la Presentación (9 Millones de Clientes + 6.36M PaySim)
Caso: DataBank Metropolitan -> Lumina Bank

Volúmenes Exactos Generados (Formato Columnar Apache Parquet + CSV/JSON de control):
- En `data/data_download/`:
    1. paysim_kaggle_dataset.parquet            -> 6,362,620 filas (SINTÉTICO: esquema y volumen de PaySim, no es el archivo de Kaggle)
    2. sec_quarterly_financials_download.parquet-> 50,000 filas reales de bigquery-public-data.sec_quarterly_financials
    3. google_trends_top_terms_download.parquet -> 50,000 filas reales de bigquery-public-data.google_trends
    4. edi_market_reference_download.parquet    -> 10,950 filas (SINTÉTICO: serie estilo EDI con acciones, dividendos y FX)

- En `data/data_generate/`:
    5. core_clientes_12_entidades.parquet       -> 9,000,000 filas (Los 9 MILLONES de clientes activos en 12 bancos y 8 distritos)
    6. transacciones_paysim_enriquecidas.parquet-> 6,362,620 filas (PaySim cruzado con los 9M clientes, 12 entidades y 8 distritos)
    7. clickstream_app_web.parquet              -> 1,000,000 filas (Eventos App/Web: jóvenes digitales vs. adultos mayores)
    8. geolocalizacion_clientes_gps.parquet     -> 1,000,000 filas (Coordenadas GPS con 89% a < 8 km de un punto de atención)
    9. telemetria_atms_iot.parquet              -> 500,000 filas (Lecturas IoT cada 30s de la red de ATMs) + dim_atm.parquet
   10. cierres_contables_12_entidades.parquet   -> 140,160 filas (365 días x 12 bancos x 8 distritos x 4 productos) + _MANIFEST.json
   11. calendario_eventos_festivos_2026.parquet -> 1,826 días (5 años en línea de feriados, quincenas y fines de mes)
"""

import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

from lumina.config import (
    CANALES,
    COORDS_DISTRITOS,
    DATA_DOWNLOAD_DIR,
    DATA_GENERATE_DIR,
    DISTRITOS,
    ENTIDADES,
    PROJECT_ID,
)

sys.stdout.reconfigure(encoding="utf-8")
rng = np.random.default_rng(42)

os.makedirs(DATA_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(DATA_GENERATE_DIR, exist_ok=True)


def limpiar_archivos_antiguos():
    """Vacía data_download y data_generate para dejar solo la data a escala real."""
    for folder in [DATA_DOWNLOAD_DIR, DATA_GENERATE_DIR]:
        for fname in os.listdir(folder):
            fpath = os.path.join(folder, fname)
            if os.path.isfile(fpath):
                os.remove(fpath)


def descargar_sec_public_dataset(bq_client, n_limit=50000):
    """Descarga estados financieros reales de bancos desde el dataset público SEC de BigQuery.

    Args:
        bq_client: Cliente de BigQuery.
        n_limit: Máximo de filas.

    Returns:
        DataFrame con los registros de bancos y financieras (SIC 6021, 6022, 6029, 6035 y 6141).
    """
    print(f"\n[DESCARGA 1/4] Extrayendo {n_limit:,} registros reales de SEC Public Dataset (BigQuery)...")
    sql = f"""
    SELECT
        submission_number,
        company_name,
        sic,
        form,
        fiscal_year,
        CAST(period_end_date AS STRING) AS period_end_date,
        measure_tag,
        units,
        value
    FROM `bigquery-public-data.sec_quarterly_financials.quick_summary`
    WHERE sic IN ('6021', '6022', '6029', '6035', '6141')
      AND value IS NOT NULL
    LIMIT {n_limit}
    """
    df = bq_client.query(sql).to_dataframe()
    out_pq = os.path.join(DATA_DOWNLOAD_DIR, "sec_quarterly_financials_download.parquet")
    df.to_parquet(out_pq, index=False, compression="zstd")
    print(f"  -> [OK] {out_pq} ({len(df):,} registros reales SEC, {os.path.getsize(out_pq) / 1024:.1f} KB)")
    return df


def descargar_google_trends_dataset(bq_client, n_limit=50000):
    """Descarga los términos más buscados de las últimas dos semanas desde Google Trends público.

    El dataset es de Estados Unidos. La columna id_distrito es una asignación por rotación sin
    significado geográfico; el pipeline SQL la descarta y no usa Trends para proyectar demanda en Lima.

    Args:
        bq_client: Cliente de BigQuery.
        n_limit: Máximo de filas.

    Returns:
        DataFrame con fecha, mercado, término, puntaje y ranking.
    """
    print(f"\n[DESCARGA 2/4] Extrayendo {n_limit:,} registros reales de Google Trends (BigQuery)...")
    sql = f"""
    SELECT
        CAST(refresh_date AS STRING) AS refresh_date,
        dma_name,
        term,
        score,
        rank
    FROM `bigquery-public-data.google_trends.top_terms`
    WHERE refresh_date >= DATE_SUB(
        (SELECT MAX(refresh_date) FROM `bigquery-public-data.google_trends.top_terms`),
        INTERVAL 14 DAY
    )
    LIMIT {n_limit}
    """
    df = bq_client.query(sql).to_dataframe()
    df["id_distrito"] = [DISTRITOS[i % len(DISTRITOS)] for i in range(len(df))]
    out_pq = os.path.join(DATA_DOWNLOAD_DIR, "google_trends_top_terms_download.parquet")
    df.to_parquet(out_pq, index=False, compression="zstd")
    print(f"  -> [OK] {out_pq} ({len(df):,} tendencias reales Google Trends, {os.path.getsize(out_pq) / 1024:.1f} KB)")
    return df


def descargar_edi_market_reference():
    """Genera una serie sintética de 1,826 días estilo EDI con precios, dividendos, tipo de cambio y tasa BCRP.

    Returns:
        Lista de fechas de la serie, en formato YYYY-MM-DD.
    """
    print("\n[DESCARGA 3/4] Generando serie sintética de 5 años estilo EDI...")
    tickers = [
        ("BAP", "Credicorp Ltd.", "NYSE", 172.50),
        ("IFS", "Intercorp Financial Services", "NYSE", 24.80),
        ("BBVA", "Banco Bilbao Vizcaya Argentaria", "NYSE", 10.40),
        ("BNS", "Bank of Nova Scotia", "NYSE", 49.20),
        ("JPM", "JPMorgan Chase & Co.", "NYSE", 210.00),
        ("SPY", "SPDR S&P 500 ETF Trust", "NYSE", 580.10),
    ]
    n_days = 1826
    base_date = datetime(2021, 10, 26).date()
    fechas = [str(base_date + timedelta(days=d)) for d in range(n_days)]

    registros = []
    for d, f_str in enumerate(fechas):
        tc = round(3.72 + np.sin(d / 30.0) * 0.08, 4)
        tasa = round(5.25 + np.cos(d / 90.0) * 0.50, 2)
        for sym, emp, bolsa, p_base in tickers:
            p_cierre = round(p_base * (1.0 + np.sin(d / 45.0) * 0.06 + rng.uniform(-0.015, 0.015)), 2)
            registros.append({
                "fecha": f_str,
                "ticker": sym,
                "empresa": emp,
                "bolsa": bolsa,
                "precio_cierre_usd": float(p_cierre),
                "volumen_transado": int(rng.integers(150000, 4800000)),
                "dividendo_usd": round(p_base * 0.007, 3) if d % 90 == 0 else 0.0,
                "tipo_cambio_pen_usd": float(tc),
                "tasa_referencia_bcrp_pct": float(tasa)
            })
    df = pd.DataFrame(registros)
    out_pq = os.path.join(DATA_DOWNLOAD_DIR, "edi_market_reference_download.parquet")
    df.to_parquet(out_pq, index=False, compression="zstd")
    print(f"  -> [OK] {out_pq} ({len(df):,} cotizaciones bursátiles de 5 años, {os.path.getsize(out_pq) / 1024:.1f} KB)")
    return fechas


def generar_y_enriquecer_paysim_completo(total_tx=6362620, batch_size=1000000):
    """Genera las transacciones con esquema y volumen de PaySim y su versión enriquecida.

    Escribe por lotes dos tablas: la cruda con las 11 columnas de PaySim en data_download, y la
    enriquecida con cliente, entidad, distrito, canal y cajero en data_generate. Como en PaySim, la tasa
    de fraude es cercana a 0.13 % y el fraude ocurre solo en TRANSFER y CASH_OUT. Las fechas cubren los
    30 días desde el 2026-09-25 para el particionamiento diario.

    Args:
        total_tx: Total de transacciones.
        batch_size: Filas por lote de escritura.
    """
    print(f"\n[DESCARGA 4/4 & CRUCE MAESTRO] Construyendo las {total_tx:,} transacciones de PaySim y cruzándolas con los 9,000,000 de clientes...")
    t0 = time.time()

    out_raw_pq = os.path.join(DATA_DOWNLOAD_DIR, "paysim_kaggle_dataset.parquet")
    out_enr_pq = os.path.join(DATA_GENERATE_DIR, "transacciones_paysim_enriquecidas.parquet")

    tipos_arr = np.array(["PAYMENT", "CASH_OUT", "CASH_IN", "TRANSFER", "DEBIT"])
    pesos_tipos = [0.338, 0.352, 0.220, 0.083, 0.007]
    distritos_arr = np.array(DISTRITOS)
    entidades_arr = np.array(ENTIDADES)
    canales_arr = np.array(CANALES)
    atms_arr = np.array([f"ATM-{i:04d}" for i in range(1, 401)])

    base_ts = np.datetime64("2026-09-25T00:00:00")

    writer_raw = None
    writer_enr = None
    sample_json = []

    rows_written = 0
    total_fraudes = 0

    while rows_written < total_tx:
        n = min(batch_size, total_tx - rows_written)

        steps = rng.integers(1, 744, size=n, dtype=np.int32)
        is_fraud = (rng.random(n) < 0.0013).astype(np.int32)
        total_fraudes += int(is_fraud.sum())

        type_idx = rng.choice(5, size=n, p=pesos_tipos)
        fraud_mask = is_fraud == 1
        type_idx[fraud_mask] = rng.choice([1, 3], size=int(fraud_mask.sum()))
        types = tipos_arr[type_idx]

        amounts = np.round(rng.lognormal(mean=4.8, sigma=1.1, size=n), 2)
        amounts[fraud_mask] = np.round(rng.uniform(9500.0, 350000.0, size=int(fraud_mask.sum())), 2)

        old_org = np.round(rng.uniform(0.0, 45000.0, size=n), 2)
        old_org[fraud_mask] = amounts[fraud_mask]
        new_org = np.maximum(0.0, np.round(old_org - amounts, 2))
        new_org[fraud_mask] = 0.0

        old_dest = np.round(rng.uniform(0.0, 80000.0, size=n), 2)
        new_dest = np.round(old_dest + amounts, 2)
        is_flagged = ((is_fraud == 1) & (amounts > 200000.0)).astype(np.int32)

        cli_nums = rng.integers(1, 9000001, size=n, dtype=np.int64)
        dest_nums = rng.integers(1, 9000001, size=n, dtype=np.int64)

        name_orig = pa.array([f"C{x}" for x in cli_nums], type=pa.string())
        name_dest = pa.array([f"M{x}" for x in dest_nums], type=pa.string())

        tbl_raw = pa.table({
            "step": pa.array(steps, type=pa.int32()),
            "type": pa.array(types, type=pa.string()),
            "amount": pa.array(amounts, type=pa.float64()),
            "nameOrig": name_orig,
            "oldbalanceOrg": pa.array(old_org, type=pa.float64()),
            "newbalanceOrig": pa.array(new_org, type=pa.float64()),
            "nameDest": name_dest,
            "oldbalanceDest": pa.array(old_dest, type=pa.float64()),
            "newbalanceDest": pa.array(new_dest, type=pa.float64()),
            "isFraud": pa.array(is_fraud, type=pa.int32()),
            "isFlaggedFraud": pa.array(is_flagged, type=pa.int32()),
        })
        if writer_raw is None:
            writer_raw = pq.ParquetWriter(out_raw_pq, tbl_raw.schema, compression="zstd")
        writer_raw.write_table(tbl_raw)

        id_cliente_arr = pa.array([f"LUM-{x:07d}" for x in cli_nums], type=pa.string())
        id_tx_arr = pa.array([f"TX-{rows_written + i + 1:07d}" for i in range(n)], type=pa.string())
        dist_idx = (cli_nums % 8).astype(np.int32)
        dist_idx[fraud_mask] = ((dist_idx[fraud_mask] + 3) % 8).astype(np.int32)
        ent_idx = (cli_nums % 12).astype(np.int32)
        atm_idx = rng.integers(0, len(atms_arr), size=n, dtype=np.int32)
        canal_idx = rng.integers(0, len(canales_arr), size=n, dtype=np.int32)

        offset_secs = rng.integers(0, 30 * 86400, size=n, dtype=np.int64)
        ts_np = (base_ts + offset_secs * np.timedelta64(1, "s")).astype("datetime64[us]")
        fechas_np = ts_np.astype("datetime64[D]")

        tbl_enr = pa.table({
            "id_transaccion": id_tx_arr,
            "step": pa.array(steps, type=pa.int32()),
            "type": pa.array(types, type=pa.string()),
            "amount": pa.array(amounts, type=pa.float64()),
            "oldbalanceOrg": pa.array(old_org, type=pa.float64()),
            "newbalanceOrig": pa.array(new_org, type=pa.float64()),
            "isFraud": pa.array(is_fraud, type=pa.int32()),
            "isFlaggedFraud": pa.array(is_flagged, type=pa.int32()),
            "id_cliente": id_cliente_arr,
            "id_entidad": pa.array(entidades_arr[ent_idx], type=pa.string()),
            "id_distrito": pa.array(distritos_arr[dist_idx], type=pa.string()),
            "id_atm": pa.array(atms_arr[atm_idx], type=pa.string()),
            "canal": pa.array(canales_arr[canal_idx], type=pa.string()),
            "fecha": pa.array(fechas_np),
            "timestamp": pa.array(ts_np, type=pa.timestamp("us", tz="UTC")),
        })
        if writer_enr is None:
            writer_enr = pq.ParquetWriter(out_enr_pq, tbl_enr.schema, compression="zstd")
        writer_enr.write_table(tbl_enr)

        if len(sample_json) == 0:
            for k in range(100):
                sample_json.append({
                    "id_transaccion": f"TX-{k + 1:07d}",
                    "id_cliente": f"LUM-{int(cli_nums[k]):07d}",
                    "id_entidad": str(entidades_arr[ent_idx[k]]),
                    "id_distrito": str(distritos_arr[dist_idx[k]]),
                    "id_atm": str(atms_arr[atm_idx[k]]),
                    "canal": str(canales_arr[canal_idx[k]]),
                    "type": str(types[k]),
                    "amount": float(amounts[k]),
                    "isFraud": int(is_fraud[k]),
                    "timestamp": str(ts_np[k]) + "Z"
                })

        rows_written += n
        print(f"     -> Procesadas {rows_written:,} / {total_tx:,} transacciones PaySim...")

    writer_raw.close()
    writer_enr.close()

    with open(os.path.join(DATA_GENERATE_DIR, "streaming_sample_tx.json"), "w", encoding="utf-8") as f:
        json.dump(sample_json, f, ensure_ascii=False, indent=2)

    el = round(time.time() - t0, 1)
    print(f"  -> [OK] {out_raw_pq} ({total_tx:,} filas, {os.path.getsize(out_raw_pq) / (1024**2):.1f} MB) en {el}s")
    print(f"  -> [OK] {out_enr_pq} ({total_tx:,} filas, {total_fraudes:,} fraudes, {os.path.getsize(out_enr_pq) / (1024**2):.1f} MB)")


def simular_9_millones_clientes(total_clientes=9000000, batch_size=1500000):
    """Genera el core de 9 millones de clientes activos de las 12 entidades en los 8 distritos.

    Args:
        total_clientes: Total de clientes.
        batch_size: Filas por lote de escritura.
    """
    print(f"\n[SIMULACIÓN 1/5] Generando los {total_clientes:,} (9 MILLONES) de clientes activos (12 Entidades, 8 Distritos)...")
    t0 = time.time()
    out_pq = os.path.join(DATA_GENERATE_DIR, "core_clientes_12_entidades.parquet")

    entidades_arr = np.array(ENTIDADES)
    distritos_arr = np.array(DISTRITOS)
    segmentos_arr = np.array(["RETAIL", "PREMIER", "PYME", "MICROCREDITO"])
    limites_arr = np.array([2500.0, 5000.0, 12000.0, 25000.0])

    writer = None
    rows_written = 0

    while rows_written < total_clientes:
        n = min(batch_size, total_clientes - rows_written)
        ids_num = np.arange(rows_written + 1, rows_written + n + 1, dtype=np.int64)

        id_cliente = pa.array([f"LUM-{x:07d}" for x in ids_num], type=pa.string())
        ult4 = (ids_num * 7919) % 10000
        dni_mascara = pa.array([f"****{x:04d}" for x in ult4], type=pa.string())

        ent_idx = (ids_num % 12).astype(np.int32)
        dist_idx = (ids_num % 8).astype(np.int32)
        edades = rng.integers(18, 85, size=n, dtype=np.int32)

        grupos = np.where(
            edades >= 62, "ADULTO_MAYOR_RESIDENCIAL",
            np.where(edades <= 35, "JOVEN_100_DIGITAL", "ADULTO_HIBRIDO")
        )
        canales_pref = np.where(
            edades >= 62, "ASESOR_REMOTO_VENTANILLA",
            np.where(edades <= 35, "APP_MOVIL", "WEB_Y_ATM")
        )
        seg_idx = rng.integers(0, 4, size=n, dtype=np.int32)
        scores = rng.integers(320, 850, size=n, dtype=np.int32)
        saldos = np.round(rng.lognormal(mean=7.2, sigma=1.0, size=n), 2)
        lim_idx = rng.integers(0, 4, size=n, dtype=np.int32)

        tbl = pa.table({
            "id_cliente": id_cliente,
            "dni_enmascarado": dni_mascara,
            "id_entidad": pa.array(entidades_arr[ent_idx], type=pa.string()),
            "id_distrito": pa.array(distritos_arr[dist_idx], type=pa.string()),
            "edad": pa.array(edades, type=pa.int32()),
            "grupo_demografico": pa.array(grupos, type=pa.string()),
            "preferencia_canal": pa.array(canales_pref, type=pa.string()),
            "segmento": pa.array(segmentos_arr[seg_idx], type=pa.string()),
            "score_crediticio": pa.array(scores, type=pa.int32()),
            "saldo_disponible_pen": pa.array(saldos, type=pa.float64()),
            "limite_diario_pen": pa.array(limites_arr[lim_idx], type=pa.float64()),
        })

        if writer is None:
            writer = pq.ParquetWriter(out_pq, tbl.schema, compression="zstd")
        writer.write_table(tbl)

        rows_written += n
        print(f"     -> Generados {rows_written:,} / {total_clientes:,} clientes...")

    writer.close()
    el = round(time.time() - t0, 1)
    print(f"  -> [OK] {out_pq} ({total_clientes:,} clientes, {os.path.getsize(out_pq) / (1024**2):.1f} MB) en {el}s")


def simular_clickstream_y_gps_masivo(n_clicks=1000000, n_gps=1000000):
    """Genera los eventos de la app y la web y los pings GPS, con muestras JSON para Pub/Sub.

    Los clientes de 62 años o más pasan más tiempo por pantalla y piden asesor con más frecuencia. Como
    indica el caso, el 89 % de los pings está a menos de 8 km de un punto de atención, y el 1.5 % es un
    salto a otro distrito.

    Args:
        n_clicks: Eventos de clickstream.
        n_gps: Pings GPS.
    """
    print(f"\n[SIMULACIÓN 2/5] Generando {n_clicks:,} eventos Clickstream y {n_gps:,} pings de Geolocalización GPS...")

    distritos_arr = np.array(DISTRITOS)
    entidades_arr = np.array(ENTIDADES)
    pantallas_arr = np.array(["LOGIN_BIOMETRICO", "DASHBOARD_SALDOS", "TRANSFERENCIA_QR", "SOLICITUD_MICROCREDITO", "BUSCAR_CAJERO_8KM", "AYUDA_ASESOR_GECX"])
    disp_arr = np.array(["ANDROID", "IOS", "WEB_DESKTOP"])
    base_ts = np.datetime64("2026-09-25T00:00:00")

    cli_nums = rng.integers(1, 9000001, size=n_clicks, dtype=np.int64)
    edades = rng.integers(18, 85, size=n_clicks, dtype=np.int32)
    es_mayor = edades >= 62
    pant_idx = rng.integers(0, len(pantallas_arr), size=n_clicks, dtype=np.int32)
    pant_idx[es_mayor] = rng.choice([1, 4, 5], size=int(es_mayor.sum()))
    duracion = np.where(es_mayor, rng.integers(90, 420, size=n_clicks), rng.integers(6, 75, size=n_clicks)).astype(np.int32)
    req_asesor = ((es_mayor & (duracion > 160)) | (pant_idx == 5)).astype(np.int32)
    ts_clicks = (base_ts + rng.integers(0, 30 * 86400, size=n_clicks) * np.timedelta64(1, "s")).astype("datetime64[us]")

    tbl_clicks = pa.table({
        "evento_click_id": pa.array([f"CLK-{i:07d}" for i in range(1, n_clicks + 1)], type=pa.string()),
        "id_cliente": pa.array([f"LUM-{x:07d}" for x in cli_nums], type=pa.string()),
        "id_distrito": pa.array(distritos_arr[cli_nums % 8], type=pa.string()),
        "id_entidad": pa.array(entidades_arr[cli_nums % 12], type=pa.string()),
        "edad_cliente": pa.array(edades, type=pa.int32()),
        "pantalla_visitada": pa.array(pantallas_arr[pant_idx], type=pa.string()),
        "dispositivo": pa.array(disp_arr[rng.integers(0, 3, size=n_clicks)], type=pa.string()),
        "tiempo_en_pantalla_seg": pa.array(duracion, type=pa.int32()),
        "requiere_asesor_humano": pa.array(req_asesor, type=pa.int32()),
        "timestamp": pa.array(ts_clicks, type=pa.timestamp("us", tz="UTC")),
    })
    out_clicks = os.path.join(DATA_GENERATE_DIR, "clickstream_app_web.parquet")
    pq.write_table(tbl_clicks, out_clicks, compression="zstd")
    print(f"  -> [OK] {out_clicks} ({n_clicks:,} filas, {os.path.getsize(out_clicks) / (1024**2):.1f} MB)")

    with open(os.path.join(DATA_GENERATE_DIR, "streaming_sample_clicks.json"), "w", encoding="utf-8") as f:
        json.dump(tbl_clicks.slice(0, 100).to_pylist(), f, default=str, ensure_ascii=False, indent=2)

    cli_g = rng.integers(1, 9000001, size=n_gps, dtype=np.int64)
    dist_hab_idx = (cli_g % 8).astype(np.int32)
    salto = (rng.random(n_gps) < 0.015).astype(np.int32)
    dist_act_idx = np.where(salto == 1, (dist_hab_idx + 2) % 8, dist_hab_idx)

    en_8km = (rng.random(n_gps) < 0.89).astype(np.int32)
    dist_km = np.where(en_8km == 1, np.round(rng.uniform(0.2, 7.9, size=n_gps), 2), np.round(rng.uniform(8.1, 14.8, size=n_gps), 2))
    atm_ids = pa.array([f"ATM-{(x % 400) + 1:04d}" for x in cli_g], type=pa.string())
    ts_gps = (base_ts + rng.integers(0, 30 * 86400, size=n_gps) * np.timedelta64(1, "s")).astype("datetime64[us]")

    tbl_gps = pa.table({
        "geo_id": pa.array([f"GEO-{i:07d}" for i in range(1, n_gps + 1)], type=pa.string()),
        "id_cliente": pa.array([f"LUM-{x:07d}" for x in cli_g], type=pa.string()),
        "id_distrito_habitual": pa.array(distritos_arr[dist_hab_idx], type=pa.string()),
        "id_distrito_actual": pa.array(distritos_arr[dist_act_idx], type=pa.string()),
        "id_atm_mas_cercano": atm_ids,
        "distancia_punto_atencion_km": pa.array(dist_km, type=pa.float64()),
        "dentro_cobertura_8km": pa.array(en_8km, type=pa.int32()),
        "salto_geografico_sospechoso": pa.array(salto, type=pa.int32()),
        "timestamp": pa.array(ts_gps, type=pa.timestamp("us", tz="UTC")),
    })
    out_gps = os.path.join(DATA_GENERATE_DIR, "geolocalizacion_clientes_gps.parquet")
    pq.write_table(tbl_gps, out_gps, compression="zstd")
    print(f"  -> [OK] {out_gps} ({n_gps:,} filas, {os.path.getsize(out_gps) / (1024**2):.1f} MB)")

    with open(os.path.join(DATA_GENERATE_DIR, "streaming_sample_geo.json"), "w", encoding="utf-8") as f:
        json.dump(tbl_gps.slice(0, 100).to_pylist(), f, default=str, ensure_ascii=False, indent=2)


def simular_red_atms_y_telemetria_iot(n_atms=400, n_lecturas=500000):
    """Genera el catálogo de cajeros y sus lecturas de telemetría IoT cada 30 segundos.

    Args:
        n_atms: Cajeros distribuidos en los 8 distritos.
        n_lecturas: Lecturas de efectivo, temperatura y latencia de red.
    """
    print(f"\n[SIMULACIÓN 3/5] Generando {n_atms} ATMs y {n_lecturas:,} lecturas de telemetría IoT cada 30s...")

    now_iso = datetime.now(timezone.utc)
    atms_dim = []
    for i in range(1, n_atms + 1):
        dist = DISTRITOS[i % len(DISTRITOS)]
        ent = ENTIDADES[i % len(ENTIDADES)]
        lat_b, lon_b = COORDS_DISTRITOS[dist]
        cap = 250000.0
        crit = (i % 10 == 0)
        efec = float(np.round(rng.uniform(9000.0, 26000.0) if crit else rng.uniform(45000.0, 240000.0), 2))
        temp = float(np.round(rng.uniform(38.5, 44.0) if (i % 14 == 0) else rng.uniform(21.0, 31.5), 1))
        lat_ms = int(rng.integers(190, 620) if (i % 16 == 0) else rng.integers(12, 55))
        salud = "ALERTA_PREDICTIVA_FALLA" if (temp > 38.0 or lat_ms > 200) else ("REQUIERE_ABASTECIMIENTO" if efec < 30000.0 else "OPTIMO")

        atms_dim.append({
            "id_atm": f"ATM-{i:04d}",
            "id_distrito": dist,
            "id_entidad_administradora": ent,
            "latitud": float(np.round(lat_b + rng.uniform(-0.015, 0.015), 6)),
            "longitud": float(np.round(lon_b + rng.uniform(-0.015, 0.015), 6)),
            "capacidad_max_pen": cap,
            "efectivo_disponible_pen": efec,
            "pct_efectivo": float(np.round((efec / cap) * 100, 1)),
            "temperatura_c": temp,
            "papel_recibos_pct": int(rng.integers(8, 100)),
            "latencia_red_ms": lat_ms,
            "estado_dispensador": "ATASCO_PREVENTIVO" if i % 19 == 0 else "OPERATIVO",
            "estado_salud": salud,
            "modelo_hardware": ["NCR_SelfServ84", "Diebold_Nixdorf_DN200", "GRG_H68N"][i % 3],
            "timestamp": now_iso
        })

    df_dim_atm = pd.DataFrame(atms_dim)
    out_dim_atm = os.path.join(DATA_GENERATE_DIR, "dim_atm_catalogo.parquet")
    df_dim_atm.to_parquet(out_dim_atm, index=False, compression="zstd")

    with open(os.path.join(DATA_GENERATE_DIR, "streaming_sample_atm.json"), "w", encoding="utf-8") as f:
        json.dump(atms_dim[:100], f, default=str, ensure_ascii=False, indent=2)

    atm_idx = rng.integers(0, n_atms, size=n_lecturas)
    atm_ids_arr = np.array([a["id_atm"] for a in atms_dim])[atm_idx]
    dist_arr = np.array([a["id_distrito"] for a in atms_dim])[atm_idx]
    ent_arr = np.array([a["id_entidad_administradora"] for a in atms_dim])[atm_idx]
    efec_arr = np.round(rng.uniform(8000.0, 245000.0, size=n_lecturas), 2)
    temp_arr = np.round(rng.normal(26.5, 4.5, size=n_lecturas), 1)
    lat_arr = rng.integers(10, 280, size=n_lecturas, dtype=np.int32)
    salud_arr = np.where(temp_arr > 38.0, "ALERTA_TEMPERATURA", np.where(efec_arr < 25000.0, "EFECTIVO_BAJO", "OPTIMO"))
    base_ts = np.datetime64("2026-10-01T00:00:00")
    ts_iot = (base_ts + np.arange(n_lecturas) * np.timedelta64(30, "s")).astype("datetime64[us]")

    tbl_iot = pa.table({
        "lectura_id": pa.array([f"IOT-{i:07d}" for i in range(1, n_lecturas + 1)], type=pa.string()),
        "id_atm": pa.array(atm_ids_arr, type=pa.string()),
        "id_distrito": pa.array(dist_arr, type=pa.string()),
        "id_entidad": pa.array(ent_arr, type=pa.string()),
        "efectivo_disponible_pen": pa.array(efec_arr, type=pa.float64()),
        "temperatura_c": pa.array(temp_arr, type=pa.float64()),
        "latencia_red_ms": pa.array(lat_arr, type=pa.int32()),
        "estado_salud": pa.array(salud_arr, type=pa.string()),
        "timestamp": pa.array(ts_iot, type=pa.timestamp("us", tz="UTC")),
    })
    out_iot = os.path.join(DATA_GENERATE_DIR, "telemetria_atms_iot.parquet")
    pq.write_table(tbl_iot, out_iot, compression="zstd")
    print(f"  -> [OK] {out_iot} ({n_lecturas:,} lecturas IoT + {n_atms} ATMs, {os.path.getsize(out_iot) / (1024**2):.1f} MB)")


def simular_cierres_contables_anuales_y_calendario():
    """Genera un año de cierres contables, su manifiesto SHA-256 y el calendario de cinco años.

    Los cierres son 365 días por 12 entidades, 8 distritos y 4 productos (140,160 filas). El calendario
    tiene 1,826 días con quincenas, fines de mes, feriados y su factor de liquidez.
    """
    print("\n[SIMULACIÓN 4/5 & 5/5] Generando 140,160 Cierres Contables Batch + _MANIFEST.json y 1,826 días de Calendario...")
    productos = ["TRANSFERENCIAS_INMEDIATAS", "MICROCREDITOS_DIGITALES", "TARJETAS_CREDITO", "AHORRO_RENTABLE"]
    base_date = datetime(2025, 10, 26).date()

    cierres = []
    for d in range(365):
        f_str = str(base_date + timedelta(days=d))
        for ent in ENTIDADES:
            for dist in DISTRITOS:
                for prod in productos:
                    tx_cnt = int(rng.integers(2500, 35000))
                    vol_pen = round(tx_cnt * float(rng.uniform(85.0, 240.0)), 2)
                    vol_usd = round(vol_pen / 3.74, 2)
                    ing_usd = round(vol_usd * float(rng.uniform(0.014, 0.025)), 2)
                    costo_usd = round(tx_cnt * float(rng.uniform(0.018, 0.029)), 2)
                    cierres.append({
                        "fecha_cierre": f_str,
                        "id_entidad": ent,
                        "id_distrito": dist,
                        "producto_financiero": prod,
                        "total_transacciones": tx_cnt,
                        "volumen_total_pen": vol_pen,
                        "volumen_total_usd": vol_usd,
                        "ingresos_producto_usd": ing_usd,
                        "costo_operativo_usd": costo_usd,
                        "costo_unitario_por_tx_usd": round(costo_usd / tx_cnt, 5),
                        "tiempo_promedio_aprobacion_ms": round(float(rng.uniform(35.0, 88.0)), 1),
                        "margen_operativo_pct": round(((ing_usd - costo_usd) / ing_usd) * 100, 2),
                        "estado_conciliacion": "CONCILIADO_OK"
                    })

    df_cierres = pd.DataFrame(cierres)
    out_cierres = os.path.join(DATA_GENERATE_DIR, "cierres_contables_12_entidades.parquet")
    df_cierres.to_parquet(out_cierres, index=False, compression="zstd")

    with open(out_cierres, "rb") as f:
        sha256_hash = hashlib.sha256(f.read()).hexdigest()

    manifest = {
        "archivo": "cierres_contables_12_entidades.parquet",
        "total_registros": len(df_cierres),
        "entidades_incluidas": 12,
        "distritos_incluidos": 8,
        "checksum_sha256": sha256_hash,
        "generado_en_utc": datetime.now(timezone.utc).isoformat()
    }
    with open(os.path.join(DATA_GENERATE_DIR, "cierres_contables_12_entidades_MANIFEST.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"  -> [OK] {out_cierres} ({len(df_cierres):,} registros, {os.path.getsize(out_cierres) / (1024**2):.1f} MB) + SHA-256")

    cal_base = datetime(2021, 10, 26).date()
    calendario = []
    for d in range(1826):
        fd = cal_base + timedelta(days=d)
        es_q = 1 if fd.day in (14, 15, 16) else 0
        es_fm = 1 if fd.day >= 28 or fd.day == 1 else 0
        es_fer = 1 if fd.weekday() == 6 or (fd.month in (7, 12) and fd.day in (28, 29, 25)) else 0
        fac = 2.5 if es_fm else (1.9 if es_q else (1.4 if es_fer else 1.0))
        calendario.append({
            "fecha": str(fd),
            "dia_mes": fd.day,
            "dia_semana": fd.strftime("%A"),
            "es_quincena": es_q,
            "es_fin_de_mes": es_fm,
            "es_feriado_o_evento": es_fer,
            "nombre_evento": "Pico Fin de Mes" if es_fm else ("Pago de Quincena" if es_q else ("Feriado / Feria" if es_fer else "Día Operativo Normal")),
            "factor_multiplicador_liquidez": fac
        })
    df_cal = pd.DataFrame(calendario)
    out_cal = os.path.join(DATA_GENERATE_DIR, "calendario_eventos_festivos_2026.parquet")
    df_cal.to_parquet(out_cal, index=False, compression="zstd")
    print(f"  -> [OK] {out_cal} ({len(df_cal):,} días de calendario de 5 años)")


def main():
    """Descarga las fuentes públicas y genera todas las fuentes sintéticas en orden."""
    print("======================================================================")
    print(" GENERACIÓN MASIVA A ESCALA DE LA PRESENTACIÓN (9M CLIENTES + 6.36M TX)")
    print("======================================================================")
    limpiar_archivos_antiguos()
    bq_client = bigquery.Client(project=PROJECT_ID)

    descargar_sec_public_dataset(bq_client, 50000)
    descargar_google_trends_dataset(bq_client, 50000)
    descargar_edi_market_reference()
    generar_y_enriquecer_paysim_completo(6362620, 1000000)

    simular_9_millones_clientes(9000000, 1500000)
    simular_clickstream_y_gps_masivo(1000000, 1000000)
    simular_red_atms_y_telemetria_iot(400, 500000)
    simular_cierres_contables_anuales_y_calendario()

    print("\n======================================================================")
    print(" [ÉXITO] MÁS DE 18.1 MILLONES DE REGISTROS GENERADOS EN PARQUET")
    print("======================================================================")


if __name__ == "__main__":
    main()
