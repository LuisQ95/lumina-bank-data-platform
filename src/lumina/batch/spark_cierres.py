"""
Pilar 1 (Entrega P2): Pipeline batch en Apache Spark (PySpark).

Qué hace:
  1. Compuerta SHA-256: descarga el lote y su _MANIFEST.json del bucket Landing y DETIENE el job si no coinciden.
  2. Limpia los cierres contables: nulos en llaves, volumen <= 0 y duplicados por llave de negocio.
  3. Cruza cierres con el tipo de cambio diario EDI y recalcula el volumen en USD.
  4. Cruza 6.36 M de transacciones con 9 M de clientes y resume por entidad y distrito.
  5. Guarda salidas en Parquet y las carga a BigQuery (silver_banco).

Ejecución (requiere Java 17 y pyspark 3.5):
    python -m lumina.batch.spark_cierres
    python -m lumina.batch.spark_cierres --sin-bigquery

No existe respaldo silencioso: si Spark no arranca, el script falla y lo informa. La sesión usa
master local[*]; para Dataproc se envía el mismo archivo y se cambia --master y las rutas por gs://.
"""

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone

from lumina.config import (
    BUCKET_LANDING,
    DATA_DIR,
    DATA_DOWNLOAD_DIR,
    DATA_GENERATE_DIR,
    PROJECT_ID,
    REGION,
    REPORTES_DIR,
)

sys.stdout.reconfigure(encoding="utf-8")

OUT_REPORTE = os.path.join(REPORTES_DIR, "reporte_spark_p2.json")
OUT_CIERRES = os.path.join(DATA_GENERATE_DIR, "silver_cierres_fx_spark.parquet")
OUT_RESUMEN = os.path.join(DATA_GENERATE_DIR, "silver_resumen_entidad_distrito.parquet")
OUT_TX_RESUMEN = os.path.join(DATA_GENERATE_DIR, "silver_tx_cliente_resumen_spark.parquet")


def compuerta_sha256(ruta_parquet, ruta_manifest):
    """Devuelve el detalle si el hash coincide. Si no coincide, detiene el pipeline."""
    with open(ruta_manifest, encoding="utf-8") as f:
        manifest = json.load(f)
    h = hashlib.sha256()
    with open(ruta_parquet, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    calculado = h.hexdigest()
    if calculado != manifest["checksum_sha256"]:
        raise SystemExit(
            f"COMPUERTA SHA-256 RECHAZADA: esperado {manifest['checksum_sha256']}, calculado {calculado}. Lote no admitido."
        )
    return {"archivo": manifest["archivo"], "registros_manifest": manifest["total_registros"], "sha256": calculado}


def descargar_lote_landing():
    """Trae el lote y su manifiesto desde el bucket Landing: la compuerta valida lo que llegó a GCS."""
    from google.cloud import storage

    destino = os.path.join(DATA_DIR, "landing_verificado")
    os.makedirs(destino, exist_ok=True)
    bucket = storage.Client(project=PROJECT_ID).bucket(BUCKET_LANDING)
    rutas = {}
    for nombre, objeto in (("parquet", "cierres/cierres_contables_12_entidades.parquet"), ("manifest", "cierres/_MANIFEST.json")):
        rutas[nombre] = os.path.join(destino, os.path.basename(objeto))
        bucket.blob(objeto).download_to_filename(rutas[nombre])
    return rutas["parquet"], rutas["manifest"]


def probar_compuerta_negativa(ruta_parquet, ruta_manifest):
    """Altera 1 byte en una copia y comprueba que la compuerta detiene el pipeline."""
    copia = ruta_parquet + ".alterado"
    with open(ruta_parquet, "rb") as f:
        datos = bytearray(f.read())
    datos[len(datos) // 2] ^= 0xFF
    with open(copia, "wb") as f:
        f.write(datos)
    try:
        compuerta_sha256(copia, ruta_manifest)
        return {"lote_alterado_bloqueado": False}
    except SystemExit as exc:
        return {"lote_alterado_bloqueado": True, "mensaje": str(exc)[:60]}
    finally:
        os.remove(copia)


def crear_sesion(master):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("LuminaBank_P2_Pilar1")
        .master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )


def transformar_cierres(spark, ruta_cierres, ruta_edi):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    crudo = spark.read.parquet(ruta_cierres)
    filas_entrada = crudo.count()

    llaves = ["fecha_cierre", "id_entidad", "id_distrito", "producto_financiero"]
    validos = crudo.filter(
        F.col("fecha_cierre").isNotNull()
        & F.col("id_entidad").isNotNull()
        & F.col("id_distrito").isNotNull()
        & (F.col("volumen_total_pen") > 0)
        & (F.col("total_transacciones") > 0)
    )
    filas_validas = validos.count()

    orden = Window.partitionBy(*llaves).orderBy(F.col("volumen_total_pen").desc())
    sin_duplicados = validos.withColumn("rn", F.row_number().over(orden)).filter("rn = 1").drop("rn")

    fx = (
        spark.read.parquet(ruta_edi)
        .filter(F.col("tipo_cambio_pen_usd") > 0)
        .groupBy(F.col("fecha").cast("string").alias("fecha_fx"))
        .agg(F.round(F.avg("tipo_cambio_pen_usd"), 4).alias("tc_edi_pen_usd"))
    )

    limpio = (
        sin_duplicados.join(F.broadcast(fx), F.col("fecha_cierre") == F.col("fecha_fx"), "left")
        .drop("fecha_fx")
        .withColumn("fecha_cierre", F.to_date("fecha_cierre"))
        .withColumn("volumen_usd_edi", F.round(F.col("volumen_total_pen") / F.col("tc_edi_pen_usd"), 2))
        .withColumn("ajuste_fx_usd", F.round(F.col("volumen_usd_edi") - F.col("volumen_total_usd"), 2))
    ).cache()

    filas_salida = limpio.count()
    sin_fx = limpio.filter(F.col("tc_edi_pen_usd").isNull()).count()

    resumen = limpio.groupBy("id_entidad", "id_distrito").agg(
        F.count("*").alias("lotes_procesados"),
        F.sum("total_transacciones").alias("tx_totales"),
        F.round(F.sum("volumen_total_pen"), 2).alias("volumen_pen"),
        F.round(F.sum("volumen_usd_edi"), 2).alias("volumen_usd_edi"),
        F.round(F.sum("ajuste_fx_usd"), 2).alias("ajuste_fx_usd"),
        F.round(F.avg("margen_operativo_pct"), 2).alias("margen_promedio_pct"),
    )
    metricas = {
        "filas_entrada": filas_entrada,
        "excluidas_por_validacion": filas_entrada - filas_validas,
        "excluidas_por_duplicado": filas_validas - filas_salida,
        "filas_salida": filas_salida,
        "filas_sin_tipo_cambio": sin_fx,
    }
    return limpio, resumen, metricas


def cruzar_tx_clientes(spark, ruta_tx, ruta_clientes):
    from pyspark.sql import functions as F

    tx = spark.read.parquet(ruta_tx).select("id_transaccion", "id_cliente", "id_distrito", "amount", "isFraud", "type")
    clientes = spark.read.parquet(ruta_clientes).select(
        "id_cliente", F.col("id_distrito").alias("distrito_habitual"), "segmento", "limite_diario_pen"
    )
    cruce = tx.join(clientes, "id_cliente", "left")
    resumen = (
        cruce.groupBy("segmento", "id_distrito")
        .agg(
            F.count("*").alias("transacciones"),
            F.countDistinct("id_cliente").alias("clientes_activos"),
            F.round(F.sum("amount"), 2).alias("volumen_pen"),
            F.sum(F.when(F.col("amount") > F.col("limite_diario_pen"), 1).otherwise(0)).alias("excesos_limite"),
            F.sum(F.when(F.col("id_distrito") != F.col("distrito_habitual"), 1).otherwise(0)).alias("fuera_distrito_habitual"),
            F.sum(F.when(F.col("distrito_habitual").isNull(), 1).otherwise(0)).alias("huerfanos"),
        )
        .orderBy("segmento", "id_distrito")
    )
    return resumen


def guardar_parquet(df_pandas, ruta):
    """Escritura con PyArrow: evita depender de winutils.exe en Windows al ejecutar en local."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pandas(df_pandas, preserve_index=False), ruta, compression="zstd")


def cargar_bigquery(ruta, tabla):
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID, location=REGION)
    destino = f"{PROJECT_ID}.silver_banco.{tabla}"
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(ruta, "rb") as f:
        job = client.load_table_from_file(f, destino, job_config=cfg)
    job.result()
    return {"tabla": destino, "job_id": job.job_id, "filas": client.get_table(destino).num_rows}


def ejecutar_pilar1_transformacion(master="local[*]", cargar=True):
    t0 = time.time()
    ruta_cierres, ruta_manifest = descargar_lote_landing()
    prueba_negativa = probar_compuerta_negativa(ruta_cierres, ruta_manifest)
    ruta_edi = os.path.join(DATA_DOWNLOAD_DIR, "edi_market_reference_download.parquet")
    ruta_tx = os.path.join(DATA_GENERATE_DIR, "transacciones_paysim_enriquecidas.parquet")
    ruta_cli = os.path.join(DATA_GENERATE_DIR, "core_clientes_12_entidades.parquet")

    verif = compuerta_sha256(ruta_cierres, ruta_manifest)
    print(f"  [1/4] Compuerta SHA-256 sobre lote de Landing aprobada: {verif['sha256'][:16]}...")
    print(f"        Prueba negativa (1 byte alterado) bloqueada: {prueba_negativa['lote_alterado_bloqueado']}")

    spark = crear_sesion(master)
    print(f"  [2/4] Spark {spark.version} iniciado (master={spark.sparkContext.master})")

    t1 = time.time()
    limpio, resumen, metricas = transformar_cierres(spark, ruta_cierres, ruta_edi)
    df_cierres = limpio.toPandas()
    df_resumen = resumen.orderBy("id_entidad", "id_distrito").toPandas()
    seg_cierres = round(time.time() - t1, 2)
    print(f"  [3/4] Cierres x EDI: {metricas['filas_salida']:,} filas, {len(df_resumen)} celdas en {seg_cierres} s")

    t2 = time.time()
    df_tx = cruzar_tx_clientes(spark, ruta_tx, ruta_cli).toPandas()
    seg_tx = round(time.time() - t2, 2)
    print(f"  [4/4] Transacciones x Clientes: {int(df_tx['transacciones'].sum()):,} tx cruzadas en {seg_tx} s")

    version = spark.version
    master_real = spark.sparkContext.master
    spark.stop()

    guardar_parquet(df_cierres, OUT_CIERRES)
    guardar_parquet(df_resumen, OUT_RESUMEN)
    guardar_parquet(df_tx, OUT_TX_RESUMEN)

    cargas = []
    if cargar:
        cargas.append(cargar_bigquery(OUT_CIERRES, "cierres_fx_spark"))
        cargas.append(cargar_bigquery(OUT_RESUMEN, "resumen_entidad_distrito_spark"))
        cargas.append(cargar_bigquery(OUT_TX_RESUMEN, "resumen_segmento_distrito_spark"))
        for c in cargas:
            print(f"        Cargado {c['tabla']}: {c['filas']:,} filas")

    reporte = {
        "ejecutado_utc": datetime.now(timezone.utc).isoformat(),
        "motor_ejecutado": f"Apache Spark {version}",
        "master": master_real,
        "manifest_sha256_ok": True,
        "origen_lote": f"gs://{BUCKET_LANDING}/cierres/",
        "prueba_negativa_compuerta": prueba_negativa,
        "sha256_hash": verif["sha256"],
        "cierres": metricas,
        "segundos_cierres_x_edi": seg_cierres,
        "tx_cruzadas": int(df_tx["transacciones"].sum()),
        "tx_huerfanas": int(df_tx["huerfanos"].sum()),
        "segundos_tx_x_clientes": seg_tx,
        "celdas_entidad_distrito": len(df_resumen),
        "ajuste_fx_total_usd": round(float(df_cierres["ajuste_fx_usd"].sum()), 2),
        "cargas_bigquery": cargas,
        "tiempo_total_seg": round(time.time() - t0, 2),
    }
    with open(OUT_REPORTE, "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2, default=str)
    print(f"  Reporte: {OUT_REPORTE}")
    return reporte


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--sin-bigquery", action="store_true")
    a = ap.parse_args()
    ejecutar_pilar1_transformacion(master=a.master, cargar=not a.sin_bigquery)
