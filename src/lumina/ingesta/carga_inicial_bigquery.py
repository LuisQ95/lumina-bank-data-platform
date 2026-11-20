"""Carga inicial de Bronze y Silver en BigQuery (9M clientes + 6.36M transacciones).
Usa load_table_from_file con Parquet. La vista core_clientes_cdc_raw y la capa Gold
se crean en lumina.batch.ejecutar_sql.
"""

import os
import sys

from google.cloud import bigquery

from lumina.config import DATA_DOWNLOAD_DIR, DATA_GENERATE_DIR, DATASETS, PROJECT_ID

sys.stdout.reconfigure(encoding="utf-8")
client = bigquery.Client(project=PROJECT_ID)


def cargar_parquet_a_bigquery(ruta_pq, dataset_id, tabla_id, partition_field=None, cluster_fields=None):
    """Carga un archivo Apache Parquet en BigQuery mediante Batch Load Job (100% gratuito en GCP)"""
    table_ref = f"{PROJECT_ID}.{dataset_id}.{tabla_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
    if cluster_fields:
        job_config.clustering_fields = cluster_fields

    with open(ruta_pq, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()
    tbl = client.get_table(table_ref)
    sz_mb = (tbl.num_bytes or 0) / (1024**2)
    print(f"  [TABLA CARGADA] {table_ref:<55} -> {tbl.num_rows:>10,} filas ({sz_mb:>6.2f} MB)")


def poblar_capa_bronze():
    print("\n======================================================================")
    print(" 1. POBLANDO CAPA BRONZE EN BIGQUERY (`bronze_banco`)")
    print("======================================================================")
    ds = DATASETS["bronze"]

    cargar_parquet_a_bigquery(
        os.path.join(DATA_DOWNLOAD_DIR, "paysim_kaggle_dataset.parquet"),
        ds, "tx_paysim_kaggle_raw"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "clickstream_app_web.parquet"),
        ds, "clickstream_app_web_raw"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "telemetria_atms_iot.parquet"),
        ds, "telemetria_atms_iot_raw"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "geolocalizacion_clientes_gps.parquet"),
        ds, "geolocalizacion_gps_raw"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "cierres_contables_12_entidades.parquet"),
        ds, "cierres_contables_batch_raw"
    )


def poblar_capa_silver():
    print("\n======================================================================")
    print(" 2. POBLANDO CAPA SILVER CONFORMADA EN BIGQUERY (`silver_banco`)")
    print("======================================================================")
    ds = DATASETS["silver"]

    # Los 9,000,000 de clientes activos conformados y clusterizados
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "core_clientes_12_entidades.parquet"),
        ds, "dim_cliente",
        cluster_fields=["id_cliente", "id_distrito", "id_entidad"]
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "dim_atm_catalogo.parquet"),
        ds, "dim_atm",
        cluster_fields=["id_atm", "id_distrito"]
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "dim_entidad_12.parquet"),
        ds, "dim_entidad"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "dim_distrito_8.parquet"),
        ds, "dim_distrito"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "calendario_eventos_festivos_2026.parquet"),
        ds, "dim_calendario_eventos"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_DOWNLOAD_DIR, "sec_quarterly_financials_download.parquet"),
        ds, "ext_sec_financials"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_DOWNLOAD_DIR, "google_trends_top_terms_download.parquet"),
        ds, "ext_google_trends"
    )
    cargar_parquet_a_bigquery(
        os.path.join(DATA_DOWNLOAD_DIR, "edi_market_reference_download.parquet"),
        ds, "ext_edi_mercado"
    )
    # Las 6,362,620 transacciones PaySim enriquecidas, particionadas por fecha y clusterizadas
    cargar_parquet_a_bigquery(
        os.path.join(DATA_GENERATE_DIR, "transacciones_paysim_enriquecidas.parquet"),
        ds, "fact_transaccion",
        partition_field="timestamp",
        cluster_fields=["id_cliente", "id_distrito"]
    )


def main():
    print("======================================================================")
    print(" CONSTRUYENDO LAKEHOUSE MEDALLION A ESCALA REAL (9M CLIENTES + 6.36M TX)")
    print("======================================================================")
    poblar_capa_bronze()
    poblar_capa_silver()
    print("\n[OK] Carga inicial terminada. Silver conformada y Gold se construyen con lumina.batch.ejecutar_sql.")


if __name__ == "__main__":
    main()
