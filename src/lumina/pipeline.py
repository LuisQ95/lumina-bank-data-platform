"""Orquestador de la Entrega 2 (P2).

Orden de ejecución:
  Fase 0  Revisa uso frente a las cuotas gratuitas y que no haya recursos cobrados por hora.
  Fase 1  Pilar 1 Spark   -> lumina.batch.spark_cierres
  Fase 2  Pilar 1 y 3 SQL -> lumina.batch.ejecutar_sql (Bronze -> Silver -> Gold)
  Fase 3  Pilar 2 y 3     -> lumina.streaming.consumidor_local
  Fase 4  Consultas sobre Gold y medición real de partición y clustering.
  Salida  reporte_ejecucion_poc_p2.json, armado solo con valores medidos.

Uso:
    set LUMINA_SPARK_PYTHON=<python del entorno con pyspark y Java 17>
    python -m lumina.pipeline
    python -m lumina.pipeline --solo-consolidar   # no repite Spark, SQL ni streaming

El costo real solo se confirma en Cloud Billing: este script no lee la facturación.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from google.cloud import bigquery, storage

from lumina.config import (
    BUCKET_BRONZE,
    BUCKET_LANDING,
    DATASETS,
    GCLOUD,
    PROJECT_ID,
    RAIZ,
    REGION,
    REPORTES_DIR,
)

sys.stdout.reconfigure(encoding="utf-8")

OUT_REPORTE = os.path.join(REPORTES_DIR, "reporte_ejecucion_poc_p2.json")

CUOTA_GCS_GIB = 5
CUOTA_BQ_ALMACEN_GIB = 10
CUOTA_BQ_CONSULTA_TIB = 1

FACT_CARGA = f"`{PROJECT_ID}.silver_banco.fact_transaccion`"
FACT_CONFORMADA = f"`{PROJECT_ID}.silver_banco.fact_transaccion_conformada`"

CONSULTAS_GOLD = {
    "V1_fraude_por_distrito": f"""
        SELECT id_distrito, SUM(tx_evaluadas) AS tx, SUM(fraudes_etiquetados) AS fraudes_etiquetados,
               SUM(alertas_cambio_distrito) AS alertas_cambio_distrito, SUM(excesos_limite) AS excesos_limite,
               ROUND(SUM(volumen_usd), 2) AS volumen_usd
        FROM `{PROJECT_ID}.gold_fraude.agg_fraude_distrito_dia`
        GROUP BY 1 ORDER BY 1""",
    "V2_liquidez_ultimo_plan": f"""
        SELECT fecha_plan, id_distrito, evento_d1, factor_d1, media_movil_7d_pen, proyeccion_d1_pen,
               efectivo_en_cajeros_pen, dias_cobertura, recarga_sugerida_pen
        FROM `{PROJECT_ID}.gold_liquidez.plan_liquidez_d1`
        WHERE fecha_plan = (SELECT MAX(fecha_plan) FROM `{PROJECT_ID}.gold_liquidez.plan_liquidez_d1`)
        ORDER BY dias_cobertura""",
    "V2_liquidez_resumen": f"""
        SELECT COUNT(*) AS planes, COUNTIF(recarga_sugerida_pen > 0) AS planes_con_recarga,
               ROUND(MIN(dias_cobertura), 2) AS cobertura_minima_dias, MAX(factor_d1) AS factor_maximo
        FROM `{PROJECT_ID}.gold_liquidez.plan_liquidez_d1`""",
    "V3_operacion_vs_sec": f"""
        SELECT producto_financiero, SUM(transacciones) AS transacciones, ROUND(SUM(ingresos_usd), 2) AS ingresos_usd,
               ROUND(SUM(costo_operativo_usd) / SUM(transacciones), 4) AS costo_por_tx_usd,
               ROUND(AVG(margen_operativo_pct), 2) AS margen_operativo_pct,
               ROUND(SUM(ajuste_fx_vs_tc_fijo_usd), 2) AS ajuste_fx_usd,
               ANY_VALUE(sec_margen_neto_mediana_pct) AS sec_margen_neto_mediana_pct,
               ANY_VALUE(sec_bancos_referencia) AS sec_bancos
        FROM `{PROJECT_ID}.gold_operacion.kpi_operacion_vs_sec`
        GROUP BY 1 ORDER BY 1""",
    "V4_salud_atm_y_cobertura": f"""
        SELECT id_distrito, COUNT(*) AS cajeros, SUM(requiere_atencion) AS requieren_atencion,
               ROUND(AVG(pct_efectivo), 1) AS pct_efectivo, ROUND(AVG(temperatura_c), 1) AS temperatura_c,
               ROUND(AVG(distancia_promedio_km), 2) AS distancia_km, ROUND(AVG(pct_lecturas_menos_8km), 1) AS pct_menos_8km
        FROM `{PROJECT_ID}.gold_operacion.salud_atm_cobertura`
        GROUP BY 1 ORDER BY 1""",
    "Digital_friccion_por_edad": f"""
        SELECT grupo_edad, SUM(eventos) AS eventos,
               ROUND(SUM(eventos * pct_requiere_asesor) / SUM(eventos), 2) AS pct_requiere_asesor
        FROM `{PROJECT_ID}.gold_operacion.agg_experiencia_digital`
        GROUP BY 1 ORDER BY 1""",
    "Stream_decisiones": f"""
        SELECT decision, es_fraude_etiqueta, COUNT(*) AS tx, ROUND(AVG(latencia_e2e_ms), 1) AS latencia_media_ms
        FROM `{PROJECT_ID}.silver_banco.stream_decisiones`
        GROUP BY 1, 2 ORDER BY 1, 2""",
}

PRUEBAS_FISICAS = {
    "A1_carga_sin_filtro": f"SELECT id_distrito, canal, SUM(amount) FROM {FACT_CARGA} GROUP BY 1, 2",
    "A2_carga_particion": f"SELECT id_distrito, canal, SUM(amount) FROM {FACT_CARGA} WHERE timestamp >= '2026-10-20' GROUP BY 1, 2",
    "A3_carga_particion_y_distrito_2a_columna": f"SELECT id_distrito, canal, SUM(amount) FROM {FACT_CARGA} WHERE timestamp >= '2026-10-20' AND id_distrito IN ('San Isidro', 'Miraflores') GROUP BY 1, 2",
    "B2_conformada_particion": f"SELECT id_distrito, canal, SUM(monto_pen) FROM {FACT_CONFORMADA} WHERE timestamp >= '2026-10-20' GROUP BY 1, 2",
    "B3_conformada_particion_y_distrito_1a_columna": f"SELECT id_distrito, canal, SUM(monto_pen) FROM {FACT_CONFORMADA} WHERE timestamp >= '2026-10-20' AND id_distrito IN ('San Isidro', 'Miraflores') GROUP BY 1, 2",
}


def gcloud_lista(args, formato="value(name)"):
    """Ejecuta gcloud sin entrada interactiva. Sin --quiet, una API apagada deja el proceso esperando."""
    r = subprocess.run(
        [GCLOUD, *args, f"--project={PROJECT_ID}", f"--format={formato}", "--quiet"],
        capture_output=True, text=True, timeout=120, stdin=subprocess.DEVNULL,
    )
    return None if r.returncode != 0 else [x for x in r.stdout.splitlines() if x.strip()]


def contar_recursos(api, args, habilitadas):
    """Una API apagada implica que no hay recursos de ese servicio; un error se informa como no verificado."""
    if api not in habilitadas:
        return "API no habilitada"
    filas = gcloud_lista(args)
    return "no verificado" if filas is None else len(filas)


def fase0_cuotas():
    print("\n[FASE 0] Uso frente a cuotas gratuitas")
    st = storage.Client(project=PROJECT_ID)
    bq = bigquery.Client(project=PROJECT_ID, location=REGION)

    gcs = {}
    for nombre in (BUCKET_LANDING, BUCKET_BRONZE):
        blobs = list(st.list_blobs(nombre))
        gcs[nombre] = {"objetos": len(blobs), "mb": round(sum(b.size or 0 for b in blobs) / 1024**2, 2)}
    gcs_mb = round(sum(v["mb"] for v in gcs.values()), 2)

    tablas = []
    for ds in DATASETS.values():
        for item in bq.list_tables(f"{PROJECT_ID}.{ds}"):
            t = bq.get_table(item.reference)
            tablas.append({
                "dataset": ds, "tabla": t.table_id, "tipo": t.table_type, "filas": t.num_rows or 0,
                "mb": round((t.num_bytes or 0) / 1024**2, 2),
                "particion": t.time_partitioning.field if t.time_partitioning else None,
                "clustering": list(t.clustering_fields) if t.clustering_fields else None,
            })
    nativas = [t for t in tablas if t["tipo"] == "TABLE"]
    bq_gib = round(sum(t["mb"] for t in nativas) / 1024, 3)

    habilitadas = set(gcloud_lista(["services", "list", "--enabled"], "value(config.name)") or [])
    recursos = {
        "bigtable_instances": contar_recursos("bigtableadmin.googleapis.com", ["bigtable", "instances", "list"], habilitadas),
        "compute_vms": contar_recursos("compute.googleapis.com", ["compute", "instances", "list"], habilitadas),
        "dataproc_clusters": contar_recursos("dataproc.googleapis.com", ["dataproc", "clusters", "list", f"--region={REGION}"], habilitadas),
        "redis_instances": contar_recursos("redis.googleapis.com", ["redis", "instances", "list", f"--region={REGION}"], habilitadas),
    }
    print(f"  GCS {gcs_mb} MB de {CUOTA_GCS_GIB} GiB | BigQuery {bq_gib} GiB de {CUOTA_BQ_ALMACEN_GIB} GiB | recursos por hora: {recursos}")
    return {
        "gcs_detalle": gcs, "gcs_total_mb": gcs_mb, "gcs_objetos": sum(v["objetos"] for v in gcs.values()),
        "bq_tablas_nativas": len(nativas),
        "bq_tablas_externas": len([t for t in tablas if t["tipo"] == "EXTERNAL"]),
        "bq_vistas": len([t for t in tablas if t["tipo"] == "VIEW"]),
        "bq_filas_tablas_nativas": sum(t["filas"] for t in nativas),
        "bq_almacenamiento_gib": bq_gib, "bq_inventario": tablas, "recursos_por_hora": recursos,
    }


def correr(cmd):
    """Ejecuta un paso del pipeline como módulo, con src/ en el PYTHONPATH."""
    print(f"\n  $ {' '.join(cmd)}")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(RAIZ / "src"), env.get("PYTHONPATH")]))
    subprocess.run(cmd, cwd=str(RAIZ), check=True, env=env)


def fase4_consultas():
    print("\n[FASE 4] Consultas Gold y medición física")
    bq = bigquery.Client(project=PROJECT_ID, location=REGION)
    consultas = {}
    for nombre, sql in CONSULTAS_GOLD.items():
        t0 = time.perf_counter()
        job = bq.query(sql, job_config=bigquery.QueryJobConfig(use_query_cache=False, maximum_bytes_billed=1024**3))
        filas = [dict(r.items()) for r in job.result()]
        consultas[nombre] = {
            "job_id": job.job_id, "latencia_ms": round((time.perf_counter() - t0) * 1000, 1),
            "mb_procesados": round((job.total_bytes_processed or 0) / 1024**2, 2), "filas": filas,
        }
        print(f"  [OK] {nombre:<30} {consultas[nombre]['latencia_ms']:>8} ms {consultas[nombre]['mb_procesados']:>8} MB")

    fisicas = {}
    for nombre, sql in PRUEBAS_FISICAS.items():
        seco = bq.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)).total_bytes_processed
        job = bq.query(sql, job_config=bigquery.QueryJobConfig(use_query_cache=False))
        job.result()
        fisicas[nombre] = {
            "job_id": job.job_id, "mb_estimados_dry_run": round(seco / 1024**2, 2),
            "mb_reales": round(job.total_bytes_processed / 1024**2, 2), "mb_facturados": round(job.total_bytes_billed / 1024**2, 2),
        }
        print(f"  [OK] {nombre:<46} real {fisicas[nombre]['mb_reales']:>8} MB")
    return consultas, fisicas


def leer(nombre):
    ruta = os.path.join(REPORTES_DIR, nombre)
    if not os.path.exists(ruta):
        return None
    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--solo-consolidar", action="store_true")
    a = ap.parse_args()

    t0 = time.time()
    if not a.solo_consolidar:
        py_spark = os.environ.get("LUMINA_SPARK_PYTHON", sys.executable)
        correr([py_spark, "-m", "lumina.batch.spark_cierres"])
        correr([sys.executable, "-m", "lumina.batch.ejecutar_sql"])
        correr([sys.executable, "-m", "lumina.streaming.consumidor_local"])

    cuotas = fase0_cuotas()
    consultas, fisicas = fase4_consultas()
    sql = leer("reporte_pipeline_sql_p2.json") or {}

    reporte = {
        "proyecto_gcp": PROJECT_ID,
        "region": REGION,
        "generado_utc": datetime.now(timezone.utc).isoformat(),
        "duracion_seg": round(time.time() - t0, 2),
        "costo": {
            "facturacion_verificada": False,
            "nota": "Este reporte mide uso frente a cuotas gratuitas; la factura real se confirma en Cloud Billing.",
            "mb_procesados_pipeline_sql": sql.get("mb_procesados_total"),
            "cuota_consulta_tib_mes": CUOTA_BQ_CONSULTA_TIB,
        },
        "cuotas_y_recursos": cuotas,
        "pilar1_spark": leer("reporte_spark_p2.json"),
        "pilar1_y_3_sql": sql,
        "pilar2_streaming": leer("reporte_streaming_p2.json"),
        "consultas_gold": consultas,
        "medicion_particion_y_clustering": fisicas,
    }
    with open(OUT_REPORTE, "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n  Reporte consolidado: {OUT_REPORTE}")


if __name__ == "__main__":
    main()
