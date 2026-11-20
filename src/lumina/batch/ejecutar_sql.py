"""Ejecutor del pipeline SQL Medallion (Bronze -> Silver -> Gold) en BigQuery.

Lee src/lumina/batch/sql/medallion.sql, ejecuta cada bloque "-- @paso:" como
un job real y guarda job_id, bytes y filas en reporte_pipeline_sql_p2.json.

Uso:
    python -m lumina.batch.ejecutar_sql                # todos los pasos
    python -m lumina.batch.ejecutar_sql --desde gold_  # desde el primer paso con ese prefijo
    python -m lumina.batch.ejecutar_sql --dry-run      # solo estima bytes

Attributes:
    MAX_BYTES_POR_JOB: Tope duro de 8 GiB facturados por consulta.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

from google.cloud import bigquery

from lumina.config import PROJECT_ID, REGION, REPORTES_DIR, SALT

sys.stdout.reconfigure(encoding="utf-8")

SQL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", "medallion.sql")
OUT_FILE = os.path.join(REPORTES_DIR, "reporte_pipeline_sql_p2.json")
MAX_BYTES_POR_JOB = 8 * 1024**3


def leer_pasos():
    """Divide medallion.sql en pasos y reemplaza la sal del token de clientes.

    Returns:
        Lista de tuplas (nombre_del_paso, sql) en el orden del archivo.
    """
    with open(SQL_FILE, encoding="utf-8") as f:
        texto = f.read()
    texto = texto.replace("{{SALT}}", SALT)
    partes = re.split(r"^-- @paso:\s*(\S+)\s*$", texto, flags=re.MULTILINE)
    return [(partes[i], partes[i + 1].strip()) for i in range(1, len(partes), 2)]


def main():
    """Ejecuta los pasos como jobs de BigQuery y guarda el reporte con calidad y conciliación.

    Las tablas de calidad y conciliación solo existen si se ejecutaron sus pasos; si se corrió un
    subconjunto, el reporte las marca como no disponibles.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    client = bigquery.Client(project=PROJECT_ID, location=REGION)
    pasos = leer_pasos()
    if args.desde:
        idx = next(i for i, (n, _) in enumerate(pasos) if n.startswith(args.desde))
        pasos = pasos[idx:]

    resultados = []
    t_total = time.time()
    for nombre, sql in pasos:
        cfg = bigquery.QueryJobConfig(
            dry_run=args.dry_run,
            use_query_cache=False,
            maximum_bytes_billed=None if args.dry_run else MAX_BYTES_POR_JOB,
        )
        t0 = time.time()
        job = client.query(sql, job_config=cfg)
        if not args.dry_run:
            job.result()
        seg = round(time.time() - t0, 2)
        mb = round((job.total_bytes_processed or 0) / 1024**2, 2)
        fila = {
            "paso": nombre,
            "job_id": job.job_id,
            "segundos": seg,
            "mb_procesados": mb,
            "mb_facturados": round((job.total_bytes_billed or 0) / 1024**2, 2),
            "tipo": job.statement_type,
        }
        destino = getattr(job, "ddl_target_table", None)
        if destino is not None and not args.dry_run:
            tabla = client.get_table(destino)
            fila["destino"] = f"{destino.dataset_id}.{destino.table_id}"
            fila["filas_destino"] = tabla.num_rows
        resultados.append(fila)
        print(f"  [OK] {nombre:<38} {seg:>7.2f} s {mb:>10.2f} MB  filas={fila.get('filas_destino', '-')}")

    if args.dry_run:
        print(f"  Total estimado: {sum(r['mb_procesados'] for r in resultados):.2f} MB")
        return

    reporte = {
        "proyecto_gcp": PROJECT_ID,
        "region": REGION,
        "ejecutado_utc": datetime.now(timezone.utc).isoformat(),
        "duracion_seg": round(time.time() - t_total, 2),
        "mb_procesados_total": round(sum(r["mb_procesados"] for r in resultados), 2),
        "mb_facturados_total": round(sum(r["mb_facturados"] for r in resultados), 2),
        "pasos": resultados,
    }

    for tabla in ("dq_resultados", "dq_conciliacion"):
        try:
            filas = client.query(f"SELECT * FROM `{PROJECT_ID}.silver_banco.{tabla}`").result()
            reporte[tabla] = [dict(r.items()) for r in filas]
        except Exception as exc:
            reporte[tabla] = f"no disponible: {exc}"

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n  Reporte: {OUT_FILE}")


if __name__ == "__main__":
    main()
