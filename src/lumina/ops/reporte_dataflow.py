"""Junta la evidencia de un despliegue de Dataflow. Ejecutar ANTES de apagar.

Uso:
    python -m lumina.ops.reporte_dataflow                                   Guarda reports/reporte_dataflow_p2.json
    python -m lumina.ops.reporte_dataflow --salida reporte_dataflow_p3.json  Guarda con otro nombre

Lee estado_despliegue_dataflow.json (lo escribe infra/desplegar_dataflow_memorystore.ps1) y consulta:
  - BigQuery: decisiones, latencias y agregados de esta corrida (consulta con tope de 1 GiB).
  - Dataflow: estado del job y contadores del grupo "lumina".
  - Memorystore: estado de la instancia y, por SSH a la VM, PING, INFO keyspace y una clave de ejemplo.
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

from google.cloud import bigquery

from lumina.config import GCLOUD, PROJECT_ID, REGION, REPORTES_DIR

sys.stdout.reconfigure(encoding="utf-8")
ZONA = "us-central1-a"


def gcloud(*args, timeout=120):
    """Ejecuta gcloud en el proyecto sin entrada interactiva.

    Args:
        *args: Argumentos de gcloud.
        timeout: Segundos máximos de espera.

    Returns:
        Tupla (código_de_salida, stdout, stderr).
    """
    r = subprocess.run([GCLOUD, *args, f"--project={PROJECT_ID}", "--quiet"], capture_output=True, text=True,
                       encoding="utf-8", errors="replace", stdin=subprocess.DEVNULL, timeout=timeout)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def bigquery_evidencia(run_id):
    """Mide en BigQuery las decisiones, latencias, agregados y rechazos de una corrida.

    La tabla de rechazos solo existe si hubo algún mensaje inválido; si no existe, se informa vacía.

    Args:
        run_id: Identificador de la corrida de Dataflow.

    Returns:
        Diccionario con `decisiones`, `agregado_1m` y `rechazos`.
    """
    bq = bigquery.Client(project=PROJECT_ID, location=REGION)
    cfg = bigquery.QueryJobConfig(maximum_bytes_billed=1024**3, query_parameters=[bigquery.ScalarQueryParameter("r", "STRING", run_id)])
    q = f"""
    SELECT COUNT(*) AS decisiones, COUNT(DISTINCT id_transaccion) AS tx_unicas,
           COUNTIF(decision = 'APROBADA') AS aprobadas, COUNTIF(decision = 'RECHAZADA_LIMITE') AS rechazadas_limite,
           COUNTIF(decision = 'REVISION_FRAUDE') AS revision_fraude, SUM(salto_gps) AS saltos_gps,
           APPROX_QUANTILES(latencia_e2e_ms, 100)[OFFSET(50)] AS e2e_p50_ms,
           APPROX_QUANTILES(latencia_e2e_ms, 100)[OFFSET(95)] AS e2e_p95_ms,
           APPROX_QUANTILES(latencia_e2e_ms, 100)[OFFSET(99)] AS e2e_p99_ms, MAX(latencia_e2e_ms) AS e2e_max_ms,
           APPROX_QUANTILES(latencia_redis_ms, 100)[OFFSET(50)] AS redis_p50_ms,
           APPROX_QUANTILES(latencia_redis_ms, 100)[OFFSET(99)] AS redis_p99_ms
    FROM `{PROJECT_ID}.silver_banco.stream_decisiones_dataflow` WHERE run_id = @r"""
    out = {"decisiones": dict(next(iter(bq.query(q, job_config=cfg).result())).items())}
    q = f"SELECT COUNT(*) AS filas, SUM(transacciones) AS tx FROM `{PROJECT_ID}.gold_fraude.agg_stream_distrito_1m` WHERE run_id = @r"
    out["agregado_1m"] = dict(next(iter(bq.query(q, job_config=cfg).result())).items())
    try:
        q = f"SELECT origen, COUNT(*) AS n FROM `{PROJECT_ID}.silver_banco.stream_rechazos` WHERE run_id = @r GROUP BY origen"
        out["rechazos"] = {r["origen"]: r["n"] for r in bq.query(q, job_config=cfg).result()}
    except Exception:
        out["rechazos"] = {}
    return out


def main(salida="reporte_dataflow_p2.json"):
    """Junta el estado del job, sus contadores, Memorystore, redis-cli y BigQuery en un solo reporte.

    Args:
        salida: Nombre del archivo JSON dentro de REPORTES_DIR.
    """
    ruta = os.path.join(REPORTES_DIR, "estado_despliegue_dataflow.json")
    with open(ruta, encoding="utf-8-sig") as f:
        estado = json.load(f)
    rep = {"generado_utc": datetime.now(timezone.utc).isoformat(), "despliegue": estado}

    rc, out, err = gcloud("dataflow", "jobs", "describe", estado["job_id"], f"--region={REGION}", "--format=json")
    if rc == 0:
        j = json.loads(out)
        rep["dataflow_job"] = {k: j.get(k) for k in ("id", "name", "currentState", "createTime", "type")}
    rc, out, err = gcloud("dataflow", "metrics", "list", estado["job_id"], f"--region={REGION}", "--source=user", "--format=json")
    if rc == 0:
        rep["dataflow_contadores"] = {
            m["name"]["name"]: m.get("scalar") or m.get("distribution")
            for m in json.loads(out) if not m["name"].get("context", {}).get("tentative")
        }

    rc, out, err = gcloud("redis", "instances", "describe", estado["redis"], f"--region={REGION}", "--format=json")
    if rc == 0:
        r = json.loads(out)
        rep["memorystore"] = {k: r.get(k) for k in ("name", "tier", "memorySizeGb", "redisVersion", "state", "host", "port", "authorizedNetwork", "connectMode")}

    if estado.get("vm"):
        h = estado["redis_host"]
        cmd = (f"redis-cli -h {h} PING; redis-cli -h {h} INFO keyspace | tail -n +2; "
               f"redis-cli -h {h} --scan --pattern 'limites:*' --count 1000 | head -1 | xargs -r redis-cli -h {h} HGETALL; "
               f"redis-cli -h {h} HGETALL clics:resumen; redis-cli -h {h} SCARD atm_alertas")
        try:
            rc, out, err = gcloud("compute", "ssh", estado["vm"], f"--zone={ZONA}", "--command", cmd,
                                  "--strict-host-key-checking=no", timeout=180)
            rep["redis_cli"] = out.splitlines() if rc == 0 else {"error": err[-500:]}
        except subprocess.TimeoutExpired:
            rep["redis_cli"] = {"error": "SSH sin respuesta en 180 s; tomar la captura desde la consola."}

    rep["bigquery"] = bigquery_evidencia(estado["run_id"])

    destino = os.path.join(REPORTES_DIR, salida)
    with open(destino, "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps(rep, ensure_ascii=False, indent=1, default=str))
    print(f"\nGuardado en {destino}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", default="reporte_dataflow_p2.json")
    main(ap.parse_args().salida)
