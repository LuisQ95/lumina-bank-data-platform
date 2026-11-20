"""Consultas de solo lectura sobre BigQuery para la capa de exposición.

Lee la capa Silver (decisiones en tiempo real que escribe Dataflow) y la capa Gold. Cada consulta es
parametrizada y lleva un tope de bytes facturados: si una consulta intentara leer más de lo previsto,
BigQuery la rechaza antes de cobrar.

Attributes:
    MAX_BYTES_FACTURADOS: Tope por consulta. Las tablas Gold pesan unos pocos KB, así que 200 MB
        solo se alcanzaría con una consulta mal escrita.
"""

from datetime import date, datetime
from functools import lru_cache

from google.cloud import bigquery

from lumina.config import PROJECT_ID

MAX_BYTES_FACTURADOS = 200 * 1024 * 1024

T_DECISIONES = f"`{PROJECT_ID}.silver_banco.stream_decisiones_dataflow`"
T_FRAUDE = f"`{PROJECT_ID}.gold_fraude.agg_fraude_distrito_dia`"
T_LIQUIDEZ = f"`{PROJECT_ID}.gold_liquidez.plan_liquidez_d1`"
T_KPI = f"`{PROJECT_ID}.gold_operacion.kpi_operacion_vs_sec`"
T_ATM = f"`{PROJECT_ID}.gold_operacion.salud_atm_cobertura`"
T_DIGITAL = f"`{PROJECT_ID}.gold_operacion.agg_experiencia_digital`"


@lru_cache(maxsize=1)
def cliente() -> bigquery.Client:
    """Devuelve un cliente de BigQuery reutilizado por todas las solicitudes del proceso."""
    return bigquery.Client(project=PROJECT_ID)


def _valor(v):
    """Convierte fechas y marcas de tiempo a ISO 8601 para que la respuesta sea JSON."""
    if isinstance(v, datetime | date):
        return v.isoformat()
    return v


def ejecutar(sql: str, **parametros) -> list[dict]:
    """Ejecuta una consulta parametrizada con tope de bytes facturados.

    Args:
        sql: Consulta con parámetros nombrados (@nombre).
        **parametros: Valores de los parámetros; el tipo de BigQuery se deduce del tipo de Python.

    Returns:
        Las filas como diccionarios, con fechas en formato ISO 8601.
    """
    tipos = {int: "INT64", float: "FLOAT64", str: "STRING"}
    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_FACTURADOS,
        query_parameters=[bigquery.ScalarQueryParameter(k, tipos[type(v)], v) for k, v in parametros.items()],
        labels={"proyecto": "lumina", "componente": "api"},
    )
    filas = cliente().query(sql, job_config=config).result()
    return [{k: _valor(v) for k, v in fila.items()} for fila in filas]


def decision(id_transaccion: str) -> dict | None:
    """Busca la decisión de una transacción en la partición del último día.

    Args:
        id_transaccion: Id publicado por la API.

    Returns:
        La primera fila de la decisión más `filas_mismo_id`, que debe ser 1 aunque el envío se
        reintente, porque Redis descarta los duplicados. None si Dataflow aún no la escribió.
    """
    filas = ejecutar(
        f"""SELECT *, COUNT(*) OVER () AS filas_mismo_id FROM {T_DECISIONES}
            WHERE evento_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND id_transaccion = @id
            ORDER BY decidido_ts LIMIT 1""",
        id=id_transaccion,
    )
    return filas[0] if filas else None


def decisiones_recientes(limite: int) -> list[dict]:
    """Devuelve las últimas decisiones de los 30 días más recientes.

    Args:
        limite: Cantidad máxima de filas.
    """
    return ejecutar(
        f"""SELECT id_transaccion, id_cliente, id_distrito, canal, tipo_operacion, monto_pen, vel_1m, salto_gps,
                   riesgo, decision, latencia_e2e_ms, evento_ts, decidido_ts
            FROM {T_DECISIONES}
            WHERE evento_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            ORDER BY decidido_ts DESC LIMIT @limite""",
        limite=limite,
    )


def resumen_decisiones(horas: int) -> list[dict]:
    """Cuenta decisiones, monto y latencia p50/p95 por tipo de decisión.

    Args:
        horas: Ventana hacia atrás desde ahora.
    """
    return ejecutar(
        f"""SELECT decision, COUNT(*) AS transacciones, ROUND(SUM(monto_pen), 2) AS monto_pen,
                   APPROX_QUANTILES(latencia_e2e_ms, 100)[OFFSET(50)] AS latencia_p50_ms,
                   APPROX_QUANTILES(latencia_e2e_ms, 100)[OFFSET(95)] AS latencia_p95_ms
            FROM {T_DECISIONES}
            WHERE evento_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @horas HOUR)
            GROUP BY decision ORDER BY transacciones DESC""",
        horas=horas,
    )


def fraude_por_distrito(dias: int) -> list[dict]:
    """Resume el fraude por distrito en los últimos días cargados.

    La ventana se cuenta desde la última fecha de la tabla y no desde hoy, porque el histórico batch
    tiene sus propias fechas.

    Args:
        dias: Días hacia atrás desde la última fecha cargada.
    """
    return ejecutar(
        f"""WITH ultima AS (SELECT MAX(fecha) AS f FROM {T_FRAUDE})
            SELECT id_distrito, SUM(tx_evaluadas) AS tx_evaluadas, SUM(fraudes_etiquetados) AS fraudes,
                   SUM(alertas_cambio_distrito) AS alertas_cambio_distrito, SUM(excesos_limite) AS excesos_limite,
                   ROUND(SUM(volumen_pen), 2) AS volumen_pen,
                   ROUND(SAFE_DIVIDE(SUM(fraudes_etiquetados), SUM(tx_evaluadas)) * 100, 3) AS tasa_fraude_pct,
                   MIN(fecha) AS desde, MAX(fecha) AS hasta
            FROM {T_FRAUDE}, ultima
            WHERE fecha > DATE_SUB(ultima.f, INTERVAL @dias DAY)
            GROUP BY id_distrito ORDER BY tasa_fraude_pct DESC""",
        dias=dias,
    )


def plan_liquidez(distrito: str | None) -> list[dict]:
    """Devuelve el plan de liquidez del día siguiente más reciente.

    Args:
        distrito: Filtra un distrito; None devuelve los ocho.
    """
    filtro = "AND id_distrito = @distrito" if distrito else ""
    parametros = {"distrito": distrito} if distrito else {}
    return ejecutar(
        f"""SELECT fecha_plan, id_distrito, evento_d1, factor_d1, ROUND(proyeccion_d1_pen, 2) AS proyeccion_d1_pen,
                   cajeros, ROUND(efectivo_en_cajeros_pen, 2) AS efectivo_en_cajeros_pen, ROUND(dias_cobertura, 2) AS dias_cobertura,
                   ROUND(recarga_sugerida_pen, 2) AS recarga_sugerida_pen
            FROM {T_LIQUIDEZ}
            WHERE fecha_plan = (SELECT MAX(fecha_plan) FROM {T_LIQUIDEZ}) {filtro}
            ORDER BY recarga_sugerida_pen DESC""",
        **parametros,
    )


def kpi_operacion(entidad: str | None) -> list[dict]:
    """Resume los KPI operativos por entidad frente al margen de referencia de la SEC.

    Args:
        entidad: Filtra una entidad; None devuelve las doce.
    """
    filtro = "WHERE id_entidad = @entidad" if entidad else ""
    parametros = {"entidad": entidad} if entidad else {}
    return ejecutar(
        f"""SELECT id_entidad, sistema_core_origen, SUM(transacciones) AS transacciones,
                   ROUND(SUM(volumen_pen), 2) AS volumen_pen,
                   ROUND(SAFE_DIVIDE(SUM(costo_operativo_usd), SUM(transacciones)), 4) AS costo_por_tx_usd,
                   ROUND(AVG(margen_operativo_pct), 2) AS margen_operativo_pct,
                   ROUND(AVG(sec_margen_neto_mediana_pct), 2) AS margen_referencia_sec_pct
            FROM {T_KPI} {filtro}
            GROUP BY id_entidad, sistema_core_origen ORDER BY id_entidad""",
        **parametros,
    )


def salud_atm(solo_alertas: bool) -> list[dict]:
    """Devuelve la salud y la cobertura de los cajeros, primero los que requieren atención.

    Args:
        solo_alertas: Si es True, solo los cajeros con efectivo bajo o temperatura alta.
    """
    filtro = "WHERE requiere_atencion = 1" if solo_alertas else ""
    return ejecutar(
        f"""SELECT id_atm, id_distrito, estado_salud, ROUND(pct_efectivo, 1) AS pct_efectivo,
                   ROUND(temperatura_c, 1) AS temperatura_c, latencia_red_ms, requiere_atencion,
                   ROUND(pct_lecturas_menos_8km, 1) AS pct_lecturas_menos_8km
            FROM {T_ATM} {filtro}
            ORDER BY requiere_atencion DESC, pct_efectivo ASC LIMIT 200"""
    )


def experiencia_digital() -> list[dict]:
    """Resume la experiencia digital por distrito y dispositivo, de mayor a menor necesidad de asesor."""
    return ejecutar(
        f"""SELECT id_distrito, dispositivo, SUM(eventos) AS eventos,
                   ROUND(AVG(tiempo_promedio_seg), 1) AS tiempo_promedio_seg,
                   ROUND(AVG(pct_requiere_asesor), 2) AS pct_requiere_asesor
            FROM {T_DIGITAL}
            GROUP BY id_distrito, dispositivo ORDER BY pct_requiere_asesor DESC"""
    )
