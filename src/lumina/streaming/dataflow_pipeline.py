"""
Pilar 2 (Entrega P2): pipeline streaming en Apache Beam para Cloud Dataflow con estado en Memorystore.

Flujo:
  Pub/Sub (transacciones, geo, atm, clicks)
    -> parseo y validación (los mensajes inválidos van a una tabla de rechazos)
    -> estado caliente en Memorystore for Redis (deduplicación, velocidad, acumulado, GPS, ATM)
    -> decisión por reglas
    -> BigQuery: silver_banco.stream_decisiones_dataflow (detalle)
                 gold_fraude.agg_stream_distrito_1m (ventanas fijas de 1 minuto)

Las reglas son las de lumina.streaming.consumidor_local. No hay modelo de ML.

Límites conocidos:
  - La deduplicación ocurre antes de escribir en BigQuery. Si Dataflow reintenta un bloque ya
    registrado en Redis, ese evento se descarta como duplicado; se prefiere esto a contar dos veces
    el acumulado del cliente.
  - Las suscripciones *-sub-dataflow no usan entrega ordenada. Las ventanas de velocidad se calculan
    con la marca de tiempo del evento, por lo que no dependen del orden de llegada.

Uso en Dataflow (ver README):
    python src/lumina/streaming/dataflow_pipeline.py --runner DataflowRunner --project ... \
        --redis_host 10.x.x.x --perfiles_gcs gs://.../perfiles_muestra.jsonl
"""

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import window
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

UMBRAL_PCT_EFECTIVO = 20.0
UMBRAL_TEMPERATURA_C = 35.0
UMBRAL_RIESGO_REVISION = 0.65
VENTANA_1M_MS = 60_000
VENTANA_5M_MS = 300_000
TTL_DEDUP_S = 3600
TTL_VELOCIDAD_S = 600
TTL_LIMITES_S = 86400
TTL_ATM_S = 3600

ESQUEMA_DECISIONES = (
    "run_id:STRING,id_transaccion:STRING,id_cliente:STRING,id_distrito:STRING,canal:STRING,"
    "tipo_operacion:STRING,monto_pen:FLOAT,vel_1m:INTEGER,vel_5m:INTEGER,salto_gps:INTEGER,"
    "riesgo:FLOAT,decision:STRING,es_fraude_etiqueta:INTEGER,latencia_e2e_ms:INTEGER,"
    "latencia_redis_ms:FLOAT,evento_ts:TIMESTAMP,decidido_ts:TIMESTAMP"
)
ESQUEMA_AGREGADO = (
    "run_id:STRING,ventana_inicio:TIMESTAMP,ventana_fin:TIMESTAMP,id_distrito:STRING,"
    "decision:STRING,transacciones:INTEGER,monto_pen:FLOAT"
)
ESQUEMA_RECHAZOS = "run_id:STRING,origen:STRING,error:STRING,mensaje:STRING,recibido_ts:TIMESTAMP"


def crear_cliente(host, port):
    import redis

    return redis.Redis(host=host, port=port, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)


def iso_ms(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def calcular_riesgo(vel_1m, salto, monto):
    return round(min(0.99, 0.08 * vel_1m + (0.45 if salto else 0.0) + (monto / 25000.0) * 0.4), 3)


def tomar_decision(riesgo, acumulado, limite):
    if riesgo >= UMBRAL_RIESGO_REVISION:
        return "REVISION_FRAUDE"
    if limite and acumulado > limite:
        return "RECHAZADA_LIMITE"
    return "APROBADA"


def evaluar_tx(r, ev, limite, run_id, ahora_ms):
    """Actualiza el estado en Redis y devuelve la decisión. Devuelve None si el evento es un duplicado."""
    tid, cid = ev["id_transaccion"], ev["id_cliente"]
    if not r.set(f"dedup:{tid}", 1, nx=True, ex=TTL_DEDUP_S):
        return None

    ems = int(ev["epoch_ms"])
    monto = float(ev["amount"])
    clave_vel, clave_lim = f"vel:{cid}", f"limites:{cid}"

    p = r.pipeline(transaction=False)
    p.zadd(clave_vel, {tid: ems})
    p.zremrangebyscore(clave_vel, "-inf", ems - VENTANA_5M_MS - 1)
    p.zcount(clave_vel, ems - VENTANA_1M_MS, ems)
    p.zcount(clave_vel, ems - VENTANA_5M_MS, ems)
    p.expire(clave_vel, TTL_VELOCIDAD_S)
    p.hincrbyfloat(clave_lim, "acumulado_dia_pen", monto)
    p.get(f"gps:{cid}")
    res = p.execute()
    vel_1m, vel_5m, acumulado, gps = int(res[2]), int(res[3]), float(res[5]), res[6]

    salto = 1 if gps is not None and gps != ev["id_distrito"] else 0
    riesgo = calcular_riesgo(vel_1m, salto, monto)
    decision = tomar_decision(riesgo, acumulado, limite)

    p = r.pipeline(transaction=False)
    p.hset(clave_lim, mapping={"limite_diario_pen": limite, "vel_1m": vel_1m, "vel_5m": vel_5m, "ultima_decision": decision})
    p.expire(clave_lim, TTL_LIMITES_S)
    p.execute()

    return {
        "run_id": run_id, "id_transaccion": tid, "id_cliente": cid, "id_distrito": ev["id_distrito"],
        "canal": ev["canal"], "tipo_operacion": ev["type"], "monto_pen": monto,
        "vel_1m": vel_1m, "vel_5m": vel_5m, "salto_gps": salto, "riesgo": riesgo, "decision": decision,
        "es_fraude_etiqueta": int(ev["isFraud"]), "latencia_e2e_ms": ahora_ms - ems,
        "evento_ts": iso_ms(ems), "decidido_ts": iso_ms(ahora_ms),
    }


def es_alerta_atm(ev):
    pct = ev.get("pct_efectivo")
    temp = ev.get("temperatura_c")
    return (pct is not None and pct < UMBRAL_PCT_EFECTIVO) or (temp is not None and temp > UMBRAL_TEMPERATURA_C)


class ParsearJson(beam.DoFn):
    """Convierte bytes en dict. Lo que no se pueda leer sale por la salida 'rechazado'."""

    def __init__(self, origen, campos, run_id):
        self.origen, self.campos, self.run_id = origen, campos, run_id
        self.rechazados = Metrics.counter("lumina", f"rechazados_{origen}")

    def process(self, mensaje):
        try:
            ev = json.loads(mensaje.decode("utf-8"))
            faltan = [c for c in self.campos if c not in ev]
            if faltan:
                raise ValueError(f"faltan campos: {faltan}")
            yield ev
        except (ValueError, TypeError, AttributeError) as e:
            self.rechazados.inc()
            yield beam.pvalue.TaggedOutput("rechazado", {
                "run_id": self.run_id, "origen": self.origen, "error": str(e)[:300],
                "mensaje": repr(mensaje)[:1000], "recibido_ts": datetime.now(timezone.utc).isoformat(),
            })


class ConRedis(beam.DoFn):
    """Abre una conexión por instancia del DoFn y la cierra al terminar."""

    def __init__(self, host, port):
        self.host, self.port = host, port
        self.r = None

    def setup(self):
        self.r = crear_cliente(self.host, self.port)
        self.r.ping()

    def teardown(self):
        if self.r is not None:
            self.r.close()


class DecidirTx(ConRedis):
    def __init__(self, host, port, perfiles_gcs, run_id):
        super().__init__(host, port)
        self.perfiles_gcs, self.run_id = perfiles_gcs, run_id
        self.limites = {}
        self.duplicados = Metrics.counter("lumina", "duplicados_filtrados")
        self.decididas = Metrics.counter("lumina", "tx_decididas")
        self.revision = Metrics.counter("lumina", "tx_revision_fraude")
        self.lat_e2e = Metrics.distribution("lumina", "latencia_e2e_ms")
        self.lat_redis = Metrics.distribution("lumina", "latencia_redis_ms")

    def setup(self):
        super().setup()
        with FileSystems.open(self.perfiles_gcs) as f:
            for linea in f:
                perfil = json.loads(linea)
                self.limites[perfil["id_cliente"]] = float(perfil.get("limite_diario_pen") or 0.0)

    def process(self, ev):
        import time

        t0 = time.perf_counter()
        ahora_ms = int(time.time() * 1000)
        fila = evaluar_tx(self.r, ev, self.limites.get(ev["id_cliente"], 0.0), self.run_id, ahora_ms)
        if fila is None:
            self.duplicados.inc()
            return
        fila["latencia_redis_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
        self.decididas.inc()
        if fila["decision"] == "REVISION_FRAUDE":
            self.revision.inc()
        self.lat_e2e.update(fila["latencia_e2e_ms"])
        self.lat_redis.update(int(fila["latencia_redis_ms"]))
        yield fila


class GuardarGps(ConRedis):
    def __init__(self, host, port):
        super().__init__(host, port)
        self.recibidos = Metrics.counter("lumina", "geo_en_redis")

    def process(self, ev):
        self.r.set(f"gps:{ev['id_cliente']}", ev["id_distrito_actual"], ex=TTL_LIMITES_S)
        self.recibidos.inc()


class GuardarAtm(ConRedis):
    def __init__(self, host, port):
        super().__init__(host, port)
        self.recibidos = Metrics.counter("lumina", "atm_en_redis")
        self.alertas = Metrics.counter("lumina", "atm_alertas")

    def process(self, ev):
        clave = f"atm_health:{ev['id_atm']}"
        p = self.r.pipeline(transaction=False)
        p.hset(clave, mapping={k: v for k, v in ev.items() if v is not None})
        p.expire(clave, TTL_ATM_S)
        if es_alerta_atm(ev):
            p.sadd("atm_alertas", ev["id_atm"])
            self.alertas.inc()
        p.execute()
        self.recibidos.inc()


class ContarClics(ConRedis):
    def __init__(self, host, port):
        super().__init__(host, port)
        self.recibidos = Metrics.counter("lumina", "clics_en_redis")

    def process(self, ev):
        p = self.r.pipeline(transaction=False)
        p.hincrby("clics:resumen", "total", 1)
        p.hincrby("clics:resumen", "requieren_asesor", int(ev.get("requiere_asesor_humano", 0)))
        p.execute()
        self.recibidos.inc()


def sumar_pares(pares):
    pares = list(pares)
    return sum(a for a, _ in pares), sum(b for _, b in pares)


class FormatearVentana(beam.DoFn):
    def __init__(self, run_id):
        self.run_id = run_id

    def process(self, elemento, ventana=beam.DoFn.WindowParam):
        (distrito, decision), (n, monto) = elemento
        yield {
            "run_id": self.run_id,
            "ventana_inicio": ventana.start.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat(),
            "ventana_fin": ventana.end.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat(),
            "id_distrito": distrito, "decision": decision, "transacciones": n, "monto_pen": round(monto, 2),
        }


def transformar(tx, geo, atm, clicks, cfg):
    """Grafo común a Dataflow y a las pruebas locales. Recibe PCollections de bytes."""
    host, port, run_id = cfg["redis_host"], cfg["redis_port"], cfg["run_id"]

    def parsear(pc, origen, campos):
        return pc | f"Validar {origen}" >> beam.ParDo(ParsearJson(origen, campos, run_id)).with_outputs("rechazado", main="ok")

    tx_p = parsear(tx, "tx", ["id_transaccion", "id_cliente", "id_distrito", "canal", "type", "amount", "isFraud", "epoch_ms"])
    geo_p = parsear(geo, "geo", ["id_cliente", "id_distrito_actual"])
    atm_p = parsear(atm, "atm", ["id_atm"])
    clk_p = parsear(clicks, "clicks", ["id_cliente"])

    geo_p.ok | "Redis GPS" >> beam.ParDo(GuardarGps(host, port))
    atm_p.ok | "Redis ATM" >> beam.ParDo(GuardarAtm(host, port))
    clk_p.ok | "Redis clics" >> beam.ParDo(ContarClics(host, port))

    decisiones = tx_p.ok | "Decidir con estado Redis" >> beam.ParDo(DecidirTx(host, port, cfg["perfiles_gcs"], run_id))

    agregado = (
        decisiones
        | "Ventana fija 1 min" >> beam.WindowInto(window.FixedWindows(60))
        | "Clave distrito y decision" >> beam.Map(lambda d: ((d["id_distrito"], d["decision"]), (1, d["monto_pen"])))
        | "Sumar por ventana" >> beam.CombinePerKey(sumar_pares)
        | "Formatear ventana" >> beam.ParDo(FormatearVentana(run_id))
    )
    rechazos = (tx_p.rechazado, geo_p.rechazado, atm_p.rechazado, clk_p.rechazado) | "Unir rechazos" >> beam.Flatten()
    return decisiones, agregado, rechazos


def escribir_bq(pc, nombre, tabla, esquema, parametros=None):
    return pc | nombre >> WriteToBigQuery(
        tabla,
        schema=esquema,
        method=WriteToBigQuery.Method.STREAMING_INSERTS,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters=parametros or {},
    )


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_host", required=True)
    ap.add_argument("--redis_port", type=int, default=6379)
    ap.add_argument("--perfiles_gcs", required=True)
    ap.add_argument("--run_id", default=datetime.now(timezone.utc).strftime("df%Y%m%d%H%M"))
    args, resto = ap.parse_known_args(argv)

    opciones = PipelineOptions(resto)
    opciones.view_as(StandardOptions).streaming = True
    opciones.view_as(SetupOptions).save_main_session = True
    proyecto = opciones.get_all_options()["project"]

    def sub(nombre):
        return f"projects/{proyecto}/subscriptions/{nombre}-sub-dataflow"

    cfg = {"redis_host": args.redis_host, "redis_port": args.redis_port, "perfiles_gcs": args.perfiles_gcs, "run_id": args.run_id}
    p = beam.Pipeline(options=opciones)
    decisiones, agregado, rechazos = transformar(
        p | "Pub/Sub transacciones" >> ReadFromPubSub(subscription=sub("transacciones")),
        p | "Pub/Sub geo" >> ReadFromPubSub(subscription=sub("geo")),
        p | "Pub/Sub atm" >> ReadFromPubSub(subscription=sub("atm")),
        p | "Pub/Sub clicks" >> ReadFromPubSub(subscription=sub("clicks")),
        cfg,
    )
    escribir_bq(
        decisiones, "BigQuery decisiones", f"{proyecto}:silver_banco.stream_decisiones_dataflow", ESQUEMA_DECISIONES,
        {"timePartitioning": {"type": "DAY", "field": "evento_ts"}, "clustering": {"fields": ["id_distrito", "decision"]}},
    )
    escribir_bq(agregado, "BigQuery agregado 1 min", f"{proyecto}:gold_fraude.agg_stream_distrito_1m", ESQUEMA_AGREGADO)
    escribir_bq(rechazos, "BigQuery rechazos", f"{proyecto}:silver_banco.stream_rechazos", ESQUEMA_RECHAZOS)

    # Un job streaming no termina solo: se envía y el lanzador sale. Se detiene con infra/apagar_y_verificar.ps1.
    resultado = p.run()
    job = getattr(resultado, "job_id", None)
    print(f"JOB_ID={job() if callable(job) else job}")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
