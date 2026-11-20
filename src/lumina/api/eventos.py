"""Transacciones originadas desde la aplicación web: validación y publicación en Pub/Sub.

Los eventos tienen el mismo formato que lee el job de Dataflow (lumina.streaming.dataflow_pipeline), así
que una transacción enviada desde la página recorre el pipeline real: Pub/Sub, Dataflow con estado en
Memorystore y BigQuery. Los clientes válidos son los perfiles que Dataflow usa para el límite diario.

Attributes:
    RUTA_PERFILES: Objeto de Cloud Storage con los perfiles, en el bucket Bronze.
    TIPOS_OPERACION: Tipos de operación del esquema PaySim.
    MONTO_MAXIMO_PEN: Monto máximo aceptado por transacción.
    PATRON_ID: Formato de los ids que genera la API; un reintento solo se acepta con este formato.
"""

import json
import re
import time
import uuid
from functools import lru_cache

from google.cloud import pubsub_v1, storage

from lumina.config import BUCKET_BRONZE, CANALES, DISTRITOS, PROJECT_ID, TOPICS

RUTA_PERFILES = "dataflow/perfiles_muestra.jsonl"
TIPOS_OPERACION = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]
MONTO_MAXIMO_PEN = 50_000.0
PATRON_ID = re.compile(r"^WEB-[0-9a-f]{12}$")


class ErrorValidacion(ValueError):
    """Datos de entrada inválidos; la API responde 400 y no publica nada."""


@lru_cache(maxsize=1)
def perfiles() -> dict[str, dict]:
    """Carga una vez los perfiles de clientes desde Cloud Storage.

    Returns:
        Perfiles indexados por id_cliente, con distrito, límite diario y segmento.
    """
    texto = storage.Client(project=PROJECT_ID).bucket(BUCKET_BRONZE).blob(RUTA_PERFILES).download_as_text()
    return {p["id_cliente"]: p for p in map(json.loads, texto.splitlines()) if p}


@lru_cache(maxsize=1)
def publicador() -> pubsub_v1.PublisherClient:
    """Devuelve un cliente de Pub/Sub reutilizado por todas las solicitudes del proceso."""
    return pubsub_v1.PublisherClient()


def publicar(clave_topico: str, evento: dict) -> str:
    """Publica un evento JSON y espera la confirmación de Pub/Sub.

    Args:
        clave_topico: Clave de lumina.config.TOPICS, por ejemplo "tx" o "geo".
        evento: Evento ya validado.

    Returns:
        El message_id asignado por Pub/Sub.
    """
    topico = publicador().topic_path(PROJECT_ID, TOPICS[clave_topico])
    return publicador().publish(topico, json.dumps(evento).encode("utf-8")).result(timeout=30)


def construir_transaccion(datos: dict, clientes: dict[str, dict], ahora_ms: int | None = None) -> dict:
    """Valida el cuerpo recibido y arma el evento de transacción.

    Si el cuerpo trae id_transaccion y epoch_ms, se trata como un reintento del mismo evento: conserva
    ambos valores para que Redis lo reconozca como duplicado.

    Args:
        datos: Cuerpo JSON de la solicitud.
        clientes: Perfiles válidos, indexados por id_cliente.
        ahora_ms: Hora del evento en milisegundos; por defecto, la hora actual del servidor.

    Returns:
        El evento con los campos que exige Dataflow.

    Raises:
        ErrorValidacion: Si algún campo no es válido.
    """
    id_cliente = str(datos.get("id_cliente", ""))
    if id_cliente not in clientes:
        raise ErrorValidacion("id_cliente no existe en el core")
    distrito = datos.get("id_distrito")
    if distrito not in DISTRITOS:
        raise ErrorValidacion(f"id_distrito debe ser uno de {DISTRITOS}")
    canal = datos.get("canal")
    if canal not in CANALES:
        raise ErrorValidacion(f"canal debe ser uno de {CANALES}")
    tipo = datos.get("tipo_operacion")
    if tipo not in TIPOS_OPERACION:
        raise ErrorValidacion(f"tipo_operacion debe ser uno de {TIPOS_OPERACION}")
    try:
        monto = round(float(datos.get("monto_pen")), 2)
    except (TypeError, ValueError):
        raise ErrorValidacion("monto_pen debe ser numérico") from None
    if not 0 < monto <= MONTO_MAXIMO_PEN:
        raise ErrorValidacion(f"monto_pen debe estar entre 0 y {MONTO_MAXIMO_PEN:,.0f}")

    id_tx = datos.get("id_transaccion")
    if id_tx is None:
        id_tx, epoch_ms = f"WEB-{uuid.uuid4().hex[:12]}", ahora_ms or int(time.time() * 1000)
    else:
        if not PATRON_ID.match(str(id_tx)):
            raise ErrorValidacion("id_transaccion inválido")
        try:
            epoch_ms = int(datos["epoch_ms"])
        except (KeyError, TypeError, ValueError):
            raise ErrorValidacion("un reintento necesita el epoch_ms original") from None

    return {
        "id_transaccion": id_tx, "id_cliente": id_cliente, "id_distrito": distrito, "canal": canal,
        "type": tipo, "amount": monto, "isFraud": 0, "epoch_ms": epoch_ms, "origen": "app_web",
    }


def construir_gps(datos: dict, clientes: dict[str, dict]) -> dict:
    """Valida y arma el evento de ubicación del celular del cliente.

    Args:
        datos: Cuerpo JSON con id_cliente y, opcionalmente, id_distrito.
        clientes: Perfiles válidos, indexados por id_cliente.

    Returns:
        El evento GPS. Sin id_distrito, usa el distrito registrado del cliente.

    Raises:
        ErrorValidacion: Si el cliente o el distrito no son válidos.
    """
    id_cliente = str(datos.get("id_cliente", ""))
    if id_cliente not in clientes:
        raise ErrorValidacion("id_cliente no existe en el core")
    distrito = datos.get("id_distrito") or clientes[id_cliente]["id_distrito"]
    if distrito not in DISTRITOS:
        raise ErrorValidacion(f"id_distrito debe ser uno de {DISTRITOS}")
    return {"geo_id": f"GEO-WEB-{uuid.uuid4().hex[:8]}", "id_cliente": id_cliente, "id_distrito_actual": distrito}
