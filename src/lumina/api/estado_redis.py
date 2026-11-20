"""Lectura del estado caliente que Dataflow escribe en Memorystore for Redis.

Solo funciona cuando el servicio de Cloud Run tiene salida a la VPC y REDIS_HOST está definido.
Memorystore se crea y elimina por sesión, así que si no está disponible las funciones devuelven None
y el resto de la API sigue operando.

Attributes:
    REDIS_HOST: IP privada de Memorystore; vacía si el servicio no tiene ruta a la VPC.
    REDIS_PORT: Puerto de Redis.
"""

import os

import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


def _conexion() -> redis.Redis | None:
    """Abre una conexión con timeouts cortos, o devuelve None si no hay REDIS_HOST."""
    if not REDIS_HOST:
        return None
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_timeout=1.5, socket_connect_timeout=1.5)


def estado_cliente(id_cliente: str, id_transaccion: str | None = None) -> dict | None:
    """Lee las claves de un cliente que usa la regla de decisión.

    Args:
        id_cliente: Cliente del core.
        id_transaccion: Si se indica, verifica si su clave de deduplicación existe.

    Returns:
        Límites y acumulado del día, último distrito GPS, transacciones en la ventana de 5 minutos y
        presencia de la clave dedup. None si Memorystore no está configurado o no responde.
    """
    r = _conexion()
    if r is None:
        return None
    try:
        p = r.pipeline(transaction=False)
        p.hgetall(f"limites:{id_cliente}")
        p.get(f"gps:{id_cliente}")
        p.zcard(f"vel:{id_cliente}")
        p.exists(f"dedup:{id_transaccion}" if id_transaccion else "dedup:-")
        limites, gps, ventana, dedup = p.execute()
        return {
            "redis_host": REDIS_HOST,
            "limites": limites,
            "gps_distrito": gps,
            "transacciones_ultimos_5_min": ventana,
            "transaccion_registrada_en_dedup": bool(dedup) if id_transaccion else None,
        }
    except redis.RedisError:
        return None
    finally:
        r.close()
