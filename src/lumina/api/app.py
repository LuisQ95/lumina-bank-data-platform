"""API y página web de Lumina Bank: capa de exposición de los datos procesados.

Uso:
    python -m lumina.api.app                    Servidor local en http://127.0.0.1:8080
    gunicorn --bind :8080 lumina.api.app:app     Servidor de producción (Dockerfile de Cloud Run)

Endpoints de lectura sobre BigQuery:
    GET  /api/salud
    GET  /api/decisiones/recientes?limite=20     Decisiones en tiempo real escritas por Dataflow
    GET  /api/decisiones/resumen?horas=24
    GET  /api/decisiones/<id_transaccion>
    GET  /api/fraude/distritos?dias=30           Gold: fraude por distrito
    GET  /api/liquidez/plan?distrito=Miraflores  Gold: plan de liquidez del día siguiente
    GET  /api/operacion/kpi?entidad=Entidad_01   Gold: KPI operativos frente a la referencia SEC
    GET  /api/atm/salud?solo_alertas=1           Gold: salud y cobertura de cajeros
    GET  /api/digital/experiencia                Gold: experiencia digital

Endpoints de operación sobre Pub/Sub y Memorystore:
    GET  /api/clientes                           Clientes del core habilitados para la demo
    POST /api/gps                                Publica la ubicación del celular del cliente
    POST /api/transacciones                      Publica una transacción real en Pub/Sub
    GET  /api/estado/<id_cliente>                Estado del cliente en Memorystore, si hay ruta a la VPC
"""

import os

from flask import Flask, jsonify, render_template, request
from google.api_core.exceptions import GoogleAPIError

from lumina.api import consultas, estado_redis, eventos
from lumina.config import CANALES, DISTRITOS, ENTIDADES, PROJECT_ID


def entero(nombre: str, defecto: int, minimo: int, maximo: int) -> int:
    """Lee un parámetro entero de la URL y lo acota a un rango.

    Args:
        nombre: Nombre del parámetro.
        defecto: Valor si falta o no es numérico.
        minimo: Límite inferior.
        maximo: Límite superior.

    Returns:
        El valor acotado a [minimo, maximo].
    """
    try:
        valor = int(request.args.get(nombre, defecto))
    except ValueError:
        valor = defecto
    return max(minimo, min(maximo, valor))


def crear_app() -> Flask:
    """Crea la aplicación Flask con sus rutas, manejadores de error y cabeceras de seguridad.

    Returns:
        La aplicación lista para gunicorn o para el cliente de pruebas.
    """
    app = Flask(__name__)
    app.json.sort_keys = False

    @app.after_request
    def cabeceras_seguridad(resp):
        """Agrega cabeceras que bloquean contenido externo, marcos y detección de tipo."""
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["Referrer-Policy"] = "no-referrer"
        resp.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:"
        return resp

    @app.errorhandler(GoogleAPIError)
    def error_google(e):
        """Responde 502 si BigQuery, Pub/Sub o Cloud Storage fallan, sin exponer el detalle interno."""
        app.logger.exception("Error de Google Cloud")
        return jsonify(error="servicio de Google Cloud no disponible", detalle=type(e).__name__), 502

    @app.errorhandler(eventos.ErrorValidacion)
    def error_validacion(e):
        """Responde 400 con el motivo de la validación."""
        return jsonify(error=str(e)), 400

    @app.get("/")
    def inicio():
        """Página web de la demo."""
        return render_template("index.html", distritos=DISTRITOS, canales=CANALES, tipos=eventos.TIPOS_OPERACION, entidades=ENTIDADES)

    @app.get("/api/salud")
    def salud():
        """Estado del servicio, proyecto, conexión a Memorystore y revisión de Cloud Run."""
        return jsonify(estado="ok", proyecto=PROJECT_ID, memorystore_configurado=bool(estado_redis.REDIS_HOST),
                       revision=os.environ.get("K_REVISION", "local"))

    @app.get("/api/decisiones/recientes")
    def recientes():
        """Últimas decisiones de Dataflow; `limite` entre 1 y 100."""
        return jsonify(consultas.decisiones_recientes(entero("limite", 20, 1, 100)))

    @app.get("/api/decisiones/resumen")
    def resumen():
        """Conteo y latencias por tipo de decisión; `horas` entre 1 y 720."""
        return jsonify(consultas.resumen_decisiones(entero("horas", 24, 1, 24 * 30)))

    @app.get("/api/decisiones/<id_transaccion>")
    def decision(id_transaccion):
        """Decisión de una transacción: 200 si ya está en BigQuery, 404 mientras está pendiente."""
        if not eventos.PATRON_ID.match(id_transaccion):
            return jsonify(error="id_transaccion inválido"), 400
        fila = consultas.decision(id_transaccion)
        if fila is None:
            return jsonify(estado="pendiente", id_transaccion=id_transaccion), 404
        return jsonify(fila)

    @app.get("/api/fraude/distritos")
    def fraude():
        """Fraude por distrito; `dias` entre 1 y 365."""
        return jsonify(consultas.fraude_por_distrito(entero("dias", 30, 1, 365)))

    @app.get("/api/liquidez/plan")
    def liquidez():
        """Plan de liquidez del día siguiente, con filtro opcional `distrito`."""
        distrito = request.args.get("distrito")
        if distrito and distrito not in DISTRITOS:
            return jsonify(error=f"distrito debe ser uno de {DISTRITOS}"), 400
        return jsonify(consultas.plan_liquidez(distrito))

    @app.get("/api/operacion/kpi")
    def kpi():
        """KPI operativos por entidad, con filtro opcional `entidad`."""
        entidad = request.args.get("entidad")
        if entidad and entidad not in ENTIDADES:
            return jsonify(error=f"entidad debe ser una de {ENTIDADES}"), 400
        return jsonify(consultas.kpi_operacion(entidad))

    @app.get("/api/atm/salud")
    def atm():
        """Salud de cajeros; `solo_alertas=1` devuelve solo los que requieren atención."""
        return jsonify(consultas.salud_atm(request.args.get("solo_alertas") == "1"))

    @app.get("/api/digital/experiencia")
    def digital():
        """Experiencia digital por distrito y dispositivo."""
        return jsonify(consultas.experiencia_digital())

    @app.get("/api/clientes")
    def clientes():
        """Dos clientes por distrito, con el límite diario más bajo y el más alto, para mostrar ambos casos en la demo."""
        por_distrito = {}
        for p in sorted(eventos.perfiles().values(), key=lambda p: (p["limite_diario_pen"] or 0, p["id_cliente"])):
            por_distrito.setdefault(p["id_distrito"], []).append(p)
        elegidos = []
        for d in DISTRITOS:
            lista = por_distrito.get(d, [])
            elegidos += lista[:1] + lista[-1:] if len(lista) > 1 else lista
        return jsonify(elegidos)

    @app.post("/api/gps")
    def gps():
        """Publica en el tópico geo la ubicación del celular del cliente."""
        ev = eventos.construir_gps(request.get_json(silent=True) or {}, eventos.perfiles())
        mensaje = eventos.publicar("geo", ev)
        return jsonify(publicado=True, pubsub_message_id=mensaje, evento=ev), 201

    @app.post("/api/transacciones")
    def transaccion():
        """Publica una transacción en el tópico transacciones; Dataflow la decide."""
        ev = eventos.construir_transaccion(request.get_json(silent=True) or {}, eventos.perfiles())
        mensaje = eventos.publicar("tx", ev)
        return jsonify(publicado=True, pubsub_message_id=mensaje, evento=ev), 201

    @app.get("/api/estado/<id_cliente>")
    def estado(id_cliente):
        """Estado del cliente en Memorystore; 503 si el servicio no tiene ruta a la VPC."""
        if id_cliente not in eventos.perfiles():
            return jsonify(error="id_cliente no existe en el core"), 404
        tid = request.args.get("id_transaccion")
        if tid and not eventos.PATRON_ID.match(tid):
            return jsonify(error="id_transaccion inválido"), 400
        datos = estado_redis.estado_cliente(id_cliente, tid)
        if datos is None:
            return jsonify(error="Memorystore no está conectado a este servicio"), 503
        return jsonify(datos)

    return app


app = crear_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "8080")), debug=False)
