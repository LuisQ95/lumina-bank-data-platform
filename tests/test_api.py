"""Pruebas de la API sin GCP: BigQuery, Pub/Sub y Cloud Storage se reemplazan por dobles en memoria."""

import pytest

from lumina.api import app as modulo_app
from lumina.api import consultas, eventos

PERFILES = {
    "LUM-0000001": {"id_cliente": "LUM-0000001", "id_distrito": "Miraflores", "limite_diario_pen": 2500.0, "segmento": "RETAIL"},
    "LUM-0000002": {"id_cliente": "LUM-0000002", "id_distrito": "Miraflores", "limite_diario_pen": 9000.0, "segmento": "PREMIER"},
    "LUM-0000003": {"id_cliente": "LUM-0000003", "id_distrito": "Lince", "limite_diario_pen": 4000.0, "segmento": "PYME"},
}
TX_VALIDA = {"id_cliente": "LUM-0000001", "id_distrito": "San Isidro", "canal": "APP_MOVIL", "tipo_operacion": "TRANSFER", "monto_pen": "350.5"}


@pytest.fixture
def entorno(monkeypatch):
    """Cliente de pruebas con perfiles, Pub/Sub y BigQuery reemplazados por dobles en memoria.

    Returns:
        Tupla (cliente, eventos_publicados, consultas_ejecutadas).
    """
    publicados, consultas_hechas = [], []
    monkeypatch.setattr(eventos, "perfiles", lambda: PERFILES)
    monkeypatch.setattr(eventos, "publicar", lambda topico, ev: publicados.append((topico, ev)) or "msg-1")

    def ejecutar_falso(sql, **parametros):
        """Simula BigQuery: encuentra la decisión solo si el id termina en 'a'."""
        consultas_hechas.append((sql, parametros))
        if "id_transaccion = @id" in sql:
            return [{"id_transaccion": parametros["id"], "decision": "APROBADA", "filas_mismo_id": 1}] if parametros["id"].endswith("a") else []
        return [{"fila": 1}]

    monkeypatch.setattr(consultas, "ejecutar", ejecutar_falso)
    cliente = modulo_app.crear_app().test_client()
    return cliente, publicados, consultas_hechas


def test_transaccion_valida_se_publica_con_el_formato_de_dataflow(entorno):
    """Una transacción válida se publica en el tópico tx con todos los campos que exige Dataflow."""
    cliente, publicados, _ = entorno
    r = cliente.post("/api/transacciones", json=TX_VALIDA)
    assert r.status_code == 201
    topico, ev = publicados[0]
    assert topico == "tx"
    for campo in ["id_transaccion", "id_cliente", "id_distrito", "canal", "type", "amount", "isFraud", "epoch_ms"]:
        assert campo in ev
    assert ev["amount"] == 350.5 and ev["isFraud"] == 0
    assert eventos.PATRON_ID.match(ev["id_transaccion"])


@pytest.mark.parametrize("cambio", [
    {"id_cliente": "LUM-9999999"}, {"id_distrito": "Callao"}, {"canal": "FAX"}, {"tipo_operacion": "X"},
    {"monto_pen": "-5"}, {"monto_pen": "999999"}, {"monto_pen": "abc"},
])
def test_transaccion_invalida_devuelve_400_y_no_publica(entorno, cambio):
    """Cada campo inválido devuelve 400 y no publica nada en Pub/Sub."""
    cliente, publicados, _ = entorno
    r = cliente.post("/api/transacciones", json={**TX_VALIDA, **cambio})
    assert r.status_code == 400
    assert publicados == []


def test_reintento_conserva_id_y_hora_del_evento(entorno):
    """Un reintento conserva id_transaccion y epoch_ms para que Redis lo trate como duplicado."""
    cliente, publicados, _ = entorno
    original = cliente.post("/api/transacciones", json=TX_VALIDA).get_json()["evento"]
    cliente.post("/api/transacciones", json={**TX_VALIDA, "id_transaccion": original["id_transaccion"], "epoch_ms": original["epoch_ms"]})
    assert publicados[1][1]["id_transaccion"] == original["id_transaccion"]
    assert publicados[1][1]["epoch_ms"] == original["epoch_ms"]


def test_reintento_con_id_arbitrario_se_rechaza(entorno):
    """Un reintento con un id que no generó la API se rechaza."""
    cliente, publicados, _ = entorno
    r = cliente.post("/api/transacciones", json={**TX_VALIDA, "id_transaccion": "otro-id", "epoch_ms": 1})
    assert r.status_code == 400 and publicados == []


def test_gps_usa_el_distrito_registrado_si_no_se_indica(entorno):
    """Sin distrito, el GPS usa el distrito registrado del cliente."""
    cliente, publicados, _ = entorno
    r = cliente.post("/api/gps", json={"id_cliente": "LUM-0000003"})
    assert r.status_code == 201
    assert publicados[0] == ("geo", {**publicados[0][1], "id_cliente": "LUM-0000003", "id_distrito_actual": "Lince"})


def test_decision_pendiente_y_encontrada(entorno):
    """La decisión responde 404 mientras está pendiente, 200 cuando existe y 400 con un id inválido."""
    cliente, _, _ = entorno
    assert cliente.get("/api/decisiones/WEB-00000000000b").status_code == 404
    r = cliente.get("/api/decisiones/WEB-00000000000a")
    assert r.status_code == 200 and r.get_json()["filas_mismo_id"] == 1
    assert cliente.get("/api/decisiones/DROP TABLE").status_code == 400


def test_parametros_se_acotan_y_filtros_se_validan(entorno):
    """Los parámetros numéricos se acotan y los filtros fuera de catálogo devuelven 400."""
    cliente, _, hechas = entorno
    cliente.get("/api/decisiones/recientes?limite=100000")
    assert hechas[-1][1] == {"limite": 100}
    assert cliente.get("/api/liquidez/plan?distrito=Callao").status_code == 400
    assert cliente.get("/api/operacion/kpi?entidad=Entidad_99").status_code == 400
    assert cliente.get("/api/liquidez/plan?distrito=Lince").status_code == 200
    assert hechas[-1][1] == {"distrito": "Lince"}


def test_endpoints_gold_responden(entorno):
    """Todos los endpoints Gold responden 200."""
    cliente, _, _ = entorno
    for ruta in ["/api/fraude/distritos", "/api/operacion/kpi", "/api/atm/salud?solo_alertas=1", "/api/digital/experiencia", "/api/decisiones/resumen"]:
        assert cliente.get(ruta).status_code == 200, ruta


def test_clientes_incluye_limite_minimo_y_maximo_por_distrito(entorno):
    """Se eligen los clientes de límite mínimo y máximo por distrito, sin repetir."""
    cliente, _, _ = entorno
    ids = [c["id_cliente"] for c in cliente.get("/api/clientes").get_json()]
    assert ids == ["LUM-0000001", "LUM-0000002", "LUM-0000003"]


def test_estado_sin_memorystore_responde_503(entorno):
    """Sin REDIS_HOST el estado responde 503, y un cliente inexistente 404."""
    cliente, _, _ = entorno
    assert cliente.get("/api/estado/LUM-0000001").status_code == 503
    assert cliente.get("/api/estado/LUM-9999999").status_code == 404


def test_pagina_y_cabeceras_de_seguridad(entorno):
    """La página carga y lleva la política de seguridad de contenido."""
    cliente, _, _ = entorno
    r = cliente.get("/")
    assert r.status_code == 200 and b"Nueva transacci" in r.data
    assert "default-src 'self'" in r.headers["Content-Security-Policy"]
