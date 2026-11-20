"""Pruebas locales del pipeline de Dataflow, sin GCP ni costo.

Redis se reemplaza por fakeredis y el grafo corre en el runner local de Beam (FnApiRunner).

Uso:
    python -m pytest tests/test_dataflow_pipeline.py
"""

import json

import apache_beam as beam
import fakeredis
import pytest
from apache_beam.runners.portability.fn_api_runner import FnApiRunner
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from lumina.streaming import dataflow_pipeline as df

T0 = 1_800_000_000_000
SERVIDOR = fakeredis.FakeServer()


def tx(tid="T1", cid="C1", distrito="Lince", monto=100.0, ems=T0, fraude=0):
    """Arma un evento de transacción con valores por defecto modificables."""
    return {"id_transaccion": tid, "id_cliente": cid, "id_distrito": distrito, "canal": "ATM", "type": "CASH_OUT",
            "amount": monto, "isFraud": fraude, "epoch_ms": ems}


@pytest.fixture
def r():
    """Redis en memoria vacío para cada prueba."""
    cliente = fakeredis.FakeRedis(decode_responses=True)
    cliente.flushall()
    return cliente


def test_aprueba_tx_normal_y_guarda_estado(r):
    """Una transacción normal se aprueba y deja estado con expiración en Redis."""
    fila = df.evaluar_tx(r, tx(), 5000.0, "t", T0 + 250)
    assert fila["decision"] == "APROBADA"
    assert fila["latencia_e2e_ms"] == 250
    assert r.hget("limites:C1", "acumulado_dia_pen") == "100"
    assert r.ttl("limites:C1") > 0 and r.ttl("vel:C1") > 0 and r.ttl("dedup:T1") > 0


def test_descarta_duplicado_sin_sumar_dos_veces(r):
    """Un id repetido se descarta y no suma dos veces al acumulado."""
    assert df.evaluar_tx(r, tx(), 5000.0, "t", T0) is not None
    assert df.evaluar_tx(r, tx(), 5000.0, "t", T0) is None
    assert float(r.hget("limites:C1", "acumulado_dia_pen")) == 100.0


def test_rechaza_por_limite_acumulado(r):
    """Si el acumulado del día supera el límite, la transacción se rechaza."""
    df.evaluar_tx(r, tx("T1", monto=900.0), 1000.0, "t", T0)
    fila = df.evaluar_tx(r, tx("T2", monto=200.0, ems=T0 + 120_000), 1000.0, "t", T0)
    assert fila["decision"] == "RECHAZADA_LIMITE"


def test_salto_gps_sube_el_riesgo(r):
    """Un GPS en otro distrito suma 0.45 al riesgo y envía la transacción a revisión."""
    r.set("gps:C1", "Miraflores")
    fila = df.evaluar_tx(r, tx(distrito="Lince", monto=10000.0), 50000.0, "t", T0)
    assert fila["salto_gps"] == 1
    assert fila["riesgo"] == round(0.08 + 0.45 + 0.16, 3)
    assert fila["decision"] == "REVISION_FRAUDE"


def test_misma_posicion_gps_no_es_salto(r):
    """Si el GPS coincide con el distrito de la operación no hay salto."""
    r.set("gps:C1", "Lince")
    assert df.evaluar_tx(r, tx(), 5000.0, "t", T0)["salto_gps"] == 0


def test_ventanas_de_velocidad_usan_tiempo_del_evento(r):
    """Las ventanas de 1 y 5 minutos se calculan con la hora del evento."""
    df.evaluar_tx(r, tx("T1", ems=T0), 0.0, "t", T0)
    df.evaluar_tx(r, tx("T2", ems=T0 + 30_000), 0.0, "t", T0)
    fila = df.evaluar_tx(r, tx("T3", ems=T0 + 90_000), 0.0, "t", T0)
    assert (fila["vel_1m"], fila["vel_5m"]) == (2, 3)
    fila = df.evaluar_tx(r, tx("T4", ems=T0 + 400_000), 0.0, "t", T0)
    assert (fila["vel_1m"], fila["vel_5m"]) == (1, 1)


def test_evento_atrasado_no_altera_el_conteo(r):
    """Un evento que llega tarde se cuenta en su propia ventana."""
    df.evaluar_tx(r, tx("T1", ems=T0 + 30_000), 0.0, "t", T0)
    fila = df.evaluar_tx(r, tx("T2", ems=T0), 0.0, "t", T0)
    assert fila["vel_1m"] == 1


def test_alerta_atm():
    """La alerta de cajero se activa por efectivo bajo o temperatura alta y tolera valores nulos."""
    assert df.es_alerta_atm({"pct_efectivo": 10.0})
    assert df.es_alerta_atm({"temperatura_c": 40.0})
    assert not df.es_alerta_atm({"pct_efectivo": 50.0, "temperatura_c": 20.0})
    assert not df.es_alerta_atm({"pct_efectivo": None, "temperatura_c": None})


def test_grafo_completo_en_runner_local(tmp_path, monkeypatch):
    """El grafo completo decide, rechaza, agrega por ventana y escribe el estado en Redis."""
    monkeypatch.setattr(df, "crear_cliente", lambda h, p: fakeredis.FakeRedis(server=SERVIDOR, decode_responses=True))
    verificador = fakeredis.FakeRedis(server=SERVIDOR, decode_responses=True)
    verificador.flushall()

    perfiles = tmp_path / "perfiles.jsonl"
    perfiles.write_text(json.dumps({"id_cliente": "C1", "limite_diario_pen": 5000.0}) + "\n", encoding="utf-8")
    cfg = {"redis_host": "fake", "redis_port": 6379, "perfiles_gcs": str(perfiles), "run_id": "t"}

    def b(ev):
        """Serializa un evento como lo entrega Pub/Sub."""
        return json.dumps(ev).encode()

    with TestPipeline(runner=FnApiRunner()) as p:
        entrada_tx = p | "tx" >> beam.Create([b(tx("T1")), b(tx("T2", cid="C2")), b"no es json", b(json.dumps({"id_cliente": "x"}))])
        entrada_tx = entrada_tx | "tiempo" >> beam.Map(lambda m: TimestampedValue(m, T0 / 1000))
        entrada_geo = p | "geo" >> beam.Create([b({"id_cliente": "C9", "id_distrito_actual": "Lince"})])
        entrada_atm = p | "atm" >> beam.Create([b({"id_atm": "A1", "pct_efectivo": 5.0, "temperatura_c": None})])
        entrada_clk = p | "clk" >> beam.Create([b({"id_cliente": "C1", "requiere_asesor_humano": 1})])
        decisiones, agregado, rechazos = df.transformar(entrada_tx, entrada_geo, entrada_atm, entrada_clk, cfg)
        assert_that(decisiones | "ids" >> beam.Map(lambda d: d["id_transaccion"]), equal_to(["T1", "T2"]), label="decisiones")
        assert_that(rechazos | "origen" >> beam.Map(lambda d: d["origen"]), equal_to(["tx", "tx"]), label="rechazos")
        assert_that(agregado | "n" >> beam.Map(lambda d: (d["id_distrito"], d["decision"], d["transacciones"])),
                    equal_to([("Lince", "APROBADA", 2)]), label="agregado")

    assert verificador.get("gps:C9") == "Lince"
    assert verificador.hget("atm_health:A1", "pct_efectivo") == "5.0"
    assert verificador.sismember("atm_alertas", "A1")
    assert verificador.hget("clics:resumen", "requieren_asesor") == "1"
