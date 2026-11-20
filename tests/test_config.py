"""La configuración se toma de variables de entorno y no depende de la carpeta desde donde se ejecuta."""

import importlib

import lumina.config as config


def test_valores_por_defecto():
    assert config.PROJECT_ID
    assert config.REGION
    assert set(config.DATASETS) == {"bronze", "silver", "gold_fraude", "gold_liquidez", "gold_operacion"}
    assert len(config.DISTRITOS) == 8 and len(config.ENTIDADES) == 12


def test_variables_de_entorno_tienen_prioridad(monkeypatch, tmp_path):
    monkeypatch.setenv("GCP_PROJECT", "proyecto-prueba")
    monkeypatch.setenv("GCP_REGION", "europe-west1")
    monkeypatch.setenv("LUMINA_REPORTS_DIR", str(tmp_path / "rep"))
    monkeypatch.setenv("LUMINA_DATA_DIR", str(tmp_path / "dat"))
    try:
        c = importlib.reload(config)
        assert c.PROJECT_ID == "proyecto-prueba"
        assert c.REGION == "europe-west1"
        assert c.BUCKET_BRONZE == "lumina-bank-bronze-proyecto-prueba"
        assert (tmp_path / "rep").is_dir() and (tmp_path / "dat").is_dir()
    finally:
        monkeypatch.undo()
        importlib.reload(config)


def test_suscripciones_derivadas_de_los_topicos():
    for clave, topico in config.TOPICS.items():
        assert config.STREAM_SUBSCRIPTIONS[clave] == f"{topico}-sub-stream"
        assert config.DATAFLOW_SUBSCRIPTIONS[clave] == f"{topico}-sub-dataflow"


def test_raiz_es_el_repositorio():
    assert (config.RAIZ / "pyproject.toml").exists()
