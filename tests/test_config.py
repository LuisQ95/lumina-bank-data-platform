"""Pruebas de lumina.config: los valores salen de variables de entorno y no dependen de la carpeta de ejecución."""

import importlib

import lumina.config as config


def test_valores_por_defecto():
    """Los valores por defecto definen proyecto, región, cinco datasets, 8 distritos y 12 entidades."""
    assert config.PROJECT_ID
    assert config.REGION
    assert set(config.DATASETS) == {"bronze", "silver", "gold_fraude", "gold_liquidez", "gold_operacion"}
    assert len(config.DISTRITOS) == 8 and len(config.ENTIDADES) == 12


def test_variables_de_entorno_tienen_prioridad(monkeypatch, tmp_path):
    """GCP_PROJECT, GCP_REGION y las carpetas se toman del entorno y los nombres derivados se recalculan."""
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
    """Cada tópico tiene su suscripción de Bronze y la de Dataflow."""
    for clave, topico in config.TOPICS.items():
        assert config.SUBSCRIPTIONS[clave] == f"{topico}-sub-bronze"
        assert config.DATAFLOW_SUBSCRIPTIONS[clave] == f"{topico}-sub-dataflow"


def test_raiz_es_el_repositorio():
    """RAIZ apunta a la raíz del repositorio."""
    assert (config.RAIZ / "pyproject.toml").exists()
