"""Configuración central de Lumina Bank.

Todos los valores se pueden cambiar con variables de entorno o con un archivo `.env` en la raíz del
repositorio (ver `.env.example`). Ningún módulo tiene rutas o nombres de proyecto escritos a mano.

Attributes:
    RAIZ: Raíz del repositorio.
    PROJECT_ID: Proyecto de Google Cloud (GCP_PROJECT).
    REGION: Región de los recursos (GCP_REGION).
    GCLOUD: Ruta del CLI de Google Cloud.
    DATA_DIR: Carpeta de datos locales (LUMINA_DATA_DIR); no se versiona.
    REPORTES_DIR: Carpeta de reportes de evidencia (LUMINA_REPORTS_DIR).
    SALT: Sal del token SHA-256 de clientes en el pipeline SQL (LUMINA_SALT). En producción vendría de
        Secret Manager.
    BUCKET_LANDING: Bucket de llegada de archivos crudos.
    BUCKET_BRONZE: Bucket de la capa Bronze; también guarda los perfiles que usa Dataflow.
    TOPICS: Tópicos de Pub/Sub por tipo de evento.
    SUBSCRIPTIONS: Suscripciones de la ingesta inicial, sin entrega ordenada.
    DATAFLOW_SUBSCRIPTIONS: Suscripciones del job de Dataflow, sin orden; las ventanas usan la hora del evento.
    DATASETS: Datasets de BigQuery por capa.
    DISTRITOS: Los ocho distritos de Lima del alcance.
    COORDS_DISTRITOS: Centro aproximado de cada distrito, usado para la cobertura a menos de 8 km.
    ENTIDADES: Las doce entidades fusionadas.
    CANALES: Canales de atención.
"""

import os
import shutil
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]


def _cargar_env(ruta: Path = RAIZ / ".env") -> None:
    """Carga pares KEY=VALUE desde .env sin pisar variables ya definidas en el entorno.

    Args:
        ruta: Archivo .env; si no existe, no hace nada.
    """
    if not ruta.exists():
        return
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        os.environ.setdefault(clave.strip(), valor.strip().strip('"').strip("'"))


_cargar_env()


def _gcloud() -> str:
    """Ubica el CLI de Google Cloud.

    Returns:
        GCLOUD_BIN si está definido, si no el gcloud del PATH, si no la instalación por defecto de Windows.
    """
    if os.environ.get("GCLOUD_BIN"):
        return os.environ["GCLOUD_BIN"]
    for nombre in ("gcloud", "gcloud.cmd"):
        ruta = shutil.which(nombre)
        if ruta:
            return ruta
    win = Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Cloud SDK" / "google-cloud-sdk" / "bin" / "gcloud.cmd"
    return str(win) if win.exists() else "gcloud"


PROJECT_ID = os.environ.get("GCP_PROJECT", "lumina-bank-utec-2026")
REGION = os.environ.get("GCP_REGION", "us-central1")
GCLOUD = _gcloud()

DATA_DIR = os.environ.get("LUMINA_DATA_DIR", str(RAIZ / "data"))
REPORTES_DIR = os.environ.get("LUMINA_REPORTS_DIR", str(RAIZ / "reports"))
DATA_DOWNLOAD_DIR = os.path.join(DATA_DIR, "data_download")
DATA_GENERATE_DIR = os.path.join(DATA_DIR, "data_generate")
os.makedirs(REPORTES_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

SALT = os.environ.get("LUMINA_SALT", "demo-utec-2026")

BUCKET_LANDING = os.environ.get("LUMINA_BUCKET_LANDING", f"lumina-bank-landing-{PROJECT_ID}")
BUCKET_BRONZE = os.environ.get("LUMINA_BUCKET_BRONZE", f"lumina-bank-bronze-{PROJECT_ID}")

TOPICS = {"tx": "transacciones", "clicks": "clicks", "atm": "atm", "geo": "geo"}
SUBSCRIPTIONS = {k: f"{v}-sub-bronze" for k, v in TOPICS.items()}
DATAFLOW_SUBSCRIPTIONS = {k: f"{v}-sub-dataflow" for k, v in TOPICS.items()}

DATASETS = {
    "bronze": "bronze_banco",
    "silver": "silver_banco",
    "gold_fraude": "gold_fraude",
    "gold_liquidez": "gold_liquidez",
    "gold_operacion": "gold_operacion",
}

DISTRITOS = ["San Isidro", "Miraflores", "San Borja", "Santiago de Surco",
             "La Molina", "San Miguel", "Magdalena del Mar", "Lince"]

COORDS_DISTRITOS = {
    "San Isidro": (-12.0972, -77.0365),
    "Miraflores": (-12.1191, -77.0290),
    "San Borja": (-12.1070, -76.9990),
    "Santiago de Surco": (-12.1350, -76.9910),
    "La Molina": (-12.0780, -76.9410),
    "San Miguel": (-12.0770, -77.0880),
    "Magdalena del Mar": (-12.0910, -77.0680),
    "Lince": (-12.0840, -77.0360),
}

ENTIDADES = [f"Entidad_{i:02d}" for i in range(1, 13)]
CANALES = ["APP_MOVIL", "WEB_BANKING", "ATM", "POS_COMERCIO", "VENTANILLA"]
