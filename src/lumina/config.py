"""
Configuración central de Lumina Bank.

Todos los valores se pueden cambiar con variables de entorno o con un archivo `.env` en la raíz del
repositorio (ver `.env.example`). Ningún módulo debe tener rutas o nombres de proyecto escritos a mano.
"""

import os
import shutil
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]


def _cargar_env(ruta: Path = RAIZ / ".env") -> None:
    """Carga KEY=VALUE desde .env sin pisar variables ya definidas en el entorno."""
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
    """Ruta del CLI de Google Cloud: GCLOUD_BIN, el PATH o la instalación por defecto de Windows."""
    if os.environ.get("GCLOUD_BIN"):
        return os.environ["GCLOUD_BIN"]
    for nombre in ("gcloud", "gcloud.cmd"):
        ruta = shutil.which(nombre)
        if ruta:
            return ruta
    win = Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Cloud SDK" / "google-cloud-sdk" / "bin" / "gcloud.cmd"
    return str(win) if win.exists() else "gcloud"


# --------------------------------------------------------------------- proyecto y rutas
PROJECT_ID = os.environ.get("GCP_PROJECT", "lumina-bank-utec-2026")
REGION = os.environ.get("GCP_REGION", "us-central1")
GCLOUD = _gcloud()

DATA_DIR = os.environ.get("LUMINA_DATA_DIR", str(RAIZ / "data"))
REPORTES_DIR = os.environ.get("LUMINA_REPORTS_DIR", str(RAIZ / "reports"))
DATA_DOWNLOAD_DIR = os.path.join(DATA_DIR, "data_download")
DATA_GENERATE_DIR = os.path.join(DATA_DIR, "data_generate")
os.makedirs(REPORTES_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Sal para el token SHA-256 de clientes en el pipeline SQL. En producción vendría de Secret Manager.
SALT = os.environ.get("LUMINA_SALT", "demo-utec-2026")

# --------------------------------------------------------------------- Cloud Storage
BUCKET_LANDING = os.environ.get("LUMINA_BUCKET_LANDING", f"lumina-bank-landing-{PROJECT_ID}")
BUCKET_BRONZE = os.environ.get("LUMINA_BUCKET_BRONZE", f"lumina-bank-bronze-{PROJECT_ID}")

# --------------------------------------------------------------------- Pub/Sub
TOPICS = {"tx": "transacciones", "clicks": "clicks", "atm": "atm", "geo": "geo"}

# Suscripciones de la ingesta inicial (sin entrega ordenada).
SUBSCRIPTIONS = {k: f"{v}-sub-bronze" for k, v in TOPICS.items()}

# Suscripciones del consumidor streaming local, con entrega ordenada. Pub/Sub no permite activar el
# orden después de crear una suscripción, por eso son distintas de las *-sub-bronze.
STREAM_SUBSCRIPTIONS = {k: f"{v}-sub-stream" for k, v in TOPICS.items()}

# Suscripciones del job de Dataflow (sin orden; las ventanas usan la hora del evento).
DATAFLOW_SUBSCRIPTIONS = {k: f"{v}-sub-dataflow" for k, v in TOPICS.items()}

# --------------------------------------------------------------------- BigQuery
DATASETS = {
    "bronze": "bronze_banco",
    "silver": "silver_banco",
    "gold_fraude": "gold_fraude",
    "gold_liquidez": "gold_liquidez",
    "gold_operacion": "gold_operacion",
}

# --------------------------------------------------------------------- negocio
DISTRITOS = ["San Isidro", "Miraflores", "San Borja", "Santiago de Surco",
             "La Molina", "San Miguel", "Magdalena del Mar", "Lince"]

# Centro aproximado de cada distrito (Lima Metropolitana), usado para la cobertura < 8 km.
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
