"""Infraestructura base de Lumina Bank en Google Cloud, sin costo fijo.

Crea los buckets Landing y Bronze en Cloud Storage, los tópicos y suscripciones de Pub/Sub y los
datasets Medallion de BigQuery. Es idempotente: lo que ya existe se informa y se conserva.

Uso:
    python -m lumina.ingesta.setup_infra
"""

import sys

from google.api_core.exceptions import AlreadyExists, Conflict
from google.cloud import bigquery, pubsub_v1, storage

from lumina.config import (
    BUCKET_BRONZE,
    BUCKET_LANDING,
    DATASETS,
    PROJECT_ID,
    REGION,
    SUBSCRIPTIONS,
    TOPICS,
)

sys.stdout.reconfigure(encoding="utf-8")


def setup_storage():
    """Crea los buckets Landing y Bronze en la región del proyecto."""
    print("\n--- 1. CONFIGURANDO CLOUD STORAGE ---")
    client = storage.Client(project=PROJECT_ID)
    for b_name in [BUCKET_LANDING, BUCKET_BRONZE]:
        try:
            client.create_bucket(b_name, location=REGION)
            print(f"  [CREADO] Bucket: gs://{b_name} en {REGION}")
        except Conflict:
            print(f"  [OK] Bucket gs://{b_name} ya existe.")
        except Exception as e:
            print(f"  [ERROR] Bucket {b_name}: {e}")


def setup_pubsub():
    """Crea un tópico por tipo de evento y su suscripción de ingesta a Bronze."""
    print("\n--- 2. CONFIGURANDO CLOUD PUB/SUB ---")
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    for key, topic_id in TOPICS.items():
        topic_path = publisher.topic_path(PROJECT_ID, topic_id)
        try:
            publisher.create_topic(request={"name": topic_path})
            print(f"  [CREADO] Tópico Pub/Sub: projects/{PROJECT_ID}/topics/{topic_id}")
        except AlreadyExists:
            print(f"  [OK] Tópico {topic_id} ya existe.")
        except Exception as e:
            print(f"  [ERROR] Tópico {topic_id}: {e}")

        sub_id = SUBSCRIPTIONS[key]
        sub_path = subscriber.subscription_path(PROJECT_ID, sub_id)
        try:
            subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
            print(f"  [CREADO] Suscripción: {sub_id} -> {topic_id}")
        except AlreadyExists:
            print(f"  [OK] Suscripción {sub_id} ya existe.")
        except Exception as e:
            print(f"  [ERROR] Suscripción {sub_id}: {e}")


def setup_bigquery():
    """Crea los datasets Bronze, Silver y Gold en la región del proyecto."""
    print("\n--- 3. CONFIGURANDO BIGQUERY (DATASETS) ---")
    client = bigquery.Client(project=PROJECT_ID)
    for key, ds_id in DATASETS.items():
        dataset = bigquery.Dataset(bigquery.DatasetReference(PROJECT_ID, ds_id))
        dataset.location = REGION
        dataset.description = f"Capa {key.upper()} para plataforma Lumina Bank"
        try:
            client.create_dataset(dataset, timeout=30)
            print(f"  [CREADO] Dataset BigQuery: {PROJECT_ID}.{ds_id} ({REGION})")
        except Conflict:
            print(f"  [OK] Dataset {ds_id} ya existe.")
        except Exception as e:
            print(f"  [ERROR] Dataset {ds_id}: {e}")


def main():
    """Provisiona toda la infraestructura base en orden."""
    print("==================================================")
    print(" Inicializando Infraestructura Base - Lumina Bank")
    print(f" Proyecto: {PROJECT_ID} | Región: {REGION}")
    print("==================================================")
    setup_storage()
    setup_pubsub()
    setup_bigquery()
    print("\n[ÉXITO] Infraestructura base provisionada correctamente con costo $0.00.")


if __name__ == "__main__":
    main()
