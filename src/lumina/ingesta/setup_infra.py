"""Setup de Infraestructura Base en Google Cloud (Costo $0.00)
- Crea Buckets en Cloud Storage (Landing y Bronze).
- Crea Tópicos y Suscripciones en Cloud Pub/Sub.
- Crea Datasets en BigQuery (Medallion: Bronze, Silver, Gold).
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
    STREAM_SUBSCRIPTIONS,
    SUBSCRIPTIONS,
    TOPICS,
)

sys.stdout.reconfigure(encoding='utf-8')

def setup_storage():
    print("\n--- 1. CONFIGURANDO CLOUD STORAGE ---")
    client = storage.Client(project=PROJECT_ID)
    buckets = [BUCKET_LANDING, BUCKET_BRONZE]

    for b_name in buckets:
        try:
            client.create_bucket(b_name, location=REGION)
            print(f"  [CREADO] Bucket: gs://{b_name} en {REGION}")
        except Conflict:
            print(f"  [OK] Bucket gs://{b_name} ya existe.")
        except Exception as e:
            print(f"  [ERROR] Bucket {b_name}: {e}")

def setup_pubsub():
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

def setup_pubsub_streaming():
    """Suscripciones con entrega ordenada. Pub/Sub no permite activarla en una suscripción existente."""
    print("\n--- 2B. SUSCRIPCIONES STREAMING CON ENTREGA ORDENADA ---")
    subscriber = pubsub_v1.SubscriberClient()
    for key, sub_id in STREAM_SUBSCRIPTIONS.items():
        request = {
            "name": subscriber.subscription_path(PROJECT_ID, sub_id),
            "topic": f"projects/{PROJECT_ID}/topics/{TOPICS[key]}",
            "enable_message_ordering": True,
            "ack_deadline_seconds": 30,
        }
        try:
            subscriber.create_subscription(request=request)
            print(f"  [CREADO] Suscripción ordenada: {sub_id}")
        except AlreadyExists:
            print(f"  [OK] Suscripción {sub_id} ya existe.")


def setup_bigquery():
    print("\n--- 3. CONFIGURANDO BIGQUERY (DATASETS) ---")
    client = bigquery.Client(project=PROJECT_ID)

    for key, ds_id in DATASETS.items():
        dataset_ref = bigquery.DatasetReference(PROJECT_ID, ds_id)
        dataset = bigquery.Dataset(dataset_ref)
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
    print("==================================================")
    print(" Inicializando Infraestructura Base - Lumina Bank")
    print(f" Proyecto: {PROJECT_ID} | Región: {REGION}")
    print("==================================================")
    setup_storage()
    setup_pubsub()
    setup_pubsub_streaming()
    setup_bigquery()
    print("\n[ÉXITO] Infraestructura base provisionada correctamente con costo $0.00.")

if __name__ == "__main__":
    main()

