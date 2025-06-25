import os
import json
from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1
from app.models.ingestion_model import IngestionPayload


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "jenni-461712")
PUBSUB_TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "jennipro-dev-supplier-inventory")



publisher = pubsub_v1.PublisherClient()
def send_to_pubsub(data: IngestionPayload, topic: str = PUBSUB_TOPIC_ID):
    if publisher is None:
        return

    topic_path = publisher.topic_path(GCP_PROJECT_ID, topic)

    try:
        message_bytes = data.json().encode("utf-8")
        future = publisher.publish(topic_path, message_bytes)
    except GoogleAPIError as e:
        raise e
