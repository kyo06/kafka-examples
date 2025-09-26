"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: Kafka consumer group example in Python demonstrating load balancing across multiple consumer instances.
"""

from kafka import KafkaConsumer
import json
import uuid
import time

# Générer un ID unique pour cette instance de consumer
consumer_id = str(uuid.uuid4())[:8]

# Configuration du consumer avec groupe
consumer = KafkaConsumer(
    'mon-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer-group-demo',  # Même groupe pour tous les consumers
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    consumer_timeout_ms=1000
)

print(f"Consumer {consumer_id} démarré, en attente de messages...")

try:
    while True:
        # Poll for messages
        message_batch = consumer.poll(timeout_ms=1000)

        if not message_batch:
            print(f"Consumer {consumer_id}: Aucun message reçu, attente...")
            time.sleep(1)
            continue

        for topic_partition, messages in message_batch.items():
            for message in messages:
                print(f"Consumer {consumer_id} - Topic: {message.topic}")
                print(f"Consumer {consumer_id} - Partition: {message.partition}")
                print(f"Consumer {consumer_id} - Offset: {message.offset}")
                print(f"Consumer {consumer_id} - Key: {message.key}")
                print(f"Consumer {consumer_id} - Value: {message.value}")
                print("-" * 60)

                # Simulation de traitement
                process_message(message.value, consumer_id)

except KeyboardInterrupt:
    print(f"Arrêt du consumer {consumer_id}")
finally:
    consumer.close()

def process_message(data, consumer_id):
    """Traite le message reçu"""
    print(f"Consumer {consumer_id} - Traitement du message ID: {data.get('id')}")
    # Simulation de traitement
    time.sleep(0.1)