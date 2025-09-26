"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: Kafka consumer example in Python demonstrating how to read messages from a Kafka topic.
"""

from kafka import KafkaConsumer
import json

# Configuration du consumer
consumer = KafkaConsumer(
    'mon-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Commencer depuis le début si pas d'offset
    enable_auto_commit=True,
    group_id='mon-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

print("Consumer démarré, en attente de messages...")

try:
    for message in consumer:
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        print(f"Value: {message.value}")
        print("-" * 50)
        
        # Traitement du message
        process_message(message.value)
        
except KeyboardInterrupt:
    print("Arrêt du consumer")
finally:
    consumer.close()

def process_message(data):
    """Traite le message reçu"""
    print(f"Traitement du message ID: {data.get('id')}")
    # Logique de traitement ici