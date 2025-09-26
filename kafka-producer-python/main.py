"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: Kafka producer example in Python demonstrating how to send messages to a Kafka topic.
"""

from kafka import KafkaProducer
import json
import time
from datetime import datetime
import hashlib

# Fonction partitioner (DefaultPartitioner: hash de clé + round-robin)
def default_partitioner(key_bytes, all_partitions, available_partitions):
    if key_bytes:
        # Partitionnement basé sur hash de la clé
        partition = int(hashlib.md5(key_bytes).hexdigest(), 16) % len(all_partitions)
        return partition
    else:
        # Round-robin si pas de clé
        return available_partitions[0]  # Simplifié; pour un vrai round-robin, utiliser un état

# Alternatives (à définir et utiliser à la place de default_partitioner):
# def round_robin_partitioner(key_bytes, all_partitions, available_partitions):
#     # Distribution cyclique, ignorant la clé
#     return available_partitions[0]  # Nécessite un état pour un vrai round-robin
#
# def uniform_sticky_partitioner(key_bytes, all_partitions, available_partitions):
#     # Sticky partitioning: batch vers la même partition, puis changement
#     return available_partitions[0]  # Nécessite un état pour la gestion

# Configuration du producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    # Configuration pour la fiabilité
    acks='all',  # Attendre l'accusé de réception de tous les réplicas
    retries=3,
    max_in_flight_requests_per_connection=1,
    # Configuration du partitioner
    partitioner=default_partitioner
)

def send_message(topic, key, message):
    try:
        # Envoi asynchrone
        future = producer.send(topic, key=key, value=message)
        # Optionnel: attendre la confirmation
        record_metadata = future.get(timeout=10)
        print(f"Message envoyé vers {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
    except Exception as e:
        print(f"Erreur lors de l'envoi: {e}")

# Exemple d'utilisation
for i in range(10):
    message = {
        'id': i,
        'timestamp': datetime.now().isoformat(),
        'data': f'Message numéro {i}'
    }
    send_message('mon-topic', i, message)
    time.sleep(1)

producer.close()