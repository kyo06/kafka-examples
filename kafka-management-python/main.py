"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: Kafka management example in Python demonstrating how to create topics using the admin client.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

def create_topic(bootstrap_servers, topic_name, num_partitions=3, replication_factor=1):
    """Crée un nouveau topic Kafka"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='admin-client'
        )
        
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' créé avec succès")
        
    except KafkaError as e:
        print(f"Erreur lors de la création du topic: {e}")
    finally:
        admin_client.close()

# Exemple d'utilisation
if __name__ == "__main__":
    create_topic(['localhost:9092'], 'mon-nouveau-topic', num_partitions=3, replication_factor=1)