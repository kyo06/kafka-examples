"""
KSQLDB Data Producer - Python
Generates sample data for KSQLDB exercises
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Configuration du producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# Données d'exemple
users = [
    {'id': 'user1', 'name': 'Alice', 'email': 'alice@example.com'},
    {'id': 'user2', 'name': 'Bob', 'email': 'bob@example.com'},
    {'id': 'user3', 'name': 'Charlie', 'email': 'charlie@example.com'},
    {'id': 'user4', 'name': 'Diana', 'email': 'diana@example.com'},
    {'id': 'user5', 'name': 'Eve', 'email': 'eve@example.com'}
]

actions = ['login', 'logout', 'view_page', 'purchase', 'search']

def send_user_event(user_id, action):
    """Envoie un événement utilisateur"""
    event = {
        'id': user_id,
        'action': action,
        'timestamp': datetime.now().isoformat(),
        'session_id': f'session_{random.randint(1000, 9999)}'
    }
    producer.send('user-events', key=user_id, value=event)
    print(f"Sent user event: {event}")

def send_user_profile(user):
    """Envoie un profil utilisateur"""
    profile = {
        'id': user['id'],
        'name': user['name'],
        'email': user['email'],
        'registration_date': datetime.now().isoformat(),
        'country': random.choice(['FR', 'US', 'UK', 'DE', 'ES'])
    }
    producer.send('user-profiles', key=user['id'], value=profile)
    print(f"Sent user profile: {profile}")

def main():
    print("Starting KSQLDB data producer...")

    # Envoyer les profils utilisateurs (une fois)
    for user in users:
        send_user_profile(user)
        time.sleep(0.5)

    # Générer des événements continuellement
    try:
        while True:
            user = random.choice(users)
            action = random.choice(actions)
            send_user_event(user['id'], action)
            time.sleep(random.uniform(1, 3))  # 1-3 secondes entre les événements

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()