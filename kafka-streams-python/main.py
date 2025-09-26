"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: Kafka Streams example in Python demonstrating stream processing with word count aggregation.
"""

from kafka import KafkaConsumer, KafkaProducer
import json
import threading
from collections import defaultdict
import time

class SimpleKafkaStreams:
    def __init__(self, input_topic, output_topic, bootstrap_servers=['localhost:9092']):
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='streams-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.word_counts = defaultdict(int)
        self.running = False
    
    def process_stream(self):
        """Traite le flux de messages"""
        self.running = True
        
        for message in self.consumer:
            if not self.running:
                break
                
            try:
                data = message.value
                
                # Transformation
                processed_data = self.transform_message(data)
                
                # Envoi vers le topic de sortie
                self.producer.send(self.output_topic, value=processed_data)
                
                # Agrégation (exemple: comptage de mots)
                self.aggregate_words(data.get('data', ''))
                
            except Exception as e:
                print(f"Erreur traitement: {e}")
    
    def transform_message(self, data):
        """Transforme le message"""
        transformed = data.copy()
        if 'data' in transformed:
            transformed['data'] = transformed['data'].upper()
            transformed['processed_at'] = time.time()
        return transformed
    
    def aggregate_words(self, text):
        """Agrège les mots (exemple simple)"""
        if text:
            words = text.lower().split()
            for word in words:
                self.word_counts[word] += 1
    
    def start(self):
        """Démarre le traitement"""
        print("Démarrage du stream processing...")
        
        # Thread pour publier les agrégations périodiquement
        aggregation_thread = threading.Thread(target=self.publish_aggregations)
        aggregation_thread.daemon = True
        aggregation_thread.start()
        
        # Traitement principal
        self.process_stream()
    
    def publish_aggregations(self):
        """Publie les agrégations périodiquement"""
        while self.running:
            time.sleep(30)  # Toutes les 30 secondes
            if self.word_counts:
                aggregation = {
                    'timestamp': time.time(),
                    'word_counts': dict(self.word_counts),
                    'total_words': sum(self.word_counts.values())
                }
                self.producer.send('word-counts-topic', aggregation)
                print(f"Agrégation publiée: {len(self.word_counts)} mots uniques")
    
    def stop(self):
        """Arrête le traitement"""
        self.running = False
        self.consumer.close()
        self.producer.close()

# Utilisation
if __name__ == "__main__":
    streams = SimpleKafkaStreams('input-topic', 'output-topic')
    try:
        streams.start()
    except KeyboardInterrupt:
        print("Arrêt du stream...")
        streams.stop()