# Exemples Kafka - Producer, Consumer, Streams et KSQLDB

## 1. Kafka Producer

### Python (kafka-python)

```python
from kafka import KafkaProducer
import json
import time
from datetime import datetime

# Configuration du producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    # Configuration pour la fiabilité
    acks='all',  # Attendre l'accusé de réception de tous les réplicas
    retries=3,
    max_in_flight_requests_per_connection=1
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
```

### Java (Apache Kafka Client)

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerExample {
    private KafkaProducer<String, String> producer;
    private ObjectMapper objectMapper = new ObjectMapper();

    public KafkaProducerExample() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, String key, Object message) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonMessage);
            
            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Erreur lors de l'envoi: " + exception.getMessage());
                } else {
                    System.out.println("Message envoyé vers " + metadata.topic() + 
                                     " partition " + metadata.partition() + 
                                     " offset " + metadata.offset());
                }
            });
        } catch (Exception e) {
            System.err.println("Erreur: " + e.getMessage());
        }
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducerExample producer = new KafkaProducerExample();
        
        for (int i = 0; i < 10; i++) {
            Message message = new Message(i, System.currentTimeMillis(), "Message numéro " + i);
            producer.sendMessage("mon-topic", String.valueOf(i), message);
            Thread.sleep(1000);
        }
        
        producer.close();
    }
}

class Message {
    public int id;
    public long timestamp;
    public String data;
    
    public Message(int id, long timestamp, String data) {
        this.id = id;
        this.timestamp = timestamp;
        this.data = data;
    }
}
```

### Batch Processing

#### Python (kafka-python)

```python
from kafka import KafkaProducer
import json
from datetime import datetime

# Configuration pour le batch processing
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    batch_size=16384,  # Taille du batch en octets
    linger_ms=10,      # Attendre 10ms pour accumuler les messages
    buffer_memory=33554432,  # 32MB de buffer
    compression_type='gzip'  # Compression
)

def send_batch_messages(topic, messages):
    """Envoie un lot de messages"""
    futures = []
    
    for message in messages:
        future = producer.send(topic, key=message['id'], value=message)
        futures.append(future)
    
    # Attendre la confirmation de tous les messages
    for future in futures:
        try:
            record_metadata = future.get(timeout=10)
            print(f"Message envoyé: partition {record_metadata.partition}, offset {record_metadata.offset}")
        except Exception as e:
            print(f"Erreur envoi: {e}")
    
    producer.flush()  # Forcer l'envoi des messages restants

# Exemple d'utilisation
messages = [
    {'id': i, 'timestamp': datetime.now().isoformat(), 'data': f'Batch message {i}'}
    for i in range(100)
]

send_batch_messages('batch-topic', messages)
producer.close()
```

#### Java (Apache Kafka Client)

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class BatchProducerExample {
    private KafkaProducer<String, String> producer;
    
    public BatchProducerExample() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432L);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        
        producer = new KafkaProducer<>(props);
    }
    
    public void sendBatchMessages(String topic, List<Message> messages) {
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        
        for (Message message : messages) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topic, 
                String.valueOf(message.id), 
                "{\"id\":" + message.id + ",\"data\":\"" + message.data + "\"}"
            );
            futures.add(producer.send(record));
        }
        
        // Attendre les confirmations
        for (Future<RecordMetadata> future : futures) {
            try {
                RecordMetadata metadata = future.get();
                System.out.println("Message envoyé: partition " + metadata.partition() + ", offset " + metadata.offset());
            } catch (Exception e) {
                System.err.println("Erreur envoi: " + e.getMessage());
            }
        }
        
        producer.flush();
    }
    
    public void close() {
        producer.close();
    }
    
    public static void main(String[] args) {
        BatchProducerExample producer = new BatchProducerExample();
        
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            messages.add(new Message(i, System.currentTimeMillis(), "Batch message " + i));
        }
        
        producer.sendBatchMessages("batch-topic", messages);
        producer.close();
    }
}
```

### Error Handling

#### Python (kafka-python)

```python
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import json
import time
from datetime import datetime

class ResilientProducer:
    def __init__(self, bootstrap_servers, dead_letter_topic='dead-letter-topic'):
        self.dead_letter_topic = dead_letter_topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            acks='all',
            retries=3,
            retry_backoff_ms=100,
            max_in_flight_requests_per_connection=1
        )
        
        # Producer pour dead letter queue
        self.dlq_producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def send_with_retry(self, topic, key, message, max_retries=3):
        """Envoie un message avec gestion d'erreur et DLQ"""
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                future = self.producer.send(topic, key=key, value=message)
                record_metadata = future.get(timeout=10)
                print(f"Message envoyé avec succès: {record_metadata}")
                return True
                
            except KafkaTimeoutError as e:
                print(f"Timeout envoi (tentative {attempt + 1}): {e}")
                last_exception = e
                time.sleep(0.1 * (2 ** attempt))  # Backoff exponentiel
                
            except KafkaError as e:
                print(f"Erreur Kafka (tentative {attempt + 1}): {e}")
                last_exception = e
                break  # Erreur non retry-able
        
        # Envoi vers DLQ si échec
        try:
            dlq_message = {
                'original_topic': topic,
                'original_key': key,
                'original_message': message,
                'error': str(last_exception),
                'timestamp': datetime.now().isoformat()
            }
            self.dlq_producer.send(self.dead_letter_topic, value=dlq_message)
            print("Message envoyé vers DLQ")
        except Exception as dlq_error:
            print(f"Erreur DLQ: {dlq_error}")
        
        return False
    
    def close(self):
        self.producer.close()
        self.dlq_producer.close()

# Exemple d'utilisation
producer = ResilientProducer(['localhost:9092'])

for i in range(10):
    message = {
        'id': i,
        'data': f'Message {i}',
        'timestamp': datetime.now().isoformat()
    }
    producer.send_with_retry('resilient-topic', i, message)

producer.close()
```

#### Java (Apache Kafka Client)

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ResilientProducerExample {
    private KafkaProducer<String, String> producer;
    private KafkaProducer<String, String> dlqProducer;
    private static final String DEAD_LETTER_TOPIC = "dead-letter-topic";
    
    public ResilientProducerExample() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        
        producer = new KafkaProducer<>(props);
        
        // Producer pour DLQ
        dlqProducer = new KafkaProducer<>(props);
    }
    
    public boolean sendWithRetry(String topic, String key, String message, int maxRetries) {
        Exception lastException = null;
        
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
                
                Future<RecordMetadata> future = producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("Erreur envoi: " + exception.getMessage());
                        } else {
                            System.out.println("Message envoyé: partition " + metadata.partition() + ", offset " + metadata.offset());
                        }
                    }
                });
                
                RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
                return true;
                
            } catch (TimeoutException e) {
                System.err.println("Timeout envoi (tentative " + (attempt + 1) + "): " + e.getMessage());
                lastException = e;
                try {
                    Thread.sleep(100L * (1L << attempt)); // Backoff exponentiel
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            } catch (Exception e) {
                System.err.println("Erreur Kafka (tentative " + (attempt + 1) + "): " + e.getMessage());
                lastException = e;
                break;
            }
        }
        
        // Envoi vers DLQ
        try {
            String dlqMessage = String.format(
                "{\"original_topic\":\"%s\",\"original_key\":\"%s\",\"original_message\":\"%s\",\"error\":\"%s\",\"timestamp\":%d}",
                topic, key, message, lastException.getMessage(), System.currentTimeMillis()
            );
            dlqProducer.send(new ProducerRecord<>(DEAD_LETTER_TOPIC, dlqMessage));
            System.out.println("Message envoyé vers DLQ");
        } catch (Exception dlqError) {
            System.err.println("Erreur DLQ: " + dlqError.getMessage());
        }
        
        return false;
    }
    
    public void close() {
        producer.close();
        dlqProducer.close();
    }
    
    public static void main(String[] args) {
        ResilientProducerExample producer = new ResilientProducerExample();
        
        for (int i = 0; i < 10; i++) {
            String message = "{\"id\":" + i + ",\"data\":\"Message " + i + "\",\"timestamp\":" + System.currentTimeMillis() + "}";
            producer.sendWithRetry("resilient-topic", String.valueOf(i), message, 3);
        }
        
        producer.close();
    }
}
```

### Advanced Configurations

#### Python (kafka-python)

```python
from kafka import KafkaProducer
import json
from datetime import datetime

# Producer avec configurations avancées
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    
    # Configurations avancées
    compression_type='lz4',  # Compression LZ4
    max_request_size=1048576,  # 1MB max par requête
    request_timeout_ms=30000,  # Timeout 30s
    metadata_max_age_ms=300000,  # Refresh metadata toutes les 5min
    
    # Partitionnement personnalisé
    partitioner=lambda key, all_partitions, available_partitions: hash(key) % len(available_partitions),
    
    # Sécurité (exemple)
    # security_protocol='SASL_SSL',
    # sasl_mechanism='PLAIN',
    # sasl_plain_username='user',
    # sasl_plain_password='password'
)

def send_with_advanced_config(topic, key, message):
    """Envoi avec configurations avancées"""
    try:
        future = producer.send(topic, key=key, value=message)
        record_metadata = future.get(timeout=30)
        print(f"Message envoyé avec compression: partition {record_metadata.partition}, offset {record_metadata.offset}")
    except Exception as e:
        print(f"Erreur: {e}")

# Exemple d'utilisation
for i in range(5):
    message = {
        'id': i,
        'data': 'A' * 1000,  # Message plus gros pour tester la compression
        'timestamp': datetime.now().isoformat()
    }
    send_with_advanced_config('advanced-topic', i, message)

producer.close()
```

#### Java (Apache Kafka Client)

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class AdvancedProducerExample {
    private KafkaProducer<String, String> producer;
    
    public AdvancedProducerExample() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        // Configurations avancées
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 300000L);
        
        // Partitionneur personnalisé
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        
        producer = new KafkaProducer<>(props);
    }
    
    public void sendMessage(String topic, String key, String message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            Future<RecordMetadata> future = producer.send(record);
            
            RecordMetadata metadata = future.get();
            System.out.println("Message envoyé avec compression: partition " + metadata.partition() + ", offset " + metadata.offset());
        } catch (Exception e) {
            System.err.println("Erreur: " + e.getMessage());
        }
    }
    
    public void close() {
        producer.close();
    }
    
    public static void main(String[] args) {
        AdvancedProducerExample producer = new AdvancedProducerExample();
        
        for (int i = 0; i < 5; i++) {
            String message = "{\"id\":" + i + ",\"data\":\"" + "A".repeat(1000) + "\",\"timestamp\":" + System.currentTimeMillis() + "}";
            producer.sendMessage("advanced-topic", String.valueOf(i), message);
        }
        
        producer.close();
    }
}

// Partitionneur personnalisé
class CustomPartitioner implements org.apache.kafka.clients.producer.Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return Math.abs(key.hashCode()) % cluster.partitionsForTopic(topic).size();
    }
    
    @Override
    public void close() {}
    
    @Override
    public void configure(Map<String, ?> configs) {}
}
```

### Real-World Scenarios

#### Python (kafka-python) - Logging System

```python
from kafka import KafkaProducer
import json
import logging
from datetime import datetime
import socket
import threading

class KafkaLoggingHandler(logging.Handler):
    """Handler de logging qui envoie les logs vers Kafka"""
    
    def __init__(self, topic, bootstrap_servers=['localhost:9092']):
        super().__init__()
        self.topic = topic
        self.hostname = socket.gethostname()
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            acks='1',  # Fiabilité suffisante pour les logs
            compression_type='gzip'
        )
        
        # Thread pour l'envoi asynchrone
        self.queue = []
        self.lock = threading.Lock()
        self.worker = threading.Thread(target=self._send_logs, daemon=True)
        self.worker.start()
    
    def emit(self, record):
        """Émet un log vers Kafka"""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'hostname': self.hostname,
            'thread': record.threadName,
            'file': record.filename,
            'line': record.lineno
        }
        
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        with self.lock:
            self.queue.append(log_entry)
    
    def _send_logs(self):
        """Envoie les logs accumulés"""
        while True:
            with self.lock:
                if self.queue:
                    logs_to_send = self.queue[:]
                    self.queue.clear()
                else:
                    logs_to_send = []
            
            for log_entry in logs_to_send:
                try:
                    self.producer.send(self.topic, key=log_entry['timestamp'], value=log_entry)
                except Exception as e:
                    print(f"Erreur envoi log: {e}")
            
            self.producer.flush()
            threading.Event().wait(1)  # Flush toutes les secondes
    
    def close(self):
        self.producer.close()
        super().close()

# Configuration du logging
logger = logging.getLogger('my_app')
logger.setLevel(logging.INFO)

kafka_handler = KafkaLoggingHandler('app-logs')
logger.addHandler(kafka_handler)

# Exemple d'utilisation
logger.info("Application démarrée")
logger.warning("Avertissement de test")
logger.error("Erreur de test", exc_info=True)

# Attendre que les logs soient envoyés
import time
time.sleep(2)
kafka_handler.close()
```

#### Java (Apache Kafka Client) - Metrics Collection

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class MetricsProducerExample {
    private KafkaProducer<String, String> producer;
    private Timer metricsTimer;
    
    public MetricsProducerExample() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        
        producer = new KafkaProducer<>(props);
        
        // Collecte des métriques toutes les 30 secondes
        metricsTimer = new Timer(true);
        metricsTimer.scheduleAtFixedRate(new MetricsTask(), 0, 30000);
    }
    
    private class MetricsTask extends TimerTask {
        private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        
        @Override
        public void run() {
            try {
                long heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
                long heapMax = memoryBean.getHeapMemoryUsage().getMax();
                double cpuLoad = osBean.getSystemLoadAverage();
                
                String metricsJson = String.format(
                    "{\"timestamp\":%d,\"heap_used\":%d,\"heap_max\":%d,\"cpu_load\":%.2f,\"hostname\":\"%s\"}",
                    System.currentTimeMillis(),
                    heapUsed,
                    heapMax,
                    cpuLoad,
                    java.net.InetAddress.getLocalHost().getHostName()
                );
                
                ProducerRecord<String, String> record = new ProducerRecord<>("system-metrics", "metrics", metricsJson);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Erreur envoi métriques: " + exception.getMessage());
                    }
                });
                
            } catch (Exception e) {
                System.err.println("Erreur collecte métriques: " + e.getMessage());
            }
        }
    }
    
    public void close() {
        metricsTimer.cancel();
        producer.close();
    }
    
    public static void main(String[] args) throws InterruptedException {
        MetricsProducerExample metricsProducer = new MetricsProducerExample();
        
        // Laisser tourner pendant 2 minutes
        Thread.sleep(120000);
        
        metricsProducer.close();
        System.out.println("Collecte de métriques arrêtée");
    }
}
```

## 2. Kafka Consumer

### Python (kafka-python)

```python
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
```

### Java (Apache Kafka Client)

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerExample {
    private KafkaConsumer<String, String> consumer;
    private ObjectMapper objectMapper = new ObjectMapper();

    public KafkaConsumerExample() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mon-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        consumer = new KafkaConsumer<>(props);
    }

    public void consume(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        
        System.out.println("Consumer démarré, en attente de messages...");
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Topic: " + record.topic());
                    System.out.println("Partition: " + record.partition());
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Key: " + record.key());
                    System.out.println("Value: " + record.value());
                    System.out.println("-".repeat(50));
                    
                    processMessage(record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("Erreur: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private void processMessage(String jsonMessage) {
        try {
            Message message = objectMapper.readValue(jsonMessage, Message.class);
            System.out.println("Traitement du message ID: " + message.id);
            // Logique de traitement ici
        } catch (Exception e) {
            System.err.println("Erreur lors du parsing: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        KafkaConsumerExample consumer = new KafkaConsumerExample();
        consumer.consume("mon-topic");
    }
}
```

### Batch Processing

#### Python (kafka-python)

```python
from kafka import KafkaConsumer
import json
import time

# Consumer configuré pour traiter des lots
consumer = KafkaConsumer(
    'batch-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # Commit manuel pour contrôle précis
    group_id='batch-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    max_poll_records=100,  # Traiter jusqu'à 100 messages par poll
    fetch_max_bytes=5242880,  # 5MB max par fetch
    fetch_max_wait_ms=500  # Attendre jusqu'à 500ms pour accumuler
)

def process_batch(messages):
    """Traite un lot de messages"""
    batch_data = []
    
    for message in messages:
        try:
            data = message.value
            batch_data.append(data)
            print(f"Message traité: {data['id']}")
        except Exception as e:
            print(f"Erreur traitement message: {e}")
    
    # Traitement par lot (ex: insertion en base)
    if batch_data:
        print(f"Traitement par lot de {len(batch_data)} messages")
        # Ici: logique de traitement par lot
        bulk_insert_to_database(batch_data)
        
        # Commit manuel après traitement réussi
        consumer.commit()

def bulk_insert_to_database(data):
    """Simulation d'insertion en base par lot"""
    print(f"Insertion de {len(data)} enregistrements en base")

print("Consumer batch démarré...")

try:
    while True:
        # Poll pour un lot de messages
        message_batch = consumer.poll(timeout_ms=1000)
        
        if message_batch:
            for topic_partition, messages in message_batch.items():
                process_batch(messages)
        else:
            print("Aucun message reçu, attente...")
            time.sleep(1)
            
except KeyboardInterrupt:
    print("Arrêt du consumer")
finally:
    consumer.close()
```

#### Java (Apache Kafka Client)

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.ArrayList;

public class BatchConsumerExample {
    private KafkaConsumer<String, String> consumer;
    private ObjectMapper objectMapper = new ObjectMapper();
    
    public BatchConsumerExample() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 5242880);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        
        consumer = new KafkaConsumer<>(props);
    }
    
    public void consumeBatch(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        
        System.out.println("Consumer batch démarré...");
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (!records.isEmpty()) {
                    List<Message> batchData = new ArrayList<>();
                    
                    records.forEach(record -> {
                        try {
                            Message message = objectMapper.readValue(record.value(), Message.class);
                            batchData.add(message);
                            System.out.println("Message traité: " + message.id);
                        } catch (Exception e) {
                            System.err.println("Erreur traitement message: " + e.getMessage());
                        }
                    });
                    
                    // Traitement par lot
                    if (!batchData.isEmpty()) {
                        System.out.println("Traitement par lot de " + batchData.size() + " messages");
                        bulkInsertToDatabase(batchData);
                        
                        // Commit manuel
                        consumer.commitSync();
                    }
                } else {
                    System.out.println("Aucun message reçu, attente...");
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            System.err.println("Erreur: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
    
    private void bulkInsertToDatabase(List<Message> data) {
        System.out.println("Insertion de " + data.size() + " enregistrements en base");
        // Logique d'insertion par lot ici
    }
    
    public static void main(String[] args) {
        BatchConsumerExample consumer = new BatchConsumerExample();
        consumer.consumeBatch("batch-topic");
    }
}
```

### Error Handling

#### Python (kafka-python)

```python
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import time

class ResilientConsumer:
    def __init__(self, topic, group_id, bootstrap_servers=['localhost:9092']):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=group_id,
            value_deserializer=self.safe_json_deserializer,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
            max_poll_interval_ms=300000  # 5 minutes pour traiter
        )
        
        # Topic pour messages défaillants
        self.dead_letter_topic = f"{topic}-dlq"
        self.dlq_producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def safe_json_deserializer(self, message):
        """Désérialiseur JSON avec gestion d'erreur"""
        try:
            return json.loads(message.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"Erreur désérialisation: {e}")
            return {'error': 'deserialization_failed', 'raw_message': message.decode('utf-8', errors='replace')}
    
    def process_message_with_retry(self, message, max_retries=3):
        """Traite un message avec retry et DLQ"""
        data = message.value
        
        if 'error' in data:
            print("Message défaillant détecté, envoi vers DLQ")
            self.send_to_dlq(message, "deserialization_error")
            return
        
        for attempt in range(max_retries):
            try:
                self.process_business_logic(data)
                self.consumer.commit()  # Commit seulement si traitement réussi
                return
            except Exception as e:
                print(f"Erreur traitement (tentative {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    print("Échec définitif, envoi vers DLQ")
                    self.send_to_dlq(message, str(e))
                    # Ne pas committer, le message sera retraité ou skipped
                else:
                    time.sleep(0.1 * (2 ** attempt))  # Backoff
    
    def process_business_logic(self, data):
        """Logique métier (simulation)"""
        if data.get('id', 0) % 10 == 0:
            raise Exception("Erreur simulée pour test")
        print(f"Traitement réussi: {data.get('id')}")
    
    def send_to_dlq(self, original_message, error_reason):
        """Envoi vers Dead Letter Queue"""
        dlq_message = {
            'original_topic': original_message.topic,
            'original_partition': original_message.partition,
            'original_offset': original_message.offset,
            'original_key': original_message.key,
            'original_value': original_message.value,
            'error_reason': error_reason,
            'timestamp': time.time()
        }
        
        try:
            self.dlq_producer.send(self.dead_letter_topic, value=dlq_message)
            self.dlq_producer.flush()
        except Exception as e:
            print(f"Erreur envoi DLQ: {e}")
    
    def consume_resiliently(self):
        """Consommation avec résilience"""
        print("Consumer résilient démarré...")
        
        try:
            while True:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self.process_message_with_retry(message)
                        
        except KeyboardInterrupt:
            print("Arrêt du consumer")
        finally:
            self.consumer.close()
            self.dlq_producer.close()

# Utilisation
consumer = ResilientConsumer('resilient-topic', 'resilient-group')
consumer.consume_resiliently()
```

#### Java (Apache Kafka Client)

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

public class ResilientConsumerExample {
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> dlqProducer;
    private ObjectMapper objectMapper = new ObjectMapper();
    private String deadLetterTopic;
    
    public ResilientConsumerExample(String topic, String groupId) {
        this.deadLetterTopic = topic + "-dlq";
        
        // Configuration consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        consumer = new KafkaConsumer<>(consumerProps);
        
        // Configuration DLQ producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        dlqProducer = new KafkaProducer<>(producerProps);
    }
    
    public void consumeResiliently(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        
        System.out.println("Consumer résilient démarré...");
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                records.forEach(record -> {
                    processMessageWithRetry(record, 3);
                });
            }
        } catch (Exception e) {
            System.err.println("Erreur consumer: " + e.getMessage());
        } finally {
            consumer.close();
            dlqProducer.close();
        }
    }
    
    private void processMessageWithRetry(ConsumerRecord<String, String> record, int maxRetries) {
        try {
            Message message = objectMapper.readValue(record.value(), Message.class);
            
            for (int attempt = 0; attempt < maxRetries; attempt++) {
                try {
                    processBusinessLogic(message);
                    consumer.commitSync(); // Commit seulement si réussi
                    return;
                } catch (Exception e) {
                    System.err.println("Erreur traitement (tentative " + (attempt + 1) + "): " + e.getMessage());
                    if (attempt == maxRetries - 1) {
                        sendToDLQ(record, e.getMessage());
                    } else {
                        Thread.sleep(100L * (1L << attempt)); // Backoff
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Erreur désérialisation: " + e.getMessage());
            sendToDLQ(record, "deserialization_error");
        }
    }
    
    private void processBusinessLogic(Message message) throws Exception {
        if (message.id % 10 == 0) {
            throw new Exception("Erreur simulée pour test");
        }
        System.out.println("Traitement réussi: " + message.id);
    }
    
    private void sendToDLQ(ConsumerRecord<String, String> originalRecord, String errorReason) {
        try {
            Map<String, Object> dlqMessage = new HashMap<>();
            dlqMessage.put("original_topic", originalRecord.topic());
            dlqMessage.put("original_partition", originalRecord.partition());
            dlqMessage.put("original_offset", originalRecord.offset());
            dlqMessage.put("original_key", originalRecord.key());
            dlqMessage.put("original_value", originalRecord.value());
            dlqMessage.put("error_reason", errorReason);
            dlqMessage.put("timestamp", System.currentTimeMillis());
            
            String dlqJson = objectMapper.writeValueAsString(dlqMessage);
            ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(deadLetterTopic, dlqJson);
            
            dlqProducer.send(dlqRecord);
            dlqProducer.flush();
            
            System.out.println("Message envoyé vers DLQ");
        } catch (Exception e) {
            System.err.println("Erreur envoi DLQ: " + e.getMessage());
        }
    }
    
    public static void main(String[] args) {
        ResilientConsumerExample consumer = new ResilientConsumerExample("resilient-topic", "resilient-group");
        consumer.consumeResiliently("resilient-topic");
    }
}
```

### Advanced Configurations

#### Python (kafka-python)

```python
from kafka import KafkaConsumer
import json
import ssl

# Consumer avec configurations avancées
consumer = KafkaConsumer(
    'advanced-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='advanced-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    
    # Configurations avancées
    fetch_min_bytes=1024,  # Attendre au moins 1KB
    fetch_max_wait_ms=1000,  # Attendre max 1s
    max_partition_fetch_bytes=1048576,  # 1MB par partition
    session_timeout_ms=30000,
    heartbeat_interval_ms=3000,
    
    # Sécurité (exemple)
    # security_protocol='SASL_SSL',
    # sasl_mechanism='PLAIN',
    # sasl_plain_username='user',
    # sasl_plain_password='password',
    # ssl_cafile='/path/to/ca.pem',
    # ssl_certfile='/path/to/client.pem',
    # ssl_keyfile='/path/to/client.key'
    
    # Métriques
    # metrics_sample_window_ms=30000,
    # metrics_num_samples=2
)

def advanced_message_processing(message):
    """Traitement avancé avec métriques"""
    data = message.value
    
    # Métriques personnalisées
    processing_start = time.time()
    
    # Traitement
    result = process_data(data)
    
    processing_time = time.time() - processing_start
    
    # Log des métriques
    print(f"Message traité en {processing_time:.3f}s - Partition: {message.partition}, Offset: {message.offset}")
    
    return result

def process_data(data):
    """Logique de traitement avancée"""
    # Simulation de traitement complexe
    return data

print("Consumer avancé démarré...")

try:
    for message in consumer:
        advanced_message_processing(message)
        
except KeyboardInterrupt:
    print("Arrêt du consumer")
finally:
    consumer.close()
```

#### Java (Apache Kafka Client)

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Metric;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AdvancedConsumerExample {
    private KafkaConsumer<String, String> consumer;
    private ObjectMapper objectMapper = new ObjectMapper();
    
    public AdvancedConsumerExample() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "advanced-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        // Configurations avancées
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        
        // Métriques
        props.put(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 30000);
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 2);
        
        consumer = new KafkaConsumer<>(props);
    }
    
    public void consumeAdvanced(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        
        System.out.println("Consumer avancé démarré...");
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                records.forEach(record -> {
                    long startTime = System.nanoTime();
                    
                    try {
                        Message message = objectMapper.readValue(record.value(), Message.class);
                        processData(message);
                        
                        long processingTime = System.nanoTime() - startTime;
                        System.out.printf("Message traité en %.3fms - Partition: %d, Offset: %d%n",
                            processingTime / 1_000_000.0, record.partition(), record.offset());
                        
                    } catch (Exception e) {
                        System.err.println("Erreur traitement: " + e.getMessage());
                    }
                });
                
                // Affichage périodique des métriques
                if (Math.random() < 0.01) { // ~1% des polls
                    printMetrics();
                }
            }
        } catch (Exception e) {
            System.err.println("Erreur: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
    
    private void processData(Message message) {
        // Logique de traitement avancée
        System.out.println("Traitement de: " + message.id);
    }
    
    private void printMetrics() {
        Metrics metrics = consumer.metrics();
        System.out.println("=== Métriques Consumer ===");
        
        for (Metric metric : metrics.metrics().values()) {
            if (metric.metricName().name().contains("records") || 
                metric.metricName().name().contains("bytes") ||
                metric.metricName().name().contains("latency")) {
                System.out.printf("%s: %.2f%n", metric.metricName().name(), (Double) metric.metricValue());
            }
        }
    }
    
    public static void main(String[] args) {
        AdvancedConsumerExample consumer = new AdvancedConsumerExample();
        consumer.consumeAdvanced("advanced-topic");
    }
}
```

### Real-World Scenarios

#### Python (kafka-python) - Data Pipeline Consumer

```python
from kafka import KafkaConsumer, KafkaProducer
import json
import psycopg2
import redis
import time
from datetime import datetime

class DataPipelineConsumer:
    def __init__(self, input_topic, output_topic, db_config, redis_config):
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        # Consumer
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='pipeline-consumer-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Producer pour résultats
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Connexions aux systèmes externes
        self.db_conn = psycopg2.connect(**db_config)
        self.redis_conn = redis.Redis(**redis_config)
        
        # Cache local
        self.cache = {}
    
    def enrich_data(self, data):
        """Enrichit les données avec cache et DB"""
        user_id = data.get('user_id')
        
        # Vérifier cache Redis
        cached_user = self.redis_conn.get(f"user:{user_id}")
        if cached_user:
            user_info = json.loads(cached_user)
        else:
            # Requête DB
            with self.db_conn.cursor() as cursor:
                cursor.execute("SELECT name, email FROM users WHERE id = %s", (user_id,))
                result = cursor.fetchone()
                if result:
                    user_info = {'name': result[0], 'email': result[1]}
                    # Mettre en cache
                    self.redis_conn.setex(f"user:{user_id}", 3600, json.dumps(user_info))
                else:
                    user_info = {'name': 'Unknown', 'email': 'unknown@example.com'}
        
        data['user_info'] = user_info
        return data
    
    def validate_data(self, data):
        """Valide les données"""
        required_fields = ['user_id', 'event_type', 'timestamp']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Champ requis manquant: {field}")
        
        if not isinstance(data.get('timestamp'), (int, float)):
            raise ValueError("Timestamp invalide")
        
        return True
    
    def process_pipeline(self):
        """Pipeline de traitement complet"""
        print("Pipeline démarré...")
        
        try:
            while True:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Validation
                            self.validate_data(message.value)
                            
                            # Enrichissement
                            enriched_data = self.enrich_data(message.value)
                            
                            # Transformation
                            processed_data = self.transform_data(enriched_data)
                            
                            # Envoi vers topic de sortie
                            self.producer.send(self.output_topic, value=processed_data)
                            
                            # Commit
                            self.consumer.commit()
                            
                            print(f"Message pipeline traité: {message.value.get('id')}")
                            
                        except Exception as e:
                            print(f"Erreur pipeline: {e}")
                            # Ici: envoi vers DLQ si nécessaire
                            self.consumer.commit()  # Skip le message défaillant
                
                # Flush producer périodiquement
                self.producer.flush()
                
        except KeyboardInterrupt:
            print("Arrêt du pipeline")
        finally:
            self.consumer.close()
            self.producer.close()
            self.db_conn.close()
            self.redis_conn.close()
    
    def transform_data(self, data):
        """Transformation finale"""
        data['processed_at'] = datetime.now().isoformat()
        data['pipeline_version'] = '1.0'
        return data

# Configuration
db_config = {
    'host': 'localhost',
    'database': 'pipeline_db',
    'user': 'pipeline_user',
    'password': 'password'
}

redis_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}

# Utilisation
pipeline = DataPipelineConsumer('raw-events', 'processed-events', db_config, redis_config)
pipeline.process_pipeline()
```

#### Java (Apache Kafka Client) - Event Processing System

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class EventProcessingConsumer {
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private ObjectMapper objectMapper = new ObjectMapper();
    private HikariDataSource dataSource;
    private JedisPool jedisPool;
    private Map<String, Object> localCache = new ConcurrentHashMap<>();
    
    public EventProcessingConsumer(String inputTopic, String outputTopic, 
                                 HikariDataSource ds, JedisPool jp) {
        this.dataSource = ds;
        this.jedisPool = jp;
        
        // Consumer config
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "event-processor-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        consumer = new KafkaConsumer<>(consumerProps);
        
        // Producer config
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        producer = new KafkaProducer<>(producerProps);
    }
    
    public void processEvents(String inputTopic, String outputTopic) {
        consumer.subscribe(Arrays.asList(inputTopic));
        
        System.out.println("Event processor démarré...");
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                records.forEach(record -> {
                    try {
                        Map<String, Object> event = objectMapper.readValue(record.value(), Map.class);
                        
                        // Validation
                        validateEvent(event);
                        
                        // Enrichissement
                        Map<String, Object> enrichedEvent = enrichEvent(event);
                        
                        // Transformation
                        Map<String, Object> processedEvent = transformEvent(enrichedEvent);
                        
                        // Envoi
                        String processedJson = objectMapper.writeValueAsString(processedEvent);
                        ProducerRecord<String, String> outputRecord = new ProducerRecord<>(outputTopic, processedJson);
                        producer.send(outputRecord);
                        
                        consumer.commitSync();
                        
                        System.out.println("Event traité: " + event.get("id"));
                        
                    } catch (Exception e) {
                        System.err.println("Erreur traitement event: " + e.getMessage());
                        consumer.commitSync(); // Skip
                    }
                });
                
                producer.flush();
            }
        } catch (Exception e) {
            System.err.println("Erreur processor: " + e.getMessage());
        } finally {
            consumer.close();
            producer.close();
        }
    }
    
    private void validateEvent(Map<String, Object> event) throws Exception {
        String[] requiredFields = {"user_id", "event_type", "timestamp"};
        for (String field : requiredFields) {
            if (!event.containsKey(field)) {
                throw new Exception("Champ requis manquant: " + field);
            }
        }
    }
    
    private Map<String, Object> enrichEvent(Map<String, Object> event) {
        String userId = (String) event.get("user_id");
        
        try (Jedis jedis = jedisPool.getResource()) {
            String cachedUser = jedis.get("user:" + userId);
            Map<String, Object> userInfo;
            
            if (cachedUser != null) {
                userInfo = objectMapper.readValue(cachedUser, Map.class);
            } else {
                // DB query
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement stmt = conn.prepareStatement("SELECT name, email FROM users WHERE id = ?")) {
                    
                    stmt.setString(1, userId);
                    ResultSet rs = stmt.executeQuery();
                    
                    if (rs.next()) {
                        userInfo = new HashMap<>();
                        userInfo.put("name", rs.getString("name"));
                        userInfo.put("email", rs.getString("email"));
                        
                        // Cache
                        jedis.setex("user:" + userId, 3600, objectMapper.writeValueAsString(userInfo));
                    } else {
                        userInfo = new HashMap<>();
                        userInfo.put("name", "Unknown");
                        userInfo.put("email", "unknown@example.com");
                    }
                }
            }
            
            event.put("user_info", userInfo);
        } catch (Exception e) {
            System.err.println("Erreur enrichissement: " + e.getMessage());
        }
        
        return event;
    }
    
    private Map<String, Object> transformEvent(Map<String, Object> event) {
        event.put("processed_at", System.currentTimeMillis());
        event.put("processor_version", "1.0");
        return event;
    }
    
    public static void main(String[] args) {
        // Configuration DB
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:postgresql://localhost:5432/event_db");
        ds.setUsername("event_user");
        ds.setPassword("password");
        
        // Configuration Redis
        JedisPool jp = new JedisPool("localhost", 6379);
        
        EventProcessingConsumer processor = new EventProcessingConsumer(
            "raw-events", "processed-events", ds, jp);
        
        processor.processEvents("raw-events", "processed-events");
    }
}
```

## 3. Kafka Streams

### Java (Kafka Streams)

```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Properties;

public class KafkaStreamsExample {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mon-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Stream principal
        KStream<String, String> sourceStream = builder.stream("input-topic");

        // Transformation et filtrage
        KStream<String, String> processedStream = sourceStream
            .filter((key, value) -> value != null && !value.isEmpty())
            .mapValues(value -> {
                try {
                    Message message = objectMapper.readValue(value, Message.class);
                    // Transformation du message
                    message.data = message.data.toUpperCase();
                    return objectMapper.writeValueAsString(message);
                } catch (Exception e) {
                    System.err.println("Erreur transformation: " + e.getMessage());
                    return value;
                }
            });

        // Envoi vers un topic de sortie
        processedStream.to("output-topic");

        // Agrégation par fenêtre temporelle
        KTable<Windowed<String>, Long> wordCounts = sourceStream
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\s+")))
            .groupBy((key, word) -> word)
            .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
            .count(Materialized.as("counts-store"));

        // Envoi des agrégations
        wordCounts.toStream()
            .map((windowedKey, count) -> 
                new KeyValue<>(windowedKey.key(), "Count: " + count))
            .to("word-counts-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        // Gestion gracieuse de l'arrêt
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        
        streams.start();
        System.out.println("Kafka Streams application démarrée");
    }
}
```

### Python (kafka-python avec logique de streaming)

```python
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
                self.producer.send(self.output_topic, processed_data)
                
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
```

### Batch Processing (Windowed Operations)

#### Java (Kafka Streams)

```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Grouped;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Properties;

public class BatchProcessingStreams {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "batch-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000); // Commit toutes les 10s

        StreamsBuilder builder = new StreamsBuilder();

        // Stream d'événements utilisateur
        KStream<String, String> userEvents = builder.stream("user-events");

        // Agrégation par fenêtre temporelle - Comptage d'événements par utilisateur
        KTable<Windowed<String>, Long> userEventCounts = userEvents
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).grace(Duration.ofSeconds(30)))
            .count(Materialized.as("user-event-counts-store"));

        // Conversion en stream pour traitement par lot
        KStream<Windowed<String>, Long> windowedCounts = userEventCounts.toStream();

        // Traitement par lot des fenêtres fermées
        windowedCounts
            .filter((windowedKey, count) -> count > 10) // Seulement les fenêtres avec > 10 événements
            .mapValues((windowedKey, count) -> {
                try {
                    return objectMapper.writeValueAsString(Map.of(
                        "user_id", windowedKey.key(),
                        "window_start", windowedKey.window().start(),
                        "window_end", windowedKey.window().end(),
                        "event_count", count,
                        "batch_processed_at", System.currentTimeMillis()
                    ));
                } catch (Exception e) {
                    return "{\"error\": \"serialization_failed\"}";
                }
            })
            .to("batch-processed-events");

        // Agrégation glissante pour détection de pics
        KTable<Windowed<String>, Long> slidingCounts = userEvents
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(10)))
            .count(Materialized.as("sliding-counts-store"));

        slidingCounts.toStream()
            .filter((windowedKey, count) -> count > 50) // Détection de pic
            .mapValues((windowedKey, count) -> 
                String.format("{\"alert\":\"high_activity\",\"user\":\"%s\",\"count\":%d,\"window\":\"%s\"}",
                    windowedKey.key(), count, windowedKey.window().toString()))
            .to("activity-alerts");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        
        streams.start();
        System.out.println("Batch processing streams application démarrée");
    }
}
```

#### Python (kafka-python avec logique de streaming)

```python
from kafka import KafkaConsumer, KafkaProducer
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta

class BatchProcessingStreams:
    def __init__(self, input_topic, output_topic, bootstrap_servers=['localhost:9092']):
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='batch-streams-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Stockage d'état pour les fenêtres
        self.window_size = timedelta(minutes=5)
        self.window_counts = defaultdict(lambda: defaultdict(int))
        self.window_timestamps = {}
    
    def process_batch_streams(self):
        """Traitement par lot avec fenêtres temporelles"""
        print("Batch processing streams démarré...")
        
        while True:
            message_batch = self.consumer.poll(timeout_ms=1000)
            
            current_time = datetime.now()
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    self.process_message_in_window(message, current_time)
            
            # Traitement périodique des fenêtres expirées
            self.process_expired_windows(current_time)
            
            # Commit périodique
            self.consumer.commit()
    
    def process_message_in_window(self, message, current_time):
        """Traite un message dans sa fenêtre temporelle"""
        data = message.value
        user_id = data.get('user_id', 'unknown')
        
        # Calcul de la fenêtre (arrondi aux 5 minutes)
        window_start = current_time.replace(second=0, microsecond=0, minute=current_time.minute // 5 * 5)
        window_key = f"{user_id}_{window_start.isoformat()}"
        
        # Incrémentation du compteur
        self.window_counts[window_key][user_id] += 1
        self.window_timestamps[window_key] = current_time
        
        print(f"Message traité pour user {user_id} dans fenêtre {window_start}")
    
    def process_expired_windows(self, current_time):
        """Traite les fenêtres qui sont complètes"""
        expired_windows = []
        
        for window_key, timestamp in self.window_timestamps.items():
            if current_time - timestamp > self.window_size:
                expired_windows.append(window_key)
        
        for window_key in expired_windows:
            user_id, window_start_str = window_key.split('_', 1)
            window_start = datetime.fromisoformat(window_start_str)
            
            count = self.window_counts[window_key][user_id]
            
            if count > 10:  # Seulement traiter les lots significatifs
                batch_result = {
                    'user_id': user_id,
                    'window_start': window_start.isoformat(),
                    'window_end': (window_start + self.window_size).isoformat(),
                    'event_count': count,
                    'batch_processed_at': current_time.isoformat(),
                    'batch_type': 'windowed_aggregation'
                }
                
                self.producer.send(self.output_topic, value=batch_result)
                print(f"Lot traité: {count} événements pour user {user_id}")
            
            # Nettoyage
            del self.window_counts[window_key]
            del self.window_timestamps[window_key]
        
        # Flush périodique
        if len(expired_windows) > 0:
            self.producer.flush()
    
    def start(self):
        """Démarre le traitement"""
        try:
            self.process_batch_streams()
        except KeyboardInterrupt:
            print("Arrêt du batch processing")
            self.consumer.close()
            self.producer.close()

# Utilisation
if __name__ == "__main__":
    batch_processor = BatchProcessingStreams('user-events', 'batch-processed-events')
    batch_processor.start()
```

### Error Handling

#### Java (Kafka Streams)

```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;

public class ErrorHandlingStreams {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "error-handling-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Configuration de gestion d'erreurs
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, 
                 LogAndContinueExceptionHandler.class.getName());
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                 LogAndContinueExceptionHandler.class.getName());
        
        // Nombre de tentatives de traitement
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("input-events");

        // Transformation avec gestion d'erreurs
        KStream<String, String> processedStream = inputStream
            .mapValues(new ValueMapper<String, String>() {
                @Override
                public String apply(String value) {
                    try {
                        return processWithErrorHandling(value);
                    } catch (Exception e) {
                        System.err.println("Erreur transformation: " + e.getMessage());
                        return createErrorMessage(value, e);
                    }
                }
            })
            .filter((key, value) -> value != null && !value.contains("\"error\"")); // Filtrer les erreurs

        // Stream séparé pour les erreurs
        KStream<String, String> errorStream = inputStream
            .mapValues(value -> {
                try {
                    processWithErrorHandling(value);
                    return null; // Pas d'erreur
                } catch (Exception e) {
                    return createErrorMessage(value, e);
                }
            })
            .filter((key, value) -> value != null);

        processedStream.to("processed-events");
        errorStream.to("error-events");

        // Processor personnalisé avec gestion d'erreurs avancée
        builder.addProcessor("error-processor", new ProcessorSupplier<String, String>() {
            @Override
            public Processor<String, String> get() {
                return new ErrorHandlingProcessor();
            }
        }, "input-events");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arrêt gracieux...");
            streams.close();
        }));
        
        streams.start();
    }

    private static String processWithErrorHandling(String value) throws Exception {
        // Logique de traitement qui peut échouer
        if (value.contains("error")) {
            throw new RuntimeException("Erreur simulée dans le traitement");
        }
        
        // Traitement normal
        Map<String, Object> data = objectMapper.readValue(value, Map.class);
        data.put("processed", true);
        data.put("processed_at", System.currentTimeMillis());
        
        return objectMapper.writeValueAsString(data);
    }

    private static String createErrorMessage(String originalValue, Exception e) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                "error", true,
                "original_message", originalValue,
                "error_message", e.getMessage(),
                "error_type", e.getClass().getSimpleName(),
                "timestamp", System.currentTimeMillis()
            ));
        } catch (Exception jsonError) {
            return "{\"error\":true,\"message\":\"JSON serialization failed\"}";
        }
    }

    static class ErrorHandlingProcessor implements Processor<String, String> {
        private ProcessorContext context;
        
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }
        
        @Override
        public void process(String key, String value) {
            try {
                String result = processWithErrorHandling(value);
                context.forward(key, result);
            } catch (Exception e) {
                // Envoi vers topic d'erreurs
                String errorMsg = createErrorMessage(value, e);
                context.forward(key, errorMsg, "error-events");
                
                // Log de l'erreur
                System.err.println("Erreur dans processor: " + e.getMessage());
            }
        }
        
        @Override
        public void close() {}
    }
}
```

#### Python (kafka-python avec logique de streaming)

```python
from kafka import KafkaConsumer, KafkaProducer
import json
import traceback
from datetime import datetime

class ErrorHandlingStreams:
    def __init__(self, input_topic, output_topic, error_topic, bootstrap_servers=['localhost:9092']):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.error_topic = error_topic
        
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='error-handling-streams-group',
            value_deserializer=self.safe_json_deserializer
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
    
    def safe_json_deserializer(self, message):
        """Désérialiseur JSON avec gestion d'erreurs"""
        try:
            return json.loads(message.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return {
                'deserialization_error': True,
                'raw_message': message.decode('utf-8', errors='replace'),
                'error_message': str(e)
            }
    
    def process_with_error_handling(self, data):
        """Traite les données avec gestion d'erreurs complète"""
        if 'deserialization_error' in data:
            raise ValueError("Erreur de désérialisation: " + data.get('error_message', 'Unknown'))
        
        # Validation des données
        if not isinstance(data, dict):
            raise TypeError("Les données doivent être un objet JSON")
        
        required_fields = ['user_id', 'event_type']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Champ requis manquant: {field}")
        
        # Traitement métier (peut échouer)
        if data.get('event_type') == 'error_simulation':
            raise RuntimeError("Erreur simulée dans le traitement métier")
        
        # Traitement réussi
        data['processed'] = True
        data['processed_at'] = datetime.now().isoformat()
        data['processor_version'] = '1.0'
        
        return data
    
    def create_error_message(self, original_data, exception, message_key=None):
        """Crée un message d'erreur structuré"""
        return {
            'error': True,
            'original_message': original_data,
            'error_message': str(exception),
            'error_type': type(exception).__name__,
            'stack_trace': traceback.format_exc(),
            'timestamp': datetime.now().isoformat(),
            'message_key': message_key,
            'processor': 'error-handling-streams'
        }
    
    def process_stream_with_errors(self):
        """Traitement du stream avec gestion d'erreurs"""
        print("Error handling streams démarré...")
        
        while True:
            try:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Tentative de traitement
                            processed_data = self.process_with_error_handling(message.value)
                            
                            # Envoi vers topic de sortie
                            self.producer.send(self.output_topic, value=processed_data)
                            
                        except Exception as e:
                            # Création du message d'erreur
                            error_message = self.create_error_message(
                                message.value, e, message.key
                            )
                            
                            # Envoi vers topic d'erreurs
                            self.producer.send(self.error_topic, value=error_message)
                            
                            print(f"Erreur traitée: {e}")
                
                # Commit après traitement du lot
                self.consumer.commit()
                self.producer.flush()
                
            except Exception as e:
                print(f"Erreur critique dans la boucle principale: {e}")
                # Ici: logging, alertes, etc.
    
    def start(self):
        """Démarre le traitement"""
        try:
            self.process_stream_with_errors()
        except KeyboardInterrupt:
            print("Arrêt du error handling streams")
        finally:
            self.consumer.close()
            self.producer.close()

# Utilisation
if __name__ == "__main__":
    error_handler = ErrorHandlingStreams(
        'input-events', 
        'processed-events', 
        'error-events'
    )
    error_handler.start()
```

### Advanced Configurations

#### Java (Kafka Streams)

```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AdvancedStreamsConfig {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "advanced-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Configurations avancées
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4); // 4 threads
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760); // 10MB cache
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000); // Commit 30s
        props.put(StreamsConfig.POLL_MS_CONFIG, 100); // Poll rapide
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 5000); // Idle max 5s
        
        // Garanties de traitement
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        
        // Métriques
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        
        StreamsBuilder builder = new StreamsBuilder();

        // Store d'état personnalisé
        StoreBuilder<KeyValueStore<String, String>> userStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("user-metadata-store"),
            Serdes.String(),
            Serdes.String()
        );
        builder.addStateStore(userStoreBuilder);

        KStream<String, String> events = builder.stream("events");

        // Transformation avec store d'état
        KStream<String, String> enrichedEvents = events.transformValues(
            () -> new UserEnrichmentTransformer("user-metadata-store"),
            "user-metadata-store"
        );

        // Agrégation avec store RocksDB optimisé
        KTable<String, Long> eventCounts = events
            .groupByKey()
            .count(Materialized.<String, Long>as("event-counts")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
                .withCachingEnabled() // Cache activé
                .withLoggingEnabled(Map.of( // Logging configuré
                    "retention.ms", "604800000", // 7 jours
                    "retention.bytes", "104857600" // 100MB
                ))
            );

        // Stream de métriques personnalisées
        events
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
            .count()
            .toStream()
            .mapValues((windowedKey, count) -> {
                // Métrique personnalisée
                return Map.of(
                    "metric_type", "event_rate",
                    "key", windowedKey.key(),
                    "count", count,
                    "window_start", windowedKey.window().start(),
                    "window_end", windowedKey.window().end(),
                    "timestamp", System.currentTimeMillis()
                );
            })
            .mapValues(value -> {
                try {
                    return objectMapper.writeValueAsString(value);
                } catch (Exception e) {
                    return "{}";
                }
            })
            .to("streams-metrics");

        enrichedEvents.to("enriched-events");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        // Métriques JMX
        streams.setStateListener((newState, oldState) -> {
            System.out.println("Streams state changed from " + oldState + " to " + newState);
        });
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(30));
        }));
        
        streams.start();
    }
}

// Transformer avec store d'état
class UserEnrichmentTransformer implements ValueTransformerWithKey<String, String, String> {
    private KeyValueStore<String, String> userStore;
    private String storeName;
    
    public UserEnrichmentTransformer(String storeName) {
        this.storeName = storeName;
    }
    
    @Override
    public void init(ProcessorContext context) {
        userStore = (KeyValueStore<String, String>) context.getStateStore(storeName);
    }
    
    @Override
    public String transform(String key, String value) {
        try {
            Map<String, Object> event = objectMapper.readValue(value, Map.class);
            String userId = (String) event.get("user_id");
            
            // Récupération des métadonnées utilisateur
            String userMetadata = userStore.get(userId);
            if (userMetadata != null) {
                Map<String, Object> metadata = objectMapper.readValue(userMetadata, Map.class);
                event.put("user_metadata", metadata);
            }
            
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            System.err.println("Erreur enrichment: " + e.getMessage());
            return value;
        }
    }
    
    @Override
    public void close() {}
}
```

#### Python (kafka-python avec logique de streaming)

```python
from kafka import KafkaConsumer, KafkaProducer
import json
import time
import threading
from collections import defaultdict
import os

class AdvancedStreamsConfig:
    def __init__(self, input_topic, output_topic, bootstrap_servers=['localhost:9092']):
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        # Consumer avec configurations avancées
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='advanced-streams-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=False,
            max_poll_records=500,  # Lots plus gros
            fetch_max_bytes=52428800,  # 50MB
            fetch_max_wait_ms=500,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='lz4',
            batch_size=32768,
            linger_ms=10,
            acks='all',
            retries=5,
            max_in_flight_requests_per_connection=5
        )
        
        # Store d'état local (simulé)
        self.state_store = defaultdict(dict)
        self.state_lock = threading.Lock()
        
        # Métriques
        self.metrics = {
            'messages_processed': 0,
            'errors': 0,
            'processing_time': 0,
            'start_time': time.time()
        }
    
    def enrich_with_state(self, data):
        """Enrichit les données avec le store d'état"""
        user_id = data.get('user_id')
        
        with self.state_lock:
            user_state = self.state_store.get(user_id, {})
            
            # Mise à jour de l'état
            user_state['last_seen'] = time.time()
            user_state['event_count'] = user_state.get('event_count', 0) + 1
            
            self.state_store[user_id] = user_state
            
            # Enrichissement
            data['user_state'] = user_state.copy()
        
        return data
    
    def process_with_advanced_config(self):
        """Traitement avec configurations avancées"""
        print("Advanced streams processing démarré...")
        
        while True:
            start_time = time.time()
            
            try:
                # Poll avec timeout optimisé
                message_batch = self.consumer.poll(timeout_ms=100)
                
                batch_size = sum(len(messages) for messages in message_batch.values())
                
                if batch_size > 0:
                    processed_count = 0
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                # Traitement avec métriques
                                process_start = time.time()
                                
                                enriched_data = self.enrich_with_state(message.value)
                                processed_data = self.transform_data(enriched_data)
                                
                                self.producer.send(self.output_topic, value=processed_data)
                                
                                process_time = time.time() - process_start
                                
                                # Mise à jour métriques
                                self.metrics['messages_processed'] += 1
                                self.metrics['processing_time'] += process_time
                                processed_count += 1
                                
                            except Exception as e:
                                self.metrics['errors'] += 1
                                print(f"Erreur traitement: {e}")
                    
                    # Commit manuel après traitement du lot
                    self.consumer.commit()
                    
                    # Flush du producer
                    self.producer.flush()
                    
                    batch_time = time.time() - start_time
                    print(f"Lot traité: {processed_count} messages en {batch_time:.3f}s")
                    
                    # Métriques périodiques
                    if self.metrics['messages_processed'] % 1000 == 0:
                        self.print_metrics()
                
            except Exception as e:
                print(f"Erreur dans la boucle principale: {e}")
                self.metrics['errors'] += 1
    
    def transform_data(self, data):
        """Transformation avancée"""
        data['processed_at'] = time.time()
        data['processor_id'] = os.getpid()
        data['processing_version'] = '2.0'
        
        # Logique métier avancée
        if data.get('event_type') == 'high_priority':
            data['priority_score'] = 100
        else:
            data['priority_score'] = 50
        
        return data
    
    def print_metrics(self):
        """Affiche les métriques"""
        uptime = time.time() - self.metrics['start_time']
        avg_processing_time = (self.metrics['processing_time'] / max(self.metrics['messages_processed'], 1)) * 1000
        
        print("=== Métriques Streams ===")
        print(f"Messages traités: {self.metrics['messages_processed']}")
        print(f"Erreurs: {self.metrics['errors']}")
        print(".2f")
        print(".2f")
        print(f"Uptime: {uptime:.0f}s")
        print("=" * 25)
    
    def start(self):
        """Démarre le traitement"""
        try:
            self.process_with_advanced_config()
        except KeyboardInterrupt:
            print("Arrêt du advanced streams")
            self.print_metrics()
        finally:
            self.consumer.close()
            self.producer.close()

# Utilisation
if __name__ == "__main__":
    advanced_processor = AdvancedStreamsConfig('events', 'processed-events')
    advanced_processor.start()
```

### Real-World Scenarios

#### Java (Kafka Streams) - Fraud Detection System

```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.JoinWindows;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

public class FraudDetectionStreams {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        StreamsBuilder builder = new StreamsBuilder();

        // Stream de transactions
        KStream<String, String> transactions = builder.stream("transactions");

        // Règles de détection de fraude
        
        // 1. Vérification de vélocité - trop de transactions dans une fenêtre
        KTable<Windowed<String>, Long> transactionVelocity = transactions
            .groupBy((key, value) -> extractUserId(value))
            .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
            .count(Materialized.as("velocity-store"));

        KStream<String, String> highVelocityAlerts = transactionVelocity
            .toStream()
            .filter((windowedKey, count) -> count > 5) // Plus de 5 transactions en 10 min
            .mapValues((windowedKey, count) -> createFraudAlert(
                windowedKey.key(), "HIGH_VELOCITY", 
                "Trop de transactions: " + count, Map.of("count", count)));

        // 2. Détection d'anomalies de montant
        KTable<String, Double> avgTransactionAmounts = transactions
            .groupBy((key, value) -> extractUserId(value))
            .aggregate(
                () -> 0.0,
                (userId, transaction, total) -> {
                    double amount = extractAmount(transaction);
                    return (total + amount) / 2.0; // Moyenne roulante simple
                },
                Materialized.as("amount-avg-store")
            );

        KStream<String, String> amountAnomalyAlerts = transactions
            .leftJoin(avgTransactionAmounts, 
                (transaction, avgAmount) -> {
                    double currentAmount = extractAmount(transaction);
                    if (avgAmount != null && currentAmount > avgAmount * 3) {
                        return createFraudAlert(
                            extractUserId(transaction), "AMOUNT_ANOMALY",
                            "Montant anormal: " + currentAmount + " vs moyenne: " + avgAmount,
                            Map.of("current_amount", currentAmount, "avg_amount", avgAmount));
                    }
                    return null;
                })
            .filter((key, alert) -> alert != null);

        // 3. Détection de transactions depuis des pays différents
        KStream<String, String> locationAnomalyAlerts = transactions
            .groupBy((key, value) -> extractUserId(value))
            .windowedBy(TimeWindows.of(Duration.ofHours(1)))
            .aggregate(
                () -> new HashMap<String, Long>(),
                (userId, transaction, countries) -> {
                    String country = extractCountry(transaction);
                    countries.put(country, System.currentTimeMillis());
                    return countries;
                },
                (userId, countries1, countries2) -> {
                    countries1.putAll(countries2);
                    return countries1;
                },
                Materialized.as("location-store"))
            .toStream()
            .filter((windowedKey, countries) -> countries.size() > 3) // Plus de 3 pays en 1h
            .mapValues((windowedKey, countries) -> createFraudAlert(
                windowedKey.key(), "LOCATION_ANOMALY",
                "Transactions depuis plusieurs pays: " + countries.keySet(),
                Map.of("countries", new ArrayList<>(countries.keySet()))));

        // Agrégation de toutes les alertes
        KStream<String, String> allAlerts = highVelocityAlerts
            .merge(amountAnomalyAlerts)
            .merge(locationAnomalyAlerts);

        // Enrichissement avec données utilisateur (jointure avec table users)
        KTable<String, String> users = builder.table("users");
        
        KStream<String, String> enrichedAlerts = allAlerts
            .leftJoin(users, 
                (alert, userData) -> {
                    try {
                        Map<String, Object> alertMap = objectMapper.readValue(alert, Map.class);
                        if (userData != null) {
                            Map<String, Object> userMap = objectMapper.readValue(userData, Map.class);
                            alertMap.put("user_info", userMap);
                        }
                        return objectMapper.writeValueAsString(alertMap);
                    } catch (Exception e) {
                        return alert;
                    }
                });

        enrichedAlerts.to("fraud-alerts");

        // Métriques de détection
        allAlerts
            .groupBy((key, value) -> "fraud_alerts")
            .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
            .count()
            .toStream()
            .mapValues((windowedKey, count) -> 
                "{\"metric\":\"fraud_detection_rate\",\"count\":" + count + 
                ",\"window_start\":" + windowedKey.window().start() + "}")
            .to("fraud-metrics");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        
        streams.start();
        System.out.println("Fraud detection system démarré");
    }

    private static String extractUserId(String transaction) {
        try {
            Map<String, Object> tx = objectMapper.readValue(transaction, Map.class);
            return (String) tx.get("user_id");
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static double extractAmount(String transaction) {
        try {
            Map<String, Object> tx = objectMapper.readValue(transaction, Map.class);
            return ((Number) tx.get("amount")).doubleValue();
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static String extractCountry(String transaction) {
        try {
            Map<String, Object> tx = objectMapper.readValue(transaction, Map.class);
            return (String) tx.get("country");
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String createFraudAlert(String userId, String alertType, String description, Map<String, Object> details) {
        Map<String, Object> alert = new HashMap<>();
        alert.put("alert_id", java.util.UUID.randomUUID().toString());
        alert.put("user_id", userId);
        alert.put("alert_type", alertType);
        alert.put("description", description);
        alert.put("severity", "HIGH");
        alert.put("timestamp", System.currentTimeMillis());
        alert.put("details", details);
        
        try {
            return objectMapper.writeValueAsString(alert);
        } catch (Exception e) {
            return "{\"error\":\"alert_creation_failed\"}";
        }
    }
}
```

#### Python (kafka-python avec logique de streaming) - Real-Time Analytics

```python
from kafka import KafkaConsumer, KafkaProducer
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
import statistics

class RealTimeAnalyticsStreams:
    def __init__(self, input_topic, output_topic, bootstrap_servers=['localhost:9092']):
        self.input_topic = input_topic
        self.output_topic = output_topic
        
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='analytics-streams-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Stores d'état pour analytics
        self.user_sessions = defaultdict(lambda: {
            'events': deque(maxlen=1000),
            'start_time': None,
            'last_activity': None
        })
        
        self.global_metrics = {
            'total_events': 0,
            'unique_users': set(),
            'event_types': defaultdict(int),
            'hourly_stats': defaultdict(lambda: {'count': 0, 'amounts': []})
        }
    
    def process_analytics_stream(self):
        """Traitement analytics en temps réel"""
        print("Real-time analytics démarré...")
        
        while True:
            message_batch = self.consumer.poll(timeout_ms=1000)
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    self.process_event(message.value)
            
            # Calcul et publication des métriques périodiques
            self.compute_and_publish_metrics()
            
            self.consumer.commit()
    
    def process_event(self, event):
        """Traite un événement individuel"""
        user_id = event.get('user_id')
        event_type = event.get('event_type')
        timestamp = event.get('timestamp', time.time())
        
        # Mise à jour session utilisateur
        user_session = self.user_sessions[user_id]
        if user_session['start_time'] is None:
            user_session['start_time'] = timestamp
        
        user_session['last_activity'] = timestamp
        user_session['events'].append(event)
        
        # Mise à jour métriques globales
        self.global_metrics['total_events'] += 1
        self.global_metrics['unique_users'].add(user_id)
        self.global_metrics['event_types'][event_type] += 1
        
        # Métriques horaires
        hour_key = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d-%H')
        hourly = self.global_metrics['hourly_stats'][hour_key]
        hourly['count'] += 1
        
        if 'amount' in event:
            hourly['amounts'].append(event['amount'])
    
    def compute_and_publish_metrics(self):
        """Calcule et publie les métriques"""
        current_time = time.time()
        
        # Nettoyage des sessions inactives (30 min)
        inactive_users = []
        for user_id, session in self.user_sessions.items():
            if current_time - session['last_activity'] > 1800:  # 30 minutes
                inactive_users.append(user_id)
        
        for user_id in inactive_users:
            del self.user_sessions[user_id]
        
        # Calcul des métriques
        metrics = {
            'timestamp': current_time,
            'total_events': self.global_metrics['total_events'],
            'active_users': len(self.user_sessions),
            'unique_users_total': len(self.global_metrics['unique_users']),
            'event_type_distribution': dict(self.global_metrics['event_types']),
            'top_event_types': sorted(
                self.global_metrics['event_types'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:5]
        }
        
        # Métriques par utilisateur actif
        user_metrics = []
        for user_id, session in list(self.user_sessions.items())[:10]:  # Top 10
            session_duration = current_time - session['start_time']
            event_count = len(session['events'])
            
            user_metrics.append({
                'user_id': user_id,
                'session_duration': session_duration,
                'event_count': event_count,
                'events_per_minute': event_count / max(session_duration / 60, 1)
            })
        
        metrics['top_users'] = user_metrics
        
        # Métriques horaires récentes
        recent_hours = []
        for hour_key, data in list(self.global_metrics['hourly_stats'].items())[-24:]:  # Dernières 24h
            hour_metrics = {
                'hour': hour_key,
                'event_count': data['count'],
                'avg_amount': statistics.mean(data['amounts']) if data['amounts'] else 0,
                'total_amount': sum(data['amounts']) if data['amounts'] else 0
            }
            recent_hours.append(hour_metrics)
        
        metrics['hourly_stats'] = recent_hours
        
        # Publication
        self.producer.send(self.output_topic, value=metrics)
        self.producer.flush()
        
        print(f"Métriques publiées: {len(self.user_sessions)} utilisateurs actifs")
    
    def start(self):
        """Démarre le traitement"""
        try:
            self.process_analytics_stream()
        except KeyboardInterrupt:
            print("Arrêt du real-time analytics")
        finally:
            self.consumer.close()
            self.producer.close()

# Utilisation
if __name__ == "__main__":
    analytics = RealTimeAnalyticsStreams('user-events', 'analytics-metrics')
    analytics.start()
```

## 4. KSQLDB

### Création de streams et tables

```sql
-- Création d'un stream depuis un topic Kafka
CREATE STREAM user_actions (
    user_id VARCHAR,
    action VARCHAR,
    timestamp BIGINT,
    metadata MAP<VARCHAR, VARCHAR>
) WITH (
    KAFKA_TOPIC='user-actions',
    VALUE_FORMAT='JSON',
    TIMESTAMP='timestamp'
);

-- Création d'une table depuis un topic compacté
CREATE TABLE users (
    id VARCHAR PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    created_at BIGINT
) WITH (
    KAFKA_TOPIC='users',
    VALUE_FORMAT='JSON'
);
```

### Requêtes de transformation

```sql
-- Stream filtré et transformé
CREATE STREAM filtered_actions AS
SELECT 
    user_id,
    action,
    UCASE(action) as action_upper,
    timestamp,
    metadata
FROM user_actions
WHERE action IN ('login', 'purchase', 'view_product')
EMIT CHANGES;

-- Agrégation par fenêtre temporelle
CREATE TABLE user_action_counts AS
SELECT 
    user_id,
    COUNT(*) as action_count,
    WINDOWSTART as window_start,
    WINDOWEND as window_end
FROM user_actions 
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY user_id
EMIT CHANGES;

-- Jointure entre stream et table
CREATE STREAM enriched_actions AS
SELECT 
    a.user_id,
    a.action,
    a.timestamp,
    u.name as user_name,
    u.email as user_email
FROM user_actions a
LEFT JOIN users u ON a.user_id = u.id
EMIT CHANGES;
```

### Client Python pour KSQLDB

```python
import requests
import json
import sseclient

class KSQLDBClient:
    def __init__(self, ksqldb_url="http://localhost:8088"):
        self.base_url = ksqldb_url
        self.session = requests.Session()
    
    def execute_statement(self, sql):
        """Exécute une instruction KSQLDB"""
        url = f"{self.base_url}/ksql"
        payload = {
            "ksql": sql,
            "streamsProperties": {}
        }
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    
    def query_stream(self, sql):
        """Exécute une requête push et retourne un générateur"""
        url = f"{self.base_url}/query-stream"
        payload = {
            "sql": sql,
            "properties": {
                "auto.offset.reset": "earliest"
            }
        }
        
        response = self.session.post(
            url, 
            json=payload,
            stream=True,
            headers={'Accept': 'application/vnd.ksqlapi.delimited.v1'}
        )
        response.raise_for_status()
        
        # Parse les résultats streaming
        for line in response.iter_lines():
            if line:
                try:
                    data = json.loads(line.decode('utf-8'))
                    yield data
                except json.JSONDecodeError:
                    continue
    
    def insert_into_stream(self, stream_name, values):
        """Insert des données dans un stream"""
        sql = f"INSERT INTO {stream_name} VALUES {values};"
        return self.execute_statement(sql)
    
    def list_streams(self):
        """Liste tous les streams"""
        return self.execute_statement("SHOW STREAMS;")
    
    def list_tables(self):
        """Liste toutes les tables"""
        return self.execute_statement("SHOW TABLES;")

# Exemple d'utilisation
client = KSQLDBClient()

# Créer un stream
create_stream_sql = """
CREATE STREAM test_stream (
    id VARCHAR,
    message VARCHAR,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='test-topic',
    VALUE_FORMAT='JSON'
);
"""

result = client.execute_statement(create_stream_sql)
print("Stream créé:", result)

# Requête push pour écouter les données
query_sql = "SELECT * FROM test_stream EMIT CHANGES;"

print("Écoute du stream...")
for row in client.query_stream(query_sql):
    print("Nouvelle ligne:", row)
    # Traiter les données ici
```

### Client Java pour KSQLDB

```java
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class KSQLDBJavaClient {
    private Client client;

    public KSQLDBJavaClient(String ksqldbUrl) {
        ClientOptions options = ClientOptions.create()
            .setHost("localhost")
            .setPort(8088);
        
        this.client = Client.create(options);
    }

    public void createStream() {
        String createStreamSql = """
            CREATE STREAM user_events (
                user_id VARCHAR,
                event_type VARCHAR,
                timestamp BIGINT,
                properties MAP<VARCHAR, VARCHAR>
            ) WITH (
                KAFKA_TOPIC='user-events',
                VALUE_FORMAT='JSON'
            );
        """;

        CompletableFuture<Void> result = client.executeStatement(createStreamSql);
        try {
            result.get();
            System.out.println("Stream créé avec succès");
        } catch (Exception e) {
            System.err.println("Erreur création stream: " + e.getMessage());
        }
    }

    public void queryStream() {
        String query = "SELECT * FROM user_events EMIT CHANGES;";
        
        CompletableFuture<StreamedQueryResult> resultFuture = client.streamQuery(query);
        
        try {
            StreamedQueryResult result = resultFuture.get();
            
            System.out.println("Écoute du stream...");
            result.subscribe(row -> {
                System.out.println("Nouvelle ligne: " + row.values());
                // Traitement des données
                processRow(row);
            }, throwable -> {
                System.err.println("Erreur: " + throwable.getMessage());
            });
            
            // Garder l'application en vie
            Thread.sleep(Long.MAX_VALUE);
            
        } catch (Exception e) {
            System.err.println("Erreur requête: " + e.getMessage());
        }
    }

    private void processRow(Row row) {
        // Traitement personnalisé des données
        String userId = row.getString("USER_ID");
        String eventType = row.getString("EVENT_TYPE");
        Long timestamp = row.getLong("TIMESTAMP");
        
        System.out.printf("User %s performed %s at %d%n", userId, eventType, timestamp);
    }

    public void insertData() {
        String insertSql = """
            INSERT INTO user_events VALUES (
                'user123',
                'page_view',
                %d,
                MAP('page' := '/home', 'browser' := 'chrome')
            );
        """.formatted(System.currentTimeMillis());

        try {
            client.executeStatement(insertSql).get();
            System.out.println("Données insérées");
        } catch (Exception e) {
            System.err.println("Erreur insertion: " + e.getMessage());
        }
    }

    public void close() {
        client.close();
    }

    public static void main(String[] args) {
        KSQLDBJavaClient ksqlClient = new KSQLDBJavaClient("localhost:8088");
        
        try {
            ksqlClient.createStream();
            ksqlClient.insertData();
            ksqlClient.queryStream();
        } finally {
            ksqlClient.close();
        }
    }
}
```

## 5. Kafka Management

### Creating Topics

#### Python (kafka-python)

```python
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
```

#### Java (Apache Kafka Admin Client)

```java
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTopicExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic("mon-nouveau-topic", 3, (short) 1);
            
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            
            // Attendre la création
            result.all().get();
            System.out.println("Topic créé avec succès");
            
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Erreur lors de la création du topic: " + e.getMessage());
        }
    }
}
```

### Listing Topics

#### Python (kafka-python)

```python
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

def list_topics(bootstrap_servers):
    """Liste tous les topics disponibles"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='admin-client'
        )
        
        topics = admin_client.list_topics()
        print("Topics disponibles:")
        for topic in topics:
            print(f"  - {topic}")
            
    except KafkaError as e:
        print(f"Erreur lors de la liste des topics: {e}")
    finally:
        admin_client.close()

# Exemple d'utilisation
if __name__ == "__main__":
    list_topics(['localhost:9092'])
```

#### Java (Apache Kafka Admin Client)

```java
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ListTopicsExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult result = adminClient.listTopics();
            
            Set<String> topics = result.names().get();
            System.out.println("Topics disponibles:");
            for (String topic : topics) {
                System.out.println("  - " + topic);
            }
            
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Erreur lors de la liste des topics: " + e.getMessage());
        }
    }
}
```

### Deleting Topics

#### Python (kafka-python)

```python
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

def delete_topic(bootstrap_servers, topic_name):
    """Supprime un topic Kafka"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='admin-client'
        )
        
        admin_client.delete_topics([topic_name])
        print(f"Topic '{topic_name}' supprimé avec succès")
        
    except KafkaError as e:
        print(f"Erreur lors de la suppression du topic: {e}")
    finally:
        admin_client.close()

# Exemple d'utilisation
if __name__ == "__main__":
    delete_topic(['localhost:9092'], 'topic-a-supprimer')
```

#### Java (Apache Kafka Admin Client)

```java
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DeleteTopicExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton("topic-a-supprimer"));
            
            result.all().get();
            System.out.println("Topic supprimé avec succès");
            
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Erreur lors de la suppression du topic: " + e.getMessage());
        }
    }
}
```

### Describing Topics

#### Python (kafka-python)

```python
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

def describe_topic(bootstrap_servers, topic_name):
    """Décrit un topic Kafka"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='admin-client'
        )
        
        topic_description = admin_client.describe_topics([topic_name])
        
        for topic in topic_description:
            print(f"Topic: {topic['topic']}")
            print(f"  Partitions: {len(topic['partitions'])}")
            for partition in topic['partitions']:
                print(f"    Partition {partition['partition']}:")
                print(f"      Leader: {partition['leader']}")
                print(f"      Replicas: {partition['replicas']}")
                print(f"      ISR: {partition['isr']}")
            
    except KafkaError as e:
        print(f"Erreur lors de la description du topic: {e}")
    finally:
        admin_client.close()

# Exemple d'utilisation
if __name__ == "__main__":
    describe_topic(['localhost:9092'], 'mon-topic')
```

#### Java (Apache Kafka Admin Client)

```java
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DescribeTopicExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton("mon-topic"));
            
            Map<String, TopicDescription> descriptions = result.all().get();
            
            for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                TopicDescription desc = entry.getValue();
                System.out.println("Topic: " + desc.name());
                System.out.println("  Partitions: " + desc.partitions().size());
                
                desc.partitions().forEach(partition -> {
                    System.out.println("    Partition " + partition.partition() + ":");
                    System.out.println("      Leader: " + partition.leader());
                    System.out.println("      Replicas: " + partition.replicas());
                    System.out.println("      ISR: " + partition.isr());
                });
            }
            
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Erreur lors de la description du topic: " + e.getMessage());
        }
    }
}
```

### Managing Consumer Groups

#### Python (kafka-python)

```python
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

def list_consumer_groups(bootstrap_servers):
    """Liste tous les groupes de consommateurs"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='admin-client'
        )
        
        groups = admin_client.list_consumer_groups()
        print("Groupes de consommateurs:")
        for group in groups:
            print(f"  - {group['group_id']}: {group['protocol_type']}")
            
    except KafkaError as e:
        print(f"Erreur lors de la liste des groupes: {e}")
    finally:
        admin_client.close()

def describe_consumer_group(bootstrap_servers, group_id):
    """Décrit un groupe de consommateurs"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='admin-client'
        )
        
        group_description = admin_client.describe_consumer_groups([group_id])
        
        for group in group_description:
            print(f"Groupe: {group['group_id']}")
            print(f"  État: {group['state']}")
            print(f"  Protocole: {group['protocol_type']}")
            print(f"  Membres: {len(group['members'])}")
            
            for member in group['members']:
                print(f"    Membre {member['member_id']}: {member['client_id']}")
            
    except KafkaError as e:
        print(f"Erreur lors de la description du groupe: {e}")
    finally:
        admin_client.close()

# Exemple d'utilisation
if __name__ == "__main__":
    list_consumer_groups(['localhost:9092'])
    describe_consumer_group(['localhost:9092'], 'mon-consumer-group')
```

#### Java (Apache Kafka Admin Client)

```java
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ConsumerGroupManagementExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            // Lister les groupes
            ListConsumerGroupsResult listResult = adminClient.listConsumerGroups();
            Set<String> groupIds = listResult.all().get();
            
            System.out.println("Groupes de consommateurs:");
            for (String groupId : groupIds) {
                System.out.println("  - " + groupId);
            }
            
            // Décrire un groupe spécifique
            if (!groupIds.isEmpty()) {
                String groupId = groupIds.iterator().next();
                DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(Collections.singleton(groupId));
                
                Map<String, ConsumerGroupDescription> descriptions = describeResult.all().get();
                
                for (ConsumerGroupDescription desc : descriptions.values()) {
                    System.out.println("Groupe: " + desc.groupId());
                    System.out.println("  État: " + desc.state());
                    System.out.println("  Protocole: " + desc.protocolType());
                    System.out.println("  Membres: " + desc.members().size());
                    
                    for (MemberDescription member : desc.members()) {
                        System.out.println("    Membre " + member.consumerId() + ": " + member.clientId());
                    }
                }
            }
            
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Erreur lors de la gestion des groupes: " + e.getMessage());
        }
    }
}
```

### Batch Processing (Bulk Operations)

```sql
-- Création de streams/tables en lot
CREATE STREAM user_events_batch (
    user_id VARCHAR,
    event_type VARCHAR,
    event_data MAP<VARCHAR, VARCHAR>,
    batch_id VARCHAR,
    batch_size INT,
    batch_timestamp BIGINT
) WITH (
    KAFKA_TOPIC='user-events-batch',
    VALUE_FORMAT='JSON',
    TIMESTAMP='batch_timestamp'
);

-- Table pour agréger les lots
CREATE TABLE batch_summaries AS
SELECT 
    batch_id,
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as unique_users,
    MIN(batch_timestamp) as batch_start,
    MAX(batch_timestamp) as batch_end,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_events
FROM user_events_batch
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY batch_id
EMIT CHANGES;

-- Stream pour traiter les lots volumineux
CREATE STREAM large_batch_processor AS
SELECT 
    batch_id,
    user_id,
    event_type,
    event_data,
    batch_size,
    CASE 
        WHEN batch_size > 1000 THEN 'LARGE'
        WHEN batch_size > 100 THEN 'MEDIUM'
        ELSE 'SMALL'
    END as batch_category
FROM user_events_batch
WHERE batch_size > 10
EMIT CHANGES;

-- Agrégation par lot avec fonctions d'agrégation avancées
CREATE TABLE batch_analytics AS
SELECT 
    batch_id,
    HISTOGRAM(event_type) as event_distribution,
    TOPK(event_type, 5) as top_events,
    PERCENTILE(amount, 0.95) as p95_amount,
    AVG(amount) as avg_amount,
    SUM(amount) as total_amount,
    COUNT(*) as event_count
FROM user_events_batch
WINDOW TUMBLING (SIZE 5 MINUTES)
WHERE event_type IN ('purchase', 'view', 'click')
GROUP BY batch_id
EMIT CHANGES;
```

### Error Handling

```sql
-- Stream avec gestion d'erreurs
CREATE STREAM error_handling_stream AS
SELECT 
    CASE 
        WHEN user_id IS NULL THEN 'ERROR: Missing user_id'
        WHEN event_type NOT IN ('login', 'logout', 'purchase', 'view') THEN 'ERROR: Invalid event_type'
        WHEN amount < 0 THEN 'ERROR: Negative amount'
        ELSE 'VALID'
    END as validation_status,
    CASE 
        WHEN user_id IS NULL THEN NULL
        ELSE user_id
    END as user_id,
    event_type,
    amount,
    CASE 
        WHEN validation_status = 'VALID' THEN amount
        ELSE 0
    END as valid_amount,
    ROWTIME as processing_timestamp
FROM user_events
EMIT CHANGES;

-- Table pour les erreurs
CREATE TABLE validation_errors AS
SELECT 
    validation_status,
    COUNT(*) as error_count,
    COLLECT_LIST(CASE WHEN validation_status != 'VALID' THEN STRUCT(user_id := user_id, event_type := event_type) END) as error_details
FROM error_handling_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
WHERE validation_status != 'VALID'
GROUP BY validation_status
EMIT CHANGES;

-- Stream nettoyé (sans erreurs)
CREATE STREAM clean_events AS
SELECT 
    user_id,
    event_type,
    amount,
    processing_timestamp
FROM error_handling_stream
WHERE validation_status = 'VALID'
EMIT CHANGES;

-- Gestion des timeouts et retries (via propriétés)
-- Dans ksqlDB CLI ou REST API
SET 'auto.offset.reset' = 'earliest';
SET 'processing.guarantee' = 'exactly_once';
SET 'commit.interval.ms' = '5000';
```

### Advanced Configurations

```sql
-- Streams avec configurations avancées
CREATE STREAM advanced_user_events (
    user_id VARCHAR,
    session_id VARCHAR,
    event_type VARCHAR,
    event_properties MAP<VARCHAR, VARCHAR>,
    event_timestamp BIGINT,
    processing_metadata STRUCT<
        source VARCHAR,
        version VARCHAR,
        retry_count INT
    >
) WITH (
    KAFKA_TOPIC='advanced-events',
    VALUE_FORMAT='JSON',
    TIMESTAMP='event_timestamp',
    -- Configurations avancées
    PARTITIONS=6,
    REPLICAS=3,
    RETENTION_MS=604800000,  -- 7 jours
    CLEANUP_POLICY='delete'
);

-- Table avec index et cache
CREATE TABLE user_profiles (
    user_id VARCHAR PRIMARY KEY,
    profile_data STRUCT<
        name VARCHAR,
        email VARCHAR,
        preferences MAP<VARCHAR, VARCHAR>,
        last_updated BIGINT
    >,
    metadata STRUCT<
        created_at BIGINT,
        updated_at BIGINT,
        version INT
    >
) WITH (
    KAFKA_TOPIC='user-profiles',
    VALUE_FORMAT='JSON',
    -- Optimisations avancées
    CACHE_MAX_BYTES_BUFFERING=10485760,  -- 10MB cache
    COMMIT_INTERVAL_MS=30000,
    -- Index pour les requêtes
    KEY_FORMAT='JSON'
);

-- Fonctions définies par l'utilisateur (UDF)
-- Enregistrement d'UDFs personnalisées
CREATE FUNCTION validate_email(email VARCHAR)
  RETURNS BOOLEAN
  AS 'com.example.udf.EmailValidator' LANGUAGE JAVA;

CREATE FUNCTION calculate_risk_score(user_id VARCHAR, amount DOUBLE, history ARRAY<DOUBLE>)
  RETURNS DOUBLE
  AS 'com.example.udf.RiskCalculator' LANGUAGE JAVA;

-- Utilisation des UDFs
CREATE STREAM risk_scored_events AS
SELECT 
    user_id,
    event_type,
    amount,
    validate_email(user_profile->email) as email_valid,
    calculate_risk_score(user_id, amount, 
        ARRAY[previous_purchase_1, previous_purchase_2, previous_purchase_3]) as risk_score,
    CASE 
        WHEN risk_score > 0.8 THEN 'HIGH_RISK'
        WHEN risk_score > 0.5 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_category
FROM user_events ue
LEFT JOIN user_profiles up ON ue.user_id = up.user_id
EMIT CHANGES;

-- Requêtes avec jointures complexes et optimisations
CREATE STREAM enriched_events AS
SELECT 
    ue.user_id,
    ue.event_type,
    ue.amount,
    up.profile_data->name as user_name,
    up.profile_data->preferences['theme'] as user_theme,
    loc.city,
    loc.country,
    dev.device_type,
    dev.os_version,
    ROWTIME as enrichment_timestamp
FROM user_events ue
LEFT JOIN user_profiles up ON ue.user_id = up.user_id
LEFT JOIN user_locations loc ON ue.user_id = loc.user_id AND ue.event_timestamp BETWEEN loc.checkin_time AND loc.checkout_time
LEFT JOIN device_info dev ON ue.session_id = dev.session_id
WHERE ue.amount > 0
EMIT CHANGES;

-- Streams avec transformation conditionnelle
CREATE STREAM conditional_transform AS
SELECT 
    *,
    CASE 
        WHEN event_type = 'purchase' AND amount > 1000 THEN TRANSFORM(event_properties, (k, v) -> CONCAT(k, '_premium'))
        WHEN event_type = 'login' THEN TRANSFORM(event_properties, (k, v) -> CONCAT(k, '_auth'))
        ELSE event_properties
    END as transformed_properties
FROM user_events
EMIT CHANGES;
```

### Real-World Scenarios

#### Data Warehousing Queries

```sql
-- Table des faits pour data warehouse
CREATE TABLE sales_facts AS
SELECT 
    user_id,
    product_id,
    category_id,
    store_id,
    EXTRACTDATE(timestamp) as sale_date,
    EXTRACTHOUR(timestamp) as sale_hour,
    amount,
    quantity,
    discount_amount,
    tax_amount,
    total_amount,
    payment_method,
    ROWTIME as fact_timestamp
FROM sales_events
EMIT CHANGES;

-- Dimensions pour data warehouse
CREATE TABLE user_dimension AS
SELECT 
    user_id,
    FIRST_VALUE(name) as name,
    FIRST_VALUE(email) as email,
    FIRST_VALUE(age) as age,
    FIRST_VALUE(gender) as gender,
    FIRST_VALUE(registration_date) as registration_date,
    MAX(last_login) as last_login,
    COUNT(*) as total_events,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
    AVG(CASE WHEN event_type = 'purchase' THEN amount END) as avg_purchase_amount
FROM user_events ue
LEFT JOIN user_profiles up ON ue.user_id = up.user_id
WINDOW TUMBLING (SIZE 1 DAY)
GROUP BY user_id
EMIT CHANGES;

-- Requêtes analytiques complexes
CREATE TABLE daily_sales_analytics AS
SELECT 
    sale_date,
    category_id,
    COUNT(*) as total_sales,
    COUNT(DISTINCT user_id) as unique_customers,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    PERCENTILE(total_amount, 0.5) as median_order_value,
    PERCENTILE(total_amount, 0.95) as p95_order_value,
    SUM(quantity) as total_items_sold,
    AVG(discount_amount) as avg_discount,
    COUNT(CASE WHEN payment_method = 'credit_card' THEN 1 END) as credit_card_payments,
    COUNT(CASE WHEN payment_method = 'paypal' THEN 1 END) as paypal_payments
FROM sales_facts
WINDOW TUMBLING (SIZE 1 DAY)
GROUP BY sale_date, category_id
EMIT CHANGES;

-- Détection d'anomalies
CREATE STREAM sales_anomalies AS
SELECT 
    sale_date,
    category_id,
    total_sales,
    total_revenue,
    LAG(total_sales, 1) OVER (PARTITION BY category_id ORDER BY sale_date) as prev_day_sales,
    LAG(total_revenue, 7) OVER (PARTITION BY category_id ORDER BY sale_date) as week_ago_revenue,
    CASE 
        WHEN total_sales > LAG(total_sales, 1) OVER (PARTITION BY category_id ORDER BY sale_date) * 2 
        THEN 'SPIKE_DETECTED'
        WHEN total_sales < LAG(total_sales, 1) OVER (PARTITION BY category_id ORDER BY sale_date) * 0.5 
        THEN 'DROP_DETECTED'
        ELSE 'NORMAL'
    END as sales_pattern
FROM daily_sales_analytics
WHERE sale_date >= '2024-01-01'
EMIT CHANGES;
```

#### IoT Sensor Data Processing

```sql
-- Stream pour données IoT
CREATE STREAM iot_sensor_data (
    sensor_id VARCHAR,
    sensor_type VARCHAR,
    location STRUCT<lat DOUBLE, lon DOUBLE, floor INT>,
    readings STRUCT<
        temperature DOUBLE,
        humidity DOUBLE,
        pressure DOUBLE,
        battery_level DOUBLE
    >,
    metadata STRUCT<
        firmware_version VARCHAR,
        calibration_date BIGINT,
        error_codes ARRAY<INT>
    >,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='iot-sensors',
    VALUE_FORMAT='JSON',
    TIMESTAMP='timestamp'
);

-- Validation et nettoyage des données
CREATE STREAM validated_sensor_data AS
SELECT 
    sensor_id,
    sensor_type,
    location,
    readings,
    metadata,
    timestamp,
    CASE 
        WHEN readings->temperature BETWEEN -50 AND 100 
             AND readings->humidity BETWEEN 0 AND 100
             AND readings->battery_level BETWEEN 0 AND 100
        THEN 'VALID'
        WHEN readings->temperature IS NULL OR readings->humidity IS NULL
        THEN 'MISSING_DATA'
        ELSE 'OUT_OF_RANGE'
    END as validation_status
FROM iot_sensor_data
EMIT CHANGES;

-- Agrégation par capteur et fenêtre temporelle
CREATE TABLE sensor_aggregates AS
SELECT 
    sensor_id,
    sensor_type,
    location,
    WINDOWSTART as window_start,
    WINDOWEND as window_end,
    COUNT(*) as reading_count,
    AVG(readings->temperature) as avg_temperature,
    MIN(readings->temperature) as min_temperature,
    MAX(readings->temperature) as max_temperature,
    STDDEV(readings->temperature) as temp_stddev,
    AVG(readings->humidity) as avg_humidity,
    AVG(readings->battery_level) as avg_battery_level,
    FIRST_VALUE(readings->battery_level) as battery_start,
    LAST_VALUE(readings->battery_level) as battery_end,
    COLLECT_LIST(CASE WHEN ARRAY_LENGTH(metadata->error_codes) > 0 THEN metadata->error_codes END) as error_codes
FROM validated_sensor_data
WINDOW TUMBLING (SIZE 5 MINUTES)
WHERE validation_status = 'VALID'
GROUP BY sensor_id, sensor_type, location
EMIT CHANGES;

-- Détection d'anomalies IoT
CREATE STREAM sensor_anomalies AS
SELECT 
    sa.sensor_id,
    sa.sensor_type,
    sa.location,
    sa.avg_temperature,
    sa.temp_stddev,
    LAG(sa.avg_temperature, 1) OVER (PARTITION BY sa.sensor_id ORDER BY sa.window_start) as prev_avg_temp,
    CASE 
        WHEN ABS(sa.avg_temperature - LAG(sa.avg_temperature, 1) OVER (PARTITION BY sa.sensor_id ORDER BY sa.window_start)) > 10 
        THEN 'TEMPERATURE_SPIKE'
        WHEN sa.temp_stddev > 5 THEN 'HIGH_VARIABILITY'
        WHEN sa.battery_end < 10 THEN 'LOW_BATTERY'
        WHEN ARRAY_LENGTH(sa.error_codes) > 0 THEN 'SENSOR_ERRORS'
        ELSE 'NORMAL'
    END as anomaly_type,
    sa.window_start,
    sa.window_end
FROM sensor_aggregates sa
EMIT CHANGES;

-- Alertes IoT
CREATE STREAM iot_alerts AS
SELECT 
    sensor_id,
    sensor_type,
    location,
    anomaly_type,
    CASE 
        WHEN anomaly_type = 'LOW_BATTERY' THEN 'CRITICAL'
        WHEN anomaly_type IN ('TEMPERATURE_SPIKE', 'SENSOR_ERRORS') THEN 'HIGH'
        WHEN anomaly_type = 'HIGH_VARIABILITY' THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity,
    CONCAT('Sensor ', sensor_id, ' detected: ', anomaly_type) as alert_message,
    STRUCT(
        sensor_id := sensor_id,
        anomaly_type := anomaly_type,
        timestamp := window_start
    ) as alert_data
FROM sensor_anomalies
WHERE anomaly_type != 'NORMAL'
EMIT CHANGES;
```

#### Advanced KSQLDB Client with Error Handling

```python
import requests
import json
import time
from typing import Dict, List, Optional
import logging

class AdvancedKSQLDBClient:
    def __init__(self, ksqldb_url="http://localhost:8088", timeout=30):
        self.base_url = ksqldb_url.rstrip('/')
        self.session = requests.Session()
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        
        # Métriques
        self.metrics = {
            'queries_executed': 0,
            'errors': 0,
            'retries': 0
        }
    
    def execute_statement_with_retry(self, sql: str, max_retries=3, retry_delay=1.0) -> Dict:
        """Exécute une instruction avec retry et gestion d'erreurs"""
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                result = self.execute_statement(sql)
                self.metrics['queries_executed'] += 1
                return result
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                self.logger.warning(f"Tentative {attempt + 1} échouée: {e}")
                self.metrics['retries'] += 1
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # Backoff exponentiel
            
            except Exception as e:
                last_exception = e
                self.logger.error(f"Erreur inattendue: {e}")
                break
        
        self.metrics['errors'] += 1
        raise last_exception
    
    def execute_statement(self, sql: str) -> Dict:
        """Exécute une instruction KSQLDB"""
        url = f"{self.base_url}/ksql"
        payload = {
            "ksql": sql,
            "streamsProperties": {
                "auto.offset.reset": "earliest",
                "processing.guarantee": "exactly_once"
            }
        }
        
        response = self.session.post(url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        
        result = response.json()
        
        # Vérification d'erreurs dans la réponse
        if isinstance(result, list):
            for item in result:
                if 'error' in item:
                    raise Exception(f"KSQLDB Error: {item['error']}")
        
        return result
    
    def create_stream_batch(self, streams_config: List[Dict]) -> List[Dict]:
        """Crée plusieurs streams en lot"""
        results = []
        
        for config in streams_config:
            sql = self._build_create_stream_sql(config)
            try:
                result = self.execute_statement_with_retry(sql)
                results.append({'stream': config['name'], 'status': 'created', 'result': result})
            except Exception as e:
                results.append({'stream': config['name'], 'status': 'error', 'error': str(e)})
        
        return results
    
    def _build_create_stream_sql(self, config: Dict) -> str:
        """Construit le SQL pour créer un stream"""
        name = config['name']
        columns = config['columns']
        topic = config['topic']
        format_type = config.get('format', 'JSON')
        
        columns_sql = ', '.join([f"{col['name']} {col['type']}" for col in columns])
        
        sql = f"""
        CREATE STREAM {name} (
            {columns_sql}
        ) WITH (
            KAFKA_TOPIC='{topic}',
            VALUE_FORMAT='{format_type}'
        );
        """
        
        return sql
    
    def query_with_error_handling(self, sql: str) -> List[Dict]:
        """Exécute une requête avec gestion d'erreurs avancée"""
        try:
            result = self.execute_statement_with_retry(sql)
            
            # Validation des résultats
            if not isinstance(result, list):
                raise Exception("Résultat inattendu de la requête")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Erreur requête: {e}")
            # Ici: envoi vers topic d'erreurs, alertes, etc.
            raise
    
    def get_stream_info(self, stream_name: str) -> Dict:
        """Récupère les informations d'un stream avec gestion d'erreurs"""
        sql = f"DESCRIBE {stream_name};"
        
        try:
            result = self.execute_statement_with_retry(sql)
            return result[0] if result else {}
        except Exception as e:
            self.logger.error(f"Erreur description stream {stream_name}: {e}")
            return {'error': str(e)}
    
    def monitor_ksqldb_health(self) -> Dict:
        """Surveille la santé de KSQLDB"""
        try:
            # Test de connectivité
            health_sql = "SHOW STREAMS;"
            result = self.execute_statement(health_sql)
            
            return {
                'status': 'healthy',
                'streams_count': len(result) if isinstance(result, list) else 0,
                'timestamp': time.time()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': time.time()
            }
    
    def get_metrics(self) -> Dict:
        """Retourne les métriques du client"""
        return self.metrics.copy()

# Exemple d'utilisation avancée
client = AdvancedKSQLDBClient()

# Configuration de streams à créer
streams_config = [
    {
        'name': 'user_events_enhanced',
        'columns': [
            {'name': 'user_id', 'type': 'VARCHAR'},
            {'name': 'event_type', 'type': 'VARCHAR'},
            {'name': 'amount', 'type': 'DOUBLE'},
            {'name': 'timestamp', 'type': 'BIGINT'}
        ],
        'topic': 'user-events-enhanced'
    },
    {
        'name': 'product_views',
        'columns': [
            {'name': 'user_id', 'type': 'VARCHAR'},
            {'name': 'product_id', 'type': 'VARCHAR'},
            {'name': 'view_duration', 'type': 'INT'},
            {'name': 'timestamp', 'type': 'BIGINT'}
        ],
        'topic': 'product-views'
    }
]

# Création en lot
creation_results = client.create_stream_batch(streams_config)
print("Résultats création:", creation_results)

# Requête avec gestion d'erreurs
try:
    analytics_query = """
    SELECT user_id, COUNT(*) as event_count, SUM(amount) as total_amount
    FROM user_events_enhanced
    WINDOW TUMBLING (SIZE 1 HOUR)
    GROUP BY user_id
    EMIT CHANGES;
    """
    
    result = client.query_with_error_handling(analytics_query)
    print("Résultats analytics:", result)
    
except Exception as e:
    print(f"Erreur analytics: {e}")

# Monitoring
health = client.monitor_ksqldb_health()
print("Santé KSQLDB:", health)

metrics = client.get_metrics()
print("Métriques client:", metrics)
```

#### Advanced Java KSQLDB Client

```java
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.BatchedQueryResult;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AdvancedKSQLDBJavaClient {
    private Client client;
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    
    // Métriques
    private Map<String, Long> metrics = new HashMap<>();
    
    public AdvancedKSQLDBJavaClient(String ksqldbUrl) {
        ClientOptions options = ClientOptions.create()
            .setHost("localhost")
            .setPort(8088);
        
        this.client = Client.create(options);
        initializeMetrics();
    }
    
    private void initializeMetrics() {
        metrics.put("queries_executed", 0L);
        metrics.put("errors", 0L);
        metrics.put("retries", 0L);
    }
    
    public CompletableFuture<Map<String, Object>> executeStatementWithRetry(String sql, int maxRetries) {
        return executeStatementWithRetry(sql, maxRetries, 1.0);
    }
    
    public CompletableFuture<Map<String, Object>> executeStatementWithRetry(String sql, int maxRetries, double retryDelay) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();
        
        executeWithRetry(sql, maxRetries, retryDelay, 0, future);
        
        return future;
    }
    
    private void executeWithRetry(String sql, int maxRetries, double retryDelay, int attempt, 
                                 CompletableFuture<Map<String, Object>> future) {
        client.executeStatement(sql)
            .thenAccept(result -> {
                metrics.put("queries_executed", metrics.get("queries_executed") + 1);
                future.complete(Map.of("status", "success", "result", result));
            })
            .exceptionally(throwable -> {
                metrics.put("errors", metrics.get("errors") + 1);
                
                if (attempt < maxRetries - 1) {
                    metrics.put("retries", metrics.get("retries") + 1);
                    
                    // Attendre avec backoff
                    long delayMs = (long) (retryDelay * 1000 * Math.pow(2, attempt));
                    try {
                        Thread.sleep(delayMs);
                        executeWithRetry(sql, maxRetries, retryDelay, attempt + 1, future);
                    } catch (InterruptedException e) {
                        future.completeExceptionally(e);
                    }
                } else {
                    future.completeExceptionally(throwable);
                }
                return null;
            });
    }
    
    public CompletableFuture<List<Map<String, Object>>> executeBatchStatements(List<String> statements) {
        CompletableFuture<List<Map<String, Object>>> batchFuture = new CompletableFuture<>();
        List<Map<String, Object>> results = new java.util.ArrayList<>();
        
        executeBatchRecursive(statements, results, 0, batchFuture);
        
        return batchFuture;
    }
    
    private void executeBatchRecursive(List<String> statements, List<Map<String, Object>> results, 
                                     int index, CompletableFuture<List<Map<String, Object>>> batchFuture) {
        if (index >= statements.size()) {
            batchFuture.complete(results);
            return;
        }
        
        String sql = statements.get(index);
        executeStatementWithRetry(sql, 3)
            .thenAccept(result -> {
                results.add(result);
                executeBatchRecursive(statements, results, index + 1, batchFuture);
            })
            .exceptionally(throwable -> {
                results.add(Map.of("status", "error", "sql", sql, "error", throwable.getMessage()));
                executeBatchRecursive(statements, results, index + 1, batchFuture);
                return null;
            });
    }
    
    public CompletableFuture<StreamedQueryResult> streamQueryWithTimeout(String sql, long timeoutSeconds) {
        return client.streamQuery(sql)
            .thenApply(result -> {
                // Configuration du timeout
                CompletableFuture<Void> timeoutFuture = new CompletableFuture<>();
                result.subscribe(
                    row -> processRow(row),
                    throwable -> timeoutFuture.completeExceptionally(throwable),
                    () -> timeoutFuture.complete(null)
                );
                
                try {
                    timeoutFuture.get(timeoutSeconds, TimeUnit.SECONDS);
                } catch (TimeoutException | ExecutionException | InterruptedException e) {
                    result.close();
                    throw new RuntimeException("Query timeout", e);
                }
                
                return result;
            });
    }
    
    private void processRow(Row row) {
        // Traitement personnalisé des lignes
        System.out.println("Row: " + row.values());
    }
    
    public CompletableFuture<Map<String, Object>> getStreamInfo(String streamName) {
        String sql = "DESCRIBE " + streamName + ";";
        
        return executeStatementWithRetry(sql, 3)
            .thenApply(result -> {
                if (result.get("status").equals("success")) {
                    return Map.of("stream", streamName, "info", result.get("result"));
                } else {
                    return Map.of("stream", streamName, "error", result.get("error"));
                }
            });
    }
    
    public Map<String, Long> getMetrics() {
        return new HashMap<>(metrics);
    }
    
    public void close() {
        client.close();
    }
    
    public static void main(String[] args) {
        AdvancedKSQLDBJavaClient client = new AdvancedKSQLDBJavaClient("localhost:8088");
        
        try {
            // Exécution d'instructions en lot
            List<String> batchStatements = List.of(
                """
                CREATE STREAM user_events_advanced (
                    user_id VARCHAR,
                    event_type VARCHAR,
                    amount DOUBLE,
                    metadata MAP<VARCHAR, VARCHAR>,
                    timestamp BIGINT
                ) WITH (
                    KAFKA_TOPIC='user-events-advanced',
                    VALUE_FORMAT='JSON'
                );
                """,
                """
                CREATE TABLE user_stats AS
                SELECT user_id, COUNT(*) as event_count, SUM(amount) as total_amount
                FROM user_events_advanced
                WINDOW TUMBLING (SIZE 1 HOUR)
                GROUP BY user_id
                EMIT CHANGES;
                """
            );
            
            client.executeBatchStatements(batchStatements)
                .thenAccept(results -> {
                    System.out.println("Batch results:");
                    results.forEach(System.out::println);
                })
                .get(60, TimeUnit.SECONDS);
            
            // Requête streaming avec timeout
            String query = "SELECT * FROM user_events_advanced EMIT CHANGES;";
            client.streamQueryWithTimeout(query, 30)
                .thenAccept(result -> {
                    System.out.println("Streaming query completed");
                    result.close();
                })
                .get(60, TimeUnit.SECONDS);
            
            // Métriques
            Map<String, Long> metrics = client.getMetrics();
            System.out.println("Client metrics: " + metrics);
            
        } catch (Exception e) {
            System.err.println("Erreur: " + e.getMessage());
        } finally {
            client.close();
        }
    }
}
```

## Configuration et Déploiement

### Docker Compose pour l'environnement de développement

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  ksqldb:
    image: confluentinc/ksqldb-server:latest
    depends_on:
      - kafka
    ports:
      - "8088:8088"
    environment:
      KSQL_BOOTSTRAP_SERVERS: kafka:9092
      KSQL_LISTENERS: http://0.0.0.0:8088
```

### Dépendances

#### Python (requirements.txt)
```
kafka-python==2.0.2
requests==2.31.0
sseclient-py==1.7.2
```

#### Java (Maven pom.xml)
```xml
<dependencies>
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>3.5.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams</artifactId>
        <version>3.5.0</version>
    </dependency>
    <dependency>
        <groupId>io.confluent.ksql</groupId>
        <artifactId>ksqldb-api-client</artifactId>
        <version>7.4.0</version>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>2.15.2</version>
    </dependency>
</dependencies>
```