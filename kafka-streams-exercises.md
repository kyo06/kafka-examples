# Kafka Streams Exercises

## Exercise 1: Simple Passthrough Stream

Create a Kafka Streams application that reads messages from an input topic "input-topic" and writes them to an output topic "output-topic" without any transformation.

### Correction

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class PassthroughStream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "passthrough-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("input-topic");
        source.to("output-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### Python Correction

```python
from kafka import KafkaConsumer, KafkaProducer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    'input-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='passthrough-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    producer.send('output-topic', value=message.value)
```

## Exercise 2: Word Count

Create a Kafka Streams application that reads sentences from "sentences-topic", splits them into words, and counts the occurrences of each word, writing the results to "word-count-topic".

### Correction

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.util.Arrays;
import java.util.Properties;

public class WordCountStream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("sentences-topic");
        KTable<String, Long> wordCounts = source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
            .groupBy((key, word) -> word)
            .count();

        wordCounts.toStream().to("word-count-topic", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### Python Correction

```python
from kafka import KafkaConsumer, KafkaProducer
import json
from collections import defaultdict
import threading
import time

word_counts = defaultdict(int)

consumer = KafkaConsumer(
    'sentences-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='wordcount-group',
    value_deserializer=lambda m: m.decode('utf-8')
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_counts():
    while True:
        time.sleep(10)  # Publish every 10 seconds
        if word_counts:
            for word, count in word_counts.items():
                producer.send('word-count-topic', key=word, value={'word': word, 'count': count})

threading.Thread(target=publish_counts, daemon=True).start()

for message in consumer:
    sentence = message.value
    words = sentence.lower().split()
    for word in words:
        word_counts[word] += 1
```

## Exercise 4: Windowed Word Count

Create a Kafka Streams application that performs word count with tumbling windows of 1 minute, reading from "sentences-topic" and writing windowed counts to "windowed-word-count-topic".

### Correction

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class WindowedWordCount {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-wordcount-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("sentences-topic");

        TimeWindowedKStream<String, String> windowed = source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
            .groupBy((key, word) -> word)
            .windowedBy(TimeWindows.of(Duration.ofMinutes(1)));

        KTable<Windowed<String>, Long> wordCounts = windowed.count();

        wordCounts.toStream().to("windowed-word-count-topic", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### Python Correction

```python
# Note: Python kafka-python doesn't have built-in windowing like Kafka Streams.
# This is a simplified version without proper windowing.
from kafka import KafkaConsumer, KafkaProducer
import json
from collections import defaultdict
import threading
import time

word_counts = defaultdict(int)
last_publish = time.time()

consumer = KafkaConsumer(
    'sentences-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='windowed-wordcount-group',
    value_deserializer=lambda m: m.decode('utf-8')
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_windowed_counts():
    global word_counts, last_publish
    while True:
        time.sleep(60)  # Every minute
        current_time = time.time()
        window_data = {
            'window_start': last_publish,
            'window_end': current_time,
            'counts': dict(word_counts)
        }
        producer.send('windowed-word-count-topic', value=window_data)
        word_counts.clear()  # Reset for next window
        last_publish = current_time

threading.Thread(target=publish_windowed_counts, daemon=True).start()

for message in consumer:
    sentence = message.value
    words = sentence.lower().split()
    for word in words:
        word_counts[word] += 1
```

## Exercise 5: Stream Join

Create a Kafka Streams application that joins two streams: "user-events" (key: userId, value: event) and "user-profiles" (key: userId, value: profile), performing an inner join and writing results to "joined-events".

### Correction

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;

public class StreamJoin {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> events = builder.stream("user-events");
        KStream<String, String> profiles = builder.stream("user-profiles");

        KStream<String, String> joined = events.join(profiles,
            (event, profile) -> event + " | " + profile,
            JoinWindows.of(Duration.ofMinutes(5))
        );

        joined.to("joined-events");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### Python Correction

```python
from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'messages-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='filter-group',
    value_deserializer=lambda m: m.decode('utf-8')
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: v.encode('utf-8')
)

for message in consumer:
    text = message.value.lower()
    if 'important' in text:
        producer.send('important-messages-topic', value=message.value)
```

### Python Correction

```python
# Simplified join using in-memory storage (not production-ready)
from kafka import KafkaConsumer, KafkaProducer
import json
import threading
import time

events_cache = {}
profiles_cache = {}

consumer_events = KafkaConsumer(
    'user-events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='join-group-events',
    value_deserializer=lambda m: m.decode('utf-8')
)

consumer_profiles = KafkaConsumer(
    'user-profiles',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='join-group-profiles',
    value_deserializer=lambda m: m.decode('utf-8')
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def process_events():
    for message in consumer_events:
        user_id = message.key.decode('utf-8') if message.key else 'unknown'
        event = message.value
        events_cache[user_id] = event
        if user_id in profiles_cache:
            joined = f"{event} | {profiles_cache[user_id]}"
            producer.send('joined-events', key=user_id, value=joined)

def process_profiles():
    for message in consumer_profiles:
        user_id = message.key.decode('utf-8') if message.key else 'unknown'
        profile = message.value
        profiles_cache[user_id] = profile
        if user_id in events_cache:
            joined = f"{events_cache[user_id]} | {profile}"
            producer.send('joined-events', key=user_id, value=joined)

threading.Thread(target=process_events, daemon=True).start()
threading.Thread(target=process_profiles, daemon=True).start()

# Keep running
while True:
    time.sleep(1)
```

## Exercise 6: Stream Branching

Create a Kafka Streams application that reads from "orders" topic and branches messages into two output topics: "high-value-orders" for orders > 100, and "low-value-orders" for others. Assume values are JSON with "amount" field.

### Correction

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;

public class StreamBranching {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branching-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> orders = builder.stream("orders");

        ObjectMapper mapper = new ObjectMapper();
        KStream<String, String>[] branches = orders.branch(
            (key, value) -> {
                try {
                    JsonNode node = mapper.readTree(value);
                    return node.get("amount").asDouble() > 100;
                } catch (Exception e) {
                    return false;
                }
            },
            (key, value) -> true
        );

        branches[0].to("high-value-orders");
        branches[1].to("low-value-orders");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### Python Correction

```python
from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='branching-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    order = message.value
    amount = order.get('amount', 0)
    if amount > 100:
        producer.send('high-value-orders', value=order)
    else:
        producer.send('low-value-orders', value=order)
```

## Exercise 3: Filter Stream

Create a Kafka Streams application that reads from "messages-topic" and filters messages that contain the word "important", writing them to "important-messages-topic".

### Correction

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class FilterStream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("messages-topic");
        KStream<String, String> filtered = source.filter((key, value) -> value.toLowerCase().contains("important"));
        filtered.to("important-messages-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}