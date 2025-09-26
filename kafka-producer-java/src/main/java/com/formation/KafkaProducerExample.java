package com.formation;
/*
 * Author: Mohammed REZGUI
 * Version: 1.0.0
 * Description: Kafka producer example in Java demonstrating how to send messages to a Kafka topic.
 */


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
        // Configuration du partitioner (DefaultPartitioner: hash de clé + round-robin)
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        // Alternatives:
        // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
        // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.UniformStickyPartitioner");

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

    public Message() {
        // Default constructor for Jackson
    }

    public Message(int id, long timestamp, String data) {
        this.id = id;
        this.timestamp = timestamp;
        this.data = data;
    }
}