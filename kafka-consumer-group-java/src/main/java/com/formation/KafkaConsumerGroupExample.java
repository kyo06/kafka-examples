package com.formation;
/*
 * Author: Mohammed REZGUI
 * Version: 1.0.0
 * Description: Kafka consumer group example in Java demonstrating load balancing across multiple consumer instances.
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class KafkaConsumerGroupExample {
    private KafkaConsumer<String, String> consumer;
    private ObjectMapper objectMapper = new ObjectMapper();
    private String consumerId;

    public KafkaConsumerGroupExample() {
        this.consumerId = UUID.randomUUID().toString().substring(0, 8);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-demo");  // Same group for all consumers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        consumer = new KafkaConsumer<>(props);
    }

    public void consume(String topic) {
        consumer.subscribe(Arrays.asList(topic));

        System.out.println("Consumer " + consumerId + " démarré, en attente de messages...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    System.out.println("Consumer " + consumerId + ": Aucun message reçu, attente...");
                    Thread.sleep(1000);
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumer " + consumerId + " - Topic: " + record.topic());
                    System.out.println("Consumer " + consumerId + " - Partition: " + record.partition());
                    System.out.println("Consumer " + consumerId + " - Offset: " + record.offset());
                    System.out.println("Consumer " + consumerId + " - Key: " + record.key());
                    System.out.println("Consumer " + consumerId + " - Value: " + record.value());
                    System.out.println("-".repeat(60));

                    processMessage(record.value(), consumerId);
                }
            }
        } catch (Exception e) {
            System.err.println("Erreur: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private void processMessage(String jsonMessage, String consumerId) {
        try {
            Message message = objectMapper.readValue(jsonMessage, Message.class);
            System.out.println("Consumer " + consumerId + " - Traitement du message ID: " + message.id);
            // Simulation de traitement
            Thread.sleep(100);
        } catch (Exception e) {
            System.err.println("Erreur lors du parsing: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        KafkaConsumerGroupExample consumer = new KafkaConsumerGroupExample();
        consumer.consume("mon-topic");
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