package com.formation;
/*
 * Author: Mohammed REZGUI
 * Version: 1.0.0
 * Description: Kafka consumer example in Java demonstrating how to read messages from a Kafka topic.
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