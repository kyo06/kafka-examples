package com.formation;
/*
 * Author: Mohammed REZGUI
 * Version: 1.0.0
 * Description: Kafka Streams example in Java demonstrating stream processing with word count aggregation.
 */


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.KeyValue;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Arrays;
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