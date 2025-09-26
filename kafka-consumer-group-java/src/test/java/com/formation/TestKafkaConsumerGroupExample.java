package com.formation;
/*
 * Author: Mohammed REZGUI
 * Version: 1.0.0
 * Description: Unit tests for the Kafka consumer group example.
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestKafkaConsumerGroupExample {

    private KafkaConsumerGroupExample consumerExample;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        consumerExample = new KafkaConsumerGroupExample();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testMessageSerialization() throws Exception {
        Message message = new Message(123, 1234567890L, "test message");
        String json = objectMapper.writeValueAsString(message);

        Message deserialized = objectMapper.readValue(json, Message.class);

        assertEquals(message.id, deserialized.id);
        assertEquals(message.timestamp, deserialized.timestamp);
        assertEquals(message.data, deserialized.data);
    }

    @Test
    void testConsumerInstantiation() {
        assertNotNull(consumerExample);
    }
}