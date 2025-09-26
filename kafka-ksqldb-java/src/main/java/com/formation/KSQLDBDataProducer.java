package com.formation;

/*
 * Author: Mohammed REZGUI
 * Version: 1.0.0
 * Description: KSQLDB data producer in Java - generates sample data for KSQLDB exercises
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KSQLDBDataProducer {
    private KafkaProducer<String, String> producer;
    private ObjectMapper objectMapper = new ObjectMapper();
    private Random random = new Random();

    // Données d'exemple
    private List<User> users = Arrays.asList(
        new User("user1", "Alice", "alice@example.com"),
        new User("user2", "Bob", "bob@example.com"),
        new User("user3", "Charlie", "charlie@example.com"),
        new User("user4", "Diana", "diana@example.com"),
        new User("user5", "Eve", "eve@example.com")
    );

    private List<String> actions = Arrays.asList("login", "logout", "view_page", "purchase", "search");
    private List<String> countries = Arrays.asList("FR", "US", "UK", "DE", "ES");

    public KSQLDBDataProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer<>(props);
    }

    public void sendUserEvent(String userId, String action) throws Exception {
        UserEvent event = new UserEvent(userId, action, System.currentTimeMillis(),
                                       "session_" + random.nextInt(9000) + 1000);
        String jsonEvent = objectMapper.writeValueAsString(event);
        ProducerRecord<String, String> record = new ProducerRecord<>("user-events", userId, jsonEvent);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Erreur envoi événement: " + exception.getMessage());
            } else {
                System.out.println("Événement envoyé: " + event);
            }
        });
    }

    public void sendUserProfile(User user) throws Exception {
        UserProfile profile = new UserProfile(user.id, user.name, user.email,
                                            System.currentTimeMillis(),
                                            countries.get(random.nextInt(countries.size())));
        String jsonProfile = objectMapper.writeValueAsString(profile);
        ProducerRecord<String, String> record = new ProducerRecord<>("user-profiles", user.id, jsonProfile);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Erreur envoi profil: " + exception.getMessage());
            } else {
                System.out.println("Profil envoyé: " + profile);
            }
        });
    }

    public void startProducing() throws Exception {
        System.out.println("Démarrage du producteur de données KSQLDB...");

        // Envoyer les profils utilisateurs (une fois)
        for (User user : users) {
            sendUserProfile(user);
            Thread.sleep(500);
        }

        // Générer des événements continuellement
        while (true) {
            User user = users.get(random.nextInt(users.size()));
            String action = actions.get(random.nextInt(actions.size()));
            sendUserEvent(user.id, action);
            Thread.sleep(random.nextInt(2000) + 1000); // 1-3 secondes
        }
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        KSQLDBDataProducer producer = new KSQLDBDataProducer();
        try {
            producer.startProducing();
        } catch (InterruptedException e) {
            System.out.println("Arrêt du producteur...");
        } finally {
            producer.close();
        }
    }

    // Classes de données
    static class User {
        public String id;
        public String name;
        public String email;

        public User() {}
        public User(String id, String name, String email) {
            this.id = id;
            this.name = name;
            this.email = email;
        }
    }

    static class UserEvent {
        public String id;
        public String action;
        public long timestamp;
        public String sessionId;

        public UserEvent() {}
        public UserEvent(String id, String action, long timestamp, String sessionId) {
            this.id = id;
            this.action = action;
            this.timestamp = timestamp;
            this.sessionId = sessionId;
        }

        @Override
        public String toString() {
            return String.format("UserEvent{id='%s', action='%s'}", id, action);
        }
    }

    static class UserProfile {
        public String id;
        public String name;
        public String email;
        public long registrationDate;
        public String country;

        public UserProfile() {}
        public UserProfile(String id, String name, String email, long registrationDate, String country) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.registrationDate = registrationDate;
            this.country = country;
        }

        @Override
        public String toString() {
            return String.format("UserProfile{id='%s', name='%s'}", id, name);
        }
    }
}