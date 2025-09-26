/*
 * Author: Mohammed REZGUI
 * Version: 1.0.0
 * Description: KSQLDB example in Java demonstrating how to create streams and query data.
 */

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class Main {
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