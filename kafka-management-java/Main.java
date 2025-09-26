/*
 * Author: Mohammed REZGUI
 * Version: 1.0.0
 * Description: Kafka management example in Java demonstrating how to create topics using the admin client.
 */

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {
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