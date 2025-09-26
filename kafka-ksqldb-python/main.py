"""
Author: Mohammed REZGUI
Version: 1.0.0
Description: KSQLDB example in Python demonstrating how to create streams and query data.
"""

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