# Kafka Examples

This repository contains examples of Apache Kafka implementations in Java and Python, covering producers, consumers, streams, ksqlDB, and management operations.

## Project Structure

### Java Examples (Maven-based)
- **kafka-producer-java**: Basic Kafka producer example
- **kafka-consumer-java**: Basic Kafka consumer example
- **kafka-consumer-group-java**: Consumer group example demonstrating load balancing
- **kafka-streams-java**: Kafka Streams processing example
- **kafka-ksqldb-java**: ksqlDB integration example
- **kafka-management-java**: Kafka administration and management example

### Python Examples
- **kafka-producer-python**: Basic Kafka producer example
- **kafka-consumer-python**: Basic Kafka consumer example
- **kafka-consumer-group-python**: Consumer group example demonstrating load balancing
- **kafka-streams-python**: Kafka Streams processing example
- **kafka-ksqldb-python**: ksqlDB integration example
- **kafka-management-python**: Kafka administration and management example

## Prerequisites

### For Java Examples
- Java 8 or higher
- Maven 3.6+
- Apache Kafka (via Docker Compose)

### For Python Examples
- Python 3.7+
- Required packages listed in `requirements.txt` (per project)
- Apache Kafka (via Docker Compose)

## Running the Examples

Each project directory contains:
- `docker-compose.yml`: For running Kafka infrastructure
- Source code and configuration files
- Build/test scripts

### General Steps
1. Navigate to the desired project directory
2. Start Kafka infrastructure: `docker-compose up -d`
3. Run the example (see project-specific README if available)
4. Stop infrastructure: `docker-compose down`

### Java Projects
```bash
cd kafka-producer-java
mvn clean compile
mvn exec:java -Dexec.mainClass="com.formation.KafkaProducerExample"
```

### Python Projects
```bash
cd kafka-producer-python
pip install -r requirements.txt
python main.py
```

## Documentation

Additional documentation can be found in the `docs/` directory.

## Contributing

Please ensure all examples follow the established patterns and include appropriate .gitignore files.

## License

This project is for educational purposes.