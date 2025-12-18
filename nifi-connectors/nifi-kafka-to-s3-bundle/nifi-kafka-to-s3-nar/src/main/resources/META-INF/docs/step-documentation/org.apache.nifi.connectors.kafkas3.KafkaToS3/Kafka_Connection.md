# Kafka Connection Configuration

This step configures the connection to your Apache Kafka cluster.

## Kafka Server Settings

Enter the bootstrap servers for your Kafka cluster. You can specify multiple brokers
as a comma-separated list (e.g., `broker1:9092,broker2:9092,broker3:9092`).

### Security Configuration

Select the appropriate security protocol based on your Kafka cluster configuration:

| Protocol | Description |
|----------|-------------|
| PLAINTEXT | No encryption or authentication |
| SSL | TLS encryption without SASL authentication |
| SASL_PLAINTEXT | SASL authentication without encryption |
| SASL_SSL | Both SASL authentication and TLS encryption (recommended) |

If using SASL authentication, provide your username and password credentials.

## Schema Registry (Optional)

If your Kafka topics use Avro, Protobuf, or JSON Schema, configure the Schema Registry
URL to enable schema-based serialization and deserialization.

