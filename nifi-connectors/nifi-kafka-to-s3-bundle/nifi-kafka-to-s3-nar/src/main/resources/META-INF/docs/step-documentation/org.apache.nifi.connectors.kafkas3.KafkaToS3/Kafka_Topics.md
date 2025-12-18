# Kafka Topics Configuration

This step configures which Kafka topics to consume from and how to consume them.

## Topic Names

Select one or more Kafka topics to consume from. The connector will automatically
fetch the list of available topics from your Kafka cluster.

## Consumer Group ID

Specify a unique consumer group ID for this connector. Kafka uses consumer groups
to track message offsets and ensure each message is processed only once within a group.

**Best Practice:** Use a descriptive name that identifies the purpose of this connector,
such as `kafka-to-s3-production` or `analytics-pipeline-consumer`.

## Offset Reset

Controls the behavior when no prior offset exists or the current offset is invalid:

| Value | Description |
|-------|-------------|
| earliest | Start reading from the oldest available message |
| latest | Start reading from the newest messages only |
| none | Fail if no prior offset exists |

## Kafka Data Format

Specify the format of messages in your Kafka topics:

- **Avro**: Messages are encoded using Apache Avro (requires Schema Registry)
- **JSON**: Messages are plain JSON objects

