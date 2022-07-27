package org.apache.nifi.processors.kafka.pubsub;

/**
 * Enumeration of strategies used by {@link ConsumeKafkaRecord_2_6} to map Kafka records to NiFi FlowFiles.
 */
public enum OutputStrategy {
    USE_VALUE,
    USE_WRAPPER;
}
