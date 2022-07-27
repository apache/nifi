package org.apache.nifi.processors.kafka.pubsub;

/**
 * Enumeration of strategies used by {@link PublishKafkaRecord_2_6} to map NiFi FlowFiles to Kafka records.
 */
public enum PublishStrategy {
    USE_VALUE,
    USE_WRAPPER;
}
