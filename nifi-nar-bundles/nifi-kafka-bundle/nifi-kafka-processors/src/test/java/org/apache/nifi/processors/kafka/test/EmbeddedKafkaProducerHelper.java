package org.apache.nifi.processors.kafka.test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.OldProducer;

/**
 * Helper class which helps to produce events targeting {@link EmbeddedKafka}
 * server.
 */
public class EmbeddedKafkaProducerHelper implements Closeable {

    private final EmbeddedKafka kafkaServer;

    private final OldProducer producer;

    /**
     * Will create an instance of EmbeddedKafkaProducerHelper based on default
     * configurations.<br>
     * Default configuration includes:<br>
     * <i>
     * metadata.broker.list=[determined from the instance of EmbeddedKafka]<br>
     * serializer.class=kafka.serializer.DefaultEncoder<br>
     * key.serializer.class=kafka.serializer.DefaultEncoder<br>
     * auto.create.topics.enable=true
     * </i><br>
     * <br>
     * If you wish to supply additional configuration properties or override
     * existing use
     * {@link EmbeddedKafkaProducerHelper#EmbeddedKafkaProducerHelper(EmbeddedKafka, Properties)}
     * constructor.
     *
     * @param kafkaServer
     *            instance of {@link EmbeddedKafka}
     */
    public EmbeddedKafkaProducerHelper(EmbeddedKafka kafkaServer) {
        this(kafkaServer, null);
    }

    /**
     * Will create an instance of EmbeddedKafkaProducerHelper based on default
     * configurations and additional configuration properties.<br>
     * Default configuration includes:<br>
     * metadata.broker.list=[determined from the instance of EmbeddedKafka]<br>
     * serializer.class=kafka.serializer.DefaultEncoder<br>
     * key.serializer.class=kafka.serializer.DefaultEncoder<br>
     * auto.create.topics.enable=true<br>
     * <br>
     *
     * @param kafkaServer
     *            instance of {@link EmbeddedKafka}
     * @param additionalProperties
     *            instance of {@link Properties} specifying additional producer
     *            configuration properties.
     */
    public EmbeddedKafkaProducerHelper(EmbeddedKafka kafkaServer, Properties additionalProperties) {
        this.kafkaServer = kafkaServer;
        Properties producerProperties = new Properties();
        producerProperties.put("metadata.broker.list", "localhost:" + this.kafkaServer.getKafkaPort());
        producerProperties.put("serializer.class", "kafka.serializer.DefaultEncoder");
        producerProperties.put("key.serializer.class", "kafka.serializer.DefaultEncoder");
        producerProperties.put("auto.create.topics.enable", "true");
        if (additionalProperties != null) {
            producerProperties.putAll(additionalProperties);
        }
        this.producer = new OldProducer(producerProperties);
    }

    /**
     * Will send an event to a Kafka topic. If topic doesn't exist it will be
     * auto-created.
     *
     * @param topicName
     *            Kafka topic name.
     * @param event
     *            string representing an event(message) to be sent to Kafka.
     */
    public void sendEvent(String topicName, String event) {
        KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>(topicName, event.getBytes());
        this.producer.send(data.topic(), data.key(), data.message());
    }

    /**
     * Will close the underlying Kafka producer.
     */
    @Override
    public void close() throws IOException {
        this.producer.close();
    }

}
