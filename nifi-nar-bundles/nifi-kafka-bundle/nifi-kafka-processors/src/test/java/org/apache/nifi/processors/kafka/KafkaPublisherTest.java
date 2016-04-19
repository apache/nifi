/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.nifi.processors.kafka.test.EmbeddedKafka;
import org.apache.nifi.processors.kafka.test.EmbeddedKafkaProducerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaPublisherTest {

    private static EmbeddedKafka kafkaLocal;

    private static EmbeddedKafkaProducerHelper producerHelper;

    @BeforeClass
    public static void bforeClass() {
        kafkaLocal = new EmbeddedKafka();
        kafkaLocal.start();
        producerHelper = new EmbeddedKafkaProducerHelper(kafkaLocal);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        producerHelper.close();
        kafkaLocal.stop();
    }

    @Test
    public void validateSuccessfulSendAsWhole() throws Exception {
        InputStream fis = new ByteArrayInputStream("Hello Kafka".getBytes(StandardCharsets.UTF_8));
        String topicName = "validateSuccessfulSendAsWhole";

        Properties kafkaProperties = this.buildProducerProperties();
        KafkaPublisher publisher = new KafkaPublisher(kafkaProperties);

        SplittableMessageContext messageContext = new SplittableMessageContext(topicName, null, null);

        publisher.publish(messageContext, fis, null, 2000);

        fis.close();
        publisher.close();

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        assertNotNull(iter.next());
        try {
            iter.next();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }
    }

    @Test
    public void validateSuccessfulSendAsDelimited() throws Exception {
        InputStream fis = new ByteArrayInputStream(
                "Hello Kafka 1\nHello Kafka 2\nHello Kafka 3\nHello Kafka 4\n".getBytes(StandardCharsets.UTF_8));
        String topicName = "validateSuccessfulSendAsDelimited";

        Properties kafkaProperties = this.buildProducerProperties();
        KafkaPublisher publisher = new KafkaPublisher(kafkaProperties);

        SplittableMessageContext messageContext = new SplittableMessageContext(topicName, null, "\n".getBytes(StandardCharsets.UTF_8));

        publisher.publish(messageContext, fis, null, 2000);
        publisher.close();

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        assertNotNull(iter.next());
        try {
            iter.next();
            fail();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }
    }

    @Test
    public void validateSuccessfulReSendOfFailedSegments() throws Exception {
        InputStream fis = new ByteArrayInputStream(
                "Hello Kafka 1\nHello Kafka 2\nHello Kafka 3\nHello Kafka 4\n".getBytes(StandardCharsets.UTF_8));
        String topicName = "validateSuccessfulReSendOfFailedSegments";

        Properties kafkaProperties = this.buildProducerProperties();

        KafkaPublisher publisher = new KafkaPublisher(kafkaProperties);

        SplittableMessageContext messageContext = new SplittableMessageContext(topicName, null, "\n".getBytes(StandardCharsets.UTF_8));
        messageContext.setFailedSegments(1, 3);

        publisher.publish(messageContext, fis, null, 2000);
        publisher.close();

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        String m1 = new String(iter.next().message());
        String m2 = new String(iter.next().message());
        assertEquals("Hello Kafka 2", m1);
        assertEquals("Hello Kafka 4", m2);
        try {
            iter.next();
            fail();
        } catch (ConsumerTimeoutException e) {
            // that's OK since this is the Kafka mechanism to unblock
        }
    }

    @Test
    public void validateWithMultiByteCharacters() throws Exception {
        String data = "僠THIS IS MY NEW TEXT.僠IT HAS A NEWLINE.";
        InputStream fis = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        String topicName = "validateWithMultiByteCharacters";

        Properties kafkaProperties = this.buildProducerProperties();

        KafkaPublisher publisher = new KafkaPublisher(kafkaProperties);

        SplittableMessageContext messageContext = new SplittableMessageContext(topicName, null, null);

        publisher.publish(messageContext, fis, null, 2000);
        publisher.close();

        ConsumerIterator<byte[], byte[]> iter = this.buildConsumer(topicName);
        String r = new String(iter.next().message(), StandardCharsets.UTF_8);
        assertEquals(data, r);
    }

    private Properties buildProducerProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "0.0.0.0:" + kafkaLocal.getKafkaPort());
        kafkaProperties.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
        kafkaProperties.setProperty("acks", "1");
        kafkaProperties.put("auto.create.topics.enable", "true");
        kafkaProperties.setProperty("partitioner.class", "org.apache.nifi.processors.kafka.Partitioners$RoundRobinPartitioner");
        kafkaProperties.setProperty("timeout.ms", "5000");
        return kafkaProperties;
    }

    private ConsumerIterator<byte[], byte[]> buildConsumer(String topic) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:" + kafkaLocal.getZookeeperPort());
        props.put("group.id", "test");
        props.put("consumer.timeout.ms", "5000");
        props.put("auto.offset.reset", "smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<>(1);
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        ConsumerIterator<byte[], byte[]> iter = streams.get(0).iterator();
        return iter;
    }
}
