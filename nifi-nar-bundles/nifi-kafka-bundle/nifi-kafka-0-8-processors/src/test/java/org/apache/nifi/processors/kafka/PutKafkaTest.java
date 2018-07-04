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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.nifi.processors.kafka.test.EmbeddedKafka;
import org.apache.nifi.processors.kafka.test.EmbeddedKafkaProducerHelper;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

// The test is valid and should be ran when working on this module. @Ignore is
// to speed up the overall build
@Ignore
@SuppressWarnings("deprecation")
public class PutKafkaTest {

    private static EmbeddedKafka kafkaLocal;

    private static EmbeddedKafkaProducerHelper producerHelper;

    @BeforeClass
    public static void beforeClass() {
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
    public void validateSingleCharacterDemarcatedMessages() {
        String topicName = "validateSingleCharacterDemarcatedMessages";
        PutKafka putKafka = new PutKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        runner.enqueue("Hello World\nGoodbye\n1\n2\n3\n4\n5".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("Hello World", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("1", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("2", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("3", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("4", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("5", new String(consumer.next().message(), StandardCharsets.UTF_8));

        runner.shutdown();
    }

    @Test
    public void validateMultiCharacterDelimitedMessages() {
        String topicName = "validateMultiCharacterDemarcatedMessagesAndCustomPartitioner";
        PutKafka putKafka = new PutKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "foo");

        runner.enqueue("Hello WorldfooGoodbyefoo1foo2foo3foo4foo5".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("Hello World", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("1", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("2", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("3", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("4", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("5", new String(consumer.next().message(), StandardCharsets.UTF_8));

        runner.shutdown();
    }

    @Test
    public void validateDemarcationIntoEmptyMessages() {
        String topicName = "validateDemarcationIntoEmptyMessages";
        PutKafka putKafka = new PutKafka();
        final TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        final byte[] bytes = "\n\n\n1\n2\n\n\n3\n4\n\n\n".getBytes(StandardCharsets.UTF_8);
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);

        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);

        assertNotNull(consumer.next());
        assertNotNull(consumer.next());
        assertNotNull(consumer.next());
        assertNotNull(consumer.next());
        try {
            consumer.next();
            fail();
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void validateComplexRightPartialDemarcatedMessages() {
        String topicName = "validateComplexRightPartialDemarcatedMessages";
        PutKafka putKafka = new PutKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDSTUFF僠>僠I Mean IT!僠<僠WILDSTUFF僠>".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("Hello World", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("I Mean IT!僠<僠WILDSTUFF僠>", new String(consumer.next().message(), StandardCharsets.UTF_8));
        runner.shutdown();
    }

    @Test
    public void validateComplexLeftPartialDemarcatedMessages() {
        String topicName = "validateComplexLeftPartialDemarcatedMessages";
        PutKafka putKafka = new PutKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDSTUFF僠>僠I Mean IT!僠<僠WILDSTUFF僠>僠<僠WILDSTUFF僠>僠".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        byte[] message = consumer.next().message();
        assertEquals("Hello World", new String(message, StandardCharsets.UTF_8));
        assertEquals("Goodbye", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("I Mean IT!", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("<僠WILDSTUFF僠>僠", new String(consumer.next().message(), StandardCharsets.UTF_8));
        runner.shutdown();
    }

    @Test
    public void validateComplexPartialMatchDemarcatedMessages() {
        String topicName = "validateComplexPartialMatchDemarcatedMessages";
        PutKafka putKafka = new PutKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDBOOMSTUFF僠>僠".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("Hello World", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye僠<僠WILDBOOMSTUFF僠>僠", new String(consumer.next().message(), StandardCharsets.UTF_8));
        runner.shutdown();
    }

    @Test
    public void validateDeprecatedPartitionStrategy() {
        String topicName = "validateDeprecatedPartitionStrategy";
        PutKafka putKafka = new PutKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        // Old configuration using deprecated property still work.
        runner.setProperty(PutKafka.PARTITION_STRATEGY, PutKafka.USER_DEFINED_PARTITIONING);
        runner.setProperty(PutKafka.PARTITION, "${partition}");

        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("partition", "0");
        runner.enqueue("Hello World\nGoodbye".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("Hello World", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye", new String(consumer.next().message(), StandardCharsets.UTF_8));

        runner.shutdown();
    }

    @Test
    public void validatePartitionOutOfBounds() {
        String topicName = "validatePartitionOutOfBounds";
        PutKafka putKafka = new PutKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");
        runner.setProperty(PutKafka.PARTITION, "${partition}");

        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("partition", "123");
        runner.enqueue("Hello World\nGoodbye".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run(1, false);

        assertTrue("Error message should be logged", runner.getLogger().getErrorMessages().size() > 0);
        runner.assertTransferCount(PutKafka.REL_SUCCESS, 0);
        runner.assertTransferCount(PutKafka.REL_FAILURE, 1);

        runner.shutdown();
    }

    private ConsumerIterator<byte[], byte[]> buildConsumer(String topic) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "0.0.0.0:" + kafkaLocal.getZookeeperPort());
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
