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
package org.apache.nifi.processors.kafka.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.nifi.processors.kafka.test.EmbeddedKafka;
import org.apache.nifi.processors.kafka.test.EmbeddedKafkaProducerHelper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class PublishKafkaTest {

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
    public void customValidationTests() {
        String topicName = "customValidationTests";
        PublishKafka publishKafka = new PublishKafka();

        /*
         * Validates that Kerberos principle is required if one of SASL set for
         * secirity protocol
         */
        TestRunner runner = TestRunners.newTestRunner(publishKafka);
        runner.setProperty(PublishKafka.TOPIC, topicName);
        runner.setProperty(PublishKafka.CLIENT_ID, "foo");
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PublishKafka.SECURITY_PROTOCOL, PublishKafka.SEC_SASL_PLAINTEXT);
        try {
            runner.run();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains("'Kerberos Service Name' is invalid because"));
        }
        runner.shutdown();
    }

    @Test
    public void testDelimitedMessagesWithKey() {
        String topicName = "testDelimitedMessagesWithKey";
        PublishKafka putKafka = new PublishKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka.TOPIC, topicName);
        runner.setProperty(PublishKafka.CLIENT_ID, "foo");
        runner.setProperty(PublishKafka.KEY, "key1");
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, "\n");

        runner.enqueue("Hello World\nGoodbye\n1\n2\n3\n4\n5".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
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

    @SuppressWarnings("unchecked")
    @Test
    public void testWithFailureAndPartialResend() throws Exception {
        String topicName = "testWithFailureAndPartialResend";
        PublishKafka putKafka = new PublishKafka();

        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka.TOPIC, topicName);
        runner.setProperty(PublishKafka.CLIENT_ID, "foo");
        runner.setProperty(PublishKafka.KEY, "key1");
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, "\n");
        runner.setProperty(PublishKafka.META_WAIT_TIME, "2 sec");

        final String text = "Hello World\nGoodbye\n1\n2";
        runner.enqueue(text.getBytes(StandardCharsets.UTF_8));
        afterClass(); // kill Kafka right before send to ensure producer fails
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(PublishKafka.REL_FAILURE).get(0);
        String lastFaildAckIdx = ff.getAttribute(PublishKafka.FAILED_LAST_ACK_IDX);
        assertEquals(-1, Integer.parseInt(lastFaildAckIdx));
        String delimiter = ff.getAttribute(PublishKafka.FAILED_DELIMITER_ATTR);
        assertEquals("\n", delimiter);
        String key = ff.getAttribute(PublishKafka.FAILED_KEY_ATTR);
        assertEquals("key1", key);
        String topic = ff.getAttribute(PublishKafka.FAILED_TOPIC_ATTR);
        assertEquals(topicName, topic);
        runner.shutdown();
        AbstractKafkaProcessor<Closeable> proc = (AbstractKafkaProcessor<Closeable>) runner.getProcessor();
        proc.close();

        beforeClass();
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.enqueue(ff);
        runner.run(1, false);
        MockFlowFile sff = runner.getFlowFilesForRelationship(PublishKafka.REL_SUCCESS).get(0);
        assertNull(sff.getAttribute(PublishKafka.FAILED_LAST_ACK_IDX));
        assertNull(sff.getAttribute(PublishKafka.FAILED_TOPIC_ATTR));
        assertNull(sff.getAttribute(PublishKafka.FAILED_KEY_ATTR));
        assertNull(sff.getAttribute(PublishKafka.FAILED_DELIMITER_ATTR));

        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);

        assertEquals("Hello World", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("1", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("2", new String(consumer.next().message(), StandardCharsets.UTF_8));
        try {
            consumer.next();
            fail();
        } catch (Exception e) {
            // ignore
        }
        runner.shutdown();
    }

    @Test
    public void testWithEmptyMessages() {
        String topicName = "testWithEmptyMessages";
        PublishKafka putKafka = new PublishKafka();
        final TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka.TOPIC, topicName);
        runner.setProperty(PublishKafka.KEY, "key1");
        runner.setProperty(PublishKafka.CLIENT_ID, "foo");
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, "\n");

        final byte[] bytes = "\n\n\n1\n2\n\n\n\n3\n4\n\n\n".getBytes(StandardCharsets.UTF_8);
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);

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
    public void testComplexRightPartialDelimitedMessages() {
        String topicName = "testComplexRightPartialDelimitedMessages";
        PublishKafka putKafka = new PublishKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka.TOPIC, topicName);
        runner.setProperty(PublishKafka.CLIENT_ID, "foo");
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDSTUFF僠>僠I Mean IT!僠<僠WILDSTUFF僠>".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("Hello World", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("I Mean IT!僠<僠WILDSTUFF僠>", new String(consumer.next().message(), StandardCharsets.UTF_8));
        runner.shutdown();
    }

    @Test
    public void testComplexLeftPartialDelimitedMessages() {
        String topicName = "testComplexLeftPartialDelimitedMessages";
        PublishKafka putKafka = new PublishKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka.TOPIC, topicName);
        runner.setProperty(PublishKafka.CLIENT_ID, "foo");
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDSTUFF僠>僠I Mean IT!僠<僠WILDSTUFF僠>僠<僠WILDSTUFF僠>僠".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        byte[] message = consumer.next().message();
        assertEquals("Hello World", new String(message, StandardCharsets.UTF_8));
        assertEquals("Goodbye", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("I Mean IT!", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("<僠WILDSTUFF僠>僠", new String(consumer.next().message(), StandardCharsets.UTF_8));
        runner.shutdown();
    }

    @Test
    public void testComplexPartialMatchDelimitedMessages() {
        String topicName = "testComplexPartialMatchDelimitedMessages";
        PublishKafka putKafka = new PublishKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PublishKafka.TOPIC, topicName);
        runner.setProperty(PublishKafka.CLIENT_ID, "foo");
        runner.setProperty(PublishKafka.BOOTSTRAP_SERVERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, "僠<僠WILDSTUFF僠>僠");

        runner.enqueue("Hello World僠<僠WILDSTUFF僠>僠Goodbye僠<僠WILDBOOMSTUFF僠>僠".getBytes(StandardCharsets.UTF_8));
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("Hello World", new String(consumer.next().message(), StandardCharsets.UTF_8));
        assertEquals("Goodbye僠<僠WILDBOOMSTUFF僠>僠", new String(consumer.next().message(), StandardCharsets.UTF_8));
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
