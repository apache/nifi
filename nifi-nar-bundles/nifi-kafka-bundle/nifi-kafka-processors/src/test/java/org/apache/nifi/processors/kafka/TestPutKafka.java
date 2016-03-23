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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
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
import org.junit.Ignore;
import org.junit.Test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class TestPutKafka {

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
    public void testDelimitedMessagesWithKey() {
        String topicName = "testDelimitedMessagesWithKey";
        PutKafka putKafka = new PutKafka();
        TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        runner.enqueue("Hello World\nGoodbye\n1\n2\n3\n4\n5".getBytes());
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);
        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);
        assertEquals("Hello World", new String(consumer.next().message()));
        assertEquals("Goodbye", new String(consumer.next().message()));
        assertEquals("1", new String(consumer.next().message()));
        assertEquals("2", new String(consumer.next().message()));
        assertEquals("3", new String(consumer.next().message()));
        assertEquals("4", new String(consumer.next().message()));
        assertEquals("5", new String(consumer.next().message()));

        runner.shutdown();
    }

    @Test
    @Ignore
    public void testWithFailureAndPartialResend() throws Exception {
        String topicName = "testWithImmediateFailure";
        PutKafka putKafka = new PutKafka();
        final TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "0.0.0.0:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        final String text = "Hello World\nGoodbye\n1\n2";
        runner.enqueue(text.getBytes());
        afterClass(); // kill Kafka right before send to ensure producer fails
        runner.run(1, false);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(PutKafka.REL_FAILURE).get(0);
        String failedSegmentsStr = ff.getAttribute(PutKafka.ATTR_FAILED_SEGMENTS);
        BitSet fs = BitSet.valueOf(failedSegmentsStr.getBytes());
        assertTrue(fs.get(0));
        assertTrue(fs.get(1));
        assertTrue(fs.get(2));
        assertTrue(fs.get(3));
        String delimiter = ff.getAttribute(PutKafka.ATTR_DELIMITER);
        assertEquals("\n", delimiter);
        String key = ff.getAttribute(PutKafka.ATTR_KEY);
        assertEquals("key1", key);
        String topic = ff.getAttribute(PutKafka.ATTR_TOPIC);
        assertEquals(topicName, topic);

        bforeClass();
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        Map<String, String> attr = new HashMap<>(ff.getAttributes());
        /*
         * So here we are emulating partial success. Basically even though all 4
         * messages failed to be sent by changing the ATTR_FAILED_SEGMENTS value
         * we essentially saying that only two failed and need to be resent.
         */
        BitSet _fs = new BitSet();
        _fs.set(1);
        _fs.set(3);
        attr.put(PutKafka.ATTR_FAILED_SEGMENTS, new String(_fs.toByteArray(), StandardCharsets.UTF_8));
        ff.putAttributes(attr);
        runner.enqueue(ff);
        runner.run(1, false);
        MockFlowFile sff = runner.getFlowFilesForRelationship(PutKafka.REL_SUCCESS).get(0);
        assertNull(sff.getAttribute(PutKafka.ATTR_FAILED_SEGMENTS));
        assertNull(sff.getAttribute(PutKafka.ATTR_TOPIC));
        assertNull(sff.getAttribute(PutKafka.ATTR_KEY));
        assertNull(sff.getAttribute(PutKafka.ATTR_DELIMITER));

        ConsumerIterator<byte[], byte[]> consumer = this.buildConsumer(topicName);

        assertEquals("Goodbye", new String(consumer.next().message()));
        assertEquals("2", new String(consumer.next().message()));
        try {
            consumer.next();
            fail();
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void testWithEmptyMessages() {
        String topicName = "testWithEmptyMessages";
        PutKafka putKafka = new PutKafka();
        final TestRunner runner = TestRunners.newTestRunner(putKafka);
        runner.setProperty(PutKafka.TOPIC, topicName);
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.CLIENT_NAME, "foo");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:" + kafkaLocal.getKafkaPort());
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        final byte[] bytes = "\n\n\n1\n2\n\n\n\n3\n4\n\n\n".getBytes();
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
