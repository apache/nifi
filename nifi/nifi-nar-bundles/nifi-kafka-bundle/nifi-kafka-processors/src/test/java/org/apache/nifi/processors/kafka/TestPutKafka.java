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
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.message.CompressionCodec;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

import scala.collection.Seq;

public class TestPutKafka {

    @Test
    public void testMultipleKeyValuePerFlowFile() {
        final TestableProcessor proc = new TestableProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");

        runner.enqueue("Hello World\nGoodbye\n1\n2\n3\n4\n5\n6\n7\n8\n9".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);

        final List<byte[]> messages = proc.getProducer().getMessages();
        assertEquals(11, messages.size());

        assertTrue(Arrays.equals("Hello World".getBytes(StandardCharsets.UTF_8), messages.get(0)));
        assertTrue(Arrays.equals("Goodbye".getBytes(StandardCharsets.UTF_8), messages.get(1)));
        assertTrue(Arrays.equals("1".getBytes(StandardCharsets.UTF_8), messages.get(2)));
        assertTrue(Arrays.equals("2".getBytes(StandardCharsets.UTF_8), messages.get(3)));
        assertTrue(Arrays.equals("3".getBytes(StandardCharsets.UTF_8), messages.get(4)));
        assertTrue(Arrays.equals("4".getBytes(StandardCharsets.UTF_8), messages.get(5)));
        assertTrue(Arrays.equals("5".getBytes(StandardCharsets.UTF_8), messages.get(6)));
        assertTrue(Arrays.equals("6".getBytes(StandardCharsets.UTF_8), messages.get(7)));
        assertTrue(Arrays.equals("7".getBytes(StandardCharsets.UTF_8), messages.get(8)));
        assertTrue(Arrays.equals("8".getBytes(StandardCharsets.UTF_8), messages.get(9)));
        assertTrue(Arrays.equals("9".getBytes(StandardCharsets.UTF_8), messages.get(10)));
    }

    @Test
    public void testWithImmediateFailure() {
        final TestableProcessor proc = new TestableProcessor(0);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");

        final String text = "Hello World\nGoodbye\n1\n2\n3\n4\n5\n6\n7\n8\n9";
        runner.enqueue(text.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKafka.REL_FAILURE, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(PutKafka.REL_FAILURE).get(0);
        mff.assertContentEquals(text);
    }

    @Test
    public void testPartialFailure() {
        final TestableProcessor proc = new TestableProcessor(2);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");
        runner.setProperty(PutKafka.MAX_BUFFER_SIZE, "1 B");

        final byte[] bytes = "1\n2\n3\n4".getBytes();
        runner.enqueue(bytes);
        runner.run();

        runner.assertTransferCount(PutKafka.REL_SUCCESS, 1);
        runner.assertTransferCount(PutKafka.REL_FAILURE, 1);

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PutKafka.REL_SUCCESS).get(0);
        successFF.assertContentEquals("1\n2\n");

        final MockFlowFile failureFF = runner.getFlowFilesForRelationship(PutKafka.REL_FAILURE).get(0);
        failureFF.assertContentEquals("3\n4");
    }

    @Test
    public void testWithEmptyMessages() {
        final TestableProcessor proc = new TestableProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");

        final byte[] bytes = "\n\n\n1\n2\n\n\n\n3\n4\n\n\n".getBytes();
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);

        final List<byte[]> msgs = proc.getProducer().getMessages();
        assertEquals(4, msgs.size());
        assertTrue(Arrays.equals("1".getBytes(), msgs.get(0)));
        assertTrue(Arrays.equals("2".getBytes(), msgs.get(1)));
        assertTrue(Arrays.equals("3".getBytes(), msgs.get(2)));
        assertTrue(Arrays.equals("4".getBytes(), msgs.get(3)));
    }

    @Test
    public void testProvenanceReporterMessagesCount() {
        final TestableProcessor processor = new TestableProcessor();

        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");

        final byte[] bytes = "\n\n\n1\n2\n\n\n\n3\n4\n\n\n".getBytes();
        runner.enqueue(bytes);
        runner.run();

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.SEND, event.getEventType());
        assertEquals("kafka://topic1", event.getTransitUri());
        assertEquals("Sent 4 messages", event.getDetails());
    }

    @Test
    public void testProvenanceReporterWithoutDelimiterMessagesCount() {
        final TestableProcessor processor = new TestableProcessor();

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");

        final byte[] bytes = "\n\n\n1\n2\n\n\n\n3\n4\n\n\n".getBytes();
        runner.enqueue(bytes);
        runner.run();

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.SEND, event.getEventType());
        assertEquals("kafka://topic1", event.getTransitUri());
    }

    @Test
    @Ignore("Intended only for local testing; requires an actual running instance of Kafka & ZooKeeper...")
    public void testKeyValuePut() {
        final TestRunner runner = TestRunners.newTestRunner(PutKafka.class);
        runner.setProperty(PutKafka.SEED_BROKERS, "192.168.0.101:9092");
        runner.setProperty(PutKafka.TOPIC, "${kafka.topic}");
        runner.setProperty(PutKafka.KEY, "${kafka.key}");
        runner.setProperty(PutKafka.TIMEOUT, "3 secs");
        runner.setProperty(PutKafka.DELIVERY_GUARANTEE, PutKafka.DELIVERY_REPLICATED.getValue());

        keyValuePutExecute(runner);
    }

    @Test
    @Ignore("Intended only for local testing; requires an actual running instance of Kafka & ZooKeeper...")
    public void testKeyValuePutAsync() {
        final TestRunner runner = TestRunners.newTestRunner(PutKafka.class);
        runner.setProperty(PutKafka.SEED_BROKERS, "192.168.0.101:9092");
        runner.setProperty(PutKafka.TOPIC, "${kafka.topic}");
        runner.setProperty(PutKafka.KEY, "${kafka.key}");
        runner.setProperty(PutKafka.TIMEOUT, "3 secs");
        runner.setProperty(PutKafka.PRODUCER_TYPE, PutKafka.PRODUCTER_TYPE_ASYNCHRONOUS.getValue());
        runner.setProperty(PutKafka.DELIVERY_GUARANTEE, PutKafka.DELIVERY_REPLICATED.getValue());

        keyValuePutExecute(runner);
    }

    private void keyValuePutExecute(final TestRunner runner) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("kafka.topic", "test");
        attributes.put("kafka.key", "key3");

        final byte[] data = "Hello, World, Again! ;)".getBytes();
        runner.enqueue(data, attributes);
        runner.enqueue(data, attributes);
        runner.enqueue(data, attributes);
        runner.enqueue(data, attributes);

        runner.run(5);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 4);
        final List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(PutKafka.REL_SUCCESS);
        final MockFlowFile mff = mffs.get(0);

        assertTrue(Arrays.equals(data, mff.toByteArray()));
    }

    @Test
    public void testProducerConfigDefault() {

        final TestableProcessor processor = new TestableProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");

        final ProcessContext context = runner.getProcessContext();
        final ProducerConfig config = processor.createConfig(context);

        // Check the codec
        final CompressionCodec codec = config.compressionCodec();
        assertTrue(codec instanceof kafka.message.NoCompressionCodec$);

        // Check compressed topics
        final Seq<String> compressedTopics = config.compressedTopics();
        assertEquals(0, compressedTopics.size());

        // Check the producer type
        final String actualProducerType = config.producerType();
        assertEquals(PutKafka.PRODUCER_TYPE.getDefaultValue(), actualProducerType);

    }

    @Test
    public void testProducerConfigAsyncWithCompression() {

        final TestableProcessor processor = new TestableProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");
        runner.setProperty(PutKafka.PRODUCER_TYPE, PutKafka.PRODUCTER_TYPE_ASYNCHRONOUS.getValue());
        runner.setProperty(PutKafka.COMPRESSION_CODEC, PutKafka.COMPRESSION_CODEC_SNAPPY.getValue());
        runner.setProperty(PutKafka.COMPRESSED_TOPICS, "topic01,topic02,topic03");

        final ProcessContext context = runner.getProcessContext();
        final ProducerConfig config = processor.createConfig(context);

        // Check that the codec is snappy
        final CompressionCodec codec = config.compressionCodec();
        assertTrue(codec instanceof kafka.message.SnappyCompressionCodec$);

        // Check compressed topics
        final Seq<String> compressedTopics = config.compressedTopics();
        assertEquals(3, compressedTopics.size());
        assertTrue(compressedTopics.contains("topic01"));
        assertTrue(compressedTopics.contains("topic02"));
        assertTrue(compressedTopics.contains("topic03"));

        // Check the producer type
        final String actualProducerType = config.producerType();
        assertEquals("async", actualProducerType);

    }

    @Test
    public void testProducerConfigAsyncQueueThresholds() {

        final TestableProcessor processor = new TestableProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");
        runner.setProperty(PutKafka.PRODUCER_TYPE, PutKafka.PRODUCTER_TYPE_ASYNCHRONOUS.getValue());
        runner.setProperty(PutKafka.QUEUE_BUFFERING_MAX, "7 secs");
        runner.setProperty(PutKafka.QUEUE_BUFFERING_MAX_MESSAGES, "535");
        runner.setProperty(PutKafka.QUEUE_ENQUEUE_TIMEOUT, "200 ms");

        final ProcessContext context = runner.getProcessContext();
        final ProducerConfig config = processor.createConfig(context);

        // Check that the queue thresholds were properly translated
        assertEquals(7000, config.queueBufferingMaxMs());
        assertEquals(535, config.queueBufferingMaxMessages());
        assertEquals(200, config.queueEnqueueTimeoutMs());

        // Check the producer type
        final String actualProducerType = config.producerType();
        assertEquals("async", actualProducerType);

    }

    @Test
    public void testProducerConfigInvalidBatchSize() {

        final TestableProcessor processor = new TestableProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");
        runner.setProperty(PutKafka.PRODUCER_TYPE, PutKafka.PRODUCTER_TYPE_ASYNCHRONOUS.getValue());
        runner.setProperty(PutKafka.BATCH_NUM_MESSAGES, "200");
        runner.setProperty(PutKafka.QUEUE_BUFFERING_MAX_MESSAGES, "100");

        runner.assertNotValid();

    }

    @Test
    public void testProducerConfigAsyncDefaultEnqueueTimeout() {

        final TestableProcessor processor = new TestableProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");
        runner.setProperty(PutKafka.PRODUCER_TYPE, PutKafka.PRODUCTER_TYPE_ASYNCHRONOUS.getValue());
        // Do not set QUEUE_ENQUEUE_TIMEOUT

        final ProcessContext context = runner.getProcessContext();
        final ProducerConfig config = processor.createConfig(context);

        // Check that the enqueue timeout defaults to -1
        assertEquals(-1, config.queueEnqueueTimeoutMs());

        // Check the producer type
        final String actualProducerType = config.producerType();
        assertEquals("async", actualProducerType);

    }

    private static class TestableProcessor extends PutKafka {

        private MockProducer producer;
        private int failAfter = Integer.MAX_VALUE;

        public TestableProcessor() {
        }

        public TestableProcessor(final int failAfter) {
            this.failAfter = failAfter;
        }

        @OnScheduled
        public void instantiateProducer(final ProcessContext context) {
            producer = new MockProducer(createConfig(context));
            producer.setFailAfter(failAfter);
        }

        @Override
        protected Producer<byte[], byte[]> createProducer(final ProcessContext context) {
            return producer;
        }

        public MockProducer getProducer() {
            return producer;
        }

        /**
         * Exposed for test verification
         */
        @Override
        public ProducerConfig createConfig(final ProcessContext context) {
            return super.createConfig(context);
        }
    }

    private static class MockProducer extends Producer<byte[], byte[]> {

        private int sendCount = 0;
        private int failAfter = Integer.MAX_VALUE;

        private final List<byte[]> messages = new ArrayList<>();

        public MockProducer(final ProducerConfig config) {
            super(config);
        }

        @Override
        public void send(final KeyedMessage<byte[], byte[]> message) {
            if (++sendCount > failAfter) {
                throw new FailedToSendMessageException("Failed to send message", new RuntimeException("Unit test told to fail after " + failAfter + " successful messages"));
            } else {
                messages.add(message.message());
            }
        }

        public List<byte[]> getMessages() {
            return messages;
        }

        @Override
        public void send(final List<KeyedMessage<byte[], byte[]>> messages) {
            for (final KeyedMessage<byte[], byte[]> msg : messages) {
                send(msg);
            }
        }

        public void setFailAfter(final int successCount) {
            failAfter = successCount;
        }
    }

}
