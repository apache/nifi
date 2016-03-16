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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import kafka.common.FailedToSendMessageException;


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
        runner.run(2); // we have to run twice because the first iteration will result in data being added to a queue in the processor; the second onTrigger call will transfer FlowFiles.

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);

        final List<ProducerRecord<byte[], byte[]>> messages = ((MockProducer) proc.getProducer()).getMessages();
        assertEquals(11, messages.size());

        assertTrue(Arrays.equals("Hello World".getBytes(StandardCharsets.UTF_8), messages.get(0).value()));
        assertTrue(Arrays.equals("Goodbye".getBytes(StandardCharsets.UTF_8), messages.get(1).value()));

        for (int i = 1; i <= 9; i++) {
            assertTrue(Arrays.equals(String.valueOf(i).getBytes(StandardCharsets.UTF_8), messages.get(i + 1).value()));
        }
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
        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_FAILURE, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(PutKafka.REL_FAILURE).get(0);
        mff.assertContentEquals(text);
    }

    @Test
    public void testPartialFailure() {
        final TestableProcessor proc = new TestableProcessor(2); // fail after sending 2 messages.
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");
        runner.setProperty(PutKafka.MAX_BUFFER_SIZE, "1 B");

        final byte[] bytes = "1\n2\n3\n4".getBytes();
        runner.enqueue(bytes);
        runner.run(2);

        runner.assertTransferCount(PutKafka.REL_SUCCESS, 1);
        runner.assertTransferCount(PutKafka.REL_FAILURE, 1);

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PutKafka.REL_SUCCESS).get(0);
        successFF.assertContentEquals("1\n2\n");

        final MockFlowFile failureFF = runner.getFlowFilesForRelationship(PutKafka.REL_FAILURE).get(0);
        failureFF.assertContentEquals("3\n4");
    }

    @Test
    public void testPartialFailureWithSuccessBeforeAndAfter() {
        final TestableProcessor proc = new TestableProcessor(2, 4); // fail after sending 2 messages, then stop failing after 4
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\\n");
        runner.setProperty(PutKafka.MAX_BUFFER_SIZE, "1 B");

        final byte[] bytes = "1\n2\n3\n4\n5\n6".getBytes();
        runner.enqueue(bytes);
        runner.run(2);

        runner.assertTransferCount(PutKafka.REL_SUCCESS, 2);
        runner.assertTransferCount(PutKafka.REL_FAILURE, 1);

        final List<MockFlowFile> success = runner.getFlowFilesForRelationship(PutKafka.REL_SUCCESS);
        for (final MockFlowFile successFF : success) {
            if ('1' == successFF.toByteArray()[0]) {
                successFF.assertContentEquals("1\n2\n");
            } else if ('5' == successFF.toByteArray()[0]) {
                successFF.assertContentEquals("5\n6");
            } else {
                Assert.fail("Wrong content for FlowFile; contained " + new String(successFF.toByteArray()));
            }
        }

        final MockFlowFile failureFF = runner.getFlowFilesForRelationship(PutKafka.REL_FAILURE).get(0);
        failureFF.assertContentEquals("3\n4\n");
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
        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);

        final List<ProducerRecord<byte[], byte[]>> msgs = ((MockProducer) proc.getProducer()).getMessages();
        assertEquals(4, msgs.size());

        for (int i = 1; i <= 4; i++) {
            assertTrue(Arrays.equals(String.valueOf(i).getBytes(), msgs.get(i - 1).value()));
        }
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
        runner.run(2);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.SEND, event.getEventType());
        assertEquals("kafka://localhost:1111/topics/topic1", event.getTransitUri());
        assertTrue(event.getDetails().startsWith("Sent 4 messages"));
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
        runner.run(2);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ProvenanceEventType.SEND, event.getEventType());
        assertEquals("kafka://localhost:1111/topics/topic1", event.getTransitUri());
    }

    @Test
    public void testRoundRobinAcrossMultipleMessages() {
        final TestableProcessor proc = new TestableProcessor();

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.PARTITION_STRATEGY, PutKafka.ROUND_ROBIN_PARTITIONING);

        runner.enqueue("hello".getBytes());
        runner.enqueue("there".getBytes());
        runner.enqueue("how are you".getBytes());
        runner.enqueue("today".getBytes());

        runner.run(5);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 4);

        final List<ProducerRecord<byte[], byte[]>> records = ((MockProducer) proc.getProducer()).getMessages();
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, records.get(i).partition().intValue());
        }

        assertEquals(1, records.get(3).partition().intValue());
    }

    @Test
    public void testRoundRobinAcrossMultipleMessagesInSameFlowFile() {
        final TestableProcessor proc = new TestableProcessor();

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.PARTITION_STRATEGY, PutKafka.ROUND_ROBIN_PARTITIONING);
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        runner.enqueue("hello\nthere\nhow are you\ntoday".getBytes());

        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);

        final List<ProducerRecord<byte[], byte[]>> records = ((MockProducer) proc.getProducer()).getMessages();
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, records.get(i).partition().intValue());
        }

        assertEquals(1, records.get(3).partition().intValue());
    }


    @Test
    public void testUserDefinedPartition() {
        final TestableProcessor proc = new TestableProcessor();

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.PARTITION_STRATEGY, PutKafka.USER_DEFINED_PARTITIONING);
        runner.setProperty(PutKafka.PARTITION, "${part}");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("part", "3");
        runner.enqueue("hello\nthere\nhow are you\ntoday".getBytes(), attrs);

        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);

        final List<ProducerRecord<byte[], byte[]>> records = ((MockProducer) proc.getProducer()).getMessages();
        for (int i = 0; i < 4; i++) {
            assertEquals(3, records.get(i).partition().intValue());
        }
    }



    @Test
    public void testUserDefinedPartitionWithInvalidValue() {
        final TestableProcessor proc = new TestableProcessor();

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.PARTITION_STRATEGY, PutKafka.USER_DEFINED_PARTITIONING);
        runner.setProperty(PutKafka.PARTITION, "${part}");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("part", "bogus");
        runner.enqueue("hello\nthere\nhow are you\ntoday".getBytes(), attrs);

        runner.run(2);

        runner.assertAllFlowFilesTransferred(PutKafka.REL_SUCCESS, 1);

        final List<ProducerRecord<byte[], byte[]>> records = ((MockProducer) proc.getProducer()).getMessages();
        // should all be the same partition, regardless of what partition it is.
        final int partition = records.get(0).partition().intValue();

        for (int i = 0; i < 4; i++) {
            assertEquals(partition, records.get(i).partition().intValue());
        }
    }


    @Test
    public void testFullBuffer() {
        final TestableProcessor proc = new TestableProcessor();

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutKafka.TOPIC, "topic1");
        runner.setProperty(PutKafka.KEY, "key1");
        runner.setProperty(PutKafka.SEED_BROKERS, "localhost:1234");
        runner.setProperty(PutKafka.MESSAGE_DELIMITER, "\n");
        runner.setProperty(PutKafka.MAX_BUFFER_SIZE, "5 B");
        proc.setMaxQueueSize(10L); // will take 4 bytes for key and 1 byte for value.

        runner.enqueue("1\n2\n3\n4\n".getBytes());
        runner.run(2);

        runner.assertTransferCount(PutKafka.REL_SUCCESS, 1);
        runner.assertTransferCount(PutKafka.REL_FAILURE, 1);

        runner.getFlowFilesForRelationship(PutKafka.REL_SUCCESS).get(0).assertContentEquals("1\n2\n");
        runner.getFlowFilesForRelationship(PutKafka.REL_FAILURE).get(0).assertContentEquals("3\n4\n");
    }


    /**
     * Used to override the {@link #getProducer()} method so that we can enforce that our MockProducer is used
     */
    private static class TestableProcessor extends PutKafka {
        private final MockProducer producer;

        public TestableProcessor() {
            this(null);
        }

        public TestableProcessor(final Integer failAfter) {
            this(failAfter, null);
        }

        public TestableProcessor(final Integer failAfter, final Integer stopFailingAfter) {
            producer = new MockProducer();
            producer.setFailAfter(failAfter);
            producer.setStopFailingAfter(stopFailingAfter);
        }

        @Override
        protected Producer<byte[], byte[]> getProducer() {
            return producer;
        }

        public void setMaxQueueSize(final long bytes) {
            producer.setMaxQueueSize(bytes);
        }
    }


    /**
     * We have our own Mock Producer, which is very similar to the Kafka-supplied one. However, with the Kafka-supplied
     * Producer, we don't have the ability to tell it to fail after X number of messages; rather, we can only tell it
     * to fail on the next message. Since we are sending multiple messages in a single onTrigger call for the Processor,
     * this doesn't allow us to test failure conditions adequately.
     */
    private static class MockProducer implements Producer<byte[], byte[]> {

        private int sendCount = 0;
        private Integer failAfter;
        private Integer stopFailingAfter;
        private long queueSize = 0L;
        private long maxQueueSize = Long.MAX_VALUE;

        private final List<ProducerRecord<byte[], byte[]>> messages = new ArrayList<>();

        public MockProducer() {
        }

        public void setMaxQueueSize(final long bytes) {
            this.maxQueueSize = bytes;
        }

        public List<ProducerRecord<byte[], byte[]>> getMessages() {
            return messages;
        }

        public void setFailAfter(final Integer successCount) {
            failAfter = successCount;
        }

        public void setStopFailingAfter(final Integer stopFailingAfter) {
            this.stopFailingAfter = stopFailingAfter;
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record) {
            return send(record, null);
        }

        @Override
        public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final Callback callback) {
            sendCount++;

            final ByteArraySerializer serializer = new ByteArraySerializer();
            final int keyBytes = serializer.serialize(record.topic(), record.key()).length;
            final int valueBytes = serializer.serialize(record.topic(), record.value()).length;
            if (maxQueueSize - queueSize < keyBytes + valueBytes) {
                throw new BufferExhaustedException("Queue size is " + queueSize + " but serialized message is " + (keyBytes + valueBytes));
            }

            queueSize += keyBytes + valueBytes;

            if (failAfter != null && sendCount > failAfter && ((stopFailingAfter == null) || (sendCount < stopFailingAfter + 1))) {
                final Exception e = new FailedToSendMessageException("Failed to send message", new RuntimeException("Unit test told to fail after " + failAfter + " successful messages"));
                callback.onCompletion(null, e);
            } else {
                messages.add(record);
                final RecordMetadata meta = new RecordMetadata(new TopicPartition(record.topic(), record.partition() == null ? 1 : record.partition()), 0L, 0L);
                callback.onCompletion(meta, null);
            }

            // we don't actually look at the Future in the processor, so we can just return null
            return null;
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            final Node leader = new Node(1, "localhost", 1111);
            final Node node2 = new Node(2, "localhost-2", 2222);
            final Node node3 = new Node(3, "localhost-3", 3333);

            final PartitionInfo partInfo1 = new PartitionInfo(topic, 1, leader, new Node[] {node2, node3}, new Node[0]);
            final PartitionInfo partInfo2 = new PartitionInfo(topic, 2, leader, new Node[] {node2, node3}, new Node[0]);
            final PartitionInfo partInfo3 = new PartitionInfo(topic, 3, leader, new Node[] {node2, node3}, new Node[0]);

            final List<PartitionInfo> infos = new ArrayList<>(3);
            infos.add(partInfo1);
            infos.add(partInfo2);
            infos.add(partInfo3);
            return infos;
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return Collections.emptyMap();
        }

        @Override
        public void close() {
        }
    }
}
