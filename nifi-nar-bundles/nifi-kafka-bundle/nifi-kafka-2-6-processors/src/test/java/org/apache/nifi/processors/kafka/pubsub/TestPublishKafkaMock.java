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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPublishKafkaMock {

    private static long ordinal = 0L;

    /**
     * JSON serialization helper.
     */
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Ensure fresh data for each test run.
     */
    private static final long TIMESTAMP = System.currentTimeMillis();

    /**
     * The name of the test kafka topic to be created.
     */
    private static final String TEST_TOPIC_PUBLISH = "nifi-publish-" + TIMESTAMP;


    @Test
    public void testPublishRecordNullKey() throws JsonProcessingException, InitializationException {
        // create flowfile to publish
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("attrKeyA", "attrValueA");
        attributes.put("attrKeyB", "attrValueB");
        final ObjectNode node = mapper.createObjectNode().put("recordA", 1).put("recordB", "valueB");
        final String value = mapper.writeValueAsString(node);
        final MockFlowFile flowFile = new MockFlowFile(++ordinal);
        flowFile.putAttributes(attributes);
        flowFile.setData(value.getBytes(UTF_8));
        // publish flowfile
        final Collection<ProducerRecord<byte[], byte[]>> producedRecords = new ArrayList<>();
        final TestRunner runner = getTestRunner(producedRecords);
        runner.setProperty("topic", TEST_TOPIC_PUBLISH);
        runner.setProperty("attribute-name-regex", ".*A");
        runner.enqueue(flowFile);
        runner.run(1);
        // verify results
        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        assertEquals(1, producedRecords.size());
        final ProducerRecord<byte[], byte[]> record = producedRecords.iterator().next();
        assertEquals(TEST_TOPIC_PUBLISH, record.topic());
        final Headers headers = record.headers();
        assertEquals(1, headers.toArray().length);
        assertEquals("attrValueA", new String(headers.lastHeader("attrKeyA").value(), UTF_8));
        assertNull(record.key());
        assertEquals(value, new String(record.value(), UTF_8));
    }

    @Test
    public void testPublishRecordStringKey() throws JsonProcessingException, InitializationException {
        // create flowfile to publish
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("attrKeyA", "attrValueA");
        attributes.put("attrKeyB", "attrValueB");
        attributes.put("messageKey", "this-is-a-key");
        final ObjectNode node = mapper.createObjectNode().put("recordA", 1).put("recordB", "valueB");
        final String value = mapper.writeValueAsString(node);
        final MockFlowFile flowFile = new MockFlowFile(++ordinal);
        flowFile.putAttributes(attributes);
        flowFile.setData(value.getBytes(UTF_8));
        // publish flowfile
        final Collection<ProducerRecord<byte[], byte[]>> producedRecords = new ArrayList<>();
        final TestRunner runner = getTestRunner(producedRecords);
        runner.setProperty("topic", TEST_TOPIC_PUBLISH);
        runner.setProperty("attribute-name-regex", ".*B");
        runner.setProperty("message-key-field", "recordB");
        runner.enqueue(flowFile);
        runner.run(1);
        // verify results
        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        assertEquals(1, producedRecords.size());
        final ProducerRecord<byte[], byte[]> record = producedRecords.iterator().next();
        assertEquals(TEST_TOPIC_PUBLISH, record.topic());
        final Headers headers = record.headers();
        assertEquals(1, headers.toArray().length);
        assertEquals("attrValueB", new String(headers.lastHeader("attrKeyB").value(), UTF_8));
        assertNotNull(record.key());
        assertEquals("valueB", new String(record.key(), UTF_8));
        assertNotNull(record.value());
        assertEquals(value, new String(record.value(), UTF_8));
    }

    @Test
    public void testPublishRecordWrapperStrategyNullKey() throws JsonProcessingException, InitializationException {
        // create flowfile to publish
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("attrKeyA", "attrValueA");
        attributes.put("attrKeyB", "attrValueB");
        attributes.put("messageKey", "this-is-a-key");
        final ObjectNode node = mapper.createObjectNode().put("recordA", 1).put("recordB", "valueB");
        final String value = mapper.writeValueAsString(node);
        final MockFlowFile flowFile = new MockFlowFile(++ordinal);
        flowFile.putAttributes(attributes);
        flowFile.setData(value.getBytes(UTF_8));
        // publish flowfile
        final Collection<ProducerRecord<byte[], byte[]>> producedRecords = new ArrayList<>();
        final TestRunner runner = getTestRunner(producedRecords);
        runner.setProperty("topic", TEST_TOPIC_PUBLISH);
        runner.setProperty("attribute-name-regex", "attr.*");
        runner.setProperty("publish-strategy", "use-wrapper");
        runner.enqueue(flowFile);
        runner.run(1);
        // verify results
        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        assertEquals(1, producedRecords.size());
        final ProducerRecord<byte[], byte[]> record = producedRecords.iterator().next();
        assertEquals(TEST_TOPIC_PUBLISH, record.topic());
        final Headers headers = record.headers();
        assertEquals(2, headers.toArray().length);
        assertEquals("attrValueA", new String(headers.lastHeader("attrKeyA").value(), UTF_8));
        assertEquals("attrValueB", new String(headers.lastHeader("attrKeyB").value(), UTF_8));
        assertNull(record.key());
        assertNotNull(record.value());
        assertEquals(value, new String(record.value(), UTF_8));
    }

    @Test
    public void testPublishRecordWrapperStrategyStringKey() throws JsonProcessingException, InitializationException {
        // create flowfile to publish
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("attrKeyA", "attrValueA");
        attributes.put("attrKeyB", "attrValueB");
        attributes.put("messageKey", "this-is-a-key");
        final ObjectNode node = mapper.createObjectNode().put("recordA", 1).put("recordB", "valueB");
        final String value = mapper.writeValueAsString(node);
        final MockFlowFile flowFile = new MockFlowFile(++ordinal);
        flowFile.putAttributes(attributes);
        flowFile.setData(value.getBytes(UTF_8));
        // publish flowfile
        final Collection<ProducerRecord<byte[], byte[]>> producedRecords = new ArrayList<>();
        final TestRunner runner = getTestRunner(producedRecords);
        runner.setProperty("topic", TEST_TOPIC_PUBLISH);
        runner.setProperty("attribute-name-regex", ".*B");
        runner.setProperty("message-key-field", "recordB");
        runner.setProperty("publish-strategy", "use-wrapper");
        runner.enqueue(flowFile);
        runner.run(1);
        // verify results
        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        assertEquals(1, producedRecords.size());
        final ProducerRecord<byte[], byte[]> record = producedRecords.iterator().next();
        assertEquals(TEST_TOPIC_PUBLISH, record.topic());
        final Headers headers = record.headers();
        assertEquals(1, headers.toArray().length);
        assertEquals("attrValueB", new String(headers.lastHeader("attrKeyB").value(), UTF_8));
        assertNotNull(record.key());
        assertEquals("valueB", new String(record.key(), UTF_8));
        assertNotNull(record.value());
        assertEquals(value, new String(record.value(), UTF_8));
    }

    @Test
    public void testPublishRecordWrapperStrategyStringKeyRecordKeyWriter() throws JsonProcessingException, InitializationException {
        // create flowfile to publish
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("attrKeyA", "attrValueA");
        attributes.put("attrKeyB", "attrValueB");
        attributes.put("messageKey", "this-is-a-key");
        final ObjectNode node = mapper.createObjectNode().put("recordA", 1).put("recordB", "valueB");
        final String value = mapper.writeValueAsString(node);
        final MockFlowFile flowFile = new MockFlowFile(++ordinal);
        flowFile.putAttributes(attributes);
        flowFile.setData(value.getBytes(UTF_8));
        // publish flowfile
        final Collection<ProducerRecord<byte[], byte[]>> producedRecords = new ArrayList<>();
        final TestRunner runner = getTestRunner(producedRecords);
        runner.setProperty("topic", TEST_TOPIC_PUBLISH);
        runner.setProperty("attribute-name-regex", ".*B");
        runner.setProperty("message-key-field", "recordB");
        runner.setProperty("publish-strategy", "use-wrapper");
        runner.setProperty("record-key-writer", "record-writer");
        runner.enqueue(flowFile);
        runner.run(1);
        // verify results (processor configuration error state)
        runner.assertTransferCount("success", 0);
        runner.assertTransferCount("failure", 1);
        assertEquals(0, producedRecords.size());
        final List<MockFlowFile> failure = runner.getFlowFilesForRelationship("failure");
        final MockFlowFile flowFileFail = failure.iterator().next();
        assertNotNull(flowFileFail.getAttributes());  // should a "kafka.failure" attribute be added by processor?
    }

    @Test
    public void testPublishRecordWrapperStrategyRecordKeyRecordKeyWriter() throws JsonProcessingException, InitializationException {
        // create flowfile to publish
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("attrKeyA", "attrValueA");
        attributes.put("attrKeyB", "attrValueB");
        final ObjectNode key = mapper.createObjectNode().put("recordKey", "recordValue");
        final String keyString = mapper.writeValueAsString(key);
        final ObjectNode node = mapper.createObjectNode()
                .put("recordA", 1).put("recordB", "valueB").set("recordKey", key);
        final String value = mapper.writeValueAsString(node);
        final MockFlowFile flowFile = new MockFlowFile(++ordinal);
        flowFile.putAttributes(attributes);
        flowFile.setData(value.getBytes(UTF_8));
        // publish flowfile
        final Collection<ProducerRecord<byte[], byte[]>> producedRecords = new ArrayList<>();
        final TestRunner runner = getTestRunner(producedRecords);
        runner.setProperty("topic", TEST_TOPIC_PUBLISH);
        runner.setProperty("publish-strategy", "use-wrapper");
        runner.setProperty("message-key-field", "recordKey");
        runner.setProperty("record-key-writer", "record-writer");
        runner.enqueue(flowFile);
        runner.run(1);
        // verify results
        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        assertEquals(1, producedRecords.size());
        final ProducerRecord<byte[], byte[]> record = producedRecords.iterator().next();
        assertEquals(TEST_TOPIC_PUBLISH, record.topic());
        final Headers headers = record.headers();
        assertEquals(0, headers.toArray().length);
        assertNotNull(record.key());
        assertEquals(keyString, new String(record.key(), UTF_8));
        assertNotNull(record.value());
        assertEquals(value, new String(record.value(), UTF_8));
    }

    private TestRunner getTestRunner(final Collection<ProducerRecord<byte[], byte[]>> producedRecords)
            throws InitializationException {
        final String readerId = "record-reader";
        final RecordReaderFactory readerService = new JsonTreeReader();
        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new JsonRecordSetWriter();
        final PublishKafkaRecord_2_6 processor = new PublishKafkaRecord_2_6() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return getPublisherPool(producedRecords, context);
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
        return runner;
    }

    private PublisherPool getPublisherPool(final Collection<ProducerRecord<byte[], byte[]>> producedRecords,
                                           final ProcessContext context) {
        final PublisherPool publisherPool = mock(PublisherPool.class);
        final PublisherLease publisherLease = getPublisherLease(producedRecords, context);
        when(publisherPool.obtainPublisher()).thenReturn(publisherLease);
        return publisherPool;
    }

    public interface ProducerBB extends Producer<byte[], byte[]> {
    }

    private PublisherLease getPublisherLease(final Collection<ProducerRecord<byte[], byte[]>> producedRecords,
                                             final ProcessContext context) {
        final String attributeNameRegex = context.getProperty("attribute-name-regex").getValue();
        final Pattern patternAttributeName = (attributeNameRegex == null) ? null : Pattern.compile(attributeNameRegex);
        final RecordSetWriterFactory keyWriterFactory = context.getProperty("record-key-writer")
                .asControllerService(RecordSetWriterFactory.class);

        final Producer<byte[], byte[]> producer = mock(ProducerBB.class);
        when(producer.send(any(), any())).then(invocation -> {
            final ProducerRecord<byte[], byte[]> record = invocation.getArgument(0);
            producedRecords.add(record);
            final Callback callback = invocation.getArgument(1);
            callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0), null);
            return null;
        });

        return new PublisherLease(
                producer,
                1024,
                1000L,
                mock(ComponentLog.class),
                true,
                patternAttributeName,
                UTF_8,
                keyWriterFactory);
    }
}
