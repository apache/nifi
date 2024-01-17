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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.BOOTSTRAP_SERVERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConsumeKafkaMock {

    /**
     * JSON serialization helper.
     */
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Kafka server endpoint (mock) for test interactions.
     */
    private static final String TEST_BOOTSTRAP_SERVER = "localhost:59092";

    /**
     * Ensure fresh data for each test run.
     */
    private static final long TIMESTAMP = System.currentTimeMillis();

    /**
     * The name of the test kafka topic to be created.
     */
    private static final String TEST_TOPIC = "nifi-consume-" + TIMESTAMP;

    /**
     * The name of the test kafka group to use.
     */
    private static final String TEST_GROUP = "nifi-group-" + TIMESTAMP;

    /**
     * Kafka topic attribute should be available to RecordReader, so that the 'ValueSeparator' character can be
     * correctly configured.
     */
    @Test
    public void testConsumeRecordDynamicReader() throws Exception {
        final String value = "ALPHA;BETA\na1;a2\nb1;b2\n";
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                TEST_TOPIC, 0, 0, null, value.getBytes(UTF_8));
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final ConsumeKafkaRecord_2_6 processor = new ConsumeKafkaRecord_2_6() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return getConsumerPool(consumerRecords, context, log);
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(BOOTSTRAP_SERVERS, TEST_BOOTSTRAP_SERVER);
        runner.setProperty("topic", TEST_TOPIC);
        runner.setProperty("topic_type", "names");
        runner.setProperty(ConsumerConfig.GROUP_ID_CONFIG, TEST_GROUP);
        runner.setProperty("auto.offset.reset", "earliest");
        final String readerId = "record-reader";
        final RecordReaderFactory readerService = new CSVReader();
        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new CSVRecordSetWriter();
        runner.addControllerService(readerId, readerService);
        runner.setProperty(readerService, CSVUtils.VALUE_SEPARATOR,
                "${kafka.topic:startsWith('nifi-consume'):ifElse(';', ',')}");
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals("ALPHA,BETA\na1,a2\nb1,b2\n", flowFile.getContent());
    }

    @Test
    public void testConsumeRecordNullKey() throws JsonProcessingException, InitializationException {
        final ObjectNode node = mapper.createObjectNode().put("a", 1).put("b", "2");
        final String value = mapper.writeValueAsString(node);
        final ArrayNode nodeRecordSet = mapper.createArrayNode().add(node);
        final String valueRecordSet = mapper.writeValueAsString(nodeRecordSet);
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                TEST_TOPIC, 0, 0, null, value.getBytes(UTF_8));
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final TestRunner runner = getTestRunner(consumerRecords, TEST_TOPIC, TEST_GROUP);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(valueRecordSet, flowFile.getContent());
        assertNull(flowFile.getAttribute("kafka.key"));
        assertEquals("0", flowFile.getAttribute("kafka.partition"));
        assertEquals(TEST_TOPIC, flowFile.getAttribute("kafka.topic"));
        assertEquals(TEST_GROUP, flowFile.getAttribute("kafka.consumer.id"));
    }

    @Test
    public void testConsumeRecordTextKey() throws Exception {
        final String key = "a-kafka-record-key";
        final ObjectNode node = mapper.createObjectNode().put("c", 3).put("d", "4");
        final String value = mapper.writeValueAsString(node);
        final ArrayNode nodeRecordSet = mapper.createArrayNode().add(node);
        final String valueRecordSet = mapper.writeValueAsString(nodeRecordSet);
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                TEST_TOPIC, 0, 0, key.getBytes(UTF_8), value.getBytes(UTF_8));
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final TestRunner runner = getTestRunner(consumerRecords, TEST_TOPIC, TEST_GROUP);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(valueRecordSet, flowFile.getContent());
        assertEquals(key, flowFile.getAttribute("kafka.key"));
        assertEquals("0", flowFile.getAttribute("kafka.partition"));
        assertEquals(TEST_TOPIC, flowFile.getAttribute("kafka.topic"));
        assertEquals(TEST_GROUP, flowFile.getAttribute("kafka.consumer.id"));
    }

    @Test
    public void testConsumeRecordJsonKeyNoKeyReader() throws Exception {
        final ObjectNode nodeKey = mapper.createObjectNode().put("key", true);
        final String key = mapper.writeValueAsString(nodeKey);
        final ObjectNode node = mapper.createObjectNode().put("e", 5).put("f", "6");
        final String value = mapper.writeValueAsString(node);
        final ArrayNode nodeRecordSet = mapper.createArrayNode().add(node);
        final String valueRecordSet = mapper.writeValueAsString(nodeRecordSet);
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                TEST_TOPIC, 0, 0, key.getBytes(UTF_8), value.getBytes(UTF_8));
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final TestRunner runner = getTestRunner(consumerRecords, TEST_TOPIC, TEST_GROUP);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        assertEquals(valueRecordSet, flowFile.getContent());
        assertEquals(key, flowFile.getAttribute("kafka.key"));
        assertEquals("0", flowFile.getAttribute("kafka.partition"));
        assertEquals(TEST_TOPIC, flowFile.getAttribute("kafka.topic"));
        assertEquals(TEST_GROUP, flowFile.getAttribute("kafka.consumer.id"));
    }

    @Test
    public void testConsumeRecordWrapperStrategyKeyFormatDefault() throws Exception {
        final ObjectNode nodeToKafkaKey = mapper.createObjectNode().put("key", true);
        final String textToKafkaKey = mapper.writeValueAsString(nodeToKafkaKey);
        final ObjectNode nodeToKafkaValue = mapper.createObjectNode().put("g", 7).put("h", "8");
        final String textToKafkaValue = mapper.writeValueAsString(nodeToKafkaValue);
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(TEST_TOPIC, 0, 0L,
                0L, TimestampType.CREATE_TIME, 0L, textToKafkaKey.length(), textToKafkaValue.length(),
                textToKafkaKey.getBytes(UTF_8), textToKafkaValue.getBytes(UTF_8), getKafkaHeaders());
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final TestRunner runner = getTestRunner(consumerRecords, TEST_TOPIC, TEST_GROUP);
        final String keyReaderId = "key-record-reader";
        final RecordReaderFactory keyReaderService = new JsonTreeReader();
        runner.addControllerService(keyReaderId, keyReaderService);
        runner.enableControllerService(keyReaderService);
        runner.setProperty(keyReaderId, keyReaderId);
        runner.setProperty("output-strategy", OutputStrategy.USE_WRAPPER.name());
        runner.setProperty("key-format", "byte-array");
        runner.run(1);
        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        // consume strategy "use-wrapper" emits ArrayNode due to JsonRecordSetWriter
        final JsonNode nodeFlowFile = mapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, nodeFlowFile);
        assertEquals(1, nodeFlowFile.size());
        // extract the NiFi json object representation of Kafka input record
        final JsonNode flowFileValue = nodeFlowFile.get(0);
        // wrapper object contains "key", "value", "headers", "metadata"
        assertEquals(4, flowFileValue.size());
        final JsonNode nodeWrapperKey = Objects.requireNonNull(flowFileValue.get("key"));
        final JsonNode nodeWrapperValue = Objects.requireNonNull(flowFileValue.get("value"));
        final JsonNode nodeWrapperHeaders = Objects.requireNonNull(flowFileValue.get("headers"));
        final JsonNode nodeWrapperMetadata = Objects.requireNonNull(flowFileValue.get("metadata"));
        // validate headers
        assertEquals(2, nodeWrapperHeaders.size());
        final JsonNode header1 = nodeWrapperHeaders.get("header1");
        assertNotNull(header1);
        assertEquals("value1", header1.asText());
        assertNotNull(nodeWrapperHeaders.get("header2"));
        // validate metadata
        assertEquals(4, nodeWrapperMetadata.size());
        assertEquals(TEST_TOPIC, nodeWrapperMetadata.get("topic").asText());
        assertEquals(0, nodeWrapperMetadata.get("partition").asInt());
        assertNotNull(nodeWrapperMetadata.get("offset"));
        assertNotNull(nodeWrapperMetadata.get("timestamp"));
        // validate value
        assertInstanceOf(ObjectNode.class, nodeWrapperValue);
        assertEquals(textToKafkaValue, mapper.writeValueAsString(nodeWrapperValue));
        // validate key
        assertInstanceOf(ArrayNode.class, nodeWrapperKey);
        ArrayNode arrayNodeKey = (ArrayNode) nodeWrapperKey;
        assertEquals(textToKafkaKey.length(), arrayNodeKey.size());
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        arrayNodeKey.forEach(b -> os.write(b.asInt()));
        assertEquals(textToKafkaKey, os.toString(UTF_8));
        // validate flowfile attributes
        assertNull(flowFile.getAttribute("foo"));
        assertEquals(TEST_TOPIC, flowFile.getAttribute("kafka.topic"));
        assertEquals(TEST_GROUP, flowFile.getAttribute("kafka.consumer.id"));
        assertEquals(textToKafkaKey, flowFile.getAttribute("kafka.key"));
    }

    @Test
    public void testConsumeRecordWrapperStrategyKeyFormatDefaultHeaderNonUTF8() throws Exception {
        final ObjectNode nodeToKafkaKey = mapper.createObjectNode().put("key", true);
        final String textToKafkaKey = mapper.writeValueAsString(nodeToKafkaKey);
        final ObjectNode nodeToKafkaValue = mapper.createObjectNode().put("i", 9).put("j", "10");
        final String textToKafkaValue = mapper.writeValueAsString(nodeToKafkaValue);
        final RecordHeaders headers = new RecordHeaders(Arrays.asList(
                new RecordHeader("header1", "nameÄËÖÜ".getBytes(ISO_8859_1)),
                new RecordHeader("header2", "value2".getBytes(UTF_8))));
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(TEST_TOPIC, 0, 0L,
                0L, TimestampType.CREATE_TIME, 0L, textToKafkaKey.length(), textToKafkaValue.length(),
                textToKafkaKey.getBytes(UTF_8), textToKafkaValue.getBytes(UTF_8), headers);
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final TestRunner runner = getTestRunner(consumerRecords, TEST_TOPIC, TEST_GROUP);
        final String keyReaderId = "key-record-reader";
        final RecordReaderFactory keyReaderService = new JsonTreeReader();
        runner.addControllerService(keyReaderId, keyReaderService);
        runner.enableControllerService(keyReaderService);
        runner.setProperty(keyReaderId, keyReaderId);
        runner.setProperty("output-strategy", OutputStrategy.USE_WRAPPER.name());
        runner.setProperty("key-format", "byte-array");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        // consume strategy "use-wrapper" emits ArrayNode due to JsonRecordSetWriter
        final JsonNode nodeFlowFile = mapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, nodeFlowFile);
        assertEquals(1, nodeFlowFile.size());
        // extract the NiFi json object representation of Kafka input record
        final JsonNode flowFileValue = nodeFlowFile.get(0);
        // wrapper object contains "key", "value", "headers", "metadata"
        assertEquals(4, flowFileValue.size());
        final JsonNode nodeWrapperHeaders = Objects.requireNonNull(flowFileValue.get("headers"));
        // validate headers
        assertEquals(2, nodeWrapperHeaders.size());
        final JsonNode header1 = nodeWrapperHeaders.get("header1");
        assertNotNull(header1);
        final String expected = new String("nameÄËÖÜ".getBytes(ISO_8859_1), UTF_8);
        assertEquals(expected, header1.asText());
        assertNotNull(nodeWrapperHeaders.get("header2"));
    }

    @Test
    public void testConsumeRecordWrapperStrategyKeyFormatString() throws Exception {
        final ObjectNode nodeToKafkaKey = mapper.createObjectNode().put("key", true);
        final String textToKafkaKey = mapper.writeValueAsString(nodeToKafkaKey);
        final ObjectNode nodeToKafkaValue = mapper.createObjectNode().put("k", 11).put("l", "12");
        final String textToKafkaValue = mapper.writeValueAsString(nodeToKafkaValue);
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(TEST_TOPIC, 0, 0L,
                0L, TimestampType.CREATE_TIME, 0L, textToKafkaKey.length(), textToKafkaValue.length(),
                textToKafkaKey.getBytes(UTF_8), textToKafkaValue.getBytes(UTF_8), getKafkaHeaders());
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final TestRunner runner = getTestRunner(consumerRecords, TEST_TOPIC, TEST_GROUP);
        final String keyReaderId = "key-record-reader";
        final RecordReaderFactory keyReaderService = new JsonTreeReader();
        runner.addControllerService(keyReaderId, keyReaderService);
        runner.enableControllerService(keyReaderService);
        runner.setProperty(keyReaderId, keyReaderId);
        runner.setProperty("output-strategy", OutputStrategy.USE_WRAPPER.name());
        runner.setProperty("key-format", "string");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        // consume strategy "use-wrapper" emits ArrayNode due to JsonRecordSetWriter
        final JsonNode nodeFlowFile = mapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, nodeFlowFile);
        assertEquals(1, nodeFlowFile.size());
        // extract the NiFi json object representation of Kafka input record
        final JsonNode flowFileValue = nodeFlowFile.get(0);
        // wrapper object contains "key", "value", "headers", "metadata"
        assertEquals(4, flowFileValue.size());
        final JsonNode nodeWrapperKey = Objects.requireNonNull(flowFileValue.get("key"));
        // validate key
        assertInstanceOf(TextNode.class, nodeWrapperKey);
        TextNode textNodeKey = (TextNode) nodeWrapperKey;
        assertEquals(textToKafkaKey.length(), textNodeKey.asText().length());
        assertEquals(textToKafkaKey, textNodeKey.textValue());
        // validate flowfile attributes
        assertNull(flowFile.getAttribute("foo"));
        assertEquals(TEST_TOPIC, flowFile.getAttribute("kafka.topic"));
        assertEquals(TEST_GROUP, flowFile.getAttribute("kafka.consumer.id"));
        assertEquals(textToKafkaKey, flowFile.getAttribute("kafka.key"));
    }

    @Test
    public void testConsumeRecordWrapperStrategyKeyFormatRecord() throws Exception {
        final ObjectNode nodeToKafkaKey = mapper.createObjectNode().put("key", true);
        final String textToKafkaKey = mapper.writeValueAsString(nodeToKafkaKey);
        final ObjectNode nodeToKafkaValue = mapper.createObjectNode().put("k", 11).put("l", "12");
        final String textToKafkaValue = mapper.writeValueAsString(nodeToKafkaValue);
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(TEST_TOPIC, 0, 0L,
                0L, TimestampType.CREATE_TIME, 0L, textToKafkaKey.length(), textToKafkaValue.length(),
                textToKafkaKey.getBytes(UTF_8), textToKafkaValue.getBytes(UTF_8), getKafkaHeaders());
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final TestRunner runner = getTestRunner(consumerRecords, TEST_TOPIC, TEST_GROUP);
        final String keyReaderId = "key-record-reader";
        final RecordReaderFactory keyReaderService = new JsonTreeReader();
        runner.addControllerService(keyReaderId, keyReaderService);
        runner.enableControllerService(keyReaderService);
        runner.setProperty(keyReaderId, keyReaderId);
        runner.setProperty("output-strategy", OutputStrategy.USE_WRAPPER.name());
        runner.setProperty("key-format", "record");
        runner.setProperty("key-record-reader", "record-reader");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        // consume strategy "use-wrapper" emits ArrayNode due to JsonRecordSetWriter
        final JsonNode nodeFlowFile = mapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, nodeFlowFile);
        assertEquals(1, nodeFlowFile.size());
        // extract the NiFi json object representation of Kafka input record
        final JsonNode flowFileValue = nodeFlowFile.get(0);
        // wrapper object contains "key", "value", "headers", "metadata"
        assertEquals(4, flowFileValue.size());
        final JsonNode nodeWrapperKey = Objects.requireNonNull(flowFileValue.get("key"));
        final JsonNode nodeWrapperValue = Objects.requireNonNull(flowFileValue.get("value"));
        final JsonNode nodeWrapperHeaders = Objects.requireNonNull(flowFileValue.get("headers"));
        final JsonNode nodeWrapperMetadata = Objects.requireNonNull(flowFileValue.get("metadata"));
        // validate headers
        assertEquals(2, nodeWrapperHeaders.size());
        final JsonNode header1 = nodeWrapperHeaders.get("header1");
        assertNotNull(header1);
        assertEquals("value1", header1.asText());
        assertNotNull(nodeWrapperHeaders.get("header2"));
        // validate metadata
        assertEquals(4, nodeWrapperMetadata.size());
        assertEquals(TEST_TOPIC, nodeWrapperMetadata.get("topic").asText());
        assertEquals(0, nodeWrapperMetadata.get("partition").asInt());
        assertNotNull(nodeWrapperMetadata.get("offset"));
        assertNotNull(nodeWrapperMetadata.get("timestamp"));
        // validate value
        assertInstanceOf(ObjectNode.class, nodeWrapperValue);
        assertEquals(textToKafkaValue, mapper.writeValueAsString(nodeWrapperValue));
        // validate key
        assertInstanceOf(ObjectNode.class, nodeWrapperKey);
        ObjectNode objectNodeKey = (ObjectNode) nodeWrapperKey;
        assertTrue(objectNodeKey.get("key").asBoolean());
        // validate flowfile attributes
        assertNull(flowFile.getAttribute("foo"));
        assertEquals(TEST_TOPIC, flowFile.getAttribute("kafka.topic"));
        assertEquals(TEST_GROUP, flowFile.getAttribute("kafka.consumer.id"));
        assertEquals("0", flowFile.getAttribute("kafka.partition"));
        assertEquals(textToKafkaKey, flowFile.getAttribute("kafka.key"));
    }

    @Test
    public void testConsumeRecordWrapperStrategyStringKeyNullValue() throws InitializationException, JsonProcessingException {
        final Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("headerA", "valueA".getBytes(UTF_8)));
        final String key = "a-string-key";
        final ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                TEST_TOPIC, 0, 0, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0L, key.length(), 0, key.getBytes(UTF_8), null, headers);
        final ConsumerRecords<byte[], byte[]> consumerRecords = getConsumerRecords(record);

        final TestRunner runner = getTestRunner(consumerRecords, TEST_TOPIC, TEST_GROUP);
        runner.setProperty("output-strategy", OutputStrategy.USE_WRAPPER.name());
        runner.setProperty("key-format", "string");

        runner.run(1);
        runner.assertAllFlowFilesTransferred(ConsumeKafkaRecord_2_6.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafkaRecord_2_6.REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();
        final JsonNode nodeFlowFiles = mapper.readTree(flowFile.getContent());
        final JsonNode nodeFlowFile = nodeFlowFiles.get(0);
        final JsonNode nodeWrapperKey = Objects.requireNonNull(nodeFlowFile.get("key"));
        assertEquals(key, nodeWrapperKey.asText());
        assertTrue(nodeFlowFile.get("value").isNull());
        final JsonNode nodeWrapperHeaders = Objects.requireNonNull(nodeFlowFile.get("headers"));
        assertEquals("valueA", nodeWrapperHeaders.get("headerA").asText());
    }

    /**
     * Construct a test runner that simulates Kafka interactions.
     */
    private TestRunner getTestRunner(final ConsumerRecords<byte[], byte[]> consumerRecords,
                                     final String topic, final String group) throws InitializationException {
        final ConsumeKafkaRecord_2_6 processor = new ConsumeKafkaRecord_2_6() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return getConsumerPool(consumerRecords, context, log);
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(BOOTSTRAP_SERVERS, TEST_BOOTSTRAP_SERVER);
        runner.setProperty("topic", topic);
        runner.setProperty("topic_type", "names");
        runner.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        runner.setProperty("auto.offset.reset", "earliest");
        final String readerId = "record-reader";
        final RecordReaderFactory readerService = new JsonTreeReader();
        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new JsonRecordSetWriter();
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
        return runner;
    }

    /**
     * Fabricate a {@link ConsumerPool} that uses a mock {@link Consumer} to simulate Kafka interactions.
     */
    private ConsumerPool getConsumerPool(final ConsumerRecords<byte[], byte[]> consumerRecords,
                                         final ProcessContext context, final ComponentLog logger) {
        final RecordReaderFactory readerFactory = context.getProperty("record-reader")
                .asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty("record-writer")
                .asControllerService(RecordSetWriterFactory.class);
        final String topic = context.getProperty("topic").getValue();
        final Pattern patternTopic = (topic == null) ? null : Pattern.compile(topic);
        final String groupId = context.getProperty(ConsumerConfig.GROUP_ID_CONFIG).getValue();
        final OutputStrategy outputStrategy = OutputStrategy.valueOf(context.getProperty("output-strategy").getValue());
        final KeyFormat keyFormat = context.getProperty("key-format").asAllowableValue(KeyFormat.class);
        final RecordReaderFactory keyReaderFactory = context.getProperty("key-record-reader")
                .asControllerService(RecordReaderFactory.class);
        return new ConsumerPool(
                readerFactory,
                writerFactory,
                Collections.emptyMap(),
                patternTopic,
                100L,
                "ssl",
                "localhost",
                logger,
                true,
                UTF_8,
                null,
                false,
                KeyEncoding.UTF8,
                null,
                true,
                outputStrategy,
                keyFormat,
                keyReaderFactory) {
            @Override
            protected Consumer<byte[], byte[]> createKafkaConsumer() {
                return getConsumer(groupId, consumerRecords);
            }
        };
    }

    public interface ConsumerBB extends Consumer<byte[], byte[]> {
    }

    /**
     * Mock Kafka {@link Consumer} to be injected into NiFi processor for testing.
     */
    private Consumer<byte[], byte[]> getConsumer(final String groupId, final ConsumerRecords<byte[], byte[]> records) {
        final Consumer<byte[], byte[]> consumer = mock(ConsumerBB.class);
        // signal polling to stop by returning an empty records response
        when(consumer.poll(any())).thenReturn(records).thenReturn(getConsumerRecords());
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata(groupId));
        return consumer;
    }

    /**
     * Fabricate a {@link ConsumerRecords} from a few {@link ConsumerRecord}.
     */
    @SafeVarargs
    private final ConsumerRecords<byte[], byte[]> getConsumerRecords(final ConsumerRecord<byte[], byte[]>... records) {
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            final TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            map.put(partition, Collections.singletonList(record));
        }
        return new ConsumerRecords<>(map);
    }

    /**
     * Construct a {@link Header} collection to include in Kafka event.
     */
    private Headers getKafkaHeaders() {
        return new RecordHeaders(Arrays.asList(
                new RecordHeader("header1", "value1".getBytes(UTF_8)),
                new RecordHeader("header2", "value2".getBytes(UTF_8))));
    }
}
