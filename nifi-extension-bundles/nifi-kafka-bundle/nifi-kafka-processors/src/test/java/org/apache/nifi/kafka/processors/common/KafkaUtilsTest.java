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
package org.apache.nifi.kafka.processors.common;

import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class KafkaUtilsTest {

    private static final String TEST_TOPIC = "test-topic";
    private static final int TEST_PARTITION = 0;
    private static final long TEST_OFFSET = 100L;
    private static final long TEST_TIMESTAMP = System.currentTimeMillis();

    @Test
    void testToAttributesWithoutPrefix() {
        final List<RecordHeader> headers = List.of(
                new RecordHeader("uuid", "test-uuid".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("custom-header", "custom-value".getBytes(StandardCharsets.UTF_8))
        );

        final ByteRecord record = createByteRecord(headers);
        final Pattern headerPattern = Pattern.compile(".*");

        final Map<String, String> attributes = KafkaUtils.toAttributes(
                record, KeyEncoding.UTF8, headerPattern, StandardCharsets.UTF_8, true);

        // Verify standard Kafka attributes
        assertEquals(TEST_TOPIC, attributes.get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals(String.valueOf(TEST_PARTITION), attributes.get(KafkaFlowFileAttribute.KAFKA_PARTITION));
        assertEquals(String.valueOf(TEST_OFFSET), attributes.get(KafkaFlowFileAttribute.KAFKA_OFFSET));

        // Verify headers are added without prefix
        assertEquals("test-uuid", attributes.get("uuid"));
        assertEquals("custom-value", attributes.get("custom-header"));
    }

    @Test
    void testToAttributesWithPrefix() {
        final List<RecordHeader> headers = List.of(
                new RecordHeader("uuid", "test-uuid".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("filename", "test-filename".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("custom-header", "custom-value".getBytes(StandardCharsets.UTF_8))
        );

        final ByteRecord record = createByteRecord(headers);
        final Pattern headerPattern = Pattern.compile(".*");
        final String prefix = "kafka.header.";

        final Map<String, String> attributes = KafkaUtils.toAttributes(
                record, KeyEncoding.UTF8, headerPattern, prefix, StandardCharsets.UTF_8, true);

        // Verify standard Kafka attributes are NOT prefixed
        assertEquals(TEST_TOPIC, attributes.get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals(String.valueOf(TEST_PARTITION), attributes.get(KafkaFlowFileAttribute.KAFKA_PARTITION));
        assertEquals(String.valueOf(TEST_OFFSET), attributes.get(KafkaFlowFileAttribute.KAFKA_OFFSET));

        // Verify headers are added WITH prefix
        assertEquals("test-uuid", attributes.get("kafka.header.uuid"));
        assertEquals("test-filename", attributes.get("kafka.header.filename"));
        assertEquals("custom-value", attributes.get("kafka.header.custom-header"));

        // Verify original header names are NOT present
        assertFalse(attributes.containsKey("uuid"));
        assertFalse(attributes.containsKey("filename"));
        assertFalse(attributes.containsKey("custom-header"));
    }

    @Test
    void testToAttributesWithPrefixAndPatternFiltering() {
        final List<RecordHeader> headers = List.of(
                new RecordHeader("uuid", "test-uuid".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("keep-this", "keep-value".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("skip-this", "skip-value".getBytes(StandardCharsets.UTF_8))
        );

        final ByteRecord record = createByteRecord(headers);
        // Pattern that matches only headers starting with "uuid" or "keep"
        final Pattern headerPattern = Pattern.compile("(uuid|keep-.*)");
        final String prefix = "msg.";

        final Map<String, String> attributes = KafkaUtils.toAttributes(
                record, KeyEncoding.UTF8, headerPattern, prefix, StandardCharsets.UTF_8, true);

        // Verify matching headers are added with prefix
        assertEquals("test-uuid", attributes.get("msg.uuid"));
        assertEquals("keep-value", attributes.get("msg.keep-this"));

        // Verify non-matching header is NOT present (with or without prefix)
        assertFalse(attributes.containsKey("msg.skip-this"));
        assertFalse(attributes.containsKey("skip-this"));

        // Verify header count includes all headers (not just filtered ones)
        assertEquals("3", attributes.get(KafkaFlowFileAttribute.KAFKA_HEADER_COUNT));
    }

    @Test
    void testToAttributesWithNullPrefix() {
        final List<RecordHeader> headers = List.of(
                new RecordHeader("header1", "value1".getBytes(StandardCharsets.UTF_8))
        );

        final ByteRecord record = createByteRecord(headers);
        final Pattern headerPattern = Pattern.compile(".*");

        // Explicitly pass null for prefix
        final Map<String, String> attributes = KafkaUtils.toAttributes(
                record, KeyEncoding.UTF8, headerPattern, null, StandardCharsets.UTF_8, true);

        // Verify header is added without prefix when prefix is null
        assertEquals("value1", attributes.get("header1"));
        assertFalse(attributes.containsKey("nullheader1"));
    }

    @Test
    void testToAttributesWithEmptyPrefix() {
        final List<RecordHeader> headers = List.of(
                new RecordHeader("header1", "value1".getBytes(StandardCharsets.UTF_8))
        );

        final ByteRecord record = createByteRecord(headers);
        final Pattern headerPattern = Pattern.compile(".*");

        // Pass empty string for prefix
        final Map<String, String> attributes = KafkaUtils.toAttributes(
                record, KeyEncoding.UTF8, headerPattern, "", StandardCharsets.UTF_8, true);

        // Verify header is added with empty prefix (effectively no prefix)
        assertEquals("value1", attributes.get("header1"));
    }

    private ByteRecord createByteRecord(final List<RecordHeader> headers) {
        return new ByteRecord(
                TEST_TOPIC,
                TEST_PARTITION,
                TEST_OFFSET,
                TEST_TIMESTAMP,
                headers,
                null,  // key
                "test-value".getBytes(StandardCharsets.UTF_8),
                1  // count
        );
    }
}
