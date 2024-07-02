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
package org.apache.nifi.kafka.processors.producer.key;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AttributeKeyFactoryTest {

    @Test
    void testNullKeyAttribute() throws UnsupportedEncodingException {
        final Map<String, String> attributes = new HashMap<>();
        final Record record = fabricateRecord();

        final AttributeKeyFactory attributeKeyFactory = new AttributeKeyFactory(null, null);
        assertNull(attributeKeyFactory.getKey(attributes, record));
    }

    @Test
    void testNullKeyAttributeValue() throws UnsupportedEncodingException {
        final Map<String, String> attributes = new HashMap<>();
        final Record record = fabricateRecord();

        final AttributeKeyFactory attributeKeyFactory = new AttributeKeyFactory("A", null);
        assertNull(attributeKeyFactory.getKey(attributes, record));
    }

    @Test
    void testNonNullKeyAttribute() throws UnsupportedEncodingException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("A", "valueA");
        attributes.put("B", "valueB");
        final Record record = fabricateRecord();

        final AttributeKeyFactory attributeKeyFactory = new AttributeKeyFactory("A", null);
        assertArrayEquals("valueA".getBytes(StandardCharsets.UTF_8), attributeKeyFactory.getKey(attributes, record));
    }

    private static Record fabricateRecord() {
        final RecordField fieldA = new RecordField("RF1", RecordFieldType.STRING.getDataType());
        final RecordField fieldB = new RecordField("RF2", RecordFieldType.STRING.getDataType());
        final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(fieldA, fieldB));
        final Map<String, Object> values = new HashMap<>();
        return new MapRecord(schema, values);
    }
}
