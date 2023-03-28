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
package org.apache.nifi.kafka.processors.producer.wrapper;

import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WrapperRecordTest {

    private static final String STREET_NAME = "street-name";
    private static final String STREET_NUMBER = "street-number";

    private static final String NAME = "name";
    private static final String STREET_ADDRESS = "street-address";
    private static final String ZIP = "zip";

    private static final String TOPIC = "my-topic";

    private static final Integer PARTITION = 0;

    private static final long TIMESTAMP = System.currentTimeMillis();

    @Test
    void testWrapperRecordSimple() {
        final RecordField fieldStreetName = new RecordField(STREET_NAME, RecordFieldType.STRING.getDataType());
        final RecordField fieldStreetNumber = new RecordField(STREET_NUMBER, RecordFieldType.INT.getDataType());
        final RecordSchema schemaStreetAddress = new SimpleRecordSchema(Arrays.asList(fieldStreetName, fieldStreetNumber));
        final Map<String, Object> valuesStreetAddress = new HashMap<>();
        valuesStreetAddress.put(STREET_NAME, "Main");
        valuesStreetAddress.put(STREET_NUMBER, 100);
        final Record recordStreetAddress = new MapRecord(schemaStreetAddress, valuesStreetAddress);

        final RecordField fieldName = new RecordField(NAME, RecordFieldType.STRING.getDataType());
        final RecordField fieldStreetAddress = new RecordField(STREET_ADDRESS, RecordFieldType.RECORD.getRecordDataType(schemaStreetAddress));
        final RecordField fieldZip = new RecordField(ZIP, RecordFieldType.STRING.getDataType());
        final RecordSchema schemaMailingAddress = new SimpleRecordSchema(Arrays.asList(fieldName, fieldStreetAddress, fieldZip));
        final Map<String, Object> valuesMailingAddress = new HashMap<>();
        valuesMailingAddress.put(NAME, "Smith");
        valuesMailingAddress.put(STREET_ADDRESS, recordStreetAddress);
        valuesMailingAddress.put(ZIP, "10000");
        final Record recordMailingAddress = new MapRecord(schemaMailingAddress, valuesMailingAddress);

        final List<RecordHeader> kafkaHeaders = Arrays.asList(
                new RecordHeader("type", "business".getBytes(UTF_8)),
                new RecordHeader("color", "grey".getBytes(UTF_8)));

        final WrapperRecord wrapperRecord = new WrapperRecord(
                recordMailingAddress, STREET_ADDRESS,
                kafkaHeaders, UTF_8, TOPIC, PARTITION, 0L, TIMESTAMP);

        final Record recordMetadata = wrapperRecord.getAsRecord(WrapperRecord.METADATA, WrapperRecord.SCHEMA_METADATA);
        assertNotNull(recordMetadata);
        assertEquals(TOPIC, recordMetadata.getAsString(WrapperRecord.TOPIC));
        assertEquals(TIMESTAMP, recordMetadata.getAsLong(WrapperRecord.TIMESTAMP));

        final Map<String, Object> recordHeaders = (Map<String, Object>) wrapperRecord.getValue(WrapperRecord.HEADERS);
        assertEquals(2, recordHeaders.size());
        assertTrue(recordHeaders.keySet().containsAll(Arrays.asList("type", "color")));

        final Record recordKey = wrapperRecord.getAsRecord(WrapperRecord.KEY, schemaStreetAddress);
        assertNotNull(recordKey);
        assertEquals("Main", recordKey.getAsString(STREET_NAME));
        assertEquals(100, recordKey.getAsInt(STREET_NUMBER));

        final Record recordValue = wrapperRecord.getAsRecord(WrapperRecord.VALUE, schemaMailingAddress);
        assertNotNull(recordValue);
        assertEquals("Smith", recordValue.getAsString(NAME));
        assertEquals(recordKey, recordValue.getAsRecord(STREET_ADDRESS, schemaStreetAddress));
        assertEquals("10000", recordValue.getAsString(ZIP));
    }
}
