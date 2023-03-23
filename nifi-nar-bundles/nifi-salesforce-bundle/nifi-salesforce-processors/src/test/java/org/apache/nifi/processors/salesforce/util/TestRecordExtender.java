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
package org.apache.nifi.processors.salesforce.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.apache.nifi.processors.salesforce.util.RecordExtender.ATTRIBUTES_RECORD_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestRecordExtender {

    private static ObjectMapper OBJECT_MAPPER;
    private static RecordSchema ORIGINAL_SCHEMA;
    private static RecordSchema EXPECTED_EXTENDED_SCHEMA;

    @BeforeAll
    public static void setup() {
        OBJECT_MAPPER = new ObjectMapper();
        ORIGINAL_SCHEMA = new SimpleRecordSchema(Arrays.asList(
                new RecordField("testRecordField1", RecordFieldType.STRING.getDataType()),
                new RecordField("testRecordField2", RecordFieldType.STRING.getDataType())
        ));
        EXPECTED_EXTENDED_SCHEMA = new SimpleRecordSchema(Arrays.asList(
                new RecordField("testRecordField1", RecordFieldType.STRING.getDataType()),
                new RecordField("testRecordField2", RecordFieldType.STRING.getDataType()),
                new RecordField("attributes", RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Arrays.asList(
                        new RecordField("type", RecordFieldType.STRING.getDataType()),
                        new RecordField("referenceId", RecordFieldType.STRING.getDataType()
                        )))))
        ));
    }

    private RecordExtender testSubject;

    @BeforeEach
    public void init() {
        testSubject = new RecordExtender(ORIGINAL_SCHEMA);
    }

    @Test
    void testGetWrappedRecordJson() throws IOException {
        ObjectNode testNode = OBJECT_MAPPER.createObjectNode();
        testNode.put("testField1", "testValue1");
        testNode.put("testField2", "testValue2");

        ObjectNode expectedWrappedNode = OBJECT_MAPPER.createObjectNode();
        expectedWrappedNode.set("records", testNode);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(testNode.toString().getBytes());

        ObjectNode actualWrappedJson = testSubject.getWrappedRecordsJson(out);

        assertEquals(expectedWrappedNode, actualWrappedJson);
    }

    @Test
    void testGetExtendedSchema() {
        final RecordSchema actualExtendedSchema = testSubject.getExtendedSchema();

        assertEquals(EXPECTED_EXTENDED_SCHEMA, actualExtendedSchema);
    }

    @Test
    void testGetExtendedRecord() {
        int referenceId = 0;
        String objectType = "Account";

        MapRecord testRecord = new MapRecord(ORIGINAL_SCHEMA, new HashMap<>() {{
            put("testRecordField1", "testRecordValue1");
            put("testRecordField2", "testRecordValue2");
        }});


        MapRecord expectedRecord = new MapRecord(EXPECTED_EXTENDED_SCHEMA, new HashMap<>() {{
            put("attributes",
                    new MapRecord(ATTRIBUTES_RECORD_SCHEMA, new HashMap<>() {{
                        put("type", objectType);
                        put("referenceId", referenceId);
                    }})
            );
            put("testRecordField1", "testRecordValue1");
            put("testRecordField2", "testRecordValue2");
        }});

        MapRecord actualRecord = testSubject.getExtendedRecord(objectType, referenceId, testRecord);

        assertEquals(expectedRecord, actualRecord);
    }

}
