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

package org.apache.nifi.processors.standard.enrichment;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestInsertRecordFieldsJoinStrategy extends TestIndexCorrelatedJoinStrategy {
    private final MockComponentLog logger = new MockComponentLog("id", "TestInsertRecordFieldsJoinStrategy");

    @Test
    public void testSimpleInsertAtRoot() {
        final RecordSchema originalSchema = getOriginalSchema();
        final RecordSchema enrichmentSchema = getEnrichmentSchema();
        final Record originalRecord = createOriginalRecord(555);
        final Record enrichmentRecord = createEnrichmentRecord(555, "John Doe", 100);

        final InsertRecordFieldsJoinStrategy strategy = new InsertRecordFieldsJoinStrategy(logger, "/");

        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);
        final List<RecordField> resultFields = resultSchema.getFields();
        assertEquals(3, resultFields.size());

        final List<RecordField> combinedFields = new ArrayList<>();
        combinedFields.add(originalSchema.getFields().getFirst());
        combinedFields.addAll(enrichmentSchema.getFields());

        // Wrap each List of fields in a hashset to combine because the order of the fields is not guaranteed when incorporating schemas.
        assertEquals(new HashSet<>(combinedFields), new HashSet<>(resultFields));

        final Record combined = strategy.combineRecords(originalRecord, enrichmentRecord, resultSchema);
        assertEquals(555, combined.getAsInt("id"));
        assertEquals("John Doe", combined.getValue("name"));
        assertEquals(100, combined.getValue("number"));
    }

    @Test
    public void testNullEnrichment() {
        final RecordSchema originalSchema = getOriginalSchema();
        final RecordSchema enrichmentSchema = getEnrichmentSchema();
        final Record originalRecord = createOriginalRecord(555);
        final Record enrichmentRecord = createEnrichmentRecord(555, "John Doe", 100);

        final InsertRecordFieldsJoinStrategy strategy = new InsertRecordFieldsJoinStrategy(logger, "/");

        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);
        final List<RecordField> resultFields = resultSchema.getFields();
        assertEquals(3, resultFields.size());

        final List<RecordField> combinedFields = new ArrayList<>();
        combinedFields.add(originalSchema.getFields().getFirst());
        combinedFields.addAll(enrichmentSchema.getFields());

        // Wrap each List of fields in a hashset to combine because the order of the fields is not guaranteed when incorporating schemas.
        assertEquals(new HashSet<>(combinedFields), new HashSet<>(resultFields));

        final Record combined = strategy.combineRecords(createOriginalRecord(555), null, resultSchema);
        assertEquals(555, combined.getAsInt("id"));
        assertNull(combined.getValue("name"));
        assertNull(combined.getValue("number"));
    }

    @Test
    public void testNullOriginalNullEnrichment() {
        final RecordSchema originalSchema = getOriginalSchema();
        final RecordSchema enrichmentSchema = getEnrichmentSchema();
        final Record originalRecord = createOriginalRecord(555);
        final Record enrichmentRecord = createEnrichmentRecord(555, "John Doe", 100);

        final InsertRecordFieldsJoinStrategy strategy = new InsertRecordFieldsJoinStrategy(logger, "/");

        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);
        final List<RecordField> resultFields = resultSchema.getFields();
        assertEquals(3, resultFields.size());

        final List<RecordField> combinedFields = new ArrayList<>();
        combinedFields.add(originalSchema.getFields().getFirst());
        combinedFields.addAll(enrichmentSchema.getFields());

        // Wrap each List of fields in a hashset to combine because the order of the fields is not guaranteed when incorporating schemas.
        assertEquals(new HashSet<>(combinedFields), new HashSet<>(resultFields));

        final Record combined = strategy.combineRecords(null, null, resultSchema);
        assertNull(combined);
    }

    @Test
    public void testRecordPathPointsToNonExistentField() {
        final Record originalRecord = createOriginalRecord(555);
        final Record enrichmentRecord = createEnrichmentRecord(555, "John Doe", 100);

        final InsertRecordFieldsJoinStrategy strategy = new InsertRecordFieldsJoinStrategy(logger, "/abc");

        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);
        final Record combined = strategy.combineRecords(originalRecord, enrichmentRecord, resultSchema);
        assertEquals(555, combined.getAsInt("id"));
        assertArrayEquals(new Integer[] {555}, combined.getValues());
    }

    @Test
    public void testRecordPathPointingToChildRecord() {
        final RecordSchema simpleOriginalSchema = getOriginalSchema();
        final List<RecordField> complexSchemaFields = new ArrayList<>(simpleOriginalSchema.getFields());
        // Add a field named 'xyz' that is a record with no fields.
        final RecordSchema emptySchema = new SimpleRecordSchema(Collections.emptyList());
        complexSchemaFields.add(new RecordField("xyz", RecordFieldType.RECORD.getRecordDataType(emptySchema)));

        final RecordSchema complexOriginalSchema = new SimpleRecordSchema(complexSchemaFields);
        final Map<String, Object> originalValues = new HashMap<>();
        originalValues.put("id", 555);
        originalValues.put("xyz", new MapRecord(emptySchema, new HashMap<>()));
        final Record originalRecord = new MapRecord(complexOriginalSchema, originalValues);

        final Record enrichmentRecord = createEnrichmentRecord(555, "John Doe", 100);

        final InsertRecordFieldsJoinStrategy strategy = new InsertRecordFieldsJoinStrategy(logger, "/xyz");

        // Check that we update the schema properly
        final RecordSchema resultSchema = strategy.createResultSchema(originalRecord, enrichmentRecord);

        final List<RecordField> resultFields = resultSchema.getFields();
        assertEquals(2, resultFields.size());
        assertTrue(resultFields.contains(simpleOriginalSchema.getFields().getFirst()));

        final DataType xyzDataType = resultSchema.getDataType("xyz").get();
        final RecordSchema xyzSchema = ((RecordDataType) xyzDataType).getChildSchema();
        assertEquals(RecordFieldType.INT, xyzSchema.getDataType("id").get().getFieldType());
        assertEquals(RecordFieldType.STRING, xyzSchema.getDataType("name").get().getFieldType());
        assertEquals(RecordFieldType.INT, xyzSchema.getDataType("number").get().getFieldType());

        final Record combined = strategy.combineRecords(originalRecord, enrichmentRecord, resultSchema);
        assertEquals(555, combined.getAsInt("id"));

        final Object xyzValue = combined.getValue("xyz");
        assertInstanceOf(Record.class, xyzValue);

        final Record xyzRecord = (Record) xyzValue;
        assertEquals("John Doe", xyzRecord.getValue("name"));
        assertEquals(100, xyzRecord.getValue("number"));
        assertEquals(555, xyzRecord.getValue("id"));
    }
}
