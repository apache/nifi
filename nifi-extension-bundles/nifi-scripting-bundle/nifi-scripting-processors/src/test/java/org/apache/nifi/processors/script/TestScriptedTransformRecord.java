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

package org.apache.nifi.processors.script;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.ArrayListRecordReader;
import org.apache.nifi.serialization.record.ArrayListRecordWriter;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestScriptedTransformRecord {

    private TestRunner testRunner;
    private ArrayListRecordReader recordReader;
    private ArrayListRecordWriter recordWriter;

    @Test
    public void testSimpleGroovyScript() throws InitializationException {
        testPassThrough("Groovy", "record");
    }

    private void testPassThrough(final String language, final String script) throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.setProperty(ScriptedTransformRecord.LANGUAGE, language);
        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, script);

        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 1)));
        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 2)));
        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 3)));

        testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        out.assertAttributeEquals("record.count", "3");
        assertEquals(3, testRunner.getCounterValue("Records Transformed").intValue());
        assertEquals(0, testRunner.getCounterValue("Records Dropped").intValue());

        final List<Record> recordsWritten = recordWriter.getRecordsWritten();
        assertEquals(3, recordsWritten.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, recordsWritten.get(i).getAsInt("num").intValue());
        }
    }

    @Test
    public void testAddFieldToSchemaWhenWriterSchemaIsDefined() throws InitializationException {
        final RecordSchema readSchema = createSimpleNumberSchema();
        final RecordSchema writeSchema = createSchemaWithAddedValue();
        setupWithDifferentSchemas(readSchema, writeSchema);

        testRunner.removeProperty(ScriptingComponentUtils.SCRIPT_BODY);
        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/groovy/AddNewField.groovy");

        recordReader.addRecord(new MapRecord(readSchema, new HashMap<>(Collections.singletonMap("num", 1))));
        recordReader.addRecord(new MapRecord(readSchema, new HashMap<>(Collections.singletonMap("num", 2))));
        recordReader.addRecord(new MapRecord(readSchema, new HashMap<>(Collections.singletonMap("num", 3))));

        testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        out.assertAttributeEquals("record.count", "3");
        assertEquals(3, testRunner.getCounterValue("Records Transformed").intValue());
        assertEquals(0, testRunner.getCounterValue("Records Dropped").intValue());

        final RecordSchema declaredSchema = recordWriter.getDeclaredSchema();
        final Optional<RecordField> addedValueOptionalField = declaredSchema.getField("added-value");
        assertTrue(addedValueOptionalField.isPresent());
        final RecordField addedField = addedValueOptionalField.get();
        assertEquals(RecordFieldType.INT, addedField.getDataType().getFieldType());

        final List<Record> written = recordWriter.getRecordsWritten();
        written.forEach(record -> assertEquals(88, record.getAsInt("added-value").intValue()));
    }

    @Test
    public void testZeroRecordInput() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        out.assertAttributeEquals("record.count", "0");
        assertEquals(0, testRunner.getCounterValue("Records Transformed").intValue());
        assertEquals(0, testRunner.getCounterValue("Records Dropped").intValue());

        final List<Record> written = recordWriter.getRecordsWritten();
        assertEquals(0, written.size());
    }

    @Test
    public void testAllRecordsFiltered() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "return null");

        recordReader.addRecord(new MapRecord(schema, new HashMap<>(Collections.singletonMap("num", 1))));
        recordReader.addRecord(new MapRecord(schema, new HashMap<>(Collections.singletonMap("num", 2))));
        recordReader.addRecord(new MapRecord(schema, new HashMap<>(Collections.singletonMap("num", 3))));

        testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        out.assertAttributeEquals("record.count", "0");
        assertEquals(0, testRunner.getCounterValue("Records Transformed").intValue());
        assertEquals(3, testRunner.getCounterValue("Records Dropped").intValue());

        final List<Record> written = recordWriter.getRecordsWritten();
        assertTrue(written.isEmpty());
    }



    @Test
    public void testCollectionOfRecords() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "[record, record, record]");

        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 1)));
        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 2)));
        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 3)));

        testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        out.assertAttributeEquals("record.count", "9");
        assertEquals(3, testRunner.getCounterValue("Records Transformed").intValue());
        assertEquals(0, testRunner.getCounterValue("Records Dropped").intValue());

        final List<Record> recordsWritten = recordWriter.getRecordsWritten();
        assertEquals(9, recordsWritten.size());

        int recordCounter = 0;
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                assertEquals(i + 1, recordsWritten.get(recordCounter++).getAsInt("num").intValue());
            }
        }
    }

    @Test
    public void testCollectionOfRecordsWithModification() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.removeProperty(ScriptingComponentUtils.SCRIPT_BODY);
        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/groovy/ForkRecordWithValueDecremented.groovy");

        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 1)));
        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 2)));
        recordReader.addRecord(new MapRecord(schema, Collections.singletonMap("num", 3)));

        testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        out.assertAttributeEquals("record.count", "6");
        assertEquals(3, testRunner.getCounterValue("Records Transformed").intValue());
        assertEquals(0, testRunner.getCounterValue("Records Dropped").intValue());

        final List<Record> recordsWritten = recordWriter.getRecordsWritten();
        assertEquals(6, recordsWritten.size());

        final int[] expectedNums = new int[] {1, 0, 2, 1, 3, 2};
        for (int i = 0; i < 6; i++) {
            assertEquals(expectedNums[i], recordsWritten.get(i).getAsInt("num").intValue());
        }
    }

    @Test
    public void testScriptThrowsException() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.removeProperty(ScriptingComponentUtils.SCRIPT_BODY);
        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/groovy/UpdateThenThrow.groovy");

        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 1)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 2)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 3)));

        final MockFlowFile inputFlowFile = testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_FAILURE, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_FAILURE).getFirst();
        out.assertAttributeNotExists("record.count");
        assertNull(testRunner.getCounterValue("Records Transformed"));
        assertNull(testRunner.getCounterValue("Records Dropped"));
        assertSame(inputFlowFile, out);
    }

    private Map<String, Object> mutableMap(final String key, final Object value) {
        final Map<String, Object> mutable = new HashMap<>();
        mutable.put(key, value);
        return mutable;
    }

    @Test
    public void testScriptTransformsRecord() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "record.setValue('i', recordIndex); record");

        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 1)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 2)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 3)));

        testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        out.assertAttributeEquals("record.count", "3");
        assertEquals(3, testRunner.getCounterValue("Records Transformed").intValue());
        assertEquals(0, testRunner.getCounterValue("Records Dropped").intValue());

        final List<Record> recordsWritten = recordWriter.getRecordsWritten();
        assertEquals(3, recordsWritten.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, recordsWritten.get(i).getAsInt("num").intValue());
            assertEquals(i, recordsWritten.get(i).getAsInt("i").intValue());
        }
    }

    @Test
    public void testScriptReturnsNull() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "if (record.getAsInt('num') % 2 == 0) { return record; } else { return null; }");

        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 1)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 2)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 3)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 4)));

        testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        out.assertAttributeEquals("record.count", "2");
        assertEquals(2, testRunner.getCounterValue("Records Transformed").intValue());
        assertEquals(2, testRunner.getCounterValue("Records Dropped").intValue());

        final List<Record> recordsWritten = recordWriter.getRecordsWritten();
        assertEquals(2, recordsWritten.size());

        assertEquals(2, recordsWritten.get(0).getAsInt("num").intValue());
        assertEquals(4, recordsWritten.get(1).getAsInt("num").intValue());
    }

    @Test
    public void testScriptReturnsWrongObject() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "if (recordIndex == 0) { return record; } else { return 88; }");

        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 1)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 2)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 3)));

        final MockFlowFile inputFlowFile = testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_FAILURE, 1);

        assertEquals(1, recordWriter.getRecordsWritten().size());

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_FAILURE).getFirst();
        out.assertAttributeNotExists("record.count");
        assertNull(testRunner.getCounterValue("Records Transformed"));
        assertNull(testRunner.getCounterValue("Records Dropped"));
        assertSame(inputFlowFile, out);
    }

    @Test
    public void testScriptReturnsCollectionWithWrongObject() throws InitializationException {
        final RecordSchema schema = createSimpleNumberSchema();
        setup(schema);

        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "[record, record, 88, record]");

        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 1)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 2)));
        recordReader.addRecord(new MapRecord(schema, mutableMap("num", 3)));

        final MockFlowFile inputFlowFile = testRunner.enqueue(new byte[0]);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_FAILURE, 1);

        assertEquals(2, recordWriter.getRecordsWritten().size());

        final MockFlowFile out = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_FAILURE).getFirst();
        out.assertAttributeNotExists("record.count");
        assertNull(testRunner.getCounterValue("Records Transformed"));
        assertNull(testRunner.getCounterValue("Records Dropped"));
        assertSame(inputFlowFile, out);
    }

    @Test
    public void testScriptWithFunctions() throws InitializationException {
        final List<RecordField> bookFields = new ArrayList<>();
        bookFields.add(new RecordField("author", RecordFieldType.STRING.getDataType()));
        bookFields.add(new RecordField("date", RecordFieldType.STRING.getDataType()));
        final RecordSchema bookSchema = new SimpleRecordSchema(bookFields);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("book", RecordFieldType.RECORD.getRecordDataType(bookSchema)));

        final RecordSchema outerSchema = new SimpleRecordSchema(fields);

        setup(outerSchema);

        testRunner.removeProperty(ScriptingComponentUtils.SCRIPT_BODY);
        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "src/test/resources/groovy/ReplaceFieldValue.groovy");

        recordReader.addRecord(createBook("John Doe", "01/01/1980", bookSchema, outerSchema));
        recordReader.addRecord(createBook("Jane Doe", "01/01/1990", bookSchema, outerSchema));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("Value to Replace", "Jane Doe");
        attributes.put("Replacement Value", "Unknown Author");

        testRunner.enqueue(new byte[0], attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ScriptedTransformRecord.REL_SUCCESS, 1);

        final MockFlowFile output = testRunner.getFlowFilesForRelationship(ScriptedTransformRecord.REL_SUCCESS).getFirst();
        output.assertAttributeEquals("record.count", "2");

        final List<Record> outputRecords = recordWriter.getRecordsWritten();
        assertEquals("John Doe", outputRecords.get(0).getAsRecord("book", bookSchema).getValue("author"));
        assertEquals("Unknown Author", outputRecords.get(1).getAsRecord("book", bookSchema).getValue("author"));
    }

    private Record createBook(final String author, final String date, final RecordSchema bookSchema, final RecordSchema outerSchema) {
        final Map<String, Object> firstBookValues = new HashMap<>();
        firstBookValues.put("author", author);
        firstBookValues.put("date", date);

        final Record firstBookRecord = new MapRecord(bookSchema, firstBookValues);
        final Map<String, Object> book1ValueMap = mutableMap("book", firstBookRecord);
        return new MapRecord(outerSchema, book1ValueMap);
    }


    private void setup(final RecordSchema schema) throws InitializationException {
        setupWithDifferentSchemas(schema, schema);
    }

    private void setupWithDifferentSchemas(final RecordSchema readerSchema, final RecordSchema writerSchema) throws InitializationException {
        testRunner = TestRunners.newTestRunner(ScriptedTransformRecord.class);
        testRunner.setProperty(ScriptedTransformRecord.RECORD_READER, "record-reader");
        testRunner.setProperty(ScriptedTransformRecord.RECORD_WRITER, "record-writer");

        recordReader = new ArrayListRecordReader(readerSchema);
        recordWriter = new ArrayListRecordWriter(writerSchema);

        testRunner.addControllerService("record-reader", recordReader);
        testRunner.addControllerService("record-writer", recordWriter);

        testRunner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, "record"); // return the input
        testRunner.setProperty(ScriptedTransformRecord.LANGUAGE, "Groovy");
        testRunner.enableControllerService(recordReader);
        testRunner.enableControllerService(recordWriter);
    }

    private RecordSchema createSimpleNumberSchema() {
        final RecordField recordField = new RecordField("num", RecordFieldType.INT.getDataType());
        final List<RecordField> recordFields = Collections.singletonList(recordField);
        final RecordSchema schema = new SimpleRecordSchema(recordFields);
        return schema;
    }

    private RecordSchema createSchemaWithAddedValue() {
        final RecordField recordFieldNum = new RecordField("num", RecordFieldType.INT.getDataType());
        final RecordField recordFieldAdd = new RecordField("added-value", RecordFieldType.INT.getDataType());
        final List<RecordField> recordFields = Arrays.asList(recordFieldNum, recordFieldAdd);
        final RecordSchema schema = new SimpleRecordSchema(recordFields);
        return schema;
    }
}
