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

package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLookupRecord {

    private TestRunner runner;
    private MapLookup lookupService;
    private MockRecordParser recordReader;
    private MockRecordWriter recordWriter;

    @Before
    public void setup() throws InitializationException {
        recordReader = new MockRecordParser();
        recordWriter = new MockRecordWriter(null, false);
        lookupService = new MapLookup();

        runner = TestRunners.newTestRunner(LookupRecord.class);
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.addControllerService("lookup", lookupService);
        runner.enableControllerService(lookupService);

        runner.setProperty(LookupRecord.RECORD_READER, "reader");
        runner.setProperty(LookupRecord.RECORD_WRITER, "writer");
        runner.setProperty(LookupRecord.LOOKUP_SERVICE, "lookup");
        runner.setProperty("lookup", "/name");
        runner.setProperty(LookupRecord.RESULT_RECORD_PATH, "/sport");
        runner.setProperty(LookupRecord.ROUTING_STRATEGY, LookupRecord.ROUTE_TO_MATCHED_UNMATCHED);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, null);
        recordReader.addRecord("Jane Doe", 47, null);
        recordReader.addRecord("Jimmy Doe", 14, null);
    }

    @Test
    public void testFlowfileAttributesPassed() {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "person");
        attrs.put("something_something", "test");

        Map<String, Object> expected = new HashMap<>();
        expected.putAll(attrs);

        lookupService.setExpectedContext(expected);

        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.enqueue("", attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,Soccer\nJane Doe,47,Basketball\nJimmy Doe,14,Football\n");
    }

    @Test
    public void testAllMatch() throws InitializationException {
        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,Soccer\nJane Doe,47,Basketball\nJimmy Doe,14,Football\n");
    }

    @Test
    public void testAllUnmatched() throws InitializationException {
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_UNMATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }

    @Test
    public void testMixtureOfMatch() throws InitializationException {
        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(LookupRecord.REL_FAILURE, 0);
        runner.assertTransferCount(LookupRecord.REL_MATCHED, 1);
        runner.assertTransferCount(LookupRecord.REL_UNMATCHED, 1);

        final MockFlowFile matched = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);
        matched.assertAttributeEquals("record.count", "2");
        matched.assertAttributeEquals("mime.type", "text/plain");
        matched.assertContentEquals("John Doe,48,Soccer\nJimmy Doe,14,Football\n");

        final MockFlowFile unmatched = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);
        unmatched.assertAttributeEquals("record.count", "1");
        unmatched.assertAttributeEquals("mime.type", "text/plain");
        unmatched.assertContentEquals("Jane Doe,47,\n");
    }


    @Test
    public void testResultPathNotFound() throws InitializationException {
        runner.setProperty(LookupRecord.RESULT_RECORD_PATH, "/other");

        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }

    @Test
    public void testLookupPathNotFound() throws InitializationException {
        runner.setProperty("lookup", "/other");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_UNMATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }

    @Test
    public void testUnparseableData() throws InitializationException {
        recordReader.failAfter(1);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_FAILURE).get(0);
        out.assertAttributeNotExists("record.count");
        out.assertContentEquals("");
    }

    @Test
    public void testNoResultPath() throws InitializationException {
        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.removeProperty(LookupRecord.RESULT_RECORD_PATH);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_MATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }


    @Test
    public void testMultipleLookupPaths() throws InitializationException {
        lookupService.addValue("John Doe", "Soccer");
        lookupService.addValue("Jane Doe", "Basketball");
        lookupService.addValue("Jimmy Doe", "Football");

        runner.setProperty("lookup", "/*");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(LookupRecord.REL_UNMATCHED, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_UNMATCHED).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("John Doe,48,\nJane Doe,47,\nJimmy Doe,14,\n");
    }

    @Test
    public void testInvalidUnlessAllRequiredPropertiesAdded() throws InitializationException {
        runner.removeProperty(new PropertyDescriptor.Builder().name("lookup").build());
        runner.setProperty("hello", "/name");
        runner.assertNotValid();

        runner.setProperty("lookup", "xx");
        runner.assertNotValid();

        runner.setProperty("lookup", "/name");
        runner.assertValid();
    }


    @Test
    public void testAddFieldsToExistingRecord() throws InitializationException, IOException {
        final RecordLookup lookupService = new RecordLookup();
        runner.addControllerService("lookup", lookupService);
        runner.enableControllerService(lookupService);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("favorite", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("least", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Record sports = new MapRecord(schema, new HashMap<String, Object>());

        sports.setValue("favorite", "basketball");
        sports.setValue("least", "soccer");

        lookupService.addValue("John Doe", sports);

        recordReader = new MockRecordParser();
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("favorite", RecordFieldType.STRING);
        recordReader.addSchemaField("least", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, null, "baseball");

        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);

        runner.setProperty("lookup", "/name");
        runner.setProperty(LookupRecord.RESULT_RECORD_PATH, "/");
        runner.setProperty(LookupRecord.RESULT_CONTENTS, LookupRecord.RESULT_RECORD_FIELDS);

        runner.enqueue("");
        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);
        out.assertContentEquals("John Doe,48,basketball,soccer\n");
    }

    /**
     * If the output fields are added to a record that doesn't exist, the result should be that a Record is
     * created and the results added to it.
     */
    @Test
    public void testAddFieldsToNonExistentRecord() throws InitializationException {
        final RecordLookup lookupService = new RecordLookup();
        runner.addControllerService("lookup", lookupService);
        runner.enableControllerService(lookupService);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("favorite", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("least", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Record sports = new MapRecord(schema, new HashMap<String, Object>());

        sports.setValue("favorite", "basketball");
        sports.setValue("least", "soccer");

        lookupService.addValue("John Doe", sports);

        recordReader = new MockRecordParser();
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.RECORD);

        recordReader.addRecord("John Doe", 48, null);

        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);

        runner.setProperty("lookup", "/name");
        runner.setProperty(LookupRecord.RESULT_RECORD_PATH, "/sport");
        runner.setProperty(LookupRecord.RESULT_CONTENTS, LookupRecord.RESULT_RECORD_FIELDS);

        runner.enqueue("");
        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        // We can't be sure of the order of the fields in the record, so we allow either 'least' or 'favorite' to be first
        final String outputContents = new String(out.toByteArray());
        assertTrue(outputContents.equals("John Doe,48,MapRecord[{favorite=basketball, least=soccer}]\n")
            || outputContents.equals("John Doe,48,MapRecord[{least=soccer, favorite=basketball}]\n"));
    }

    /**
     * If the output fields are added to a non-record field, then the result should be that the field
     * becomes a UNION that does allow the Record and the value is set to a Record.
     */
    @Test
    public void testAddFieldsToNonRecordField() throws InitializationException {
        final RecordLookup lookupService = new RecordLookup();
        runner.addControllerService("lookup", lookupService);
        runner.enableControllerService(lookupService);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("favorite", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("least", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Record sports = new MapRecord(schema, new HashMap<String, Object>());

        sports.setValue("favorite", "basketball");
        sports.setValue("least", "soccer");

        lookupService.addValue("John Doe", sports);

        recordReader = new MockRecordParser();
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, null);

        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);

        runner.setProperty("lookup", "/name");
        runner.setProperty(LookupRecord.RESULT_RECORD_PATH, "/sport");
        runner.setProperty(LookupRecord.RESULT_CONTENTS, LookupRecord.RESULT_RECORD_FIELDS);

        runner.enqueue("");
        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(LookupRecord.REL_MATCHED).get(0);

        // We can't be sure of the order of the fields in the record, so we allow either 'least' or 'favorite' to be first
        final String outputContents = new String(out.toByteArray());
        assertTrue(outputContents.equals("John Doe,48,MapRecord[{favorite=basketball, least=soccer}]\n")
            || outputContents.equals("John Doe,48,MapRecord[{least=soccer, favorite=basketball}]\n"));
    }


    private static class MapLookup extends AbstractControllerService implements StringLookupService {
        private final Map<String, String> values = new HashMap<>();
        private Map<String, Object> expectedContext;

        public void addValue(final String key, final String value) {
            values.put(key, value);
        }

        @Override
        public Class<?> getValueType() {
            return String.class;
        }

        public Optional<String> lookup(final Map<String, Object> coordinates, Map<String, String> context) {
            validateContext(context);
            return lookup(coordinates);
        }

        @Override
        public Optional<String> lookup(final Map<String, Object> coordinates) {
            if (coordinates == null || coordinates.get("lookup") == null) {
                return Optional.empty();
            }

            final String key = (String)coordinates.get("lookup");
            if (key == null) {
                return Optional.empty();
            }

            return Optional.ofNullable(values.get(key));
        }

        @Override
        public Set<String> getRequiredKeys() {
            return Collections.singleton("lookup");
        }

        public void setExpectedContext(Map<String, Object> expectedContext) {
            this.expectedContext = expectedContext;
        }

        private void validateContext(Map<String, String> context) {
            if (expectedContext != null) {
                for (Map.Entry<String, Object> entry : expectedContext.entrySet()) {
                    Assert.assertTrue(String.format("%s was not in coordinates.", entry.getKey()),
                            context.containsKey(entry.getKey()));
                    Assert.assertEquals("Wrong value", entry.getValue(), context.get(entry.getKey()));
                }
            }
        }
    }

    private static class RecordLookup extends AbstractControllerService implements RecordLookupService {
        private final Map<String, Record> values = new HashMap<>();

        public void addValue(final String key, final Record value) {
            values.put(key, value);
        }

        @Override
        public Class<?> getValueType() {
            return String.class;
        }

        @Override
        public Optional<Record> lookup(final Map<String, Object> coordinates) {
            if (coordinates == null || coordinates.get("lookup") == null) {
                return Optional.empty();
            }

            final String key = (String)coordinates.get("lookup");
            if (key == null) {
                return Optional.empty();
            }

            return Optional.ofNullable(values.get(key));
        }

        @Override
        public Set<String> getRequiredKeys() {
            return Collections.singleton("lookup");
        }
    }
}
