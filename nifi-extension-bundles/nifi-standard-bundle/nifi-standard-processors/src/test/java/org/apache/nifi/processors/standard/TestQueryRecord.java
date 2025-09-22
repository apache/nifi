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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.ArrayListRecordReader;
import org.apache.nifi.serialization.record.ArrayListRecordWriter;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQueryRecord {

    private static final String REL_NAME = "success";

    private static final String ISO_DATE_FORMAT = "yyyy-MM-dd";
    private static final String ISO_DATE = "2018-02-04";
    private static final String INSTANT_FORMATTED = String.format("%sT10:20:55Z", ISO_DATE);
    private static final Instant INSTANT = Instant.parse(INSTANT_FORMATTED);
    private static final Date INSTANT_DATE = Date.from(INSTANT);
    private static final long INSTANT_EPOCH_MILLIS = INSTANT.toEpochMilli();

    public TestRunner getRunner() {
        TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);

        /*
         * we have to disable validation of expression language because the scope of the evaluation
         * depends of the value of another property: if we are caching the schema/queries or not. If
         * we don't disable the validation, it'll throw an error saying that the scope is incorrect.
         */
        runner.setValidateExpressionUsage(false);

        return runner;
    }


    @Test
    public void testRecordPathFunctions() throws InitializationException {
        final Record record = createHierarchicalRecord();
        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
            "SELECT RPATH_STRING(person, '/name') AS name," +
            " RPATH_INT(person, '/age') AS age," +
            " RPATH(person, '/name') AS nameObj," +
            " RPATH(person, '/age') AS ageObj," +
            " RPATH(person, '/favoriteColors') AS colors," +
            " RPATH(person, '//name') AS names," +
            " RPATH_DATE(person, '/dob') AS dob," +
            " RPATH_LONG(person, '/dobTimestamp') AS dobTimestamp," +
            " RPATH_DATE(person, 'toDate(/joinDate, \"yyyy-MM-dd\")') AS joinTime, " +
            " RPATH_DOUBLE(person, '/weight') AS weight" +
            " FROM FLOWFILE");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
        final List<MockFlowFile> flowFilesOriginal = runner.getFlowFilesForRelationship(QueryRecord.REL_ORIGINAL);
        flowFilesOriginal.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, QueryRecord.REL_ORIGINAL.getName());

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("John Doe", output.getValue("nameObj"));
        assertEquals(30, output.getValue("age"));
        assertEquals(30, output.getValue("ageObj"));
        assertArrayEquals(new String[] {"red", "green"}, (Object[]) output.getValue("colors"));
        assertArrayEquals(new String[] {"John Doe", "Jane Doe"}, (Object[]) output.getValue("names"));

        assertEquals(java.time.LocalDate.parse(ISO_DATE), output.getAsLocalDate("joinTime", ISO_DATE_FORMAT));
        assertEquals(Double.valueOf(180.8D), output.getAsDouble("weight"));
    }

    @Test
    public void testRecordPathInAggregate() throws InitializationException {
        final Record record = createHierarchicalRecord();

        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        for (int i = 0; i < 100; i++) {
            final Record toAdd = createHierarchicalRecord();
            final Record person = (Record) toAdd.getValue("person");

            person.setValue("name", "Person " + i);
            person.setValue("age", i);
            recordReader.addRecord(toAdd);
        }

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
            "SELECT RPATH_STRING(person, '/name') AS name FROM FLOWFILE" +
                " WHERE RPATH_INT(person, '/age') > (" +
                "   SELECT AVG( RPATH_INT(person, '/age') ) FROM FLOWFILE" +
                ")");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(50, written.size());

        int i = 50;
        for (final Record writtenRecord : written) {
            final String name = writtenRecord.getAsString("name");
            assertEquals("Person " + i, name);
            i++;
        }
    }


    @Test
    public void testRecordPathWithArray() throws InitializationException {
        final Record record = createHierarchicalArrayRecord();
        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
            "SELECT title, name" +
            "    FROM FLOWFILE" +
            "    WHERE RPATH(addresses, '/state[/label = ''home'']') <>" +
            "          RPATH(addresses, '/state[/label = ''work'']')");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("Software Engineer", output.getValue("title"));
    }

    @Test
    public void testCollectionFunctionsWithArray() throws InitializationException {
        final Record record = createHierarchicalArrayRecord();
        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
                "SELECT title, name, jobLevel" +
                        "    FROM FLOWFILE" +
                        "    WHERE CARDINALITY(addresses) > 1");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("Software Engineer", output.getValue("title"));
        assertEquals(JobLevel.IC2, output.getValue("jobLevel"));
    }

    @Test
    public void testCollectionFunctionsWithoutCastFailure() throws InitializationException {
        final Record record = createHierarchicalArrayRecord();
        final Record record2 = createHierarchicalArrayRecord();
        record2.setValue("height", 30);

        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);
        recordReader.addRecord(record2);
        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
                "SELECT title, name, sum(height) as height_total " +
                "FROM FLOWFILE " +
                "GROUP BY title, name");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("Software Engineer", output.getValue("title"));
        assertEquals(new BigDecimal("90.500000000"), output.getValue("height_total"));
    }

    @Test
    public void testCollectionFunctionsWithCastChoice() throws InitializationException {
        final Record record = createHierarchicalArrayRecord();

        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
                "SELECT title, name, " +
                    "sum(CAST(height AS DOUBLE)) as height_total_double, " +
                    "sum(CAST(height AS REAL)) as height_total_float " +
                "FROM FLOWFILE " +
                "GROUP BY title, name");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Number height = 121.0;
        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("Software Engineer", output.getValue("title"));
        assertEquals(height.doubleValue(), output.getValue("height_total_double"));
        assertEquals(height.floatValue(), output.getValue("height_total_float"));
    }

    @Test
    public void testCollectionFunctionsWithCastChoiceWithInts() throws InitializationException {
        final Record record = createHierarchicalArrayRecord();
        record.setValue("height", 30);

        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
            "SELECT title, name, " +
                "sum(CAST(height AS INT)) as height_total_int, " +
                "sum(CAST(height AS BIGINT)) as height_total_long " +
                "FROM FLOWFILE " +
                "GROUP BY title, name");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Number height = 60;
        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("Software Engineer", output.getValue("title"));
        assertEquals(height.longValue(), output.getValue("height_total_long"));
        assertEquals(height.intValue(), output.getValue("height_total_int"));
    }

    @Test
    public void testCollectionFunctionsWithWhereClause() throws InitializationException {
        final Record sample = createTaggedRecord("1", "a", "b", "c");

        final ArrayListRecordReader recordReader = new ArrayListRecordReader(sample.getSchema());
        recordReader.addRecord(createTaggedRecord("1", "a", "d", "g"));
        recordReader.addRecord(createTaggedRecord("2", "b", "e"));
        recordReader.addRecord(createTaggedRecord("3", "c", "f", "h"));


        final ArrayListRecordWriter writer = new ArrayListRecordWriter(sample.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
                "SELECT id, tags FROM FLOWFILE CROSS JOIN UNNEST(FLOWFILE.tags) AS f(tag) WHERE tag IN ('a','b')");
        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(2, written.size());

        final Record output0 = written.get(0);
        assertEquals("1", output0.getValue("id"));
        assertArrayEquals(new Object[]{"a", "d", "g"}, (Object[]) output0.getValue("tags"));

        final Record output1 = written.get(1);
        assertEquals("2", output1.getValue("id"));
        assertArrayEquals(new Object[]{"b", "e"}, (Object[]) output1.getValue("tags"));
    }


    @Test
    public void testArrayColumnWithIndex() throws InitializationException {
        final Record sample = createTaggedRecord("1", "a", "b", "c");

        final ArrayListRecordReader recordReader = new ArrayListRecordReader(sample.getSchema());
        recordReader.addRecord(createTaggedRecord("1", "a", "d", "g"));
        recordReader.addRecord(createTaggedRecord("2", "b", "e"));
        recordReader.addRecord(createTaggedRecord("3", "c", "f", "h"));


        final ArrayListRecordWriter writer = new ArrayListRecordWriter(sample.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
                "SELECT id, tags[1] as first, tags[2] as \"second\", tags[CARDINALITY(tags)] as last FROM FLOWFILE");
        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(3, written.size());

        final Record output0 = written.get(0);
        assertEquals("1", output0.getValue("id"));
        assertEquals("a", output0.getValue("first"));
        assertEquals("d", output0.getValue("second"));
        assertEquals("g", output0.getValue("last"));

        final Record output1 = written.get(1);
        assertEquals("2", output1.getValue("id"));
        assertEquals("b", output1.getValue("first"));
        assertEquals("e", output1.getValue("second"));
        assertEquals("e", output1.getValue("last"));

        final Record output2 = written.get(2);
        assertEquals("3", output2.getValue("id"));
        assertEquals("c", output2.getValue("first"));
        assertEquals("f", output2.getValue("second"));
        assertEquals("h", output2.getValue("last"));
    }

    @Test
    public void testCompareResultsOfTwoRecordPathsAgainstArray() throws InitializationException {
        final Record record = createHierarchicalArrayRecord();

        // Change the value of the 'state' field of both addresses to NY.
        // This allows us to use an equals operator to ensure that we do get back the same values,
        // whereas the unit test above tests <> and that may result in 'false confidence' if the software
        // were to provide the wrong values but values that were not equal.
        Record[] addresses = (Record[]) record.getValue("addresses");
        for (final Record address : addresses) {
            address.setValue("state", "NY");
        }

        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
            "SELECT title, name" +
                "    FROM FLOWFILE" +
                "    WHERE RPATH(addresses, '/state[/label = ''home'']') =" +
                "          RPATH(addresses, '/state[/label = ''work'']')");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("Software Engineer", output.getValue("title"));
    }


    @Test
    public void testRecordPathWithArrayAndOnlyOneElementMatchingRPath() throws InitializationException {
        final Record record = createHierarchicalArrayRecord();
        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
            "SELECT title, name" +
                "    FROM FLOWFILE" +
                "    WHERE RPATH(addresses, '/state[. = ''NY'']') = 'NY'");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("Software Engineer", output.getValue("title"));
    }


    @Test
    public void testLikeWithRecordPath() throws InitializationException {
        final Record record = createHierarchicalArrayRecord();
        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
            "SELECT title, name" +
                "    FROM FLOWFILE" +
                "    WHERE RPATH_STRING(addresses, '/state[. = ''NY'']') LIKE 'N%'");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record output = written.get(0);
        assertEquals("John Doe", output.getValue("name"));
        assertEquals("Software Engineer", output.getValue("title"));
    }


    @Test
    public void testRecordPathWithMap() throws InitializationException {
        final Record record = createHierarchicalRecord();
        final ArrayListRecordReader recordReader = new ArrayListRecordReader(record.getSchema());
        recordReader.addRecord(record);

        final ArrayListRecordWriter writer = new ArrayListRecordWriter(record.getSchema());

        TestRunner runner = getRunner();
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(REL_NAME,
            "SELECT RPATH(favoriteThings, '.[''sport'']') AS sport," +
                " RPATH_STRING(person, '/name') AS nameObj" +
                " FROM FLOWFILE" +
                " WHERE RPATH(favoriteThings, '.[''color'']') = 'green'");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(REL_NAME, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        final List<Record> written = writer.getRecordsWritten();
        assertEquals(1, written.size());

        final Record output = written.get(0);
        assertEquals("basketball", output.getValue("sport"));
        assertEquals("John Doe", output.getValue("nameObj"));
    }

    /**
     * Returns a Record that, if written in JSON, would look like:
     * <code><pre>
     * {
     *    "person": {
     *        "name": "John Doe",
     *        "age": 30,
     *        "favoriteColors": [ "red", "green" ],
     *        "dob": 598741575825,
     *        "dobTimestamp": 598741575825,
     *        "joinDate": "2018-02-04",
     *        "weight": 180.8,
     *        "mother": {
     *          "name": "Jane Doe"
     *        }
     *    },
     *    "favouriteThings": {
     *       "sport": "basketball",
     *       "color": "green",
     *       "roses": "raindrops",
     *       "kittens": "whiskers"
     *    }
     * }
     * </pre></code>
     *
     * @return the Record
     */
    private Record createHierarchicalRecord() {
        final List<RecordField> namedPersonFields = new ArrayList<>();
        namedPersonFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema namedPersonSchema = new SimpleRecordSchema(namedPersonFields);

        final List<RecordField> personFields = new ArrayList<>();
        personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        personFields.add(new RecordField("age", RecordFieldType.INT.getDataType()));
        personFields.add(new RecordField("favoriteColors", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        personFields.add(new RecordField("dob", RecordFieldType.DATE.getDataType()));
        personFields.add(new RecordField("dobTimestamp", RecordFieldType.LONG.getDataType()));
        personFields.add(new RecordField("joinDate", RecordFieldType.STRING.getDataType()));
        personFields.add(new RecordField("weight", RecordFieldType.DOUBLE.getDataType()));
        personFields.add(new RecordField("height", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.LONG.getDataType(), RecordFieldType.INT.getDataType())));
        personFields.add(new RecordField("mother", RecordFieldType.RECORD.getRecordDataType(namedPersonSchema)));
        final RecordSchema personSchema = new SimpleRecordSchema(personFields);

        final List<RecordField> outerSchemaFields = new ArrayList<>();
        outerSchemaFields.add(new RecordField("person", RecordFieldType.RECORD.getRecordDataType(personSchema)));
        outerSchemaFields.add(new RecordField("favoriteThings", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())));
        final RecordSchema recordSchema = new SimpleRecordSchema(outerSchemaFields);

        final Record mother = new MapRecord(namedPersonSchema, Collections.singletonMap("name", "Jane Doe"));

        final Map<String, String> favorites = new HashMap<>();
        favorites.put("sport", "basketball");
        favorites.put("color", "green");
        favorites.put("roses", "raindrops");
        favorites.put("kittens", "whiskers");


        final Map<String, Object> map = new HashMap<>();
        map.put("name", "John Doe");
        map.put("age", 30);
        map.put("favoriteColors", new String[] {"red", "green"});
        map.put("dob", INSTANT_DATE);
        map.put("dobTimestamp", INSTANT_EPOCH_MILLIS);
        map.put("joinDate", ISO_DATE);
        map.put("weight", 180.8D);
        map.put("height", 60.5);
        map.put("mother", mother);
        final Record person = new MapRecord(personSchema, map);

        final Map<String, Object> personValues = new HashMap<>();
        personValues.put("person", person);
        personValues.put("favoriteThings", favorites);

        return new MapRecord(recordSchema, personValues);
    }


    /**
     * Returns a Record that, if written in JSON, would look like:
     * <code><pre>
     * {
     *    "id": &gt;id&lt;,
     *    "tags": [&gt;tag1&lt;,&gt;tag2&lt;...]
     * }
     * </pre></code>
     *
     * @return the Record
     */
    private Record createTaggedRecord(String id, String... tags) {
        final List<RecordField> recordSchemaFields = new ArrayList<>();
        recordSchemaFields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        recordSchemaFields.add(new RecordField("tags", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        final RecordSchema recordSchema = new SimpleRecordSchema(recordSchemaFields);

        final Map<String, Object> map = new HashMap<>();
        map.put("id", id);
        map.put("tags", Arrays.asList(tags));

        return new MapRecord(recordSchema, map);
    }

    /**
     * Returns a Record that, if written in JSON, would look like:
     * <code><pre>
     *          {
     *               "name": "John Doe",
     *               "title": "Software Engineer",
     *               "jobLevel": "IC2",
     *               "age": 40,
     *               "addresses": [{
     *                   "streetNumber": 4820,
     *                   "street": "My Street",
     *                   "apartment": null,
     *                   "city": "New York",
     *                   "state": "NY",
     *                   "country": "USA",
     *                   "label": "work"
     *               }, {
     *                   "streetNumber": 327,
     *                   "street": "Small Street",
     *                   "apartment": 309,
     *                   "city": "Los Angeles",
     *                   "state": "CA",
     *                   "country": "USA",
     *                   "label": "home"
     *               }]
     *             }
     * </pre></code>
     *
     * @return the Record
     */
    private Record createHierarchicalArrayRecord() {
        final List<RecordField> addressFields = new ArrayList<>();
        addressFields.add(new RecordField("streetNumber", RecordFieldType.INT.getDataType()));
        addressFields.add(new RecordField("street", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("apartment", RecordFieldType.INT.getDataType()));
        addressFields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("country", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("label", RecordFieldType.STRING.getDataType()));
        final RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

        final List<RecordField> personFields = new ArrayList<>();
        personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        personFields.add(new RecordField("age", RecordFieldType.INT.getDataType()));
        personFields.add(new RecordField("title", RecordFieldType.STRING.getDataType()));
        personFields.add(new RecordField("jobLevel", RecordFieldType.ENUM.getDataType()));
        personFields.add(new RecordField("height", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.DOUBLE.getDataType(), RecordFieldType.INT.getDataType())));
        personFields.add(new RecordField("addresses", RecordFieldType.ARRAY.getArrayDataType( RecordFieldType.RECORD.getRecordDataType(addressSchema)) ));
        final RecordSchema personSchema = new SimpleRecordSchema(personFields);

        final Map<String, Object> workMap = new HashMap<>();
        workMap.put("streetNumber", 4820);
        workMap.put("street", "My Street");
        workMap.put("apartment", null);
        workMap.put("city", "New York City");
        workMap.put("state", "NY");
        workMap.put("country", "USA");
        workMap.put("label", "work");
        final Record workAddress = new MapRecord(addressSchema, workMap);

        final Map<String, Object> homeMap = new HashMap<>();
        homeMap.put("streetNumber", 327);
        homeMap.put("street", "Small Street");
        homeMap.put("apartment", 302);
        homeMap.put("city", "Los Angeles");
        homeMap.put("state", "CA");
        homeMap.put("country", "USA");
        homeMap.put("label", "home");
        final Record homeAddress = new MapRecord(addressSchema, homeMap);

        final Map<String, Object> map = new HashMap<>();
        map.put("name", "John Doe");
        map.put("age", 30);
        map.put("height", 60.5);
        map.put("title", "Software Engineer");
        map.put("jobLevel", JobLevel.IC2);
        map.put("addresses", new Record[] {homeAddress, workAddress});
        return new MapRecord(personSchema, map);
    }


    @Test
    public void testStreamClosedWhenBadData() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.failAfter(0);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);
        parser.addRecord("Tom", 49);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        TestRunner runner = getRunner();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, age from FLOWFILE WHERE name <> ''");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertTransferCount(QueryRecord.REL_FAILURE, 1);
    }

    @Test
    public void testSimple() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);
        parser.addRecord("Tom", 49);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        TestRunner runner = getRunner();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, age from FLOWFILE WHERE name <> ''");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        final int numIterations = 1;
        for (int i = 0; i < numIterations; i++) {
            runner.enqueue(new byte[0]);
        }

        runner.setThreadCount(4);
        runner.run(2 * numIterations);

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        out.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
        out.assertContentEquals("\"name\",\"points\"\n\"Tom\",\"49\"\n");
    }

    @Test
    public void testNullable() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING, true);
        parser.addSchemaField("age", RecordFieldType.INT, true);
        parser.addRecord("Tom", 49);
        parser.addRecord("Alice", null);
        parser.addRecord(null, 36);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        TestRunner runner = getRunner();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, age from FLOWFILE");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        final int numIterations = 1;
        for (int i = 0; i < numIterations; i++) {
            runner.enqueue(new byte[0]);
        }

        runner.setThreadCount(4);
        runner.run(2 * numIterations);

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        out.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
        out.assertContentEquals("\"name\",\"points\"\n\"Tom\",\"49\"\n\"Alice\",\n,\"36\"\n");
    }

    @Test
    public void testParseFailure() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);
        parser.addRecord("Tom", 49);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        TestRunner runner = getRunner();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, age from FLOWFILE WHERE name <> ''");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        final int numIterations = 1;
        for (int i = 0; i < numIterations; i++) {
            runner.enqueue(new byte[0]);
        }

        runner.setThreadCount(4);
        runner.run(2 * numIterations);

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        out.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
        out.assertContentEquals("\"name\",\"points\"\n\"Tom\",\"49\"\n");
    }

    @Test
    public void testNoRecordsInput() throws InitializationException {
        TestRunner runner = getRunner();

        CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"age\"");

        runner.addControllerService("csv-reader", csvReader);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(csvReader);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name from FLOWFILE WHERE age > 23");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "csv-reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(QueryRecord.INCLUDE_ZERO_RECORD_FLOWFILES, "true");

        runner.enqueue("name,age\n");
        runner.run();
        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        out.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
        out.assertContentEquals("\"name\",\"age\"\n");
    }


    @Test
    public void testTransformCalc() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("ID", RecordFieldType.INT);
        parser.addSchemaField("AMOUNT1", RecordFieldType.FLOAT);
        parser.addSchemaField("AMOUNT2", RecordFieldType.FLOAT);
        parser.addSchemaField("AMOUNT3", RecordFieldType.FLOAT);

        parser.addRecord(8, 10.05F, 15.45F, 89.99F);
        parser.addRecord(100, 20.25F, 25.25F, 45.25F);
        parser.addRecord(105, 20.05F, 25.05F, 45.05F);
        parser.addRecord(200, 34.05F, 25.05F, 75.05F);

        final MockRecordWriter writer = new MockRecordWriter("\"NAME\",\"POINTS\"");

        TestRunner runner = getRunner();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select ID, AMOUNT1+AMOUNT2+AMOUNT3 as TOTAL from FLOWFILE where ID=100");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        out.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);

        out.assertContentEquals("\"NAME\",\"POINTS\"\n\"100\",\"90.75\"\n");
    }

    @Test
    public void testHandlingWithInvalidSchema() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("favorite_color", RecordFieldType.STRING);
        parser.addSchemaField("address", RecordFieldType.STRING);
        parser.addRecord("Tom", "blue", null);
        parser.addRecord("Jerry", "red", null);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        TestRunner runner = getRunner();
        runner.enforceReadStreamsClosed(false);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(QueryRecord.INCLUDE_ZERO_RECORD_FLOWFILES, "false");
        runner.setProperty("rel1", "select * from FLOWFILE where address IS NOT NULL");
        runner.setProperty("rel2", "select name, CAST(favorite_color AS DOUBLE) AS num from FLOWFILE");
        runner.setProperty("rel3", "select * from FLOWFILE where address IS NOT NULL");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(QueryRecord.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryRecord.REL_FAILURE);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, QueryRecord.REL_FAILURE.getName());
    }

    @Test
    public void testAggregateFunction() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("points", RecordFieldType.INT);
        parser.addRecord("Tom", 1);
        parser.addRecord("Jerry", 2);
        parser.addRecord("Tom", 99);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        TestRunner runner = getRunner();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, sum(points) as points from FLOWFILE GROUP BY name ORDER BY points DESC");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile flowFileOut = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        flowFileOut.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
        flowFileOut.assertContentEquals("\"name\",\"points\"\n\"Tom\",\"100\"\n\"Jerry\",\"2\"\n");
    }

    @Test
    public void testNullValueInSingleField() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("points", RecordFieldType.INT);
        parser.addRecord("Tom", 1);
        parser.addRecord("Jerry", null);
        parser.addRecord("Tom", null);
        parser.addRecord("Jerry", 3);

        final MockRecordWriter writer = new MockRecordWriter(null, false);

        TestRunner runner = getRunner();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select points from FLOWFILE");
        runner.setProperty("count", "select count(*) as c from flowfile where points is null");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(REL_NAME, 1);
        runner.assertTransferCount("count", 1);
        final MockFlowFile flowFileOut = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        flowFileOut.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
        flowFileOut.assertContentEquals("1\n\n\n3\n");

        final MockFlowFile countFlowFile = runner.getFlowFilesForRelationship("count").get(0);
        countFlowFile.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, "count");
        countFlowFile.assertContentEquals("2\n");
    }

    @Test
    public void testColumnNames() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("points", RecordFieldType.INT);
        parser.addSchemaField("greeting", RecordFieldType.STRING);
        parser.addRecord("Tom", 1, "Hello");
        parser.addRecord("Jerry", 2, "Hi");
        parser.addRecord("Tom", 99, "Howdy");

        final List<String> colNames = new ArrayList<>();
        colNames.add("name");
        colNames.add("points");
        colNames.add("greeting");
        colNames.add("FAV_GREETING");
        final ResultSetValidatingRecordWriter writer = new ResultSetValidatingRecordWriter(colNames);

        TestRunner runner = getRunner();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select *, greeting AS FAV_GREETING from FLOWFILE");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(REL_NAME, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NAME);
        flowFiles.get(0).assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
    }

    @Test
    public void testReturnsNoResultWithArrayColumn() throws InitializationException {
        TestRunner runner = getRunner();

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.enableControllerService(jsonWriter);

        runner.setProperty(REL_NAME, "SELECT * from FLOWFILE WHERE status = 'failure'");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(QueryRecord.INCLUDE_ZERO_RECORD_FLOWFILES, "true");

        runner.enqueue("{\"status\": \"starting\",\"myArray\": [{\"foo\": \"foo\"}]}");
        runner.run();

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile flowFileOut = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        flowFileOut.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, REL_NAME);
        flowFileOut.assertContentEquals("[]");
    }


    private static class ResultSetValidatingRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        private final List<String> columnNames;

        public ResultSetValidatingRecordWriter(final List<String> colNames) {
            this.columnNames = new ArrayList<>(colNames);
        }

        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) {
            final List<RecordField> recordFields = columnNames.stream()
                .map(name -> new RecordField(name, RecordFieldType.STRING.getDataType()))
                .collect(Collectors.toList());
            return new SimpleRecordSchema(recordFields);
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) {
            return new RecordSetWriter() {

                @Override
                public void flush() throws IOException {
                    out.flush();
                }

                @Override
                public WriteResult write(final RecordSet rs) throws IOException {
                    final int colCount = rs.getSchema().getFieldCount();
                    assertEquals(columnNames.size(), colCount);

                    final List<String> colNames = new ArrayList<>(colCount);
                    for (int i = 0; i < colCount; i++) {
                        colNames.add(rs.getSchema().getField(i).getFieldName());
                    }

                    assertEquals(columnNames, colNames);

                    // Iterate over the rest of the records to ensure that we read the entire stream. If we don't
                    // do this, we won't consume all of the data and as a result we will not close the stream properly
                    while (rs.next() != null) {
                    }

                    return WriteResult.of(0, Collections.emptyMap());
                }

                @Override
                public String getMimeType() {
                    return "text/plain";
                }

                @Override
                public WriteResult write(Record record) {
                    return null;
                }

                @Override
                public void close() throws IOException {
                    out.close();
                }

                @Override
                public void beginRecordSet() {
                }

                @Override
                public WriteResult finishRecordSet() {
                    return WriteResult.EMPTY;
                }
            };
        }

    }

    public enum JobLevel {
        IC1,
        IC2,
        IC3
    }

}
