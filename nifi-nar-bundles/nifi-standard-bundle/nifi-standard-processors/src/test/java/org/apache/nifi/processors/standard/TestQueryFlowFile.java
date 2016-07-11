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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.RowRecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

public class TestQueryFlowFile {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.standard.SQLTransform", "debug");
    }

    private static final String REL_NAME = "success";

    @Test
    public void testSimple() throws InitializationException, IOException, SQLException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);
        parser.addRecord("Tom", 49);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        final TestRunner runner = TestRunners.newTestRunner(QueryFlowFile.class);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, age from FLOWFILE WHERE name <> ''");
        runner.setProperty(QueryFlowFile.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryFlowFile.RECORD_WRITER_FACTORY, "writer");

        final int numIterations = 1;
        for (int i = 0; i < numIterations; i++) {
            runner.enqueue(new byte[0]);
        }

        runner.setThreadCount(4);
        runner.run(2 * numIterations);

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        System.out.println(new String(out.toByteArray()));
        out.assertContentEquals("\"name\",\"points\"\n\"Tom\",\"49\"\n");
    }

    @Test
    public void testParseFailure() throws InitializationException, IOException, SQLException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("age", RecordFieldType.INT);
        parser.addRecord("Tom", 49);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        final TestRunner runner = TestRunners.newTestRunner(QueryFlowFile.class);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, age from FLOWFILE WHERE name <> ''");
        runner.setProperty(QueryFlowFile.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryFlowFile.RECORD_WRITER_FACTORY, "writer");

        final int numIterations = 1;
        for (int i = 0; i < numIterations; i++) {
            runner.enqueue(new byte[0]);
        }

        runner.setThreadCount(4);
        runner.run(2 * numIterations);

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_NAME).get(0);
        System.out.println(new String(out.toByteArray()));
        out.assertContentEquals("\"name\",\"points\"\n\"Tom\",\"49\"\n");
    }


    @Test
    public void testTransformCalc() throws InitializationException, IOException, SQLException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("ID", RecordFieldType.INT);
        parser.addSchemaField("AMOUNT1", RecordFieldType.FLOAT);
        parser.addSchemaField("AMOUNT2", RecordFieldType.FLOAT);
        parser.addSchemaField("AMOUNT3", RecordFieldType.FLOAT);

        parser.addRecord("008", 10.05F, 15.45F, 89.99F);
        parser.addRecord("100", 20.25F, 25.25F, 45.25F);
        parser.addRecord("105", 20.05F, 25.05F, 45.05F);
        parser.addRecord("200", 34.05F, 25.05F, 75.05F);

        final MockRecordWriter writer = new MockRecordWriter("\"NAME\",\"POINTS\"");

        final TestRunner runner = TestRunners.newTestRunner(QueryFlowFile.class);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select ID, AMOUNT1+AMOUNT2+AMOUNT3 as TOTAL from FLOWFILE where ID=100");
        runner.setProperty(QueryFlowFile.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryFlowFile.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(REL_NAME).get(0);

        out.assertContentEquals("\"NAME\",\"POINTS\"\n\"100\",\"90.75\"\n");
    }


    @Test
    public void testAggregateFunction() throws InitializationException, IOException {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("points", RecordFieldType.INT);
        parser.addRecord("Tom", 1);
        parser.addRecord("Jerry", 2);
        parser.addRecord("Tom", 99);

        final MockRecordWriter writer = new MockRecordWriter("\"name\",\"points\"");

        final TestRunner runner = TestRunners.newTestRunner(QueryFlowFile.class);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, sum(points) as points from FLOWFILE GROUP BY name");
        runner.setProperty(QueryFlowFile.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryFlowFile.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(REL_NAME, 1);
        final MockFlowFile flowFileOut = runner.getFlowFilesForRelationship(ExecuteProcess.REL_SUCCESS).get(0);
        flowFileOut.assertContentEquals("\"name\",\"points\"\n\"Tom\",\"100\"\n\"Jerry\",\"2\"\n");
    }

    @Test
    public void testColumnNames() throws InitializationException, IOException {
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

        final TestRunner runner = TestRunners.newTestRunner(QueryFlowFile.class);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select *, greeting AS FAV_GREETING from FLOWFILE");
        runner.setProperty(QueryFlowFile.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryFlowFile.RECORD_WRITER_FACTORY, "writer");

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(REL_NAME, 1);
    }


    private static class ResultSetValidatingRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        private final List<String> columnNames;

        public ResultSetValidatingRecordWriter(final List<String> colNames) {
            this.columnNames = new ArrayList<>(colNames);
        }

        @Override
        public RecordSetWriter createWriter(ComponentLog logger) {
            return new RecordSetWriter() {
                @Override
                public WriteResult write(final RecordSet rs, final OutputStream out) throws IOException {
                    final int colCount = rs.getSchema().getFieldCount();
                    Assert.assertEquals(columnNames.size(), colCount);

                    final List<String> colNames = new ArrayList<>(colCount);
                    for (int i = 0; i < colCount; i++) {
                        colNames.add(rs.getSchema().getField(i).getFieldName());
                    }

                    Assert.assertEquals(columnNames, colNames);

                    return WriteResult.of(0, Collections.emptyMap());
                }

                @Override
                public String getMimeType() {
                    return "text/plain";
                }

                @Override
                public WriteResult write(Record record, OutputStream out) throws IOException {
                    return null;
                }
            };
        }

    }

    private static class MockRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        private final String header;

        public MockRecordWriter(final String header) {
            this.header = header;
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger) {
            return new RecordSetWriter() {
                @Override
                public WriteResult write(final RecordSet rs, final OutputStream out) throws IOException {
                    out.write(header.getBytes());
                    out.write("\n".getBytes());

                    int recordCount = 0;
                    final int numCols = rs.getSchema().getFieldCount();
                    Record record = null;
                    while ((record = rs.next()) != null) {
                        recordCount++;
                        int i = 0;
                        for (final String fieldName : record.getSchema().getFieldNames()) {
                            final String val = record.getAsString(fieldName);
                            out.write("\"".getBytes());
                            out.write(val.getBytes());
                            out.write("\"".getBytes());

                            if (i++ < numCols - 1) {
                                out.write(",".getBytes());
                            }
                        }
                        out.write("\n".getBytes());
                    }

                    return WriteResult.of(recordCount, Collections.emptyMap());
                }

                @Override
                public String getMimeType() {
                    return "text/plain";
                }

                @Override
                public WriteResult write(Record record, OutputStream out) throws IOException {
                    return null;
                }
            };
        }
    }

    private static class MockRecordParser extends AbstractControllerService implements RowRecordReaderFactory {
        private final List<Object[]> records = new ArrayList<>();
        private final List<RecordField> fields = new ArrayList<>();
        private final int failAfterN;

        public MockRecordParser() {
            this(-1);
        }

        public MockRecordParser(final int failAfterN) {
            this.failAfterN = failAfterN;
        }


        public void addSchemaField(final String fieldName, final RecordFieldType type) {
            fields.add(new RecordField(fieldName, type.getDataType()));
        }

        public void addRecord(Object... values) {
            records.add(values);
        }

        @Override
        public RecordReader createRecordReader(InputStream in, ComponentLog logger) throws IOException {
            final Iterator<Object[]> itr = records.iterator();

            return new RecordReader() {
                private int recordCount = 0;

                @Override
                public void close() throws IOException {
                }

                @Override
                public Record nextRecord() throws IOException, MalformedRecordException {
                    if (failAfterN >= recordCount) {
                        throw new MalformedRecordException("Intentional Unit Test Exception because " + recordCount + " records have been read");
                    }
                    recordCount++;

                    if (!itr.hasNext()) {
                        return null;
                    }

                    final Object[] values = itr.next();
                    final Map<String, Object> valueMap = new HashMap<>();
                    int i = 0;
                    for (final RecordField field : fields) {
                        final String fieldName = field.getFieldName();
                        valueMap.put(fieldName, values[i++]);
                    }

                    return new MapRecord(new SimpleRecordSchema(fields), valueMap);
                }

                @Override
                public RecordSchema getSchema() {
                    return new SimpleRecordSchema(fields);
                }
            };
        }
    }
}
