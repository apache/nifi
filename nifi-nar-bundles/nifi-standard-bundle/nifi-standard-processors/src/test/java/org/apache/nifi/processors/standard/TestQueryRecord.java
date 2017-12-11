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
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
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
import org.junit.Assert;
import org.junit.Test;

public class TestQueryRecord {

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

        final TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);
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

        final TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);
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

        final TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);
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

        final TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);
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

        final TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(REL_NAME, "select name, sum(points) as points from FLOWFILE GROUP BY name");
        runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");

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

        final TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);
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
    }


    private static class ResultSetValidatingRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        private final List<String> columnNames;

        public ResultSetValidatingRecordWriter(final List<String> colNames) {
            this.columnNames = new ArrayList<>(colNames);
        }

        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
            final List<RecordField> recordFields = columnNames.stream()
                .map(name -> new RecordField(name, RecordFieldType.STRING.getDataType()))
                .collect(Collectors.toList());
            return new SimpleRecordSchema(recordFields);
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) {
            return new RecordSetWriter() {

                @Override
                public void flush() throws IOException {
                    out.flush();
                }

                @Override
                public WriteResult write(final RecordSet rs) throws IOException {
                    final int colCount = rs.getSchema().getFieldCount();
                    Assert.assertEquals(columnNames.size(), colCount);

                    final List<String> colNames = new ArrayList<>(colCount);
                    for (int i = 0; i < colCount; i++) {
                        colNames.add(rs.getSchema().getField(i).getFieldName());
                    }

                    Assert.assertEquals(columnNames, colNames);

                    // Iterate over the rest of the records to ensure that we read the entire stream. If we don't
                    // do this, we won't consume all of the data and as a result we will not close the stream properly
                    Record record;
                    while ((record = rs.next()) != null) {
                        System.out.println(record);
                    }

                    return WriteResult.of(0, Collections.emptyMap());
                }

                @Override
                public String getMimeType() {
                    return "text/plain";
                }

                @Override
                public WriteResult write(Record record) throws IOException {
                    return null;
                }

                @Override
                public void close() throws IOException {
                    out.close();
                }

                @Override
                public void beginRecordSet() throws IOException {
                }

                @Override
                public WriteResult finishRecordSet() throws IOException {
                    return WriteResult.EMPTY;
                }
            };
        }

    }

}
