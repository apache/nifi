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
import java.nio.file.Paths;

import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestConvertRecord {

    @Test
    public void testSuccessfulConversion() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConvertRecord.RECORD_READER, "reader");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "writer");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("header\nJohn Doe,48\nJane Doe,47\nJimmy Doe,14\n");
    }

    @Test
    public void testDropEmpty() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConvertRecord.INCLUDE_ZERO_RECORD_FLOWFILES, "false");
        runner.setProperty(ConvertRecord.RECORD_READER, "reader");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "writer");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 0);
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_FAILURE, 0);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("header\nJohn Doe,48\nJane Doe,47\nJimmy Doe,14\n");
    }

    @Test
    public void testReadFailure() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser(2);
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConvertRecord.RECORD_READER, "reader");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "writer");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        final MockFlowFile original = runner.enqueue("hello");
        runner.run();

        // Original FlowFile should be routed to 'failure' relationship without modification
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertRecord.REL_FAILURE).get(0);
        assertTrue(original == out);
    }


    @Test
    public void testWriteFailure() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false, 2);

        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConvertRecord.RECORD_READER, "reader");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "writer");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        final MockFlowFile original = runner.enqueue("hello");
        runner.run();

        // Original FlowFile should be routed to 'failure' relationship without modification
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertRecord.REL_FAILURE).get(0);
        assertTrue(original == out);
    }

    @Test
    public void testUpdateCSVtoAvroWithNonAvroCompatibleColumnNames() throws InitializationException, IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        runner.setProperty(ConvertRecord.RECORD_READER, "reader");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "writer");

        // reader
        final CSVReader reader = new CSVReader();
        runner.addControllerService("reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "csv-header-derived");
        runner.setProperty(reader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        runner.setProperty(reader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        runner.setProperty(reader, CSVUtils.TRAILING_DELIMITER, "false");
        runner.enableControllerService(reader);

        // writer
        final AvroRecordSetWriter writer = new AvroRecordSetWriter();
        runner.addControllerService("writer", writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(writer, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(writer);

        // test
        runner.enqueue(Paths.get("src/test/resources/TestConvertRecord/dummy-csv-data.csv"));
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        String schema = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).getAttribute("avro.schema");
        assertTrue(schema.contains("First_Name"));
        assertTrue(schema.contains("_1"));
    }

}
