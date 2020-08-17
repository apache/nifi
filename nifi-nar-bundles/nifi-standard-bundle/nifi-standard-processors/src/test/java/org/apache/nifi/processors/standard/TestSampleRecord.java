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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSampleRecord {
    @Test
    public void testIntervalSampling() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SampleRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SampleRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(SampleRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(SampleRecord.SAMPLING_STRATEGY, SampleRecord.INTERVAL_SAMPLING_KEY);
        runner.assertNotValid();
        runner.setProperty(SampleRecord.SAMPLING_INTERVAL, "4");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        for (int i = 0; i < 100; i++) {
            readerService.addRecord(UUID.randomUUID(), 48);
        }
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "25");

        runner.clearTransferState();
        runner.setProperty(SampleRecord.SAMPLING_INTERVAL, "1");
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        out = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "100");
    }

    @Test
    public void testIntervalSamplingWithNoRecords() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SampleRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SampleRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(SampleRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(SampleRecord.SAMPLING_STRATEGY, SampleRecord.INTERVAL_SAMPLING_KEY);
        runner.assertNotValid();
        runner.setProperty(SampleRecord.SAMPLING_INTERVAL, "0");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        for (int i = 0; i < 100; i++) {
            readerService.addRecord(UUID.randomUUID(), 48);
        }
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);

        out.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testProbabilisticSamplingWithSeed() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SampleRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SampleRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(SampleRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(SampleRecord.SAMPLING_STRATEGY, SampleRecord.PROBABILISTIC_SAMPLING_KEY);
        runner.assertNotValid();
        runner.setProperty(SampleRecord.SAMPLING_PROBABILITY, "10");
        runner.setProperty(SampleRecord.RANDOM_SEED, "1");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        Random randomAge = new Random();
        for (int i = 0; i < 100; i++) {
            readerService.addRecord(UUID.randomUUID(), randomAge.nextInt());
        }
        runner.enqueue("");
        runner.run();
        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        final MockFlowFile first = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);
        String firstContent = new String(runner.getContentAsByteArray(first));

        runner.clearTransferState();
        runner.enqueue("");
        runner.run();
        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        final MockFlowFile second = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);
        String secondContent = new String(runner.getContentAsByteArray(second));

        assertEquals(firstContent, secondContent);
    }

    @Test
    public void testProbabilisticSamplingWithAllRecords() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SampleRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SampleRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(SampleRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(SampleRecord.SAMPLING_STRATEGY, SampleRecord.PROBABILISTIC_SAMPLING_KEY);
        runner.assertNotValid();
        runner.setProperty(SampleRecord.SAMPLING_PROBABILITY, "100");

        RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType());
        RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType());

        readerService.addSchemaField(nameField);
        readerService.addSchemaField(ageField);
        List<RecordField> recordFieldList = Arrays.asList(nameField, ageField);

        RecordSchema recordSchema = new SimpleRecordSchema(recordFieldList);

        Random randomAge = new Random();
        final List<Record> records = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            HashMap<String, Object> recordMap = new HashMap<>();
            Object name = UUID.randomUUID();
            Object age = randomAge.nextInt();
            recordMap.put("name", name);
            recordMap.put("age", age);
            records.add(new MapRecord(recordSchema, recordMap));
            readerService.addRecord(name, age);
        }
        runner.enqueue("");
        runner.run();
        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        final MockFlowFile first = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);
        String[] content = new String(runner.getContentAsByteArray(first)).split(".*\n");
        for (int i = 0; i < content.length; i++) {
            assertEquals(content[i], records.get(i).toString());
        }
    }

    @Test
    public void testProbabilisticSamplingWithNoRecords() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SampleRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SampleRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(SampleRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(SampleRecord.SAMPLING_STRATEGY, SampleRecord.PROBABILISTIC_SAMPLING_KEY);
        runner.assertNotValid();
        runner.setProperty(SampleRecord.SAMPLING_PROBABILITY, "0");

        RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType());
        RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType());

        readerService.addSchemaField(nameField);
        readerService.addSchemaField(ageField);

        Random randomAge = new Random();
        for (int i = 0; i < 100; i++) {
            readerService.addRecord(UUID.randomUUID(), randomAge.nextInt());
        }
        runner.enqueue("");
        runner.run();
        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        final MockFlowFile first = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);
        String content = new String(runner.getContentAsByteArray(first));
        assertTrue(content.isEmpty());
    }

    @Test
    public void testReservoirSampling() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SampleRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SampleRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(SampleRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(SampleRecord.SAMPLING_STRATEGY, SampleRecord.RESERVOIR_SAMPLING_KEY);
        runner.assertNotValid();
        runner.setProperty(SampleRecord.RESERVOIR_SIZE, "10");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        for (int i = 0; i < 100; i++) {
            readerService.addRecord(UUID.randomUUID(), 48);
        }
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);

        out.assertAttributeEquals("record.count", "10");
    }

    @Test
    public void testReservoirSamplingWithSeed() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SampleRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SampleRecord.RECORD_READER_FACTORY, "reader");
        runner.setProperty(SampleRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(SampleRecord.SAMPLING_STRATEGY, SampleRecord.RESERVOIR_SAMPLING_KEY);
        runner.setProperty(SampleRecord.RANDOM_SEED, "1");
        runner.assertNotValid();
        runner.setProperty(SampleRecord.RESERVOIR_SIZE, "10");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        for (int i = 0; i < 100; i++) {
            readerService.addRecord(UUID.randomUUID(), 48);
        }
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        final MockFlowFile first = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);
        String firstContent = new String(runner.getContentAsByteArray(first));
        first.assertAttributeEquals("record.count", "10");

        runner.clearTransferState();
        runner.enqueue("");
        runner.run();
        runner.assertTransferCount(SampleRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SampleRecord.REL_ORIGINAL, 1);
        final MockFlowFile second = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);
        String secondContent = new String(runner.getContentAsByteArray(second));

        assertEquals(firstContent, secondContent);
    }
}
