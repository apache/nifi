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
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.UUID;

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
        final MockFlowFile out = runner.getFlowFilesForRelationship(SampleRecord.REL_SUCCESS).get(0);

        out.assertAttributeEquals("record.count", "25");
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
}