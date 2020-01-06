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


import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPutRecord {

    private TestRunner testRunner;
    private MockRecordParser recordReader;
    private MockRecordSinkService mockRecordSinkService;

    @Before
    public void setup() {
        PutRecord processor = new PutRecord();
        testRunner = TestRunners.newTestRunner(processor);
        recordReader = new MockRecordParser();
        testRunner.setProperty(PutRecord.RECORD_READER, "reader");
        mockRecordSinkService = new MockRecordSinkService();
        testRunner.setProperty(PutRecord.RECORD_SINK, "MockRecordSinkService");
    }

    @Test
    public void testSimplePut() throws Exception {
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);

        testRunner.addControllerService("MockRecordSinkService", mockRecordSinkService);
        testRunner.enableControllerService(mockRecordSinkService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testNoRows() throws Exception {
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);

        testRunner.addControllerService("MockRecordSinkService", mockRecordSinkService);
        testRunner.enableControllerService(mockRecordSinkService);

        testRunner.setProperty(PutRecord.INCLUDE_ZERO_RECORD_RESULTS, "false");

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        testRunner.enqueue("");
        testRunner.run();

        assertTrue(mockRecordSinkService.getRows().isEmpty());
        assertFalse(mockRecordSinkService.isTransmitted());
        // Original flow file is still transferred
        testRunner.assertAllFlowFilesTransferred(PutRecord.REL_SUCCESS, 1);
        testRunner.clearTransferState();

        // Send an empty record set anyway
        testRunner.setProperty(PutRecord.INCLUDE_ZERO_RECORD_RESULTS, "true");
        testRunner.enqueue("");
        testRunner.run();

        assertTrue(mockRecordSinkService.getRows().isEmpty());
        assertTrue(mockRecordSinkService.isTransmitted());
        // Original flow file is still transferred
        testRunner.assertAllFlowFilesTransferred(PutRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testBadRecords() throws Exception {
        recordReader = new MockRecordParser(1);
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);

        testRunner.addControllerService("MockRecordSinkService", mockRecordSinkService);
        testRunner.enableControllerService(mockRecordSinkService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");
        recordReader.addRecord("Jane Doe", 47, "Tennis");
        recordReader.addRecord("Sally Doe", 47, "Curling");
        recordReader.addRecord("Jimmy Doe", 14, null);
        recordReader.addRecord("Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutRecord.REL_FAILURE, 1);
    }

    @Test
    public void testRetryableError() throws Exception {
        recordReader = new MockRecordParser();
        testRunner.addControllerService("reader", recordReader);
        testRunner.enableControllerService(recordReader);

        mockRecordSinkService.setFailWithRetryableError(true);
        testRunner.addControllerService("MockRecordSinkService", mockRecordSinkService);
        testRunner.enableControllerService(mockRecordSinkService);

        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("John Doe", 48, "Soccer");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutRecord.REL_RETRY, 1);
    }
}