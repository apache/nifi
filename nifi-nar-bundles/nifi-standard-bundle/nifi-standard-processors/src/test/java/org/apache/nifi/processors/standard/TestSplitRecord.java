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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSplitRecord {

    @Test
    public void testIndividualRecordPerSplit() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SplitRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SplitRecord.RECORD_READER, "reader");
        runner.setProperty(SplitRecord.RECORD_WRITER, "writer");
        runner.setProperty(SplitRecord.RECORDS_PER_SPLIT, "1");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        final MockFlowFile inputFlowFile = runner.enqueue("");
        runner.run();

        runner.assertTransferCount(SplitRecord.REL_SPLITS, 3);
        runner.assertTransferCount(SplitRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitRecord.REL_FAILURE, 0);
        final List<MockFlowFile> out = runner.getFlowFilesForRelationship(SplitRecord.REL_SPLITS);

        int fragmentIndex = 0;
        String fragmentUUID = null;
        for (final MockFlowFile mff : out) {
            if (fragmentUUID == null) {
                fragmentUUID = mff.getAttribute(SplitRecord.FRAGMENT_ID);
            } else {
                mff.assertAttributeEquals(SplitRecord.FRAGMENT_ID, fragmentUUID);
            }
            mff.assertAttributeEquals(SplitRecord.FRAGMENT_COUNT, "3");
            mff.assertAttributeEquals(SplitRecord.FRAGMENT_INDEX, String.valueOf(fragmentIndex));
            mff.assertAttributeEquals(SplitRecord.SEGMENT_ORIGINAL_FILENAME, inputFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
            mff.assertAttributeEquals("record.count", "1");
            mff.assertAttributeEquals("mime.type", "text/plain");
            fragmentIndex++;
        }


        assertEquals(1, out.stream().filter(mff -> mff.isContentEqual("header\nJohn Doe,48\n")).count());
        assertEquals(1, out.stream().filter(mff -> mff.isContentEqual("header\nJane Doe,47\n")).count());
        assertEquals(1, out.stream().filter(mff -> mff.isContentEqual("header\nJimmy Doe,14\n")).count());
    }

    @Test
    public void testMultipleRecordsPerSplit() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SplitRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SplitRecord.RECORD_READER, "reader");
        runner.setProperty(SplitRecord.RECORD_WRITER, "writer");
        runner.setProperty(SplitRecord.RECORDS_PER_SPLIT, "2");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(SplitRecord.REL_SPLITS, 2);
        runner.assertTransferCount(SplitRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitRecord.REL_FAILURE, 0);
        final List<MockFlowFile> out = runner.getFlowFilesForRelationship(SplitRecord.REL_SPLITS);

        assertEquals(1, out.stream().filter(mff -> mff.getAttribute("record.count").equals("1")).count());
        assertTrue(out.stream().allMatch(mff -> mff.getAttribute("mime.type").equals("text/plain")));

        assertEquals(1, out.stream().filter(mff -> mff.isContentEqual("header\nJohn Doe,48\nJane Doe,47\n")).count());
        assertEquals(1, out.stream().filter(mff -> mff.isContentEqual("header\nJimmy Doe,14\n")).count());
    }

    @Test
    public void testAllSplitsOneDesintation() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SplitRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SplitRecord.RECORD_READER, "reader");
        runner.setProperty(SplitRecord.RECORD_WRITER, "writer");
        runner.setProperty(SplitRecord.RECORDS_PER_SPLIT, "3");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(SplitRecord.REL_SPLITS, 1);
        runner.assertTransferCount(SplitRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitRecord.REL_FAILURE, 0);
        final MockFlowFile out = runner.getFlowFilesForRelationship(SplitRecord.REL_SPLITS).get(0);

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");

        out.assertContentEquals("header\nJohn Doe,48\nJane Doe,47\nJimmy Doe,14\n");
    }


    @Test
    public void testReadFailure() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser(2);
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        final TestRunner runner = TestRunners.newTestRunner(SplitRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(SplitRecord.RECORD_READER, "reader");
        runner.setProperty(SplitRecord.RECORD_WRITER, "writer");
        runner.setProperty(SplitRecord.RECORDS_PER_SPLIT, "1");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        final MockFlowFile original = runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(SplitRecord.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(SplitRecord.REL_FAILURE).get(0);
        assertTrue(original == failed);
    }

}
