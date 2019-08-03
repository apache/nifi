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
import org.apache.nifi.serialization.record.CommaSeparatedRecordReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StandardProcessorTestRunner;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMergeRecord {
    private TestRunner runner;
    private CommaSeparatedRecordReader readerService;
    private MockRecordWriter writerService;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(new MergeRecord());

        readerService = new CommaSeparatedRecordReader();
        writerService = new MockRecordWriter("header", false, true);

        runner.addControllerService("reader", readerService);

        runner.enableControllerService(readerService);

        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(MergeRecord.RECORD_READER, "reader");
        runner.setProperty(MergeRecord.RECORD_WRITER, "writer");
        runner.setValidateExpressionUsage(true);
    }

    /**
     * Utility method to avoid side effects after a test
     */
    private void clearRunner() {
        runner.clearTransferState();
        runner.clearProvenanceEvents();
        runner.clearProperties();

        ((StandardProcessorTestRunner) runner).clearQueue();
        ((MergeRecord) runner.getProcessor()).resetState();

        runner.setProperty(MergeRecord.RECORD_READER, "reader");
        runner.setProperty(MergeRecord.RECORD_WRITER, "writer");
    }

    @Test
    public void testSmallOutputIsFlushed() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "1");
        runner.setProperty(MergeRecord.MAX_RECORDS, "1");

        runner.enqueue("Name, Age\nJohn, 35\nJane, 34");

        runner.run(1);
        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED).get(0);
        mff.assertAttributeEquals("record.count", "2");
        mff.assertContentEquals("header\nJohn,35\nJane,34\n");

        runner.getFlowFilesForRelationship(MergeRecord.REL_ORIGINAL).forEach(
            ff -> assertEquals(mff.getAttribute(CoreAttributes.UUID.key()), ff.getAttribute(MergeRecord.MERGE_UUID_ATTRIBUTE)));
    }

    @Test
    public void testMergeSimple() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "2");
        runner.setProperty(MergeRecord.MAX_RECORDS, "2");

        runner.enqueue("Name, Age\nJohn, 35");
        runner.enqueue("Name, Age\nJane, 34");

        runner.run(2);
        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 2);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED).get(0);
        mff.assertAttributeEquals("record.count", "2");
        mff.assertContentEquals("header\nJohn,35\nJane,34\n");

        runner.getFlowFilesForRelationship(MergeRecord.REL_ORIGINAL).forEach(
                ff -> assertEquals(mff.getAttribute(CoreAttributes.UUID.key()), ff.getAttribute(MergeRecord.MERGE_UUID_ATTRIBUTE)));
    }


    // Verify that FlowFiles are grouped with like schemas.
    @Test
    public void testDifferentSchema() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "2");
        runner.setProperty(MergeRecord.MAX_RECORDS, "2");

        runner.enqueue("Name, Age\nJohn, 35");
        runner.enqueue("Name, Color\nJane, Red");

        runner.run(2, false, true);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);

        runner.enqueue("Name, Age\nJane, 34");
        runner.enqueue("Name, Color\nJohn, Blue");

        runner.run(2, true, false);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 2);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 4);

        final List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED);
        assertEquals(1L, mffs.stream()
            .filter(ff -> "2".equals(ff.getAttribute("record.count")))
            .filter(ff -> "header\nJohn,35\nJane,34\n".equals(new String(ff.toByteArray())))
            .count());

        assertEquals(1L, mffs.stream()
            .filter(ff -> "2".equals(ff.getAttribute("record.count")))
            .filter(ff -> "header\nJane,Red\nJohn,Blue\n".equals(new String(ff.toByteArray())))
            .count());
    }

    @Test
    public void testFailureToParse() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "2");
        runner.setProperty(MergeRecord.MAX_RECORDS, "3");

        readerService.failAfter(2);

        runner.enqueue("Name, Age\nJohn, 35");
        runner.enqueue("Name, Age\nJane, 34");
        runner.enqueue("Name, Age\nJake, 3");

        runner.run();

        // We have to route all of the FlowFiles in the same bin to the 'failure' relationship.
        // Otherwise, we may have read some of the records from the failing FlowFile and then
        // routed it to failure, which would result in some of its records moving on and others not.
        // This, in turn, would result in the same records being added to potentially many FlowFiles.
        runner.assertAllFlowFilesTransferred(MergeRecord.REL_FAILURE, 3);
    }

    @Test
    public void testDefragment() {
        runner.setProperty(MergeRecord.MERGE_STRATEGY, MergeRecord.MERGE_STRATEGY_DEFRAGMENT);

        final Map<String, String> attr1 = new HashMap<>();
        attr1.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr1.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "1");
        attr1.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "0");

        final Map<String, String> attr2 = new HashMap<>();
        attr2.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr2.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "1");
        attr2.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "1");

        final Map<String, String> attr3 = new HashMap<>();
        attr3.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr3.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "2");
        attr3.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "0");

        final Map<String, String> attr4 = new HashMap<>();
        attr4.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr4.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "3");
        attr4.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "0");

        final Map<String, String> attr5 = new HashMap<>();
        attr5.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr5.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "3");
        attr5.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("Name, Age\nJohn, 35", attr1);
        runner.enqueue("Name, Age\nJane, 34", attr2);

        runner.enqueue("Name, Age\nJay, 24", attr3);

        runner.enqueue("Name, Age\nJake, 3", attr4);
        runner.enqueue("Name, Age\nJan, 2", attr5);

        runner.run(1);

        assertEquals("Fragment id=2 should remain in the incoming connection", 1, runner.getQueueSize().getObjectCount());
        runner.assertTransferCount(MergeRecord.REL_MERGED, 2);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 4);

        final List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED);
        assertEquals(1L, mffs.stream()
            .filter(ff -> "2".equals(ff.getAttribute("record.count")))
            .filter(ff -> "header\nJohn,35\nJane,34\n".equals(new String(ff.toByteArray())))
            .count());

        assertEquals(1L, mffs.stream()
            .filter(ff -> "2".equals(ff.getAttribute("record.count")))
            .filter(ff -> "header\nJake,3\nJan,2\n".equals(new String(ff.toByteArray())))
            .count());
    }

    @Test
    public void testDefragmentOverMultipleCalls() {
        runner.setProperty(MergeRecord.MERGE_STRATEGY, MergeRecord.MERGE_STRATEGY_DEFRAGMENT);

        final Map<String, String> attr1 = new HashMap<>();
        attr1.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr1.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "1");
        attr1.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "0");

        runner.enqueue("Name, Age\nJohn, 35", attr1);
        runner.run(2);

        assertEquals("Fragment should remain in the incoming connection", 1, runner.getQueueSize().getObjectCount());
        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 0);

        final Map<String, String> attr2 = new HashMap<>();
        attr2.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr2.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "1");
        attr2.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("Name, Age\nJane, 34", attr2);
        runner.run(1);

        assertEquals("Fragments should merge", 0, runner.getQueueSize().getObjectCount());
        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 2);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 0);

        final List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED);
        assertEquals(1L, mffs.stream()
                .filter(ff -> "2".equals(ff.getAttribute("record.count")))
                .filter(ff -> "header\nJohn,35\nJane,34\n".equals(new String(ff.toByteArray())))
                .count());
    }

    @Test
    public void testDefragmentWithMultipleRecords() {
        runner.setProperty(MergeRecord.MERGE_STRATEGY, MergeRecord.MERGE_STRATEGY_DEFRAGMENT);

        final Map<String, String> attr1 = new HashMap<>();
        attr1.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr1.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "1");
        attr1.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "0");
        attr1.put("record.count", "2");

        final Map<String, String> attr2 = new HashMap<>();
        attr2.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr2.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "1");
        attr2.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "1");
        attr2.put("record.count", "2");

        final Map<String, String> attr3 = new HashMap<>();
        attr3.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "2");
        attr3.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "2");
        attr3.put(MergeRecord.FRAGMENT_INDEX_ATTRIBUTE, "0");
        attr3.put("record.count", "2");

        runner.enqueue("Name, Age\nJohn, 35\nJane, 34", attr1);

        runner.enqueue("Name, Age\nJake, 3\nJan, 2", attr2);

        runner.enqueue("Name, Age\nJay, 24\nJade, 28", attr3);

        runner.run(1);

        assertEquals("Fragment id=2 should remain in the incoming connection", 1, runner.getQueueSize().getObjectCount());
        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 2);

        final List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED);
        assertEquals(1L, mffs.stream()
            .filter(ff -> "4".equals(ff.getAttribute("record.count")))
            .filter(ff -> "header\nJohn,35\nJane,34\nJake,3\nJan,2\n".equals(new String(ff.toByteArray())))
            .count());

    }


    @Test
    public void testMinSize() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "2");
        runner.setProperty(MergeRecord.MAX_RECORDS, "2");
        runner.setProperty(MergeRecord.MIN_SIZE, "500 B");

        runner.enqueue("Name, Age\nJohn, 35");
        runner.enqueue("Name, Age\nJane, 34");

        runner.run();
        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);

        final StringBuilder sb = new StringBuilder("Name, Age\n");
        for (int i = 0; i < 100; i++) {
            sb.append("Person " + i + ", " + i + "\n");
        }
        runner.enqueue(sb.toString());

        runner.run();

        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 3);

        clearRunner();

        runner.setVariable("min_size", "1 KB");
        runner.removeProperty(MergeRecord.MIN_RECORDS);
        runner.setProperty(MergeRecord.MIN_SIZE, "${min_size}");
        runner.setProperty(MergeRecord.MAX_RECORDS, "2");

        runner.enqueue("Name, Age\nJohn, 35"); // About 20 Bytes
        runner.enqueue("Name, Age\nJane, 34");

        runner.run();
        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);

        runner.enqueue(sb.toString()); // About 1300 Bytes

        runner.run();

        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 3);
    }

    @Test
    public void testValidation() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "103");
        runner.setProperty(MergeRecord.MAX_RECORDS, "2");
        runner.setProperty(MergeRecord.MIN_SIZE, "500 B");
        runner.assertNotValid();

        runner.setProperty(MergeRecord.MIN_RECORDS, "2");
        runner.setProperty(MergeRecord.MAX_RECORDS, "103");
        runner.assertValid();

        runner.setVariable("min_records", "-3");
        runner.setVariable("max_records", "-1");

        // This configuration breaks the "<Minimum Number of Records> property cannot be negative or zero" rule
        runner.setProperty(MergeRecord.MIN_RECORDS, "${min_records}");
        runner.setProperty(MergeRecord.MAX_RECORDS, "3");
        runner.assertNotValid();

        // This configuration breaks the "<Minimum Number of Records> property cannot be negative or zero" and the
        // "<Maximum Number of Records> property cannot be negative or zero" rules
        runner.setProperty(MergeRecord.MIN_RECORDS, "${min_records}");
        runner.setProperty(MergeRecord.MAX_RECORDS, "${max_records}");
        runner.assertNotValid();

        // This configuration breaks the "<Maximum Number of Records> property cannot be smaller than <Minimum Number of Records> property"
        // and the "<Maximum Number of Records> property cannot be negative or zero" rules
        runner.setProperty(MergeRecord.MIN_RECORDS, "3");
        runner.setProperty(MergeRecord.MAX_RECORDS, "${max_records}");
        runner.assertNotValid();

        // This configuration breaks the "<Maximum Number of Records> property cannot be smaller than <Minimum Number of Records> property"
        // and the "<Maximum Number of Records> property cannot be negative or zero" rules
        runner.removeProperty(MergeRecord.MIN_RECORDS); // Will use the default value of 1
        runner.setProperty(MergeRecord.MAX_RECORDS, "${max_records}");
        runner.assertNotValid();

        // This configuration breaks the "<Maximum Number of Records> property cannot be smaller than <Minimum Number of Records> property",
        // the "<Minimum Number of Records> property cannot be negative or zero" and the "<Maximum Number of Records>
        // property cannot be negative or zero" rules
        runner.setVariable("min_records", "-1");
        runner.setVariable("max_records", "-3");
        runner.setProperty(MergeRecord.MIN_RECORDS, "${min_records}");
        runner.setProperty(MergeRecord.MAX_RECORDS, "${max_records}");
        runner.assertNotValid();

        // This configuration is valid
        runner.setVariable("min_records", "1");
        runner.setVariable("max_records", "5");
        runner.setProperty(MergeRecord.MIN_RECORDS, "${min_records}");
        runner.setProperty(MergeRecord.MAX_RECORDS, "${max_records}");
        runner.assertValid();

        runner.removeProperty(MergeRecord.MIN_RECORDS);
        runner.removeProperty(MergeRecord.MAX_RECORDS);

        // This configuration is valid
        runner.setVariable("min_size", "0 B");
        runner.setVariable("max_size", "100 B");
        runner.setProperty(MergeRecord.MIN_SIZE, "${min_size}");
        runner.setProperty(MergeRecord.MAX_SIZE, "${max_size}");

        // This configuration breaks the "<Maximum Bin Size> property cannot be smaller than <Minimum Bin Size> property"
        runner.setVariable("min_size", "1000 B");
        runner.setVariable("max_size", "100 B");
        runner.setProperty(MergeRecord.MIN_SIZE, "${min_size}");
        runner.setProperty(MergeRecord.MAX_SIZE, "${max_size}");
        runner.assertNotValid();

        // This configuration breaks the "<Maximum Bin Size> property cannot be smaller than <Minimum Bin Size> property"
        runner.setVariable("min_size", "10 MB");
        runner.setVariable("max_size", "100 KB");
        runner.setProperty(MergeRecord.MIN_SIZE, "${min_size}");
        runner.setProperty(MergeRecord.MAX_SIZE, "${max_size}");
        runner.assertNotValid();

        // This configuration breaks the "<Minimum Bin Size> Must be of format <Data Size> <Data Unit> where <Data Size>
        // is a non-negative integer and <Data Unit> is a supported Data Unit, such as: B, KB, MB, GB, TB"
        runner.setVariable("min_size", "50 L");
        runner.setProperty(MergeRecord.MIN_SIZE, "${min_size}");
        runner.assertNotValid();

        // This configuration breaks the "<Maximum Bin Size> Must be of format <Data Size> <Data Unit> where <Data Size>
        // is a non-negative integer and <Data Unit> is a supported Data Unit, such as: B, KB, MB, GB, TB" rule
        runner.setVariable("min_size", "50 B");
        runner.setVariable("max_size", "100 L");
        runner.setProperty(MergeRecord.MIN_SIZE, "${min_size}");
        runner.setProperty(MergeRecord.MAX_SIZE, "${max_size}");
        runner.assertNotValid();

        // This configuration breaks the "<Maximum Bin Size> Must be of format <Data Size> <Data Unit> where <Data Size>
        // is a non-negative integer and <Data Unit> is a supported Data Unit, such as: B, KB, MB, GB, TB" and the
        // "<Maximum Bin Size> Must be of format <Data Size> <Data Unit> where <Data Size> is a non-negative integer and
        // <Data Unit> is a supported Data Unit, such as: B, KB, MB, GB, TB" rules
        runner.setVariable("min_size", "50 F");
        runner.setVariable("max_size", "100 L");
        runner.setProperty(MergeRecord.MIN_SIZE, "${min_size}");
        runner.setProperty(MergeRecord.MAX_SIZE, "${max_size}");
        runner.assertNotValid();

        runner.removeProperty(MergeRecord.MIN_SIZE);
        runner.removeProperty(MergeRecord.MAX_SIZE);

        // This configuration is valid
        runner.setVariable("max_bin_age", "50 s");
        runner.setProperty(MergeRecord.MAX_BIN_AGE, "${max_bin_age}");
        runner.assertValid();

        // This configuration breaks the "<Maximum Bin Size> property must be of format <duration> <TimeUnit> where <duration>
        // is a non-negative integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days" rule
        runner.setVariable("max_bin_age", "50 B");
        runner.setProperty(MergeRecord.MAX_BIN_AGE, "${max_bin_age}");
        runner.assertNotValid();

        // This configuration breaks the "<Maximum Bin Size> property must be of format <duration> <TimeUnit> where <duration>
        // is a non-negative integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days" rule
        runner.setVariable("max_bin_age", "-50 s");
        runner.setProperty(MergeRecord.MAX_BIN_AGE, "${max_bin_age}");
        runner.assertNotValid();

        // This configuration breaks the "<Maximum Bin Size> property must be of format <duration> <TimeUnit> where <duration>
        // is a non-negative integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days" rule
        runner.setVariable("max_bin_age", "-30");
        runner.setProperty(MergeRecord.MAX_BIN_AGE, "${max_bin_age}");
        runner.assertNotValid();

        runner.removeProperty(MergeRecord.MAX_BIN_AGE);

        // This configuration is valid
        runner.setVariable("max_bin_count", "30");
        runner.setProperty(MergeRecord.MAX_BIN_COUNT, "${max_bin_count}");
        runner.assertValid();

        // This configuration is breaks the "because <Max Bin Count> property cannot be negative or zero" rule
        runner.setVariable("max_bin_count", "0");
        runner.setProperty(MergeRecord.MAX_BIN_COUNT, "${max_bin_count}");
        runner.assertNotValid();

        runner.removeProperty(MergeRecord.MAX_BIN_COUNT);
    }

    @Test
    public void testMinRecords() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "103");
        runner.setProperty(MergeRecord.MAX_RECORDS, "110");
        runner.setProperty(MergeRecord.MIN_SIZE, "500 B");

        runner.enqueue("Name, Age\nJohn, 35");
        runner.enqueue("Name, Age\nJane, 34");

        final StringBuilder sb = new StringBuilder("Name, Age\n");
        for (int i = 0; i < 100; i++) {
            sb.append("Person " + i + ", " + i + "\n");
        }
        runner.enqueue(sb.toString());

        runner.run();
        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);

        runner.enqueue("Name, Age\nJohn, 35");
        runner.run(2);
        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 4);

        clearRunner();

        runner.setValidateExpressionUsage(true);
        runner.setVariable("min_records", "3");
        runner.setProperty(MergeRecord.MIN_RECORDS, "${min_records}");
        runner.setProperty(MergeRecord.MAX_RECORDS, "3");
        runner.removeProperty(MergeRecord.MIN_SIZE);

        runner.enqueue("Name, Age\nJohn, 35");
        runner.enqueue("Name, Age\nJane, 34");
        runner.enqueue("Name, Age\nAlex, 28");

        runner.run(1);
        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 3);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED).get(0);
        mff.assertAttributeEquals("record.count", "3");
        mff.assertContentEquals("header\nJohn,35\nJane,34\nAlex,28\n");
        runner.removeProperty(MergeRecord.MIN_RECORDS);
        runner.removeProperty(MergeRecord.MAX_RECORDS);
    }

    @Test
    public void testMaxRecords() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "5");
        runner.setProperty(MergeRecord.MAX_RECORDS, "10");

        for (int i = 0; i < 34; i++) {
            runner.enqueue("Name, Age\nJohn, 35");
        }

        runner.run();
        runner.assertTransferCount(MergeRecord.REL_MERGED, 3);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 30);

        assertEquals(4, runner.getQueueSize().getObjectCount());

        runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED).stream().forEach(ff -> ff.assertAttributeEquals("record.count", "10"));
        clearRunner();

        runner.setValidateExpressionUsage(true);
        runner.setVariable("max_records", "3");
        runner.setProperty(MergeRecord.MIN_RECORDS, "1");
        runner.setProperty(MergeRecord.MAX_RECORDS, "${max_records}");
        runner.enqueue("Name, Age\nJohn, 35");
        runner.enqueue("Name, Age\nJane, 34");
        runner.enqueue("Name, Age\nAlex, 28");
        runner.enqueue("Name, Age\nDonna, 48");
        runner.enqueue("Name, Age\nJoey, 45");

        runner.run(2);
        runner.assertTransferCount(MergeRecord.REL_MERGED, 2);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 5);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED).get(0);
        mff1.assertAttributeEquals("record.count", "3");
        mff1.assertContentEquals("header\nJohn,35\nJane,34\nAlex,28\n");

        final MockFlowFile mff2 = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED).get(1);
        mff2.assertAttributeEquals("record.count", "2");
        mff2.assertContentEquals("header\nDonna,48\nJoey,45\n");
        runner.clearTransferState();

        runner.removeProperty(MergeRecord.MIN_RECORDS);
        runner.removeProperty(MergeRecord.MAX_RECORDS);
    }

    @Test
    public void testMaxSize() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "5");
        runner.setProperty(MergeRecord.MAX_SIZE, "100 B");

        for (int i = 0; i < 36; i++) {
            runner.enqueue("Name, Age\nJohnny, 5");
        }

        runner.run();
        runner.assertTransferCount(MergeRecord.REL_MERGED, 3);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 33);

        assertEquals(3, runner.getQueueSize().getObjectCount());

        clearRunner();
        runner.clearTransferState();
        runner.setVariable("max_size", "50 B");
        runner.removeProperty(MergeRecord.MIN_RECORDS);
        runner.setProperty(MergeRecord.MAX_SIZE, "${max_size}");

        for (int i = 0; i < 10; i++) {
            runner.enqueue("Name, Age\nJohnny, 5");
        }

        runner.run();
        runner.assertTransferCount(MergeRecord.REL_MERGED, 2);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 10);

        assertEquals(0, runner.getQueueSize().getObjectCount());
    }

    @Test
    @Ignore("This unit test depends on timing and could potentially cause problems in an automated build environment. However, it can be useful for manual testing")
    public void testTimeout() throws InterruptedException {
        runner.setVariable("max_bin_age", "500 millis");
        runner.setProperty(MergeRecord.MIN_RECORDS, "500");
        runner.setProperty(MergeRecord.MAX_BIN_AGE, "${max_bin_age}");

        for (int i = 0; i < 100; i++) {
            runner.enqueue("Name, Age\nJohnny, 5");
        }

        runner.run(1, false);
        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);

        Thread.sleep(750);
        runner.run(1, true, false);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 100);
    }


    @Test
    public void testBinCount() {
        runner.setVariable("max_bin_count", "5");
        runner.setProperty(MergeRecord.MIN_RECORDS, "5");
        runner.setProperty(MergeRecord.MAX_RECORDS, "10");
        runner.setProperty(MergeRecord.MAX_BIN_COUNT, "${max_bin_count}");
        runner.setProperty(MergeRecord.CORRELATION_ATTRIBUTE_NAME, "correlationId");

        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            attrs.put("correlationId", String.valueOf(i));
            runner.enqueue("Name, Age\nJohn, 3" + i, attrs);
        }

        runner.run(1, false);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 0);

        attrs.put("correlationId", "5");
        runner.enqueue("Name, Age\nJohn, 35", attrs);
        assertEquals(5, ((MergeRecord) runner.getProcessor()).getBinCount());
        runner.run(1, false, false);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 0);
        assertEquals(5, ((MergeRecord) runner.getProcessor()).getBinCount());
    }

    @Test
    public void testCorrelationId(){

        runner.setVariable("merge_id", "correlationId");
        runner.setProperty(MergeRecord.MAX_RECORDS, "2");
        runner.setProperty(MergeRecord.MIN_RECORDS, "1");
        runner.setProperty(MergeRecord.CORRELATION_ATTRIBUTE_NAME, "${merge_id}");

        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            attrs.put("correlationId", String.valueOf(i));
            runner.enqueue("Name, Age\nJohn, 3" + i, attrs);
            runner.enqueue("Name, Age\nJane, 2" + i, attrs);
        }
        runner.run();

        runner.assertTransferCount(MergeRecord.REL_MERGED, 5);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 10);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 0);

        final List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED);
        final Set<String> ffStrings = mffs.stream().map(ff -> new String(ff.toByteArray())).collect(Collectors.toSet());
        for(int i = 0; i < 5; i++) {
            assertTrue(ffStrings.contains("header\nJohn,3" + i  + "\nJane,2" + i  + "\n"));
        }
    }

    @Test
    public void testDefragmentOldestBinFailsWhenTooManyBins() {
        runner.setProperty(MergeRecord.MIN_RECORDS, "5");
        runner.setProperty(MergeRecord.MAX_RECORDS, "10");
        runner.setProperty(MergeRecord.MAX_BIN_COUNT, "5");
        runner.setProperty(MergeRecord.MERGE_STRATEGY, MergeRecord.MERGE_STRATEGY_DEFRAGMENT);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "5");
        for (int i = 0; i < 5; i++) {
            attrs.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, String.valueOf(i));
            runner.enqueue("Name, Age\nJohn, 3" + i, attrs);
        }

        runner.run(1, false);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 0);

        attrs.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "5");
        runner.enqueue("Name, Age\nJohn, 35", attrs);
        assertEquals(5, ((MergeRecord) runner.getProcessor()).getBinCount());
        runner.run(1, false, false);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 1);
        assertEquals(5, ((MergeRecord) runner.getProcessor()).getBinCount());
    }

    @Test
    public void testDefragmentExpiredBinFailsOnTimeout() throws InterruptedException {
        runner.setProperty(MergeRecord.MAX_BIN_COUNT, "5");
        runner.setProperty(MergeRecord.MERGE_STRATEGY, MergeRecord.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeRecord.MAX_BIN_AGE, "1 millis");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(MergeRecord.FRAGMENT_COUNT_ATTRIBUTE, "5");
        attrs.put(MergeRecord.FRAGMENT_ID_ATTRIBUTE, "0");
        runner.enqueue("Name, Age\nJohn, 30", attrs);

        runner.run(1, false);

        Thread.sleep(50L);
        runner.run(1, true, false);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 0);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 0);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 1);
    }
}
