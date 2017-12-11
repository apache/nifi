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

package org.apache.nifi.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.nifi.hbase.HBaseTestUtil.getHBaseClientService;

public class TestPutHBaseRecord {

    public static final String DEFAULT_TABLE_NAME = "nifi";
    public static final String DEFAULT_COLUMN_FAMILY = "family1";

    private TestRunner getTestRunner(String table, String columnFamily, String batchSize) {
        final TestRunner runner = TestRunners.newTestRunner(PutHBaseRecord.class);
        runner.enforceReadStreamsClosed(false);
        runner.setProperty(PutHBaseJSON.TABLE_NAME, table);
        runner.setProperty(PutHBaseJSON.COLUMN_FAMILY, columnFamily);
        runner.setProperty(PutHBaseJSON.BATCH_SIZE, batchSize);
        return runner;
    }

    private static final List<Integer> KEYS = Arrays.asList(1, 2,3, 4);
    private static final List<String> NAMES = Arrays.asList("rec1", "rec2", "rec3", "rec4");
    private static final List<Long> CODES = Arrays.asList(101L, 102L, 103L, 104L);

    private void generateTestData(TestRunner runner) throws IOException {

        final MockRecordParser parser = new MockRecordParser();
        try {
            runner.addControllerService("parser", parser);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(parser);
        runner.setProperty(PutHBaseRecord.RECORD_READER_FACTORY, "parser");

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.LONG);

        for (int x = 0; x < KEYS.size(); x++) {
            parser.addRecord(KEYS.get(x), NAMES.get(x), CODES.get(x));
        }
    }

    private void basicPutSetup(String encodingStrategy, PutValidator validator) throws Exception {
        basicPutSetup(encodingStrategy, validator, "1000", 4);
    }

    private void basicPutSetup(String encodingStrategy, PutValidator validator, String batchSize, int expectedPuts) throws Exception {
        Assert.assertEquals(1L, 1L);
        TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, batchSize);
        runner.setProperty(PutHBaseRecord.ROW_FIELD_NAME, "id");
        runner.setProperty(PutHBaseRecord.FIELD_ENCODING_STRATEGY, encodingStrategy);
        MockHBaseClientService client = getHBaseClientService(runner);
        generateTestData(runner);
        runner.enqueue("Test".getBytes("UTF-8")); // This is to coax the processor into reading the data in the reader.l
        runner.run();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PutHBaseRecord.REL_SUCCESS);
        Assert.assertTrue("Wrong count", results.size() == 1);

        Assert.assertEquals("Wrong number of PutFlowFiles ", client.getFlowFilePuts().get("nifi").size(), expectedPuts);
        for (PutFlowFile putFlowFile : client.getFlowFilePuts().get("nifi")) {
            Iterator<PutColumn> columnIterator = putFlowFile.getColumns().iterator();
            PutColumn name = columnIterator.next();
            PutColumn code = columnIterator.next();
            Assert.assertNotNull("Name was null", name);
            Assert.assertNotNull("Code was null", code);

            String nFamName = new String(name.getColumnFamily());
            String cFamName = new String(code.getColumnFamily());
            String nQual    = new String(name.getColumnQualifier());
            String cQual    = new String(code.getColumnQualifier());

            Assert.assertEquals("Name column family didn't match", nFamName, DEFAULT_COLUMN_FAMILY);
            Assert.assertEquals("Code column family didn't match", cFamName, DEFAULT_COLUMN_FAMILY);
            Assert.assertEquals("Name qualifier didn't match", nQual, "name");
            Assert.assertEquals("Code qualifier didn't match", cQual, "code");

            validator.handle(name, code);
        }
    }

    @Test
    public void testByteEncodedPut() throws Exception {
        basicPutSetup(PutHBaseRecord.BYTES_ENCODING_VALUE, (PutColumn[] columns) -> {
            PutColumn name = columns[0];
            PutColumn code = columns[1];
            String nameVal = Bytes.toString(name.getBuffer());
            Long codeVal = Bytes.toLong(code.getBuffer());
            Assert.assertTrue("Name was not found", NAMES.contains(nameVal));
            Assert.assertTrue("Code was not found ", CODES.contains(codeVal));
        });
    }

    private void innertTest(PutColumn[] columns) {
        PutColumn name = columns[0];
        PutColumn code = columns[1];
        String nameVal = Bytes.toString(name.getBuffer());
        String codeVal = Bytes.toString(code.getBuffer());
        Assert.assertTrue("Name was not found", NAMES.contains(nameVal));
        Assert.assertTrue("Code was not found ", CODES.contains(new Long(codeVal)));
    }

    @Test
    public void testStringEncodedPut() throws Exception {
        basicPutSetup(PutHBaseRecord.STRING_ENCODING_VALUE, (PutColumn[] columns) -> {
            innertTest(columns);
        });
    }

    @Test
    public void testBatchOfOne() throws Exception {
        basicPutSetup(PutHBaseRecord.STRING_ENCODING_VALUE, (PutColumn[] columns) -> {
            innertTest(columns);
        }, "1", 1);
    }

    @Test
    public void testBatchOfTwo() throws Exception {
        basicPutSetup(PutHBaseRecord.STRING_ENCODING_VALUE, (PutColumn[] columns) -> {
            innertTest(columns);
        }, "2", 2);
    }

    @Test
    public void testFailure() throws Exception {
        Assert.assertEquals(1L, 1L);
        TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "2");
        runner.setProperty(PutHBaseRecord.ROW_FIELD_NAME, "id");
        runner.setProperty(PutHBaseRecord.FIELD_ENCODING_STRATEGY, PutHBaseRecord.STRING_ENCODING_VALUE);
        MockHBaseClientService client = getHBaseClientService(runner);
        client.setTestFailure(true);
        client.setFailureThreshold(2);
        generateTestData(runner);
        runner.enqueue("Test".getBytes("UTF-8")); // This is to coax the processor into reading the data in the reader.
        runner.run();
        List<MockFlowFile> result = runner.getFlowFilesForRelationship(PutHBaseRecord.REL_FAILURE);
        Assert.assertEquals("Size was wrong", result.size(), 1);
        Assert.assertEquals("Wrong # of PutFlowFiles", client.getFlowFilePuts().get("nifi").size(), 2);
        Assert.assertTrue(runner.getFlowFilesForRelationship(PutHBaseRecord.REL_SUCCESS).size() == 0);

        MockFlowFile mff = result.get(0);
        Assert.assertNotNull("Missing restart index attribute", mff.getAttribute("restart.index"));
        List<PutFlowFile> old = client.getFlowFilePuts().get("nifi");
        client.setTestFailure(false);
        runner.enqueue("test");
        runner.run();

        Assert.assertEquals("Size was wrong", result.size(), 1);
        Assert.assertEquals("Wrong # of PutFlowFiles", client.getFlowFilePuts().get("nifi").size(), 2);

        List<PutFlowFile> newPFF = client.getFlowFilePuts().get("nifi");
        for (PutFlowFile putFlowFile : old) {
            Assert.assertFalse("Duplication", newPFF.contains(putFlowFile));
        }
    }

    interface PutValidator {
        void handle(PutColumn... columns);
    }
}