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

import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPutHBaseCell {

    @Test
    public void testSingleFlowFileNoTimestamp() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final TestRunner runner = TestRunners.newTestRunner(PutHBaseCell.class);
        runner.setProperty(PutHBaseCell.TABLE_NAME, tableName);
        runner.setProperty(PutHBaseCell.ROW_ID, row);
        runner.setProperty(PutHBaseCell.COLUMN_FAMILY, columnFamily);
        runner.setProperty(PutHBaseCell.COLUMN_QUALIFIER, columnQualifier);
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "1");

        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(tableName);
        assertEquals(1, puts.size());
        verifyPut(row, columnFamily, columnQualifier, null, content, puts.get(0));

        assertEquals(1, runner.getProvenanceEvents().size());
    }

    @Test
    public void testSingleFlowFileWithTimestamp() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";
        final Long timestamp = 1L;

        final TestRunner runner = TestRunners.newTestRunner(PutHBaseCell.class);
        runner.setProperty(PutHBaseCell.TABLE_NAME, tableName);
        runner.setProperty(PutHBaseCell.ROW_ID, row);
        runner.setProperty(PutHBaseCell.COLUMN_FAMILY, columnFamily);
        runner.setProperty(PutHBaseCell.COLUMN_QUALIFIER, columnQualifier);
        runner.setProperty(PutHBaseCell.TIMESTAMP, timestamp.toString());
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "1");

        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(tableName);
        assertEquals(1, puts.size());
        verifyPut(row, columnFamily, columnQualifier, timestamp, content, puts.get(0));

        assertEquals(1, runner.getProvenanceEvents().size());
    }

    @Test
    public void testSingleFlowFileWithInvalidTimestamp() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";
        final String timestamp = "not-a-timestamp";

        final PutHBaseCell proc = new PutHBaseCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        runner.setProperty(PutHBaseCell.TIMESTAMP, "${hbase.timestamp}");
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "1");

        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final String content = "some content";
        final Map<String, String> attributes = getAttributeMapWithEL(tableName, row, columnFamily, columnQualifier);
        attributes.put("hbase.timestamp", timestamp);
        runner.enqueue(content.getBytes("UTF-8"), attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_FAILURE, 1);
    }

    @Test
    public void testSingleFlowFileWithEL() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";
        final Long timestamp = 1L;

        final PutHBaseCell proc = new PutHBaseCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        runner.setProperty(PutHBaseCell.TIMESTAMP, "${hbase.timestamp}");
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "1");

        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final String content = "some content";
        final Map<String, String> attributes = getAttributeMapWithEL(tableName, row, columnFamily, columnQualifier);
        attributes.put("hbase.timestamp", timestamp.toString());
        runner.enqueue(content.getBytes("UTF-8"), attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(tableName);
        assertEquals(1, puts.size());
        verifyPut(row, columnFamily, columnQualifier, timestamp, content, puts.get(0));

        assertEquals(1, runner.getProvenanceEvents().size());
    }

    @Test
    public void testSingleFlowFileWithELMissingAttributes() throws IOException, InitializationException {
        final PutHBaseCell proc = new PutHBaseCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "1");

        final MockHBaseClientService hBaseClient = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClient);
        runner.enableControllerService(hBaseClient);
        runner.setProperty(PutHBaseCell.HBASE_CLIENT_SERVICE, "hbaseClient");

        getHBaseClientService(runner);

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"), new HashMap<String, String>());
        runner.run();

        runner.assertTransferCount(PutHBaseCell.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHBaseCell.REL_FAILURE, 1);

        assertEquals(0, runner.getProvenanceEvents().size());
    }

    @Test
    public void testMultipleFlowFileWithELOneMissingAttributes() throws IOException, InitializationException {
        final PutHBaseCell proc = new PutHBaseCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "10");

        final MockHBaseClientService hBaseClient = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClient);
        runner.enableControllerService(hBaseClient);
        runner.setProperty(PutHBaseCell.HBASE_CLIENT_SERVICE, "hbaseClient");

        getHBaseClientService(runner);

        // this one will go to failure
        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"), new HashMap<String, String>());

        // this will go to success
        final String content2 = "some content2";
        final Map<String, String> attributes = getAttributeMapWithEL("table", "row", "cf", "cq");
        runner.enqueue(content2.getBytes("UTF-8"), attributes);

        runner.run();
        runner.assertTransferCount(PutHBaseCell.REL_SUCCESS, 1);
        runner.assertTransferCount(PutHBaseCell.REL_FAILURE, 1);

        assertEquals(1, runner.getProvenanceEvents().size());
    }

    @Test
    public void testMultipleFlowFilesSameTableDifferentRow() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row1 = "row1";
        final String row2 = "row2";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final PutHBaseCell proc = new PutHBaseCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final String content1 = "some content1";
        final Map<String, String> attributes1 = getAttributeMapWithEL(tableName, row1, columnFamily, columnQualifier);
        runner.enqueue(content1.getBytes("UTF-8"), attributes1);

        final String content2 = "some content1";
        final Map<String, String> attributes2 = getAttributeMapWithEL(tableName, row2, columnFamily, columnQualifier);
        runner.enqueue(content2.getBytes("UTF-8"), attributes2);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content1);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(tableName);
        assertEquals(2, puts.size());
        verifyPut(row1, columnFamily, columnQualifier, null, content1, puts.get(0));
        verifyPut(row2, columnFamily, columnQualifier, null, content2, puts.get(1));

        assertEquals(2, runner.getProvenanceEvents().size());
    }

    @Test
    public void testMultipleFlowFilesSameTableDifferentRowFailure() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row1 = "row1";
        final String row2 = "row2";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final PutHBaseCell proc = new PutHBaseCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        hBaseClient.setThrowException(true);

        final String content1 = "some content1";
        final Map<String, String> attributes1 = getAttributeMapWithEL(tableName, row1, columnFamily, columnQualifier);
        runner.enqueue(content1.getBytes("UTF-8"), attributes1);

        final String content2 = "some content1";
        final Map<String, String> attributes2 = getAttributeMapWithEL(tableName, row2, columnFamily, columnQualifier);
        runner.enqueue(content2.getBytes("UTF-8"), attributes2);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_FAILURE, 2);

        assertEquals(0, runner.getProvenanceEvents().size());
    }

    @Test
    public void testMultipleFlowFilesSameTableSameRow() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "row1";
        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final PutHBaseCell proc = new PutHBaseCell();
        final TestRunner runner = getTestRunnerWithEL(proc);
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final String content1 = "some content1";
        final Map<String, String> attributes1 = getAttributeMapWithEL(tableName, row, columnFamily, columnQualifier);
        runner.enqueue(content1.getBytes("UTF-8"), attributes1);

        final String content2 = "some content1";
        runner.enqueue(content2.getBytes("UTF-8"), attributes1);

        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content1);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(tableName);
        assertEquals(2, puts.size());
        verifyPut(row, columnFamily, columnQualifier, null, content1, puts.get(0));
        verifyPut(row, columnFamily, columnQualifier, null, content2, puts.get(1));

        assertEquals(2, runner.getProvenanceEvents().size());
    }

    @Test
    public void testSingleFlowFileWithBinaryRowKey() throws IOException, InitializationException {
        final String tableName = "nifi";
        final String row = "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
                "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
                "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
                "\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
                "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x01\\x00\\x00\\x00\\x00\\x00" +
                "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
                "\\x00\\x00\\x00\\x01\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00" +
                "\\x01\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x01\\x01\\x01\\x00\\x01\\x00\\x01\\x01\\x01\\x00\\x00\\x00" +
                "\\x00\\x00\\x00\\x01\\x01\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x01\\x00\\x01\\x00\\x01\\x00" +
                "\\x00\\x01\\x01\\x01\\x01\\x00\\x00\\x01\\x01\\x01\\x00\\x01\\x00\\x00";

        final String columnFamily = "family1";
        final String columnQualifier = "qualifier1";

        final TestRunner runner = TestRunners.newTestRunner(PutHBaseCell.class);
        runner.setProperty(PutHBaseCell.TABLE_NAME, tableName);
        runner.setProperty(PutHBaseCell.ROW_ID, row);
        runner.setProperty(PutHBaseCell.ROW_ID_ENCODING_STRATEGY,PutHBaseCell.ROW_ID_ENCODING_BINARY.getValue());
        runner.setProperty(PutHBaseCell.COLUMN_FAMILY, columnFamily);
        runner.setProperty(PutHBaseCell.COLUMN_QUALIFIER, columnQualifier);
        runner.setProperty(PutHBaseCell.BATCH_SIZE, "1");

        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);

        final byte[] expectedRowKey = hBaseClient.toBytesBinary(row);

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(tableName);
        assertEquals(1, puts.size());
        verifyPut(expectedRowKey, columnFamily.getBytes(StandardCharsets.UTF_8), columnQualifier.getBytes(StandardCharsets.UTF_8), null, content, puts.get(0));

        assertEquals(1, runner.getProvenanceEvents().size());
    }

    private Map<String, String> getAttributeMapWithEL(String tableName, String row, String columnFamily, String columnQualifier) {
        final Map<String,String> attributes1 = new HashMap<>();
        attributes1.put("hbase.tableName", tableName);
        attributes1.put("hbase.row", row);
        attributes1.put("hbase.columnFamily", columnFamily);
        attributes1.put("hbase.columnQualifier", columnQualifier);
        return attributes1;
    }

    private TestRunner getTestRunnerWithEL(PutHBaseCell proc) {
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHBaseCell.TABLE_NAME, "${hbase.tableName}");
        runner.setProperty(PutHBaseCell.ROW_ID, "${hbase.row}");
        runner.setProperty(PutHBaseCell.COLUMN_FAMILY, "${hbase.columnFamily}");
        runner.setProperty(PutHBaseCell.COLUMN_QUALIFIER, "${hbase.columnQualifier}");
        return runner;
    }

    private MockHBaseClientService getHBaseClientService(TestRunner runner) throws InitializationException {
        final MockHBaseClientService hBaseClient = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClient);
        runner.enableControllerService(hBaseClient);
        runner.setProperty(PutHBaseCell.HBASE_CLIENT_SERVICE, "hbaseClient");
        return hBaseClient;
    }

    private void verifyPut(String row, String columnFamily, String columnQualifier, Long timestamp, String content, PutFlowFile put) {
        verifyPut(row.getBytes(StandardCharsets.UTF_8),columnFamily.getBytes(StandardCharsets.UTF_8),
                                columnQualifier.getBytes(StandardCharsets.UTF_8), timestamp, content, put);
    }
    private void verifyPut(byte[] row, byte[] columnFamily, byte[] columnQualifier, Long timestamp, String content, PutFlowFile put) {
        assertEquals(new String(row, StandardCharsets.UTF_8), new String(put.getRow(), StandardCharsets.UTF_8));

        assertNotNull(put.getColumns());
        assertEquals(1, put.getColumns().size());

        final PutColumn column = put.getColumns().iterator().next();
        assertEquals(new String(columnFamily, StandardCharsets.UTF_8), new String(column.getColumnFamily(), StandardCharsets.UTF_8));
        assertEquals(new String(columnQualifier, StandardCharsets.UTF_8), new String(column.getColumnQualifier(), StandardCharsets.UTF_8));
        assertEquals(content, new String(column.getBuffer(), StandardCharsets.UTF_8));
        assertEquals(timestamp, column.getTimestamp());
    }

}
