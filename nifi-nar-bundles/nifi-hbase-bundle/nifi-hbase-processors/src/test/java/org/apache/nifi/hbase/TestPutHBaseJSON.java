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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.apache.nifi.hbase.HBaseTestUtil.getHBaseClientService;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestPutHBaseJSON {

    public static final String DEFAULT_TABLE_NAME = "nifi";
    public static final String DEFAULT_ROW = "row1";
    public static final String DEFAULT_COLUMN_FAMILY = "family1";
    public static final Long DEFAULT_TIMESTAMP = 1L;

    @Test
    public void testCustomValidate() throws InitializationException {
        // missing row id and row id field name should be invalid
        TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getHBaseClientService(runner);
        runner.assertNotValid();

        // setting both properties should still be invalid
        runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, "rowId");
        runner.setProperty(PutHBaseJSON.ROW_FIELD_NAME, "rowFieldName");
        runner.assertNotValid();

        // only a row id field name should make it valid
        runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_FIELD_NAME, "rowFieldName");
        runner.assertValid();

        // only a row id should make it valid
        runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, "rowId");
        runner.assertValid();
    }

    @Test
    public void testSingleJsonDocAndProvidedRowId() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", hBaseClient.toBytes("value1"));
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals("hbase://" + DEFAULT_TABLE_NAME + "/" + DEFAULT_ROW, event.getTransitUri());
    }

    @Test
    public void testSingleJsonDocAndProvidedRowIdwithNonString() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        runner.setProperty(PutHBaseJSON.FIELD_ENCODING_STRATEGY, PutHBaseJSON.BYTES_ENCODING_VALUE);

        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);

        final String content = "{ \"field1\" : 1.23456, \"field2\" : 2345235, \"field3\" : false }";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", hBaseClient.toBytes(1.23456d));
        expectedColumns.put("field2", hBaseClient.toBytes(2345235l));
        expectedColumns.put("field3", hBaseClient.toBytes(false));
        HBaseTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals("hbase://" + DEFAULT_TABLE_NAME + "/" + DEFAULT_ROW, event.getTransitUri());
    }

    @Test
    public void testSingJsonDocAndExtractedRowId() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_FIELD_NAME, "rowField");

        final String content = "{ \"rowField\" : \"myRowId\", \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should be a put with row id of myRowId, and rowField shouldn't end up in the columns
        final Map<String,byte[]> expectedColumns1 = new HashMap<>();
        expectedColumns1.put("field1", hBaseClient.toBytes("value1"));
        expectedColumns1.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut("myRowId", DEFAULT_COLUMN_FAMILY, expectedColumns1, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        HBaseTestUtil.verifyEvent(runner.getProvenanceEvents(), "hbase://" + DEFAULT_TABLE_NAME + "/myRowId", ProvenanceEventType.SEND);
    }

    @Test
    public void testSingJsonDocAndExtractedRowIdMissingField() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_FIELD_NAME, "rowField");

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_FAILURE).get(0);
        outFile.assertContentEquals(content);

        // should be no provenance events
        assertEquals(0, runner.getProvenanceEvents().size());

        // no puts should have made it to the client
        assertEquals(0, hBaseClient.getFlowFilePuts().size());
    }

    @Test
    public void testMultipleJsonDocsRouteToFailure() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);

        final String content1 = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        final String content2 = "{ \"field3\" : \"value3\", \"field4\" : \"value4\" }";
        final String content = "[ " + content1 + " , " + content2 + " ]";

        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_FAILURE).get(0);
        outFile.assertContentEquals(content);

        // should be no provenance events
        assertEquals(0, runner.getProvenanceEvents().size());

        // no puts should have made it to the client
        assertEquals(0, hBaseClient.getFlowFilePuts().size());
    }

    @Test
    public void testELWithProvidedRowId() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner("${hbase.table}", "${hbase.colFamily}", "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, "${hbase.rowId}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("hbase.table", "myTable");
        attributes.put("hbase.colFamily", "myColFamily");
        attributes.put("hbase.rowId", "myRowId");

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes("UTF-8"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get("myTable");
        assertEquals(1, puts.size());

        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", hBaseClient.toBytes("value1"));
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut("myRowId", "myColFamily", expectedColumns, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        HBaseTestUtil.verifyEvent(runner.getProvenanceEvents(), "hbase://myTable/myRowId", ProvenanceEventType.SEND);
    }

    @Test
    public void testELWithExtractedRowId() throws IOException, InitializationException {
        final TestRunner runner = getTestRunner("${hbase.table}", "${hbase.colFamily}", "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_FIELD_NAME, "${hbase.rowField}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("hbase.table", "myTable");
        attributes.put("hbase.colFamily", "myColFamily");
        attributes.put("hbase.rowField", "field1");

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes("UTF-8"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get("myTable");
        assertEquals(1, puts.size());

        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut("value1", "myColFamily", expectedColumns, puts);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        HBaseTestUtil.verifyEvent(runner.getProvenanceEvents(), "hbase://myTable/value1", ProvenanceEventType.SEND);
    }

    @Test
    public void testNullAndArrayElementsWithWarnStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutHBaseJSON.COMPLEX_FIELD_STRATEGY, PutHBaseJSON.COMPLEX_FIELD_WARN.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }], \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should have skipped field1 and field3
        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);
    }

    @Test
    public void testNullAndArrayElementsWithIgnoreStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutHBaseJSON.COMPLEX_FIELD_STRATEGY, PutHBaseJSON.COMPLEX_FIELD_IGNORE.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }], \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should have skipped field1 and field3
        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);
    }

    @Test
    public void testNullAndArrayElementsWithFailureStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutHBaseJSON.COMPLEX_FIELD_STRATEGY, PutHBaseJSON.COMPLEX_FIELD_FAIL.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }], \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_FAILURE).get(0);
        outFile.assertContentEquals(content);

        // should be no provenance events
        assertEquals(0, runner.getProvenanceEvents().size());

        // no puts should have made it to the client
        assertEquals(0, hBaseClient.getFlowFilePuts().size());
    }

    @Test
    public void testNullAndArrayElementsWithTextStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutHBaseJSON.COMPLEX_FIELD_STRATEGY, PutHBaseJSON.COMPLEX_FIELD_TEXT.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }], \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should have skipped field1 and field3
        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", hBaseClient.toBytes("[{\"child_field1\":\"child_value1\"}]"));
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);
    }

    @Test
    public void testNestedDocWithTextStrategy() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutHBaseJSON.COMPLEX_FIELD_STRATEGY, PutHBaseJSON.COMPLEX_FIELD_TEXT.getValue());

        // should route to success because there is at least one valid field
        final String content = "{ \"field1\" : { \"child_field1\" : \"child_value1\" }, \"field2\" : \"value2\", \"field3\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        // should have skipped field1 and field3
        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", hBaseClient.toBytes("{\"child_field1\":\"child_value1\"}"));
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, expectedColumns, puts);
    }

    @Test
    public void testAllElementsAreNullOrArrays() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutHBaseJSON.COMPLEX_FIELD_STRATEGY, PutHBaseJSON.COMPLEX_FIELD_WARN.getValue());

        // should route to failure since it would produce a put with no columns
        final String content = "{ \"field1\" : [{ \"child_field1\" : \"child_value1\" }],  \"field2\" : null }";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_FAILURE).get(0);
        outFile.assertContentEquals(content);

        // should be no provenance events
        assertEquals(0, runner.getProvenanceEvents().size());

        // no puts should have made it to the client
        assertEquals(0, hBaseClient.getFlowFilePuts().size());
    }

    @Test
    public void testInvalidJson() throws InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);

        final String content = "NOT JSON";
        runner.enqueue(content.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_FAILURE, 1);
    }

    @Test
    public void testTimestamp() throws UnsupportedEncodingException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutHBaseJSON.TIMESTAMP, DEFAULT_TIMESTAMP.toString());

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", hBaseClient.toBytes("value1"));
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, DEFAULT_TIMESTAMP, expectedColumns, puts);
    }

    @Test
    public void testTimestampWithEL() throws UnsupportedEncodingException, InitializationException {
        final TestRunner runner = getTestRunner(DEFAULT_TABLE_NAME, DEFAULT_COLUMN_FAMILY, "1");
        final MockHBaseClientService hBaseClient = getHBaseClientService(runner);
        runner.setProperty(PutHBaseJSON.ROW_ID, DEFAULT_ROW);
        runner.setProperty(PutHBaseJSON.TIMESTAMP, "${hbase.timestamp}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("hbase.timestamp", DEFAULT_TIMESTAMP.toString());

        final String content = "{ \"field1\" : \"value1\", \"field2\" : \"value2\" }";
        runner.enqueue(content.getBytes("UTF-8"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutHBaseCell.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PutHBaseCell.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        assertNotNull(hBaseClient.getFlowFilePuts());
        assertEquals(1, hBaseClient.getFlowFilePuts().size());

        final List<PutFlowFile> puts = hBaseClient.getFlowFilePuts().get(DEFAULT_TABLE_NAME);
        assertEquals(1, puts.size());

        final Map<String,byte[]> expectedColumns = new HashMap<>();
        expectedColumns.put("field1", hBaseClient.toBytes("value1"));
        expectedColumns.put("field2", hBaseClient.toBytes("value2"));
        HBaseTestUtil.verifyPut(DEFAULT_ROW, DEFAULT_COLUMN_FAMILY, DEFAULT_TIMESTAMP, expectedColumns, puts);
    }

    private TestRunner getTestRunner(String table, String columnFamily, String batchSize) {
        final TestRunner runner = TestRunners.newTestRunner(PutHBaseJSON.class);
        runner.setProperty(PutHBaseJSON.TABLE_NAME, table);
        runner.setProperty(PutHBaseJSON.COLUMN_FAMILY, columnFamily);
        runner.setProperty(PutHBaseJSON.BATCH_SIZE, batchSize);
        return runner;
    }

}
