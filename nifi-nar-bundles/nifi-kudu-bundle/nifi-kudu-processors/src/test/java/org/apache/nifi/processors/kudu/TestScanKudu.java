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

package org.apache.nifi.processors.kudu;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestScanKudu {

    private MockScanKudu kuduScan;
    private TestRunner runner;
    public static final String DEFAULT_TABLE_NAME = "Nifi-Kudu-Table";
    public static final String DEFAULT_MASTERS = "testLocalHost:7051";

    @Rule
    public KuduTestHarness harness = new KuduTestHarness(
        new MiniKuduCluster.MiniKuduClusterBuilder()
            .addMasterServerFlag("--use_hybrid_clock=false")
            .addTabletServerFlag("--use_hybrid_clock=false")
    );

    @Before
    public void setup() throws InitializationException {
        kuduScan = new MockScanKudu();
        kuduScan.kuduClient = harness.getClient();
        runner = TestRunners.newTestRunner(kuduScan);

        setUpTestRunner(runner);
    }

    private void setUpTestRunner(TestRunner testRunner) throws InitializationException {
        testRunner.setProperty(PutKudu.KUDU_MASTERS, DEFAULT_MASTERS);
        testRunner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
    }

    @Test
    public void testKuduProjectedColumnsValidation() {
        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.assertValid();

        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "c1,c2");
        runner.assertValid();

        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "c1");
        runner.assertValid();

        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "c1 c2,c3");
        runner.assertNotValid();

        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "c1:,c2,c3");
        runner.assertNotValid();

        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "c1,c1,");
        runner.assertNotValid();
    }

    @Test
    public void testKuduPredicatesValidation() {
        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.assertValid();

        runner.setProperty(ScanKudu.PREDICATES, "column1>val1");
        runner.assertValid();

        runner.setProperty(ScanKudu.PREDICATES, "column1<val1");
        runner.assertValid();

        runner.setProperty(ScanKudu.PREDICATES, "column1<=val1");
        runner.assertValid();

        runner.setProperty(ScanKudu.PREDICATES, "column1>=val1");
        runner.assertValid();

        runner.setProperty(ScanKudu.PREDICATES, "column1=>=val1");
        runner.assertNotValid();

        runner.setProperty(ScanKudu.PREDICATES, "column1:val1");
        runner.assertNotValid();
    }

    @Test
    public void testNoIncomingFlowFile() {
        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");

        runner.run(1, false);
        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);
    }

    @Test
    public void testInvalidKuduTableName() {
        runner.setProperty(ScanKudu.TABLE_NAME, "${table1}");
        runner.setProperty(ScanKudu.PREDICATES, "column5=val2");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column");
        runner.setValidateExpressionUsage(false);
        runner.enqueue("trigger flow file");
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

    }

    @Test
    public void testInvalidPredicatesPattern() {
        runner.setProperty(ScanKudu.TABLE_NAME, "${kudu.table}");
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column1");
        runner.setValidateExpressionUsage(false);
        runner.enqueue("trigger flow file");
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

    }

    @Test
    public void testResultsNotFound() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val1");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "key1=val3");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key1");

        runner.setValidateExpressionUsage(false);
        runner.enqueue("trigger flow file");
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_ORIGINAL).get(0);
        flowFile.assertAttributeEquals("scankudu.results.found", Boolean.FALSE.toString());
    }

    @Test
    public void testScanKuduProcessorJsonOutput() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val1");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "key1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key");

        runner.setValidateExpressionUsage(false);
        runner.setIncomingConnection(false);

        // Test JSON output
        runner.run(1, false, true);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);
        List<MockFlowFile> files = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS);
        assertNotNull(files);
        assertEquals("One file should be transferred to success", 1, files.size());
        assertEquals("[{\"rows\":[{\"key\":\"val1\"}]}]", new String(files.get(0).toByteArray()));
    }

    @Test
    public void testKuduScanToContentWithStringValues() throws InitializationException, KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val1");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "key=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key,key1");
        runner.setProperty(ScanKudu.BATCH_SIZE, "2");

        runner.setValidateExpressionUsage(false);
        runner.setIncomingConnection(false);

        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("[{\"rows\":[{\"key\":\"val1\",\"key1\":\"val1\"}]}]");
        flowFile.assertAttributeEquals(ScanKudu.KUDU_ROWS_COUNT_ATTR, "1");

        flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_ORIGINAL).get(0);
        flowFile.assertAttributeEquals("scankudu.results.found", Boolean.TRUE.toString());

    }

    @Test
    public void testKuduScanToContentWithPredicateAndValueJSON() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val2");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "key=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key1");
        runner.setProperty(ScanKudu.BATCH_SIZE, "2");
        runner.setValidateExpressionUsage(false);

        runner.enqueue("trigger flow file");
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("[{\"rows\":[{\"key1\":\"val2\"}]}]");

    }

    @Test
    public void testKuduScanWithExpressionLanguage() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val2");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, "${kudu.table}");
        runner.setProperty(ScanKudu.PREDICATES, "${kudu.predicate}");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "${kudu.cols}");
        runner.setProperty(ScanKudu.BATCH_SIZE, "2");

        runner.setValidateExpressionUsage(false);

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("kudu.table", DEFAULT_TABLE_NAME);
        attributes.put("kudu.predicate", "key=val1");
        attributes.put("kudu.cols", "key1");

        runner.enqueue("trigger flow file", attributes);
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("[{\"rows\":[{\"key1\":\"val2\"}]}]");
    }

    @Test
    public void testKuduScanWithMultiPredicates() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val2");

        final Map<String, String> rows1 = new HashMap<>();
        rows1.put("key", "val2");
        rows1.put("key1", "val2");

        final Map<String, String> rows2 = new HashMap<>();
        rows2.put("key", "val3");
        rows2.put("key1", "val3");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);
        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows1);
        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows2);

        runner.setProperty(ScanKudu.TABLE_NAME, "${kudu.table}");
        runner.setProperty(ScanKudu.PREDICATES, "${kudu.predicate}");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "${kudu.cols}");
        runner.setProperty(ScanKudu.BATCH_SIZE, "2");

        runner.setValidateExpressionUsage(false);

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("kudu.table", DEFAULT_TABLE_NAME);
        attributes.put("kudu.predicate", "key=val1,key1=val2");
        attributes.put("kudu.cols", "key1");

        runner.enqueue("trigger flow file", attributes);
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("[{\"rows\":[{\"key1\":\"val2\"}]}]");
    }

    @Test
    public void testKuduScanWhenScanThrowsException() {

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "key1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key1");
        runner.setValidateExpressionUsage(false);

        runner.enqueue("trigger flow file");
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

    }

    @Test
    public void testKuduScanWhenKuduScanThrowsExceptionWithWrongColumn() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val2");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "key2=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key1");
        runner.setValidateExpressionUsage(false);

        runner.enqueue("trigger flow file");
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

    }

}