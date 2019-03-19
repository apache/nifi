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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestScanKudu {

    private MockKuduScan kuduScan;
    private TestRunner runner;
    public static final String DEFAULT_TABLE_NAME = "Nifi-Kudu-Table";

    @Before
    public void setup() throws InitializationException {
        kuduScan = new MockKuduScan();
        runner = TestRunners.newTestRunner(kuduScan);

        setUpTestRunner(runner);
    }

    private void setUpTestRunner(TestRunner testRunner) throws InitializationException {
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

        runner.run();
        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

        Assert.assertEquals(0, kuduScan.getNumScans());
    }

    @Test
    public void testInvalidKuduTableName() {
        runner.setProperty(ScanKudu.TABLE_NAME, "${kudu.table}");
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column");
        runner.setValidateExpressionUsage(false);
        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

        Assert.assertEquals(0, kuduScan.getNumScans());
    }

    @Test
    public void testInvalidPredicatesPattern() {
        runner.setProperty(ScanKudu.TABLE_NAME, "${kudu.table}");
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column1");
        runner.setValidateExpressionUsage(false);
        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

        Assert.assertEquals(0, kuduScan.getNumScans());
    }

    @Test
    public void testResultsNotFound() throws InitializationException {
        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column1");

        runner.setValidateExpressionUsage(false);
        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_ORIGINAL).get(0);
        flowFile.assertAttributeEquals("scankudu.results.found", Boolean.FALSE.toString());
        Assert.assertEquals(1, kuduScan.getNumScans());
    }

    @Test
    public void testScanKuduProcessorJsonOutput() throws InitializationException {
        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column");

        runner.setValidateExpressionUsage(false);
        runner.setIncomingConnection(false);

        runner.enqueue("{\"user_id\":\"user1\",\"first_name\":\"Joe\",\"last_name\":\"Smith\"}");
        // Test JSON output
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(ScanKudu.REL_ORIGINAL, 1);
        List<MockFlowFile> files = runner.getFlowFilesForRelationship(ScanKudu.REL_ORIGINAL);
        assertNotNull(files);
        assertEquals("One file should be transferred to success", 1, files.size());
        assertEquals("{\"user_id\":\"user1\",\"first_name\":\"Joe\",\"last_name\":\"Smith\"}",
                new String(files.get(0).toByteArray()));
    }

    @Test
    public void testKuduScanToContentWithStringValues() throws InitializationException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("column1", "val1");
        rows.put("column2", "val2");

        kuduScan.addResult(rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column1");

        runner.setValidateExpressionUsage(false);
        runner.setIncomingConnection(false);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("[{\"rows\":[{\"column1\":\"val1\"}]}]");
        flowFile.assertAttributeEquals(ScanKudu.KUDU_ROWS_COUNT_ATTR, "1");

        flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_ORIGINAL).get(0);
        flowFile.assertAttributeEquals("scankudu.results.found", Boolean.TRUE.toString());

        Assert.assertEquals(1, kuduScan.getNumScans());
    }

    @Test
    public void testKuduScanToContentWithPredicateAndValueJSON() {
        final Map<String, String> rows = new HashMap<>();
        rows.put("column1", "val1");
        rows.put("column2", "val2");

        kuduScan.addResult( rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column1");
        runner.setValidateExpressionUsage(false);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("[{\"rows\":[{\"column1\":\"val1\"}]}]");

        Assert.assertEquals(1, kuduScan.getNumScans());
    }

    @Test
    public void testKuduScanWithExpressionLanguage() {
        final Map<String, String> rows = new HashMap<>();
        rows.put("column1", "val1");
        rows.put("column2", "val2");

        kuduScan.addResult(rows);

        runner.setProperty(ScanKudu.TABLE_NAME, "${kudu.table}");
        runner.setProperty(ScanKudu.PREDICATES, "${kudu.predicate}");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "${kudu.cols}");

        runner.setValidateExpressionUsage(false);

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("kudu.table", "table1");
        attributes.put("kudu.predicate", "column1=val1");
        attributes.put("kudu.cols", "column1");

        runner.enqueue("trigger flow file", attributes);
        runner.run();

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("[{\"rows\":[{\"column1\":\"val1\"}]}]");

        Assert.assertEquals(1, kuduScan.getNumScans());
    }

    @Test
    public void testKuduScanWhenScanThrowsException() {
        kuduScan.setThrowException(true);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val1");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column1");
        runner.setValidateExpressionUsage(false);

        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

        Assert.assertEquals(0, kuduScan.getNumScans());
    }

    @Test
    public void testKuduScanWhenKuduScanThrowsExceptionAfterLineN() {
        kuduScan.setLinesBeforeException(1);

        final Map<String, String> rows = new HashMap<>();
        rows.put("column1", "val1");
        rows.put("column2", "val2");

        kuduScan.addResult(rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "column1=val");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column1");
        runner.setValidateExpressionUsage(false);

        runner.enqueue("trigger flow file");
        runner.run();

        kuduScan.setLinesBeforeException(-1);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);

        Assert.assertEquals(0, kuduScan.getNumScans());
    }

}