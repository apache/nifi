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
package org.apache.nifi.accumulo.processors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.io.Text;
import org.apache.nifi.accumulo.controllerservices.MockAccumuloService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

public class ScanAccumuloIT {

    public static final String DEFAULT_COLUMN_FAMILY = "family1";
    private static final MockRecordWriter PARSER = new MockRecordWriter();

    /**
     * Though deprecated in 2.0 it still functions very well
     */
    private static MiniAccumuloCluster accumulo;

    @BeforeClass
    public static void setupInstance() throws IOException, InterruptedException {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
        Path tempDirectory = Files.createTempDirectory("acc"); // JUnit and Guava supply mechanisms for creating temp directories
        accumulo = new MiniAccumuloCluster(tempDirectory.toFile(), "password");
        accumulo.start();
    }

    @Test
    public void testPullDatWithFlowFile() throws Exception {
        TestRunner runner = createTestEnvironment("","","","",false,"",null);
        // This is to coax the processor into reading the data in the reader.
        runner.enqueue("Test".getBytes(StandardCharsets.UTF_8));

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertRecordCount(results, 5);
    }

    @Test
    public void testPullDatWithOutFlowFile() throws Exception {
        TestRunner runner = createTestEnvironment("","","","",false,"",null);

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertRecordCount(results, 5);
    }

    @Test
    public void testSameRowCf() throws Exception {
        TestRunner runner = createTestEnvironment("2019","2019","family1","family2",false,"",null);

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertRecordCount(results, 1);
    }

    @Test
    public void testSameRowCfValueInCq() throws Exception {
        TestRunner runner = createTestEnvironment("2019","2019","family1","family2",true,"",null);

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertRecordCount(results, 5);
    }

    @Test
    public void testSameRowCfValueInCqWithAuths() throws Exception {
        TestRunner runner = createTestEnvironment("2019","2019","family1","family2",true,"abcd",new Authorizations("abcd"));

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertRecordCount(results, 5);
    }

    @Test(expected = AssertionError.class)
    public void testSameRowCfValueInCqErrorCfEnd() throws Exception {
        TestRunner runner = createTestEnvironment("2019","2019","family1","",true,"",null);

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertRecordCount(results, 5);
    }

    @Test(expected = AssertionError.class)
    public void testSameRowCfValueInCqErrorCf() throws Exception {
        TestRunner runner = createTestEnvironment("2019","2019","","family2",true,"",null);

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertRecordCount(results, 5);
    }

    @Test(expected = AssertionError.class)
    public void testSameRowCfValueInCqErrorNotLess() throws Exception {
        TestRunner runner = createTestEnvironment("2019","2019","family1","family1",true,"",null);

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertRecordCount(results, 5);
    }

    @Test
    public void testValueIsPresentByDefault() throws Exception {
        TestRunner runner = createTestEnvironment("2019","2019","family1","family2",false,"",null);

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertValueInResult(results, "\"Test\"\n");
    }

    @Test
    public void testValueIsNotPresentWhenDisabled() throws Exception {
        TestRunner runner = createTestEnvironment("2019", "2019", "family1", "family2", false, "", null);
        runner.setProperty(ScanAccumulo.VALUE_INCLUDED_IN_RESULT, "False");

        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);

        Assert.assertEquals("Wrong count, received " + results.size(), 1, results.size());
        assertValueInResult(results, "\n");
    }

    private TestRunner createTestEnvironment(String row, String endrow, String cf, String endcf, boolean valueincq,
                                             String auths, Authorizations defaultVis) throws Exception {
        String tableName = createTable(defaultVis);
        TestRunner runner = configureTestRunner(row, endrow, cf, endcf, auths, tableName);
        generateTestData(row,tableName,valueincq, auths);
        return runner;
    }

    private String createTable(Authorizations defaultVis) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        String tableName = UUID.randomUUID().toString();
        tableName=tableName.replace("-","a");
        if (null != defaultVis)
            accumulo.getConnector("root","password").securityOperations().changeUserAuthorizations("root", defaultVis);
        accumulo.getConnector("root","password").tableOperations().create(tableName);
        return tableName;
    }

    private TestRunner configureTestRunner(String row, String endrow, String cf, String endcf, String auths, String tableName) throws InitializationException {
        TestRunner runner = getTestRunner();
        runner.setProperty(ScanAccumulo.TABLE_NAME, tableName);
        runner.setProperty(ScanAccumulo.START_KEY, row);
        if (!cf.isEmpty())
            runner.setProperty(ScanAccumulo.COLUMNFAMILY, cf);
        if (!endcf.isEmpty())
            runner.setProperty(ScanAccumulo.COLUMNFAMILY_END, endcf);
        runner.setProperty(ScanAccumulo.AUTHORIZATIONS, auths);
        runner.setProperty(ScanAccumulo.END_KEY, endrow);
        return runner;
    }

    private TestRunner getTestRunner() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ScanAccumulo.class);
        runner.enforceReadStreamsClosed(false);

        MockAccumuloService.getService(runner,accumulo.getZooKeepers(),accumulo.getInstanceName(),"root","password");

        runner.addControllerService("parser", PARSER);
        runner.enableControllerService(PARSER);
        runner.setProperty(ScanAccumulo.RECORD_WRITER,"parser");

        return runner;
    }

    private void generateTestData(String definedRow, String table, boolean valueincq, String cv)
            throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setMaxWriteThreads(2);
        writerConfig.setMaxMemory(1024*1024);
        MultiTableBatchWriter writer  = accumulo.getConnector("root","password").createMultiTableBatchWriter(writerConfig);

        long ts = System.currentTimeMillis();
        ColumnVisibility colViz = new ColumnVisibility();
        if (null != cv)
            colViz = new ColumnVisibility(cv);
        for (int x = 0; x < 5; x++) {
            final String row = definedRow.isEmpty() ? UUID.randomUUID().toString() : definedRow;
            final String cq = UUID.randomUUID().toString();
            Text keyCq = new Text("code");
            if (valueincq){
                keyCq.append(cq.getBytes(),0,cq.length());
            }
            Mutation m = new Mutation(row);
            m.put(new Text(DEFAULT_COLUMN_FAMILY),new Text(keyCq),colViz,ts, new Value("Test"));
            writer.getBatchWriter(table).addMutation(m);
        }
        writer.flush();
    }

    private void assertRecordCount(List<MockFlowFile> results, int expected) {
        for (MockFlowFile ff : results){
            String attr = ff.getAttribute("record.count");
            Assert.assertEquals(expected, Integer.valueOf(attr).intValue());
        }
    }

    private void assertValueInResult(List<MockFlowFile> results, String expected) {
        for (MockFlowFile ff : results) {
            Assert.assertEquals(expected, ff.getContent());
        }
    }
}
