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
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.apache.nifi.accumulo.controllerservices.AccumuloService;
import org.apache.nifi.accumulo.controllerservices.MockAccumuloService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class TestScanAccumulo {

    public static final String DEFAULT_COLUMN_FAMILY = "family1";

    /**
     * Though deprecated in 2.0 it still functions very well
     */
    private static MiniAccumuloCluster accumulo;

    private TestRunner getTestRunner(String table, String columnFamily) {
        final TestRunner runner = TestRunners.newTestRunner(ScanAccumulo.class);
        runner.enforceReadStreamsClosed(false);
        runner.setProperty(ScanAccumulo.TABLE_NAME, table);
        return runner;
    }




    @BeforeClass
    public static void setupInstance() throws IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException {
        Path tempDirectory = Files.createTempDirectory("acc"); // JUnit and Guava supply mechanisms for creating temp directories
        accumulo = new MiniAccumuloCluster(tempDirectory.toFile(), "password");
        accumulo.start();
    }

    private Set<Key> generateTestData(TestRunner runner, String definedRow, String table, boolean valueincq, String delim, String cv)
            throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setMaxWriteThreads(2);
        writerConfig.setMaxMemory(1024*1024);
        MultiTableBatchWriter writer  = accumulo.getConnector("root","password").createMultiTableBatchWriter(writerConfig);

        long ts = System.currentTimeMillis();


        final MockRecordWriter parser = new MockRecordWriter();
        try {
            runner.addControllerService("parser", parser);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(parser);
        runner.setProperty(ScanAccumulo.RECORD_WRITER,"parser");


        Set<Key> expectedKeys = new HashSet<>();
        ColumnVisibility colViz = new ColumnVisibility();
        if (null != cv)
            colViz = new ColumnVisibility(cv);
        Random random = new Random();
        for (int x = 0; x < 5; x++) {
            //final int row = random.nextInt(10000000);
            final String row = definedRow.isEmpty() ? UUID.randomUUID().toString() : definedRow;
            final String cf = UUID.randomUUID().toString();
            final String cq = UUID.randomUUID().toString();
            Text keyCq = new Text("name");
            if (valueincq){
                if (null != delim && !delim.isEmpty())
                    keyCq.append(delim.getBytes(),0,delim.length());
                keyCq.append(cf.getBytes(),0,cf.length());
            }
            expectedKeys.add(new Key(new Text(row), new Text(DEFAULT_COLUMN_FAMILY), keyCq, colViz,ts));
            keyCq = new Text("code");
            if (valueincq){
                if (null != delim && !delim.isEmpty())
                    keyCq.append(delim.getBytes(),0,delim.length());
                keyCq.append(cq.getBytes(),0,cq.length());
            }
            expectedKeys.add(new Key(new Text(row), new Text(DEFAULT_COLUMN_FAMILY), keyCq, colViz, ts));
            Mutation m = new Mutation(row);
            m.put(new Text(DEFAULT_COLUMN_FAMILY),new Text(keyCq),colViz,ts, new Value());
            writer.getBatchWriter(table).addMutation(m);
        }
        writer.flush();
        return expectedKeys;
    }

    void verifyKey(String tableName, Set<Key> expectedKeys, Authorizations auths) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (null == auths)
            auths = new Authorizations();
        try(BatchScanner scanner = accumulo.getConnector("root","password").createBatchScanner(tableName,auths,1)) {
            List<Range> ranges = new ArrayList<>();
            ranges.add(new Range());
            scanner.setRanges(ranges);
            for (Map.Entry<Key, Value> kv : scanner) {
                Assert.assertTrue(kv.getKey() + " not in expected keys",expectedKeys.remove(kv.getKey()));
            }
        }
        Assert.assertEquals(0, expectedKeys.size());

    }

    private void basicPutSetup(boolean sendFlowFile, boolean valueincq) throws Exception {
        basicPutSetup(sendFlowFile,"","","","",valueincq,null,"",null,false,5);
    }

    private void basicPutSetup(boolean sendFlowFile, boolean valueincq, final String delim) throws Exception {
        basicPutSetup(sendFlowFile,"","","","",valueincq,delim,"",null,false,5);
    }

    private void basicPutSetup(boolean sendFlowFile,String row,String endrow, String cf,String endcf, boolean valueincq,String delim,
                               String auths, Authorizations defaultVis, boolean deletes, int expected) throws Exception {
        String tableName = UUID.randomUUID().toString();
        tableName=tableName.replace("-","a");
        accumulo.getConnector("root","password").tableOperations().create(tableName);
        if (null != defaultVis)
        accumulo.getConnector("root","password").securityOperations().changeUserAuthorizations("root",defaultVis);
        TestRunner runner = getTestRunner(tableName, DEFAULT_COLUMN_FAMILY);
        runner.setProperty(ScanAccumulo.START_KEY, row);
        if (!cf.isEmpty())
        runner.setProperty(ScanAccumulo.COLUMNFAMILY, cf);
        if (!endcf.isEmpty())
        runner.setProperty(ScanAccumulo.COLUMNFAMILY_END, endcf);
        runner.setProperty(ScanAccumulo.AUTHORIZATIONS, auths);
        runner.setProperty(ScanAccumulo.END_KEY, endrow);

        AccumuloService client = MockAccumuloService.getService(runner,accumulo.getZooKeepers(),accumulo.getInstanceName(),"root","password");
        Set<Key> expectedKeys = generateTestData(runner,row,tableName,valueincq,delim, auths);
        if (sendFlowFile) {
            runner.enqueue("Test".getBytes("UTF-8")); // This is to coax the processor into reading the data in the reader.l
        }
        runner.run();


        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ScanAccumulo.REL_SUCCESS);
        for(MockFlowFile ff : results){
            String attr = ff.getAttribute("record.count");
            Assert.assertEquals(expected,Integer.valueOf(attr).intValue());
        }
        Assert.assertTrue("Wrong count, received " + results.size(), results.size() == 1);
    }




    @Test
    public void testPullDatWithFlowFile() throws Exception {
        basicPutSetup(true,false);
    }

    @Test
    public void testPullDatWithOutFlowFile() throws Exception {
        basicPutSetup(false,false);
    }

    @Test
    public void testSameRowCf() throws Exception {
        basicPutSetup(false,"2019","2019","family1","family2",false,null,"",null,false,1);
    }

    @Test
    public void testSameRowCfValueInCq() throws Exception {
        basicPutSetup(false,"2019","2019","family1","family2",true,null,"",null,false,5);
    }

    @Test
    public void testSameRowCfValueInCqWithAuths() throws Exception {
        basicPutSetup(false,"2019","2019","family1","family2",true,null,"abcd",new Authorizations("abcd"),false,5);
    }

    @Test(expected = AssertionError.class)
    public void testSameRowCfValueInCqErrorCfEnd() throws Exception {
        basicPutSetup(false,"2019","2019","family1","",true,null,"",null,false,5);
    }

    @Test(expected = AssertionError.class)
    public void testSameRowCfValueInCqErrorCf() throws Exception {
        basicPutSetup(false,"2019","2019","","family2",true,null,"",null,false,5);
    }

    @Test(expected = AssertionError.class)
    public void testSameRowCfValueInCqErrorNotLess() throws Exception {
        basicPutSetup(false,"2019","2019","family1","family1",true,null,"",null,false,5);
    }



}
