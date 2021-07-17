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
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.io.Text;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.nifi.accumulo.controllerservices.AccumuloService;
import org.apache.nifi.accumulo.controllerservices.MockAccumuloService;

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

public class PutRecordIT {

    public static final String DEFAULT_COLUMN_FAMILY = "family1";

    /**
     * Though deprecated in 2.0 it still functions very well
     */
    private static MiniAccumuloCluster accumulo;

    private TestRunner getTestRunner(String table, String columnFamily) {
        final TestRunner runner = TestRunners.newTestRunner(PutAccumuloRecord.class);
        runner.enforceReadStreamsClosed(false);
        runner.setProperty(PutAccumuloRecord.TABLE_NAME, table);
        runner.setProperty(PutAccumuloRecord.COLUMN_FAMILY, columnFamily);
        return runner;
    }




    @BeforeClass
    public static void setupInstance() throws IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
        Path tempDirectory = Files.createTempDirectory("acc"); // JUnit and Guava supply mechanisms for creating temp directories
        accumulo = new MiniAccumuloCluster(tempDirectory.toFile(), "password");
        accumulo.start();
    }

    private Set<Key> generateTestData(TestRunner runner, boolean valueincq, String delim, String cv) throws IOException {

        final MockRecordParser parser = new MockRecordParser();
        try {
            runner.addControllerService("parser", parser);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(parser);
        runner.setProperty(PutAccumuloRecord.RECORD_READER_FACTORY, "parser");

        long ts = System.currentTimeMillis();

        parser.addSchemaField("id", RecordFieldType.STRING);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.STRING);
        parser.addSchemaField("timestamp", RecordFieldType.LONG);

        Set<Key> expectedKeys = new HashSet<>();
        ColumnVisibility colViz = new ColumnVisibility();
        if (null != cv)
            colViz = new ColumnVisibility(cv);
        Random random = new Random();
        for (int x = 0; x < 5; x++) {
            //final int row = random.nextInt(10000000);
            final String row = UUID.randomUUID().toString();
            final String cf = UUID.randomUUID().toString();
            final String cq = UUID.randomUUID().toString();
            Text keyCq = new Text("name");
            if (valueincq){
                if (null != delim && !delim.isEmpty())
                    keyCq.append(delim.getBytes(),0,delim.length());
                keyCq.append(cf.getBytes(),0,cf.length());
            }
            expectedKeys.add(new Key(new Text(row), new Text("family1"), keyCq, colViz,ts));
            keyCq = new Text("code");
            if (valueincq){
                if (null != delim && !delim.isEmpty())
                    keyCq.append(delim.getBytes(),0,delim.length());
                keyCq.append(cq.getBytes(),0,cq.length());
            }
            expectedKeys.add(new Key(new Text(row), new Text("family1"), keyCq, colViz, ts));
            parser.addRecord(row, cf, cq, ts);
        }

        return expectedKeys;
    }

    void verifyKey(String tableName, Set<Key> expectedKeys, Authorizations auths) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (null == auths)
            auths = new Authorizations();
        try(BatchScanner scanner = accumulo.createAccumuloClient("root", new PasswordToken("password")).createBatchScanner(tableName,auths,1)) {
            List<Range> ranges = new ArrayList<>();
            ranges.add(new Range());
            scanner.setRanges(ranges);
            for (Map.Entry<Key, Value> kv : scanner) {
                Assert.assertTrue(kv.getKey() + " not in expected keys",expectedKeys.remove(kv.getKey()));
            }
        }
        Assert.assertEquals(0, expectedKeys.size());

    }

    private void basicPutSetup(boolean valueincq) throws Exception {
        basicPutSetup(valueincq,null,null,null,false);
    }

    private void basicPutSetup(boolean valueincq, final String delim) throws Exception {
        basicPutSetup(valueincq,delim,null,null,false);
    }

    private void basicPutSetup(boolean valueincq,String delim, String auths, Authorizations defaultVis, boolean deletes) throws Exception {
        String tableName = UUID.randomUUID().toString();
        tableName=tableName.replace("-","a");
        if (null != defaultVis)
            accumulo.createAccumuloClient("root", new PasswordToken("password")).securityOperations().changeUserAuthorizations("root",defaultVis);

        TestRunner runner = getTestRunner(tableName, DEFAULT_COLUMN_FAMILY);
        runner.setProperty(PutAccumuloRecord.CREATE_TABLE, "True");
        runner.setProperty(PutAccumuloRecord.ROW_FIELD_NAME, "id");
        runner.setProperty(PutAccumuloRecord.COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY);
        runner.setProperty(PutAccumuloRecord.TIMESTAMP_FIELD, "timestamp");
        if (valueincq) {
            if (null != delim){
                runner.setProperty(PutAccumuloRecord.FIELD_DELIMITER, delim);
            }
            runner.setProperty(PutAccumuloRecord.RECORD_IN_QUALIFIER, "True");
        }
        if (null != defaultVis){
            runner.setProperty(PutAccumuloRecord.DEFAULT_VISIBILITY, auths);
        }
        AccumuloService client = MockAccumuloService.getService(runner,accumulo.getZooKeepers(),accumulo.getInstanceName(),"root","password");
        Set<Key> expectedKeys = generateTestData(runner,valueincq,delim, auths);
        runner.enqueue("Test".getBytes("UTF-8")); // This is to coax the processor into reading the data in the reader.l
        runner.run();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PutAccumuloRecord.REL_SUCCESS);
        Assert.assertTrue("Wrong count", results.size() == 1);
        verifyKey(tableName, expectedKeys, defaultVis);
        if (deletes){
            runner.setProperty(PutAccumuloRecord.DELETE_KEY, "true");
            runner.enqueue("Test".getBytes("UTF-8")); // This is to coax the processor into reading the data in the reader.l
            runner.run();
            runner.getFlowFilesForRelationship(PutAccumuloRecord.REL_SUCCESS);
            verifyKey(tableName, new HashSet<>(), defaultVis);
        }

    }




    @Test
    public void testByteEncodedPut() throws Exception {
        basicPutSetup(false);
    }

    @Test
    public void testByteEncodedPutThenDelete() throws Exception {
        basicPutSetup(true,null,"A&B",new Authorizations("A","B"),true);
    }


    @Test
    public void testByteEncodedPutCq() throws Exception {
        basicPutSetup(true);
    }

    @Test
    public void testByteEncodedPutCqDelim() throws Exception {
        basicPutSetup(true,"\u0000");
    }

    @Test
    public void testByteEncodedPutCqWithVis() throws Exception {
        basicPutSetup(true,null,"A&B",new Authorizations("A","B"),false);
    }
}
