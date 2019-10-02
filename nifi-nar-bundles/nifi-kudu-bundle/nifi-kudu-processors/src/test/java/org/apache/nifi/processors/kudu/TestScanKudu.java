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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
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
    public void testInvalidKuduTableName() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val1");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, "${table1}");
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
    public void testInvalidPredicateReferencingNonExistingColumn() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val1");

        String patternReferencingNonExistingColumn = "column1=val1";

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, patternReferencingNonExistingColumn);
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "column1");
        runner.setValidateExpressionUsage(false);
        runner.enqueue("trigger flow file");
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 1);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);
    }

    @Test
    public void testInvalidPredicatesPattern() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val1");

        String invalidPattern = "key1==val1";

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, invalidPattern);
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS,  "column1");
        runner.setValidateExpressionUsage(false);
        runner.enqueue("trigger flow file");

        runner.assertNotValid();
        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 0);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 0);
    }

    @Test
    public void testScanKuduWithMultipleTypes() throws KuduException {
        List<ColumnSchema> columns = Arrays.asList(
            new ColumnSchema.ColumnSchemaBuilder("key", Type.INT8).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.INT16.getName(), Type.INT16).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.INT32.getName(), Type.INT32).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.INT64.getName(), Type.INT64).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.BINARY.getName(), Type.BINARY).key(false).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.STRING.getName(), Type.STRING).key(false).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.BOOL.getName(), Type.BOOL).key(false).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.FLOAT.getName(), Type.FLOAT).key(false).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.DOUBLE.getName(), Type.DOUBLE).key(false).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.UNIXTIME_MICROS.getName(), Type.UNIXTIME_MICROS).key(false).build(),
            new ColumnSchema.ColumnSchemaBuilder(Type.DECIMAL.getName(), Type.DECIMAL).typeAttributes(
                new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                    .precision(20)
                    .scale(4)
                    .build()
            ).key(false).build()
        );

        Instant now = Instant.now();

        KuduTable kuduTable = kuduScan.getKuduTable(DEFAULT_TABLE_NAME, columns);
        Insert insert = kuduTable.newInsert();
        PartialRow rows = insert.getRow();
        rows.addByte("key", (byte) 1);
        rows.addShort(Type.INT16.getName(), (short)20);
        rows.addInt(Type.INT32.getName(), 300);
        rows.addLong(Type.INT64.getName(), 4000L);
        rows.addBinary(Type.BINARY.getName(), new byte[]{55, 89});
        rows.addString(Type.STRING.getName(), "stringValue");
        rows.addBoolean(Type.BOOL.getName(), true);
        rows.addFloat(Type.FLOAT.getName(), 1.5F);
        rows.addDouble(Type.DOUBLE.getName(), 10.28);
        rows.addTimestamp(Type.UNIXTIME_MICROS.getName(), Timestamp.from(now));
        rows.addDecimal(Type.DECIMAL.getName(), new BigDecimal("3.1415"));

        KuduSession kuduSession = kuduScan.kuduClient.newSession();
        kuduSession.apply(insert);
        kuduSession.close();

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);

        runner.setIncomingConnection(false);
        runner.enqueue();
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        Object timestamp = ChronoUnit.MICROS.between(Instant.EPOCH, now);
        flowFile.assertContentEquals("[{\"rows\":[{\"key\":\"1\",\"int16\":\"20\",\"int32\":\"300\",\"int64\":"
            + "\"4000\",\"binary\":\"0x3759\",\"string\":\"stringValue\",\"bool\":\"true\",\"float\":\"1.5\","
            + "\"double\":\"10.28\",\"unixtime_micros\":\"" + timestamp + "\",\"decimal\":\"3.1415\"}]}]");
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
    public void testBatchSizeKuduScan() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val2");

        final Map<String, String> rows1 = new HashMap<>();
        rows1.put("key", "val2");
        rows1.put("key1", "val2");

        final Map<String, String> rows2 = new HashMap<>();
        rows2.put("key", "val3");
        rows2.put("key1", "val2");

        final Map<String, String> rows3 = new HashMap<>();
        rows3.put("key", "val4");
        rows3.put("key1", "val2");

        final Map<String, String> rows4 = new HashMap<>();
        rows4.put("key", "val5");
        rows4.put("key1", "val2");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);
        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows1);
        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows2);
        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows3);
        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows4);


        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, "key1=val2");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key,key1");
        runner.setProperty(ScanKudu.BATCH_SIZE, "2");
        runner.setValidateExpressionUsage(false);

        runner.enqueue("trigger flow file");
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 3);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS);
        assertEquals(3, flowFiles.size());
    }

    @Test
    public void testKuduScanWithExpressionLanguage() throws KuduException {
        final Map<String, String> rows = new HashMap<>();
        rows.put("key", "val1");
        rows.put("key1", "val2");

        kuduScan.insertTestRecordsToKuduTable(DEFAULT_TABLE_NAME, rows);

        runner.setProperty(ScanKudu.TABLE_NAME, "${kudu.table}");
        runner.setProperty(ScanKudu.PREDICATES, "${kudu.predicate}");
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key1");
        runner.setProperty(ScanKudu.BATCH_SIZE, "2");

        runner.setValidateExpressionUsage(false);

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("kudu.table", DEFAULT_TABLE_NAME);
        attributes.put("kudu.predicate", "key=val1");

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
        runner.setProperty(ScanKudu.PROJECTED_COLUMNS, "key1");
        runner.setProperty(ScanKudu.BATCH_SIZE, "2");

        runner.setValidateExpressionUsage(false);

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("kudu.table", DEFAULT_TABLE_NAME);
        attributes.put("kudu.predicate", "key=val1,key1=val2");

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