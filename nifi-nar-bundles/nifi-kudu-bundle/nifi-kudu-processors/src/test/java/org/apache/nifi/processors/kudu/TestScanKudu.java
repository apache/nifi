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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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
        flowFile.assertContentEquals("[{\"key\":\"1\",\"int16\":\"20\",\"int32\":\"300\",\"int64\":"
            + "\"4000\",\"binary\":\"3759\",\"string\":\"stringValue\",\"bool\":\"true\",\"float\":\"1.5\","
            + "\"double\":\"10.28\",\"unixtime_micros\":\"" + timestamp + "\",\"decimal\":\"3.1415\"}]");
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
        assertEquals("[{\"key\":\"val1\"}]", new String(files.get(0).toByteArray()));
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
        flowFile.assertContentEquals("[{\"key\":\"val1\",\"key1\":\"val1\"}]");
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
        flowFile.assertContentEquals("[{\"key1\":\"val2\"}]");

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
        flowFile.assertContentEquals("[{\"key1\":\"val2\"}]");
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
        flowFile.assertContentEquals("[{\"key1\":\"val2\"}]");
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

    @Test
    public void testPredicateEqualsFalse() throws Exception {
        List<Boolean> values = Arrays.asList(false, true, false);

        String predicate = "value=false";
        String expectedContent = "[{\"key\":\"1\",\"value\":\"false\"},\n{\"key\":\"3\",\"value\":\"false\"}]";

        testPredicate(predicate, expectedContent, Type.BOOL, (row, value) -> row.addBoolean("value", value), values);
    }

    @Test
    public void testPredicateGreaterThanInteger() throws Exception {
        List<Integer> values = Arrays.asList(10, 15, 10, 12);

        String predicate = "value>10";
        String expectedContent = "[{\"key\":\"2\",\"value\":\"15\"},\n{\"key\":\"4\",\"value\":\"12\"}]";

        testPredicate(predicate, expectedContent, Type.INT32, (row, value) -> row.addInt("value", value), values);
    }

    @Test
    public void testPredicateTimestamp() throws Exception {
        List<Timestamp> values = Arrays.asList(new Timestamp(1570138043));

        String predicate = "value=1570138043000";
        String expectedContent = "[{\"key\":\"1\",\"value\":\"1570138043000\"}]";

        testPredicate(predicate, expectedContent, Type.UNIXTIME_MICROS, (row, value) -> row.addTimestamp("value", value), values);
    }

    @Test
    public void testPredicateGreaterOrEqualsInteger() throws Exception {
        List<Integer> values = Arrays.asList(9, 15, 8, 12);

        String predicate = "value>=10";
        String expectedContent = "[{\"key\":\"2\",\"value\":\"15\"},\n{\"key\":\"4\",\"value\":\"12\"}]";

        testPredicate(predicate, expectedContent, Type.INT32, (row, value) -> row.addInt("value", value), values);
    }

    @Test
    public void testPredicateLowerThanOrEqualsFloat() throws Exception {
        List<Float> values = Arrays.asList(3.231F, 3.232F, 3.233F);

        String predicate = "value<=3.232";
        String expectedContent = "[{\"key\":\"1\",\"value\":\"3.231\"},\n{\"key\":\"2\",\"value\":\"3.232\"}]";

        testPredicate(predicate, expectedContent, Type.FLOAT, (row, value) -> row.addFloat("value", value), values);
    }

    @Test
    public void testPredicateLowerThanOrEqualsDouble() throws Exception {
        List<Double> values = Arrays.asList(3.2312092090, 3.232090192, 3.23319829892);

        String predicate = "value<=3.231392090";
        String expectedContent = "[{\"key\":\"1\",\"value\":\"3.231209209\"}]";

        testPredicate(predicate, expectedContent, Type.DOUBLE, (row, value) -> row.addDouble("value", value), values);
    }

    @Test
    public void testPredicateLowerOrEqualsByte() throws Exception {
        List<Byte> values = Arrays.asList((byte)11, (byte)9, (byte)10, (byte)8);

        String predicate = "value<=10";
        String expectedContent = "[{\"key\":\"2\",\"value\":\"9\"},\n{\"key\":\"3\",\"value\":\"10\"},\n{\"key\":\"4\",\"value\":\"8\"}]";

        testPredicate(predicate, expectedContent, Type.INT8, (row, value) -> row.addByte("value", value), values);
    }

    @Test
    public void testPredicateLowerThanShort() throws Exception {
        List<Short> values = Arrays.asList((short)11, (short)9, (short)10, (short)-8);

        String predicate = "value<10";
        String expectedContent = "[{\"key\":\"2\",\"value\":\"9\"},\n{\"key\":\"4\",\"value\":\"-8\"}]";

        testPredicate(predicate, expectedContent, Type.INT16, (row, value) -> row.addShort("value", value), values);
    }

    @Test
    public void testPredicateEqualsLong() throws Exception {
        List<Long> values = Arrays.asList(11L, 9L, 10L, 8L);

        String predicate = "value=10";
        String expectedContent = "[{\"key\":\"3\",\"value\":\"10\"}]";

        testPredicate(predicate, expectedContent, Type.INT64, (row, value) -> row.addLong("value", value), values);
    }

    @Test
    public void testPredicatesEqualsBinary() throws Exception {
        List<byte[]> values = Arrays.asList(
            new byte[] {127, 10, 28},
            new byte[] {-128, 10, 28},
            new byte[] {0, 10, 28}
        );

        String predicate = "value=7f0a1c";
        String expectedContent = "[{\"key\":\"1\",\"value\":\"7f0a1c\"}]";

        testPredicate(predicate, expectedContent, Type.BINARY, (row, value) -> row.addBinary("value", value), values);
    }

    @Test
    public void testPredicateGreaterOrEqualsBigDecimal() throws Exception {
        List<BigDecimal> values = Arrays.asList(
            new BigDecimal("15643691156541351512356.1264865416"),
            new BigDecimal("69879841523159494156164.3198491561316"),
            new BigDecimal("99165198489191894987456.61561616")
        );

        String predicate = "value>=69879841523159494156164.3198491561316";
        String expectedContent = "[{\"key\":\"2\",\"value\":\"69879841523159494156164.3198491561316\"},\n" +
            "{\"key\":\"3\",\"value\":\"99165198489191894987456.6156161600000\"}]";

        testPredicate(
            predicate,
            expectedContent,
            new ColumnSchema.ColumnSchemaBuilder("value", Type.DECIMAL).key(false).typeAttributes(
                new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                    .precision(38)
                    .scale(13)
                    .build()
            ).build(),
            (row, value) -> row.addDecimal("value", value),
            values
        );
    }

    private <V> void testPredicate(String predicate, String expectedContent, Type valueType,
        BiConsumer<PartialRow, V> valueSetter, List<V> values) throws Exception {
        testPredicate(
            predicate,
            expectedContent,
            new ColumnSchema.ColumnSchemaBuilder("value", valueType).key(false).build(),
            valueSetter,
            values
        );
    }

    private <V> void testPredicate(String predicate, String expectedContent,
        ColumnSchema valueColumnSchema, BiConsumer<PartialRow, V> valueSetter, List<V> values) throws Exception {
        List<ColumnSchema> columns = Arrays.asList(
            new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
            valueColumnSchema
        );

        KuduTable kuduTable = kuduScan.getKuduTable(DEFAULT_TABLE_NAME, columns);
        KuduSession kuduSession = kuduScan.kuduClient.newSession();
        AtomicReference<Integer> key = new AtomicReference<>(1);
        values.forEach(value -> {
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("key", key.getAndSet(key.get() + 1));
            valueSetter.accept(row, value);

            try {
                kuduSession.apply(insert);
            } catch (KuduException e) {
                throw new RuntimeException(e);
            }
        });
        kuduSession.close();

        runner.setProperty(ScanKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        runner.setProperty(ScanKudu.PREDICATES, predicate);

        runner.setValidateExpressionUsage(false);

        runner.setIncomingConnection(false);
        runner.enqueue();
        runner.run(1, false);

        runner.assertTransferCount(ScanKudu.REL_FAILURE, 0);
        runner.assertTransferCount(ScanKudu.REL_SUCCESS, 1);
        runner.assertTransferCount(ScanKudu.REL_ORIGINAL, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ScanKudu.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(expectedContent);
    }
}