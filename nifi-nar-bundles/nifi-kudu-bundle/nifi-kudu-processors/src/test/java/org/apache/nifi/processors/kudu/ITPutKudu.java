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

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class ITPutKudu {

    public static final String DEFAULT_TABLE_NAME = "Nifi-Kudu-Table";

    public static final Timestamp NOW = new Timestamp(System.currentTimeMillis());

    // The KuduTestHarness automatically starts and stops a real Kudu cluster
    // when each test is run. Kudu persists its on-disk state in a temporary
    // directory under a location defined by the environment variable TEST_TMPDIR
    // if set, or under /tmp otherwise. That cluster data is deleted on
    // successful exit of the test. The cluster output is logged through slf4j.
    @Rule
    public KuduTestHarness harness = new KuduTestHarness(
            new MiniKuduCluster.MiniKuduClusterBuilder()
                .addMasterServerFlag("--use_hybrid_clock=false")
                .addTabletServerFlag("--use_hybrid_clock=false")
                .enableKerberos()
                .principal("oryx")
    );

    private TestRunner testRunner;

    private PutKudu processor;

    private MockRecordParser readerFactory;

    private final java.sql.Date today = new java.sql.Date(System.currentTimeMillis());

    @Before
    public void setUp() throws Exception {
        processor = new PutKudu();
        testRunner = TestRunners.newTestRunner(processor);
        createKuduTable();
        setUpTestRunner(testRunner);
    }

    @After
    public void tearDown() {
        testRunner = null;
    }

    private void setUpTestRunner(TestRunner testRunner) {
        testRunner.setProperty(PutKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        testRunner.setProperty(PutKudu.KUDU_MASTERS, harness.getMasterAddressesAsString());
        testRunner.setProperty(PutKudu.SKIP_HEAD_LINE, "false");
        testRunner.setProperty(PutKudu.IGNORE_NULL, "true");
        testRunner.setProperty(PutKudu.LOWERCASE_FIELD_NAMES, "false");
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");
        testRunner.setProperty(PutKudu.INSERT_OPERATION, OperationType.INSERT_IGNORE.toString());
        testRunner.setProperty(PutKudu.KERBEROS_PRINCIPAL, "test-user");
        testRunner.setProperty(PutKudu.KERBEROS_PASSWORD, "test-user");
        testRunner.setProperty(PutKudu.KUDU_SASL_PROTOCOL_NAME, "oryx");
    }

    private void createKuduTable() throws KuduException {
        KuduClient client =  harness.getClient();
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("stringval", Type.STRING).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("varcharval", Type.VARCHAR).typeAttributes(
                new ColumnTypeAttributes.ColumnTypeAttributesBuilder().length(256).build()
        ).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("num32val", Type.INT32).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("timestampval", Type.UNIXTIME_MICROS).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("dateval", Type.DATE).build());
        Schema schema = new Schema(columns);
        CreateTableOptions opts = new CreateTableOptions()
            .addHashPartitions(Collections.singletonList("id"), 4);
        client.createTable(DEFAULT_TABLE_NAME, schema, opts);
    }

    private void createRecordReader(int numOfRecord) throws InitializationException {
        readerFactory = new MockRecordParser();
        readerFactory.addSchemaField("id", RecordFieldType.INT);
        readerFactory.addSchemaField("stringVal", RecordFieldType.STRING);
        readerFactory.addSchemaField("varcharval", RecordFieldType.STRING);
        readerFactory.addSchemaField("num32Val", RecordFieldType.INT);
        readerFactory.addSchemaField("timestampVal", RecordFieldType.TIMESTAMP);
        readerFactory.addSchemaField("dateval", RecordFieldType.DATE);
        // Add two extra columns to test handleSchemaDrift = true.
        readerFactory.addSchemaField("doubleVal", RecordFieldType.DOUBLE);
        readerFactory.addSchemaField("floatVal", RecordFieldType.FLOAT);

        for (int i = 0; i < numOfRecord; i++) {
            readerFactory.addRecord(i, "val_" + i, "varchar_val_" + i, 1000 + i, NOW, today, 100.88 + i, 100.88 + i);
        }

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
    }

    @Test
    public void testWriteKudu() throws IOException, InitializationException {
        final int recordCount = 100;
        final int numFlowFiles = 5;
        createRecordReader(recordCount);

        final String filename = "testWriteKudu-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        // Use values to ensure multiple batches and multiple flow files per-trigger
        testRunner.setProperty(PutKudu.BATCH_SIZE, "10");
        testRunner.setProperty(PutKudu.FLOWFILE_BATCH_SIZE, "2");

        // Set the operation type.
        flowFileAttributes.put("kudu.operation.type", "upsert");
        testRunner.setProperty(PutKudu.INSERT_OPERATION, "${kudu.operation.type}");

        // Don't ignore null values.
        flowFileAttributes.put("kudu.ignore.null", "false");
        testRunner.setProperty(PutKudu.IGNORE_NULL, "${kudu.ignore.null}");

        // Enable lowercase handling.
        flowFileAttributes.put("kudu.lowercase.field.names", "true");
        testRunner.setProperty(PutKudu.LOWERCASE_FIELD_NAMES, "${kudu.lowercase.field.names}");

        // Enable schema drift handling.
        flowFileAttributes.put("kudu.handle.schema.drift", "true");
        testRunner.setProperty(PutKudu.HANDLE_SCHEMA_DRIFT, "${kudu.handle.schema.drift}");

        // Increase the thread count to better simulate a production environment
        testRunner.setThreadCount(4);

        // Trigger the flow
        IntStream.range(0, numFlowFiles).forEach(i ->
            testRunner.enqueue("trigger", flowFileAttributes));
        testRunner.run(numFlowFiles);
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, numFlowFiles);

        // verify the successful flow file has the expected content & attributes
        final MockFlowFile mockFlowFile =
            testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), filename);
        mockFlowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "100");
        mockFlowFile.assertContentEquals("trigger");

        // verify we generated provenance events
        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        Assert.assertEquals(numFlowFiles, provEvents.size());

        // verify it was a SEND event with the correct URI
        final ProvenanceEventRecord provEvent = provEvents.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());

        KuduClient client = harness.getClient();
        KuduTable kuduTable = client.openTable(DEFAULT_TABLE_NAME);

        // Verify the extra field was added.
        Assert.assertEquals(8, kuduTable.getSchema().getColumnCount());
        Assert.assertTrue(kuduTable.getSchema().hasColumn("doubleval"));
        Assert.assertTrue(kuduTable.getSchema().hasColumn("floatval"));

        // Verify Kudu record count.
        KuduScanner scanner = client.newScannerBuilder(kuduTable).build();
        int count = 0;
        for (RowResult row : scanner) {
            Assert.assertEquals(NOW, row.getTimestamp("timestampval"));
            // Comparing string representations, because java.sql.Date does not override
            // java.util.Date.equals method and therefore compares milliseconds instead of
            // comparing dates, even though java.sql.Date is supposed to ignore time
            Assert.assertEquals(today.toString(), row.getDate("dateval").toString());
            count++;
        }
        Assert.assertEquals(recordCount, count);
    }
}
