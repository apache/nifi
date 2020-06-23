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
package org.apache.nifi.controller.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.apache.kudu.util.DecimalUtil;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ITestKuduLookupService {

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
    );
    private TestRunner testRunner;
    private KuduLookupService kuduLookupService;

    private final java.sql.Date today = new java.sql.Date(System.currentTimeMillis());
    private final java.sql.Date pastDate = java.sql.Date.valueOf("2019-01-01");
    private long nowMillis = System.currentTimeMillis();

    public static class SampleProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }
    }

    @Before
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(SampleProcessor.class);
        testRunner.setValidateExpressionUsage(false);
        final String tableName = "table1";

        KuduClient client =  harness.getClient();
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("binary", Type.BINARY).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bool", Type.BOOL).build());
        columns.add(new ColumnSchema
                .ColumnSchemaBuilder("decimal", Type.DECIMAL)
                .typeAttributes(DecimalUtil.typeAttributes(DecimalUtil.MAX_DECIMAL64_PRECISION, 1))
                .build()
        );
        columns.add(new ColumnSchema.ColumnSchemaBuilder("double", Type.DOUBLE).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("float", Type.FLOAT).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("int8", Type.INT8).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("int16", Type.INT16).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("int32", Type.INT32).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("int64", Type.INT64).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("unixtime_micros", Type.UNIXTIME_MICROS).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("sql_date", Type.DATE).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("varchar_3", Type.VARCHAR).typeAttributes(
                new ColumnTypeAttributes.ColumnTypeAttributesBuilder().length(3).build()
        ).build());
        Schema schema = new Schema(columns);

        CreateTableOptions opts = new CreateTableOptions().setRangePartitionColumns(Collections.singletonList("string"));
        client.createTable(tableName, schema, opts);

        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();

        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString("string", "string1");
        row.addBinary("binary", "binary1".getBytes());
        row.addBoolean("bool",true);
        row.addDecimal("decimal", BigDecimal.valueOf(0.1));
        row.addDouble("double",0.2);
        row.addFloat("float",0.3f);
        row.addByte("int8", (byte) 1);
        row.addShort("int16", (short) 2);
        row.addInt("int32",3);
        row.addLong("int64",4L);
        row.addTimestamp("unixtime_micros", new Timestamp(nowMillis));
        row.addDate("sql_date", today);
        row.addVarchar("varchar_3", "SFO");
        session.apply(insert);

        insert = table.newInsert();
        row = insert.getRow();
        row.addString("string", "string2");
        row.addBinary("binary", "binary2".getBytes());
        row.addBoolean("bool",false);
        row.addDecimal("decimal", BigDecimal.valueOf(0.1));
        row.addDouble("double",1.2);
        row.addFloat("float",1.3f);
        row.addByte("int8", (byte) 11);
        row.addShort("int16", (short) 12);
        row.addInt("int32",13);
        row.addLong("int64",14L);
        row.addTimestamp("unixtime_micros", new Timestamp(nowMillis+(1000L * 60 * 60 * 24 * 365))); //+ 1 year
        row.addDate("sql_date", pastDate);
        row.addVarchar("varchar_3", "SJC");
        session.apply(insert);

        session.close();

        kuduLookupService = new KuduLookupService();
        testRunner.addControllerService("kuduLookupService", kuduLookupService);
        testRunner.setProperty(kuduLookupService, KuduLookupService.KUDU_MASTERS, "testLocalHost:7051");
        testRunner.setProperty(kuduLookupService, KuduLookupService.KUDU_REPLICA_SELECTION, KuduLookupService.LEADER_ONLY);
        testRunner.setProperty(kuduLookupService, KuduLookupService.TABLE_NAME, tableName);
        kuduLookupService.kuduClient = client;
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalid_key() {
        testRunner.setProperty(kuduLookupService, KuduLookupService.RETURN_COLUMNS, "*");

        testRunner.enableControllerService(kuduLookupService);

        Map<String,Object> map = new HashMap<>();
        map.put("invalid", "invalid key");
        kuduLookupService.lookup(map);
    }
    @Test
    public void row_not_found() {
        testRunner.setProperty(kuduLookupService, KuduLookupService.RETURN_COLUMNS, "*");

        testRunner.enableControllerService(kuduLookupService);

        Map<String,Object> map = new HashMap<>();
        map.put("string", "key not found");
        Optional<Record> result = kuduLookupService.lookup(map);
        assertFalse(result.isPresent());
    }

    @Test
    public void single_key() {
        testRunner.setProperty(kuduLookupService, KuduLookupService.RETURN_COLUMNS, "*");

        testRunner.enableControllerService(kuduLookupService);

        Map<String,Object> map = new HashMap<>();
        map.put("string", "string1");
        Record result = kuduLookupService.lookup(map).get();
        validateRow1(result);
    }
    @Test
    public void multi_key() {
        testRunner.setProperty(kuduLookupService, KuduLookupService.RETURN_COLUMNS, "*");

        testRunner.enableControllerService(kuduLookupService);

        Map<String,Object> map = new HashMap<>();
        map.put("string", "string1");
        map.put("binary", "binary1".getBytes());
        map.put("bool",true);
        map.put("decimal", BigDecimal.valueOf(0.1));
        map.put("double",0.2);
        map.put("float",0.3f);
        map.put("int8", (byte) 1);
        map.put("int16", (short) 2);
        map.put("int32",3);
        map.put("int64",4L);
        map.put("unixtime_micros", new Timestamp(nowMillis));
        map.put("sql_date", today);
        map.put("varchar_3", "SFO");
        Record result = kuduLookupService.lookup(map).get();
        validateRow1(result);
    }
    @Test
    public void specific_return_columns() {
        testRunner.setProperty(kuduLookupService, KuduLookupService.RETURN_COLUMNS, "binary,bool");

        testRunner.enableControllerService(kuduLookupService);

        Map<String,Object> map = new HashMap<>();
        map.put("string", "string1");
        Record result = kuduLookupService.lookup(map).get();

        assertEquals(2,result.getValues().length);

        assertEquals(Base64.getEncoder().encodeToString("binary1".getBytes()), result.getValue("binary"));
        assertEquals(true, result.getAsBoolean("bool"));
    }
    private void validateRow1(Record result){
        assertEquals("string1", result.getAsString("string"));
        assertEquals(Base64.getEncoder().encodeToString("binary1".getBytes()), result.getValue("binary"));
        assertEquals(true, result.getAsBoolean("bool"));
        assertEquals(BigDecimal.valueOf(0.1), result.getValue("decimal"));
        assertEquals(0.2, result.getAsDouble("double"),0);
        assertEquals(0.3f, result.getAsFloat("float"),0);
        assertEquals((byte)1, result.getValue("int8"));
        assertEquals((short)2, result.getValue("int16"));
        assertEquals(3, (int)result.getAsInt("int32"));
        assertEquals(4L, (long)result.getAsLong("int64"));
        assertEquals(new Timestamp(nowMillis), result.getValue("unixtime_micros"));
        assertEquals("SFO", result.getValue("varchar_3"));
        assertEquals(today.toString(), result.getValue("sql_date").toString());
    }

}
