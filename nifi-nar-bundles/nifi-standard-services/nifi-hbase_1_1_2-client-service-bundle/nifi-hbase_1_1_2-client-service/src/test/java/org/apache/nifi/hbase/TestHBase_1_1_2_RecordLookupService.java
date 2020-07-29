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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

public class TestHBase_1_1_2_RecordLookupService {

    static final String TABLE_NAME = "guids";
    static final String ROW = "row1";
    static final String COLS = "cf1:cq1,cf2:cq2";

    private TestRunner runner;
    private HBase_1_1_2_RecordLookupService lookupService;
    private MockHBaseClientService clientService;
    private TestRecordLookupProcessor testLookupProcessor;

    @Before
    public void before() throws Exception {
        testLookupProcessor = new TestRecordLookupProcessor();
        runner = TestRunners.newTestRunner(testLookupProcessor);

        // setup mock HBaseClientService
        final Table table = Mockito.mock(Table.class);
        when(table.getName()).thenReturn(TableName.valueOf(TABLE_NAME));

        final KerberosProperties kerberosProperties = new KerberosProperties(new File("src/test/resources/krb5.conf"));
        clientService = new MockHBaseClientService(table, "family", kerberosProperties);
        runner.addControllerService("clientService", clientService);
        runner.setProperty(clientService, HBase_1_1_2_ClientService.HADOOP_CONF_FILES, "src/test/resources/hbase-site.xml");
        runner.enableControllerService(clientService);

        // setup HBase LookupService
        lookupService = new HBase_1_1_2_RecordLookupService();
        runner.addControllerService("lookupService", lookupService);
        runner.setProperty(lookupService, HBase_1_1_2_RecordLookupService.HBASE_CLIENT_SERVICE, "clientService");
        runner.setProperty(lookupService, HBase_1_1_2_RecordLookupService.TABLE_NAME, TABLE_NAME);
        runner.enableControllerService(lookupService);

        // setup test processor
        runner.setProperty(TestRecordLookupProcessor.HBASE_LOOKUP_SERVICE, "lookupService");
        runner.setProperty(TestRecordLookupProcessor.HBASE_ROW, ROW);
    }

    @Test
    public void testSuccessfulLookupAllColumns() {
        // setup some staged data in the mock client service
        final Map<String,String> cells = new HashMap<>();
        cells.put("cq1", "v1");
        cells.put("cq2", "v2");
        clientService.addResult("row1", cells, System.currentTimeMillis());

        // run the processor
        runner.enqueue("trigger flow file");
        runner.run();
        runner.assertAllFlowFilesTransferred(TestRecordLookupProcessor.REL_SUCCESS);

        final List<Record> records = testLookupProcessor.getLookedupRecords();
        Assert.assertNotNull(records);
        Assert.assertEquals(1, records.size());

        final Record record = records.get(0);
        Assert.assertEquals("v1", record.getAsString("cq1"));
        Assert.assertEquals("v2", record.getAsString("cq2"));
    }

    @Test
    public void testLookupWithNoResults() {
        // run the processor
        runner.enqueue("trigger flow file");
        runner.run();
        runner.assertAllFlowFilesTransferred(TestRecordLookupProcessor.REL_FAILURE);

        final List<Record> records = testLookupProcessor.getLookedupRecords();
        Assert.assertNotNull(records);
        Assert.assertEquals(0, records.size());
    }

    @Test
    public void testLookupWhenMissingRowKeyCoordinate() {
        runner.removeProperty(TestRecordLookupProcessor.HBASE_ROW);

        // run the processor
        runner.enqueue("trigger flow file");
        runner.run();
        runner.assertAllFlowFilesTransferred(TestRecordLookupProcessor.REL_FAILURE);

        final List<Record> records = testLookupProcessor.getLookedupRecords();
        Assert.assertNotNull(records);
        Assert.assertEquals(0, records.size());
    }
}
