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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TestHBase_1_1_2_ListLookupService {

    static final String TABLE_NAME = "guids";

    private TestRunner runner;
    private HBase_1_1_2_ListLookupService lookupService;
    private MockHBaseClientService clientService;
    private NoOpProcessor processor;

    @Before
    public void before() throws Exception {
        processor = new NoOpProcessor();
        runner = TestRunners.newTestRunner(processor);

        // setup mock HBaseClientService
        final Table table = Mockito.mock(Table.class);
        when(table.getName()).thenReturn(TableName.valueOf(TABLE_NAME));

        final KerberosProperties kerberosProperties = new KerberosProperties(new File("src/test/resources/krb5.conf"));
        clientService = new MockHBaseClientService(table, "family", kerberosProperties);
        runner.addControllerService("clientService", clientService);
        runner.setProperty(clientService, HBase_1_1_2_ClientService.HADOOP_CONF_FILES, "src/test/resources/hbase-site.xml");
        runner.enableControllerService(clientService);

        // setup HBase LookupService
        lookupService = new HBase_1_1_2_ListLookupService();
        runner.addControllerService("lookupService", lookupService);
        runner.setProperty(lookupService, HBase_1_1_2_ListLookupService.HBASE_CLIENT_SERVICE, "clientService");
        runner.setProperty(lookupService, HBase_1_1_2_ListLookupService.TABLE_NAME, TABLE_NAME);
        runner.enableControllerService(lookupService);
    }

    private Optional<List> setupAndRun() throws Exception {
        // setup some staged data in the mock client service
        final Map<String,String> cells = new HashMap<>();
        cells.put("cq1", "v1");
        cells.put("cq2", "v2");
        clientService.addResult("row1", cells, System.currentTimeMillis());

        Map<String, Object> lookup = new HashMap<>();
        lookup.put("rowKey", "row1");

        return lookupService.lookup(lookup);
    }

    @Test
    public void testLookupKeyList() throws Exception {
        Optional<List> results = setupAndRun();

        assertTrue(results.isPresent());
        List result = results.get();
        assertTrue(result.size() == 2);
        assertTrue(result.contains("cq1"));
        assertTrue(result.contains("cq2"));
    }

    @Test
    public void testLookupValueList() throws Exception {
        runner.disableControllerService(lookupService);
        runner.setProperty(lookupService, HBase_1_1_2_ListLookupService.RETURN_TYPE, HBase_1_1_2_ListLookupService.VALUE_LIST);
        runner.enableControllerService(lookupService);
        Optional<List> results = setupAndRun();

        assertTrue(results.isPresent());
        List result = results.get();
        assertTrue(result.size() == 2);
        assertTrue(result.contains("v1"));
        assertTrue(result.contains("v2"));
    }

    // Processor that does nothing just so we can create a TestRunner
    private static class NoOpProcessor extends AbstractProcessor {

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.emptyList();
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }
    }

}
