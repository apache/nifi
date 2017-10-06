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

import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Ignore("This is an integration test. It requires a table with the name guids and a column family named property. Example" +
        "Docker Compose configuration:" +
        "  hbase-docker:\n" +
        "    container_name: hbase-docker\n" +
        "    image: \"dajobe/hbase\"\n" +
        "    ports:\n" +
        "      - 16010:16010\n" +
        "      - 2181:2181\n" +
        "      - 60000:60000\n" +
        "      - 60010:60010\n" +
        "      - 60020:60020\n" +
        "      - 60030:60030\n" +
        "      - 9090:9090\n" +
        "      - 9095:9095\n" +
        "    hostname: hbase-docker")
public class TestHBase_1_1_2_LookupService {

    TestRunner runner;
    HBase_1_1_2_LookupService service;

    static final byte[] FAM = "property".getBytes();
    static final byte[] QUAL1 = "uuid".getBytes();
    static final byte[] QUAL2 = "uuid2".getBytes();

    static final String TABLE_NAME = "guids";

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestLookupProcessor.class);
        service = new HBase_1_1_2_LookupService();
        runner.addControllerService("lookupService", service);
        runner.setProperty(service, HBaseClientService.ZOOKEEPER_QUORUM, "hbase-docker");
        runner.setProperty(service, HBaseClientService.ZOOKEEPER_CLIENT_PORT, "2181");
        runner.setProperty(service, HBaseClientService.ZOOKEEPER_ZNODE_PARENT, "/hbase");
        runner.setProperty(service, HBaseClientService.HBASE_CLIENT_RETRIES, "3");
        runner.setProperty(service, HBase_1_1_2_LookupService.TABLE_NAME, TABLE_NAME);
        runner.setProperty(service, HBase_1_1_2_LookupService.RETURN_CFS, "property");
        runner.setProperty(service, HBase_1_1_2_LookupService.CHARSET, "UTF-8");
    }

    @After
    public void after() throws Exception {
        service.shutdown();
    }

    @Test
    public void testSingleLookup() throws Exception {
        runner.enableControllerService(service);
        runner.assertValid(service);

        String uuid = UUID.randomUUID().toString();
        String rowKey = String.format("x-y-z-%d", Calendar.getInstance().getTimeInMillis());

        PutColumn column = new PutColumn(FAM, QUAL1, uuid.getBytes());

        service.put(TABLE_NAME, rowKey.getBytes(), Arrays.asList(column));

        Map<String, String> lookup = new HashMap<>();
        lookup.put("rowKey", rowKey);
        Optional result = service.lookup(lookup);

        Assert.assertNotNull("Result was null", result);
        Assert.assertNotNull("The value was null", result.get());
        MapRecord record = (MapRecord)result.get();
        Assert.assertEquals("The value didn't match.", uuid, record.getAsString("uuid"));
    }


    @Test
    public void testMultipleLookup() throws Exception {
        runner.enableControllerService(service);
        runner.assertValid(service);

        String uuid = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        String rowKey = String.format("x-y-z-%d", Calendar.getInstance().getTimeInMillis());

        List<PutColumn> columns = new ArrayList<>();
        columns.add(new PutColumn(FAM, QUAL1, uuid.getBytes()));
        columns.add(new PutColumn(FAM, QUAL2, uuid2.getBytes()));

        service.put(TABLE_NAME, rowKey.getBytes(), columns);

        Map<String, String> lookup = new HashMap<>();
        lookup.put("rowKey", rowKey);
        Optional result = service.lookup(lookup);

        Assert.assertNotNull("Result was null", result);
        Assert.assertNotNull("The value was null", result.get());
        Assert.assertTrue("Wrong type.", result.get() instanceof MapRecord);
        MapRecord record = (MapRecord)result.get();
        Assert.assertEquals("Qual 1 was wrong", uuid, record.getAsString("uuid"));
        Assert.assertEquals("Qual 2 was wrong", uuid2, record.getAsString("uuid2"));
    }
}
