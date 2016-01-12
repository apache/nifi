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

import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.util.StringSerDe;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestGetHBase {

    private TestRunner runner;
    private MockGetHBase proc;
    private MockCacheClient cacheClient;
    private MockHBaseClientService hBaseClient;

    @Before
    public void setup() throws InitializationException {
        proc = new MockGetHBase();
        runner = TestRunners.newTestRunner(proc);

        cacheClient = new MockCacheClient();
        runner.addControllerService("cacheClient", cacheClient);
        runner.enableControllerService(cacheClient);

        hBaseClient = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClient);
        runner.enableControllerService(hBaseClient);

        runner.setProperty(GetHBase.TABLE_NAME, "nifi");
        runner.setProperty(GetHBase.DISTRIBUTED_CACHE_SERVICE, "cacheClient");
        runner.setProperty(GetHBase.HBASE_CLIENT_SERVICE, "hbaseClient");
    }

    @After
    public void cleanup() {
        final File file = proc.getStateFile();
        if (file.exists()) {
            file.delete();
        }
        Assert.assertFalse(file.exists());
    }

    @Test
    public void testColumnsValidation() {
        runner.assertValid();

        runner.setProperty(GetHBase.COLUMNS, "cf1:cq1");
        runner.assertValid();

        runner.setProperty(GetHBase.COLUMNS, "cf1");
        runner.assertValid();

        runner.setProperty(GetHBase.COLUMNS, "cf1:cq1,cf2:cq2,cf3:cq3");
        runner.assertValid();

        runner.setProperty(GetHBase.COLUMNS, "cf1,cf2:cq1,cf3");
        runner.assertValid();

        runner.setProperty(GetHBase.COLUMNS, "cf1 cf2,cf3");
        runner.assertNotValid();

        runner.setProperty(GetHBase.COLUMNS, "cf1:,cf2,cf3");
        runner.assertNotValid();

        runner.setProperty(GetHBase.COLUMNS, "cf1:cq1,");
        runner.assertNotValid();
    }

    @Test
    public void testRowCounts() {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 4);

        hBaseClient.addResult("row4", cells, now + 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 5);
    }

    @Test
    public void testPersistAndRecoverFromLocalState() throws InitializationException {
        final File stateFile = new File("target/test-recover-state.bin");
        if (!stateFile.delete() && stateFile.exists()) {
            Assert.fail("Could not delete state file " + stateFile);
        }
        proc.setStateFile(stateFile);

        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 4);

        hBaseClient.addResult("row4", cells, now + 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 5);

        proc = new MockGetHBase(stateFile);
        final TestRunner newRunner = TestRunners.newTestRunner(proc);

        newRunner.addControllerService("cacheClient", cacheClient);
        newRunner.enableControllerService(cacheClient);

        newRunner.addControllerService("hbaseClient", hBaseClient);
        newRunner.enableControllerService(hBaseClient);

        newRunner.setProperty(GetHBase.TABLE_NAME, "nifi");
        newRunner.setProperty(GetHBase.DISTRIBUTED_CACHE_SERVICE, "cacheClient");
        newRunner.setProperty(GetHBase.HBASE_CLIENT_SERVICE, "hbaseClient");

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);

        newRunner.run(100);
        newRunner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 0);
    }

    @Test
    public void testBecomePrimaryWithNoLocalState() throws InitializationException {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 4);

        hBaseClient.addResult("row4", cells, now + 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 5);

        // delete the processor's local state to simulate becoming the primary node
        // for the first time, should use the state from distributed cache
        final File stateFile = proc.getStateFile();
        if (!stateFile.delete() && stateFile.exists()) {
            Assert.fail("Could not delete state file " + stateFile);
        }
        proc.onPrimaryNodeChange(PrimaryNodeState.ELECTED_PRIMARY_NODE);

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);
        hBaseClient.addResult("row4", cells, now + 1);

        runner.clearTransferState();
        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 0);
    }

    @Test
    public void testBecomePrimaryWithNewerLocalState() throws InitializationException {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 4);

        // trick for testing so that row4 gets written to local state but not to the real cache
        final MockCacheClient otherCacheClient = new MockCacheClient();
        runner.addControllerService("otherCacheClient", otherCacheClient);
        runner.enableControllerService(otherCacheClient);
        runner.setProperty(GetHBase.DISTRIBUTED_CACHE_SERVICE, "otherCacheClient");

        hBaseClient.addResult("row4", cells, now + 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 5);

        // set back the original cache cacheClient which is missing row4
        runner.setProperty(GetHBase.DISTRIBUTED_CACHE_SERVICE, "cacheClient");

        // become the primary node, but we have existing local state with rows 0-4
        // so we shouldn't get any output because we should use the local state
        proc.onPrimaryNodeChange(PrimaryNodeState.ELECTED_PRIMARY_NODE);

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);
        hBaseClient.addResult("row4", cells, now + 1);

        runner.clearTransferState();
        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 0);
    }

    @Test
    public void testOnRemovedClearsState() throws IOException {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 4);

        // should have a local state file and a cache entry before removing
        Assert.assertTrue(proc.getStateFile().exists());
        Assert.assertTrue(cacheClient.containsKey(proc.getKey(), new StringSerDe()));

        proc.onRemoved(runner.getProcessContext());

        // onRemoved should have cleared both
        Assert.assertFalse(proc.getStateFile().exists());
        Assert.assertFalse(cacheClient.containsKey(proc.getKey(), new StringSerDe()));
    }

    @Test
    public void testChangeTableNameClearsState() {
        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 4);

        // change the table name and run again, should get all the data coming out
        // again because previous state will be wiped
        runner.setProperty(GetHBase.TABLE_NAME, "otherTable");

        hBaseClient.addResult("row0", cells, now - 2);
        hBaseClient.addResult("row1", cells, now - 1);
        hBaseClient.addResult("row2", cells, now - 1);
        hBaseClient.addResult("row3", cells, now);

        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 4);
    }

    @Test
    public void testInitialTimeCurrentTime() {
        runner.setProperty(GetHBase.INITIAL_TIMERANGE, GetHBase.CURRENT_TIME);

        final long now = System.currentTimeMillis();

        final Map<String, String> cells = new HashMap<>();
        cells.put("greeting", "hello");
        cells.put("name", "nifi");

        hBaseClient.addResult("row0", cells, now - 4000);
        hBaseClient.addResult("row1", cells, now - 3000);
        hBaseClient.addResult("row2", cells, now - 2000);
        hBaseClient.addResult("row3", cells, now - 1000);

        // should not get any output because the mock results have a time before current time
        runner.run(100);
        runner.assertAllFlowFilesTransferred(GetHBase.REL_SUCCESS, 0);
    }

    @Test
    public void testParseColumns() {
        runner.setProperty(GetHBase.COLUMNS, "cf1,cf2:cq1,cf3");
        proc.parseColumns(runner.getProcessContext());

        final List<Column> expectedCols = new ArrayList<>();
        expectedCols.add(new Column("cf1".getBytes(Charset.forName("UTF-8")), null));
        expectedCols.add(new Column("cf2".getBytes(Charset.forName("UTF-8")), "cq1".getBytes(Charset.forName("UTF-8"))));
        expectedCols.add(new Column("cf3".getBytes(Charset.forName("UTF-8")), null));

        final List<Column> actualColumns = proc.getColumns();
        Assert.assertNotNull(actualColumns);
        Assert.assertEquals(expectedCols.size(), actualColumns.size());

        for (final Column expectedCol : expectedCols) {
            boolean found = false;
            for (final Column providedCol : actualColumns) {
                if (expectedCol.equals(providedCol)) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("Didn't find expected column", found);
        }
    }

    @Test
    public void testCustomValidate() throws CharacterCodingException {
        runner.setProperty(GetHBase.FILTER_EXPRESSION, "PrefixFilter ('Row') AND PageFilter (1) AND FirstKeyOnlyFilter ()");
        runner.assertValid();

        runner.setProperty(GetHBase.COLUMNS, "colA");
        runner.assertNotValid();
    }

    // Mock processor to override the location of the state file
    private static class MockGetHBase extends GetHBase {

        private static final String DEFAULT_STATE_FILE_NAME = "target/TestGetHBase.bin";

        private File stateFile;

        public MockGetHBase() {
            this(new File(DEFAULT_STATE_FILE_NAME));
        }

        public MockGetHBase(final File stateFile) {
            this.stateFile = stateFile;
        }

        public void setStateFile(final File stateFile) {
            this.stateFile = stateFile;
        }

        @Override
        protected int getBatchSize() {
            return 2;
        }

        @Override
        protected File getStateDir() {
            return new File("target");
        }

        @Override
        protected File getStateFile() {
            return stateFile;
        }
    }

    private class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private final ConcurrentMap<Object, Object> values = new ConcurrentHashMap<>();
        private boolean failOnCalls = false;

        private void verifyNotFail() throws IOException {
            if ( failOnCalls ) {
                throw new IOException("Could not call to remote cacheClient because Unit Test marked cacheClient unavailable");
            }
        }

        @Override
        public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            final Object retValue = values.putIfAbsent(key, value);
            return (retValue == null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                                          final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.putIfAbsent(key, value);
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            verifyNotFail();
            return values.containsKey(key);
        }

        @Override
        public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            values.put(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.get(key);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            verifyNotFail();
            values.remove(key);
            return true;
        }
    }

}
