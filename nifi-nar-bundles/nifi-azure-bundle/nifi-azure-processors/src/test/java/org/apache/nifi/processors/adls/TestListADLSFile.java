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
package org.apache.nifi.processors.adls;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.ADLStoreOptions;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.nifi.processors.adls.ListADLSFile.LISTING_TIMESTAMP_KEY;


public class TestListADLSFile {

    private MockWebServer server;
    private ADLStoreClient client;
    private Processor processor;
    private TestRunner runner;
    private MockCacheClient service;

    private static final String resFileStatusThreeFilesABCTxt = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":377986,\"pathSuffix\":\"a.txt\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1503562285642,\"modificationTime\":1503562287537,\"replication\":1,\"permission\":\"770\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":1854,\"pathSuffix\":\"b.txt\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1503562288814,\"modificationTime\":1503562289144,\"replication\":1,\"permission\":\"770\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":3698,\"pathSuffix\":\"c.txt\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1503562290371,\"modificationTime\":1503562290729,\"replication\":1,\"permission\":\"770\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false}]}}";
    private static final String resFileStatusRecursiveOuterDir = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":66,\"pathSuffix\":\"fruits.txt\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333725463,\"modificationTime\":1497333725589,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":0,\"pathSuffix\":\"gutenberg\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1497333719180,\"modificationTime\":1497333723572,\"replication\":0,\"permission\":\"755\",\"owner\":\"nifi\",\"group\":\"nifi\",\"aclBit\":false},{\"length\":77,\"pathSuffix\":\"people.json\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333729518,\"modificationTime\":1497333729641,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":0,\"pathSuffix\":\"people.parquet\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1497333735225,\"modificationTime\":1497333742323,\"replication\":0,\"permission\":\"755\",\"owner\":\"nifi\",\"group\":\"nifi\",\"aclBit\":false},{\"length\":0,\"pathSuffix\":\"people.seq\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1497333744289,\"modificationTime\":1497333749007,\"replication\":0,\"permission\":\"755\",\"owner\":\"nifi\",\"group\":\"nifi\",\"aclBit\":false},{\"length\":97884,\"pathSuffix\":\"sample.log\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333752253,\"modificationTime\":1497333753053,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":62,\"pathSuffix\":\"yellowthings.txt\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333755185,\"modificationTime\":1497333755282,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false}]}}";
    private static final String resFileStatusRecursiveInnerDirOne = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":1395667,\"pathSuffix\":\"fruits.txt\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333720228,\"modificationTime\":1497333720468,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":662001,\"pathSuffix\":\"people.json\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333722332,\"modificationTime\":1497333722767,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":1539989,\"pathSuffix\":\"sample.log\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333723195,\"modificationTime\":1497333723378,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false}]}}";
    private static final String resFileStatusRecursiveInnerDirTwo = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\"_SUCCESS\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333736413,\"modificationTime\":1497333736413,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":280,\"pathSuffix\":\"_common_metadata\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333737709,\"modificationTime\":1497333737995,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":749,\"pathSuffix\":\"_metadata\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333738845,\"modificationTime\":1497333739579,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":537,\"pathSuffix\":\"part-r-00000-c42cb485-e97a-4e7f-ad7f-7592a3faba8a.gz.parquet\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333740920,\"modificationTime\":1497333741656,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":529,\"pathSuffix\":\"part-r-00001-c42cb485-e97a-4e7f-ad7f-7592a3faba8a.gz.parquet\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333741873,\"modificationTime\":1497333741957,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false}]}}";
    private static final String resFileStatusRecursiveInnerDirThree = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\"_SUCCESS\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333744384,\"modificationTime\":1497333744384,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":120,\"pathSuffix\":\"part-00000\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333747105,\"modificationTime\":1497333747246,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false},{\"length\":103,\"pathSuffix\":\"part-00001\",\"type\":\"FILE\",\"blockSize\":268435456,\"accessTime\":1497333747935,\"modificationTime\":1497333748552,\"replication\":1,\"permission\":\"644\",\"owner\":\"nifi\",\"group\":\"nifi\",\"msExpirationTime\":0,\"aclBit\":false}]}}";
    private static final String resFileStatusEmptyDirectory = "{\"FileStatuses\":{\"FileStatus\":[]}}";

    @Before
    public void init() throws IOException, InitializationException {
        server = new MockWebServer();
        QueueDispatcher dispatcher = new QueueDispatcher();
        dispatcher.setFailFast(new MockResponse().setResponseCode(400));
        server.setDispatcher(dispatcher);
        server.start();
        String accountFQDN = server.getHostName() + ":" + server.getPort();
        String dummyToken = "testDummyAadToken";

        client = ADLStoreClient.createClient(accountFQDN, dummyToken);
        client.setOptions(new ADLStoreOptions().setInsecureTransport());

        processor = new ListADLSWithMockClient(client);
        runner = TestRunners.newTestRunner(processor);

        service = new MockCacheClient();
        runner.addControllerService("service", service);
        runner.enableControllerService(service);

        runner.setProperty(ADLSConstants.ACCOUNT_NAME, accountFQDN);
        runner.setProperty(ADLSConstants.CLIENT_ID, "foobar");
        runner.setProperty(ADLSConstants.CLIENT_SECRET, "foobar");
        runner.setProperty(ADLSConstants.AUTH_TOKEN_ENDPOINT, "foobar");
        runner.setProperty(ListADLSFile.DIRECTORY, "/test/");
        runner.setProperty(ListADLSFile.RECURSE_SUBDIRS, "false");
        runner.setProperty(ListADLSFile.DISTRIBUTED_CACHE_SERVICE, "service");
        runner.setProperty(ListADLSFile.FILE_FILTER, "[^\\.].*");
        runner.setRunSchedule(500L);
    }

    @Test
    public void testListBasic() {

        clearState();
        server.enqueue(getMockResponse(resFileStatusThreeFilesABCTxt));
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 3);
        long validFileCount = runner.getFlowFilesForRelationship(ADLSConstants.REL_SUCCESS)
                .stream()
                .filter(flowFile ->
                        flowFile.isAttributeEqual("filename", "a.txt")
                        || flowFile.isAttributeEqual("filename", "b.txt")
                        || flowFile.isAttributeEqual("filename", "c.txt"))
                .count();
        Assert.assertEquals(3, validFileCount);
    }

    @Test
    public void testEmptyDirectory() {

        clearState();
        server.enqueue(getMockResponse(resFileStatusEmptyDirectory));
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 0);
    }

    @Test
    public void testRecursive() {
        clearState();
        runner.setProperty(ListADLSFile.RECURSE_SUBDIRS, "true");
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirOne));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirTwo));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirThree));
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 15);

        runner.setProperty(ListADLSFile.RECURSE_SUBDIRS, "false");
    }

    @Test
    public void testMinimumTimeBetweenRuns() throws InterruptedException {
        clearState();
        ListADLSFile listADLSProcessor = (ListADLSFile) this.processor;
        long originalMultiplicationFactor = listADLSProcessor.testingMinimumDurationMultiplicationFactor;
        //minimum duration - 1.5sec
        listADLSProcessor.testingMinimumDurationMultiplicationFactor = 15;

        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir.replaceFirst("1497333755282", String.valueOf(System.currentTimeMillis()))));
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir.replaceFirst("1497333755282", String.valueOf(System.currentTimeMillis()))));

        //time between runs is 500msec < 1.5 sec
        runner.run(2);
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 4);

        //sleep for > 1.5sec
        Thread.sleep(1550);

        runner.run();
        //second run shouldn't have updated last run time, so third time will get one flowfile, in total 5
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 5);

        listADLSProcessor.testingMinimumDurationMultiplicationFactor = originalMultiplicationFactor;
    }

    @Test
    public void testNegDirProperty() {
        clearState();
        runner.setProperty(ListADLSFile.DIRECTORY, "");
        runner.run();

        runner.setProperty(ListADLSFile.DIRECTORY, " ");
        runner.run();

        runner.setProperty(ListADLSFile.DIRECTORY, " 123 ");
        runner.run();

        runner.setProperty(ListADLSFile.DIRECTORY, "-");
        runner.run();
    }

    @Test
    public void testListFailure() {
        clearState();

        server.enqueue(new MockResponse().setResponseCode(400).setBody(resFileStatusThreeFilesABCTxt));
        server.enqueue(new MockResponse().setResponseCode(404).setBody(resFileStatusThreeFilesABCTxt));
        server.enqueue(new MockResponse().setResponseCode(500).setBody(resFileStatusThreeFilesABCTxt));
        runner.run(3);
        runner.assertTransferCount(ADLSConstants.REL_SUCCESS, 0);
        runner.assertTransferCount(ADLSConstants.REL_FAILURE, 0);
    }


    @Test
    public void testFileFilter() {
        clearState();
        runner.setProperty(ListADLSWithMockClient.RECURSE_SUBDIRS, "true");

        runner.setProperty(ListADLSWithMockClient.FILE_FILTER, "(.*)\\.txt");
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirOne));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirTwo));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirThree));
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 3);

        runner.setProperty(ListADLSWithMockClient.RECURSE_SUBDIRS, "false");
        runner.setProperty(ListADLSFile.FILE_FILTER, "[^\\.].*");
    }

    @Test
    public void testFileFilterAnother() {
        clearState();
        runner.setProperty(ListADLSWithMockClient.RECURSE_SUBDIRS, "true");

        runner.setProperty(ListADLSWithMockClient.FILE_FILTER, "(.*)-(.*)");
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirOne));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirTwo));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirThree));
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 4);

        runner.setProperty(ListADLSWithMockClient.RECURSE_SUBDIRS, "false");
        runner.setProperty(ListADLSFile.FILE_FILTER, "[^\\.].*");
    }

    @Test
    public void testFlowFileAttributes() {
        clearState();

        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));

        runner.run();
        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(ADLSConstants.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals("filename", "fruits.txt");
        mockFlowFile.assertAttributeEquals("adls.owner", "nifi");
        mockFlowFile.assertAttributeEquals("adls.group", "nifi");
        mockFlowFile.assertAttributeEquals("adls.lastModified", new Date(Long.parseLong("1497333725589")).toString());
        mockFlowFile.assertAttributeEquals("adls.length", "66");
        mockFlowFile.assertAttributeEquals("adls.permissions", "644");
    }

    @Test
    public void testStateWOutModification() throws InterruptedException, IOException {
        clearState();
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));

        runner.run(2);
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 4);
    }

    @Test
    public void testStateWithModification() throws InterruptedException, IOException {
        clearState();
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir.replaceFirst("1497333755282", String.valueOf(1497333755282L+10))));
        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir.replaceFirst("1497333755282", String.valueOf(1497333755282L+20))));

        runner.run(3);
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 6);
    }


    @Test
    public void testNonRecursive() {
        clearState();
        runner.setProperty(ListADLSFile.RECURSE_SUBDIRS, "false");

        server.enqueue(getMockResponse(resFileStatusRecursiveOuterDir));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirOne));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirTwo));
        server.enqueue(getMockResponse(resFileStatusRecursiveInnerDirThree));
        runner.run();
        runner.assertAllFlowFilesTransferred(ADLSConstants.REL_SUCCESS, 4);

        //only one call will go to server for non-recursive listing
        //so dequeue the remaining three responses
        try {
            client.enumerateDirectory("/");
            client.enumerateDirectory("/");
            client.enumerateDirectory("/");
        } catch (IOException e) {
            //nothing
        }
    }

    private void clearState() {
        try {
            service.remove(LISTING_TIMESTAMP_KEY, null);
        } catch (IOException e) {
            //nothing
        }
    }

    private MockResponse getMockResponse(String body) {
        return (new MockResponse())
                .setResponseCode(200)
                .setBody(body);
    }

    public class ListADLSWithMockClient extends ListADLSFile {
        ListADLSWithMockClient(ADLStoreClient client) {
            this.adlStoreClient = client;
        }
    }

    public class FetchADLSWithMockClient extends FetchADLSFile {
        FetchADLSWithMockClient(ADLStoreClient client) {
            this.adlStoreClient = client;
        }
    }

    private class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private final ConcurrentMap<Object, Object> values = new ConcurrentHashMap<>();
        private boolean failOnCalls = false;

        private void verifyNotFail() throws IOException {
            if ( failOnCalls ) {
                throw new IOException("Could not call to remote service because Unit Test marked service unavailable");
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

        @Override
        public long removeByPattern(String regex) throws IOException {
            verifyNotFail();
            final List<Object> removedRecords = new ArrayList<>();
            Pattern p = Pattern.compile(regex);
            for (Object key : values.keySet()) {
                // Key must be backed by something that array() returns a byte[] that can be converted into a String via the default charset
                Matcher m = p.matcher(key.toString());
                if (m.matches()) {
                    removedRecords.add(values.get(key));
                }
            }
            final long numRemoved = removedRecords.size();
            removedRecords.forEach(values::remove);
            return numRemoved;
        }
    }

}
