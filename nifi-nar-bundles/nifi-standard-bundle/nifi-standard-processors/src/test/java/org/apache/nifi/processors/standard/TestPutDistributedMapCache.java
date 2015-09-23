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
package org.apache.nifi.processors.standard;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

public class TestPutDistributedMapCache {

    private TestRunner runner;
    private MockCacheClient service;
    private PutDistributedMapCache processor;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(PutDistributedMapCache.class);

        service = new MockCacheClient();
        runner.addControllerService("service", service);
        runner.enableControllerService(service);
        runner.setProperty(PutDistributedMapCache.DISTRIBUTED_CACHE_SERVICE, "service");
    }

    @Test
    public void testNoCacheKey() throws InitializationException {

        runner.setProperty(PutDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "${caheKeyAttribute}");
        runner.enqueue(new byte[]{});

        runner.run();

        // no cache key attribute
        runner.assertAllFlowFilesTransferred(PutDistributedMapCache.REL_FAILURE, 1);
        runner.assertTransferCount(PutDistributedMapCache.REL_FAILURE, 1);
        runner.clearTransferState();
    }

    @Test
    public void testSingleFlowFile() throws InitializationException, IOException {
        runner.setProperty(PutDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "${caheKeyAttribute}");

        final Map<String, String> props = new HashMap<>();
        props.put("caheKeyAttribute", "1");

        String flowFileContent = "content";
        runner.enqueue(flowFileContent.getBytes("UTF-8"), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutDistributedMapCache.REL_SUCCESS, 1);
        runner.assertTransferCount(PutDistributedMapCache.REL_SUCCESS, 1);
        byte[] value = service.get("1", new PutDistributedMapCache.StringSerializer(), new PutDistributedMapCache.CacheValueDeserializer());
        assertEquals(flowFileContent, new String(value, "UTF-8"));

        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(PutDistributedMapCache.REL_SUCCESS).get(0);
        outputFlowFile.assertAttributeEquals("cached", "true");
        outputFlowFile.assertContentEquals(flowFileContent);
        runner.clearTransferState();

    }

    @Test
    public void testNothingToCache() throws InitializationException, IOException {
        runner.setProperty(PutDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "${caheKeyAttribute}");

        final Map<String, String> props = new HashMap<>();
        props.put("caheKeyAttribute", "2");

        // flow file without content
        runner.enqueue(new byte[]{}, props);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutDistributedMapCache.REL_FAILURE, 1);
        runner.assertTransferCount(PutDistributedMapCache.REL_FAILURE, 1);
    }

    @Test
    public void testMaxCacheEntrySize() throws InitializationException, IOException {

        runner.setProperty(PutDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "${uuid}");
        runner.setProperty(PutDistributedMapCache.CACHE_ENTRY_MAX_BYTES, "10 B");

        // max length is 10 bytes, flow file content is 20 bytes
        String flowFileContent = "contentwhichistoobig";
        runner.enqueue(flowFileContent.getBytes("UTF-8"));

        runner.run();

        // no cache key attribute
        runner.assertAllFlowFilesTransferred(PutDistributedMapCache.REL_FAILURE, 1);
        runner.assertTransferCount(PutDistributedMapCache.REL_FAILURE, 1);

        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(PutDistributedMapCache.REL_FAILURE).get(0);
        outputFlowFile.assertAttributeNotExists("cached");
        outputFlowFile.assertContentEquals(flowFileContent);


        runner.clearTransferState();
        runner.setProperty(PutDistributedMapCache.CACHE_ENTRY_MAX_BYTES, "1 MB");
    }

    @Test
    public void testCacheStrategyReplace() throws InitializationException, IOException {

        runner.setProperty(PutDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "${caheKeyAttribute}");
        runner.setProperty(PutDistributedMapCache.CACHE_UPDATE_STRATEGY, PutDistributedMapCache.CACHE_UPDATE_REPLACE.getValue());

        final Map<String, String> props = new HashMap<>();
        props.put("caheKeyAttribute", "replaceme");

        String original = "original";
        runner.enqueue(original.getBytes("UTF-8"), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutDistributedMapCache.REL_SUCCESS, 1);
        runner.assertTransferCount(PutDistributedMapCache.REL_SUCCESS, 1);

        MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(PutDistributedMapCache.REL_SUCCESS).get(0);
        outputFlowFile.assertAttributeEquals("cached", "true");
        outputFlowFile.assertContentEquals(original);

        runner.clearTransferState();
        byte[] value = service.get("replaceme", new PutDistributedMapCache.StringSerializer(), new PutDistributedMapCache.CacheValueDeserializer());
        assertEquals(original, new String(value, "UTF-8"));

        String replaced = "replaced";
        runner.enqueue(replaced.getBytes("UTF-8"), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutDistributedMapCache.REL_SUCCESS, 1);
        runner.assertTransferCount(PutDistributedMapCache.REL_SUCCESS, 1);

        outputFlowFile = runner.getFlowFilesForRelationship(PutDistributedMapCache.REL_SUCCESS).get(0);
        outputFlowFile.assertAttributeEquals("cached", "true");
        outputFlowFile.assertContentEquals(replaced);

        runner.clearTransferState();

        //we expect that the cache entry is replaced
        value = service.get("replaceme", new PutDistributedMapCache.StringSerializer(), new PutDistributedMapCache.CacheValueDeserializer());
        assertEquals(replaced, new String(value, "UTF-8"));
    }

    @Test
    public void testCacheStrategyKeepOriginal() throws InitializationException, IOException {

        runner.setProperty(PutDistributedMapCache.CACHE_ENTRY_IDENTIFIER, "${caheKeyAttribute}");
        runner.setProperty(PutDistributedMapCache.CACHE_UPDATE_STRATEGY, PutDistributedMapCache.CACHE_UPDATE_KEEP_ORIGINAL.getValue());

        final Map<String, String> props = new HashMap<>();
        props.put("caheKeyAttribute", "replaceme");

        String original = "original";
        runner.enqueue(original.getBytes("UTF-8"), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutDistributedMapCache.REL_SUCCESS, 1);
        runner.assertTransferCount(PutDistributedMapCache.REL_SUCCESS, 1);

        MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(PutDistributedMapCache.REL_SUCCESS).get(0);
        outputFlowFile.assertAttributeEquals("cached", "true");
        outputFlowFile.assertContentEquals(original);

        runner.clearTransferState();
        byte[] value = service.get("replaceme", new PutDistributedMapCache.StringSerializer(), new PutDistributedMapCache.CacheValueDeserializer());
        assertEquals(original, new String(value, "UTF-8"));

        String replaced = "replaced";
        runner.enqueue(replaced.getBytes("UTF-8"), props);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutDistributedMapCache.REL_FAILURE, 1);
        runner.assertTransferCount(PutDistributedMapCache.REL_FAILURE, 1);

        outputFlowFile = runner.getFlowFilesForRelationship(PutDistributedMapCache.REL_FAILURE).get(0);
        outputFlowFile.assertAttributeEquals("cached", "false");
        outputFlowFile.assertContentEquals(replaced);

        runner.clearTransferState();

        //we expect that the cache entry is NOT replaced
        value = service.get("replaceme", new PutDistributedMapCache.StringSerializer(), new PutDistributedMapCache.CacheValueDeserializer());
        assertEquals(original, new String(value, "UTF-8"));
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
    }


}