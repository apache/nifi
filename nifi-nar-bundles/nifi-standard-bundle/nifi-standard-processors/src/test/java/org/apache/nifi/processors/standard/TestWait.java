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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processors.standard.util.FlowFileAttributesSerializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestWait {

    private TestRunner runner;
    private MockCacheClient service;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(Wait.class);

        service = new MockCacheClient();
        runner.addControllerService("service", service);
        runner.enableControllerService(service);
        runner.setProperty(Wait.DISTRIBUTED_CACHE_SERVICE, "service");
    }

    @Test
    public void testWait() throws InitializationException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        runner.enqueue(new byte[] {},props);

        runner.run();

        // no cache key attribute
        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        runner.clearTransferState();
    }

    @Test
    public void testExpired() throws InitializationException, InterruptedException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.EXPIRATION_DURATION, "100 ms");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        runner.enqueue(new byte[] {},props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_WAIT, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(Wait.REL_WAIT).get(0);

        runner.clearTransferState();
        runner.enqueue(ff);

        Thread.sleep(101L);
        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_EXPIRED, 1);
        runner.clearTransferState();
    }

    @Test
    public void testBadWaitStartTimestamp() throws InitializationException, InterruptedException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.EXPIRATION_DURATION, "100 ms");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        props.put("wait.start.timestamp", "blue bunny");
        runner.enqueue(new byte[] {},props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_FAILURE, 1);
        runner.clearTransferState();
    }

    @Test
    public void testEmptyReleaseSignal() throws InitializationException, InterruptedException {
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");

        final Map<String, String> props = new HashMap<>();
        runner.enqueue(new byte[] {},props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_FAILURE, 1);
        runner.clearTransferState();
    }

    @Test
    public void testFailingCacheService() throws InitializationException, IOException {
        service.setFailOnCalls(true);
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "2");
        runner.enqueue(new byte[] {}, props);
        runner.run();

        //Expect the processor to receive an IO exception from the cache service and route to failure
        runner.assertAllFlowFilesTransferred(Wait.REL_FAILURE, 1);
        runner.assertTransferCount(Wait.REL_FAILURE, 1);

        service.setFailOnCalls(false);
    }

    @Test
    public void testReplaceAttributes() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("both", "notifyValue");
        cachedAttributes.put("uuid", "notifyUuid");
        cachedAttributes.put("notify.only", "notifyValue");

        service.put("key", cachedAttributes, new Wait.StringSerializer(), new FlowFileAttributesSerializer());
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.ATTRIBUTE_COPY_MODE, Wait.ATTRIBUTE_COPY_REPLACE.getValue());

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("wait.only", "waitValue");
        waitAttributes.put("both", "waitValue");
        waitAttributes.put("uuid", UUID.randomUUID().toString());
        String flowFileContent = "content";
        runner.enqueue(flowFileContent.getBytes("UTF-8"), waitAttributes);

        // make sure the key is in the cache before Wait runs
        assertNotNull(service.get("key", new Wait.StringSerializer(), new FlowFileAttributesSerializer()));

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_SUCCESS, 1);
        runner.assertTransferCount(Wait.REL_SUCCESS, 1);

        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(Wait.REL_SUCCESS).get(0);

        // show a new attribute was copied from the cache
        assertEquals("notifyValue", outputFlowFile.getAttribute("notify.only"));
        // show that uuid was not overwritten
        assertEquals(waitAttributes.get("uuid"), outputFlowFile.getAttribute("uuid"));
        // show that the original attributes are still there
        assertEquals("waitValue", outputFlowFile.getAttribute("wait.only"));

        // here's the important part: show that the cached attribute replaces the original
        assertEquals("notifyValue", outputFlowFile.getAttribute("both"));
        runner.clearTransferState();

        // make sure Wait removed this key from the cache
        assertNull(service.get("key", new Wait.StringSerializer(), new FlowFileAttributesSerializer()));
    }

    @Test
    public void testKeepOriginalAttributes() throws InitializationException, IOException {
        Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("both", "notifyValue");
        cachedAttributes.put("uuid", "notifyUuid");
        cachedAttributes.put("notify.only", "notifyValue");

        service.put("key", cachedAttributes, new Wait.StringSerializer(), new FlowFileAttributesSerializer());
        runner.setProperty(Wait.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Wait.ATTRIBUTE_COPY_MODE, Wait.ATTRIBUTE_COPY_KEEP_ORIGINAL.getValue());

        final Map<String, String> waitAttributes = new HashMap<>();
        waitAttributes.put("releaseSignalAttribute", "key");
        waitAttributes.put("wait.only", "waitValue");
        waitAttributes.put("both", "waitValue");
        waitAttributes.put("uuid", UUID.randomUUID().toString());
        String flowFileContent = "content";
        runner.enqueue(flowFileContent.getBytes("UTF-8"), waitAttributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(Wait.REL_SUCCESS, 1);
        runner.assertTransferCount(Wait.REL_SUCCESS, 1);

        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(Wait.REL_SUCCESS).get(0);

        // show a new attribute was copied from the cache
        assertEquals("notifyValue", outputFlowFile.getAttribute("notify.only"));
        // show that uuid was not overwritten
        assertEquals(waitAttributes.get("uuid"), outputFlowFile.getAttribute("uuid"));
        // show that the original attributes are still there
        assertEquals("waitValue", outputFlowFile.getAttribute("wait.only"));

        // here's the important part: show that the original attribute is kept
        assertEquals("waitValue", outputFlowFile.getAttribute("both"));
        runner.clearTransferState();
    }

    private class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private final ConcurrentMap<Object, Object> values = new ConcurrentHashMap<>();
        private boolean failOnCalls = false;

        public void setFailOnCalls(boolean failOnCalls){
            this.failOnCalls = failOnCalls;
        }


        private void verifyNotFail() throws IOException {
            if (failOnCalls) {
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
            if(values.containsKey(key)) {
                return (V) values.get(key);
            } else {
                return null;
            }
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