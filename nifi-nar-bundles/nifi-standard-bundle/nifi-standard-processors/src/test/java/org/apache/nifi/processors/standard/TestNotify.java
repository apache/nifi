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
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processors.standard.WaitNotifyProtocol.Signal;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNotify {

    private TestRunner runner;
    private MockCacheClient service;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(Notify.class);

        service = new MockCacheClient();
        runner.addControllerService("service", service);
        runner.enableControllerService(service);
        runner.setProperty(Notify.DISTRIBUTED_CACHE_SERVICE, "service");
    }

    @Test
    public void testNotify() throws InitializationException, IOException {
        runner.setProperty(Notify.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Notify.ATTRIBUTE_CACHE_REGEX, ".*");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        props.put("key", "value");
        runner.enqueue(new byte[] {},props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Notify.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(Notify.REL_SUCCESS).get(0).assertAttributeEquals(Notify.NOTIFIED_ATTRIBUTE_NAME, "true");
        runner.clearTransferState();

        final Signal signal = new WaitNotifyProtocol(service).getSignal("1");
        Map<String, String> cachedAttributes = signal.getAttributes();
        assertEquals("value", cachedAttributes.get("key"));
        assertTrue(signal.isTotalCountReached(1));
    }

    @Test
    public void testNotifyCounters() throws InitializationException, IOException {
        runner.setProperty(Notify.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Notify.ATTRIBUTE_CACHE_REGEX, ".*");
        runner.setProperty(Notify.SIGNAL_COUNTER_NAME, "${status}");

        final Map<String, String> props1 = new HashMap<>();
        props1.put("releaseSignalAttribute", "someDataProcessing");
        props1.put("key", "data1");
        props1.put("status", "success");
        runner.enqueue(new byte[]{}, props1);

        final Map<String, String> props2 = new HashMap<>();
        props2.put("releaseSignalAttribute", "someDataProcessing");
        props2.put("key", "data2");
        props2.put("status", "success");
        runner.enqueue(new byte[]{}, props2);

        final Map<String, String> props3 = new HashMap<>();
        props3.put("releaseSignalAttribute", "someDataProcessing");
        props3.put("key", "data3");
        props3.put("status", "failure");
        runner.enqueue(new byte[]{}, props3);

        runner.run(3);

        runner.assertAllFlowFilesTransferred(Notify.REL_SUCCESS, 3);
        runner.getFlowFilesForRelationship(Notify.REL_SUCCESS).forEach(flowFile -> flowFile.assertAttributeEquals(Notify.NOTIFIED_ATTRIBUTE_NAME, "true"));
        runner.clearTransferState();

        final Signal signal = new WaitNotifyProtocol(service).getSignal("someDataProcessing");
        Map<String, String> cachedAttributes = signal.getAttributes();
        assertEquals("Same attribute key will be overwritten by the latest signal", "data3", cachedAttributes.get("key"));
        assertTrue(signal.isTotalCountReached(3));
        assertEquals(2, signal.getCount("success"));
        assertEquals(1, signal.getCount("failure"));
    }

    @Test
    public void testNotifyCountersBatch() throws InitializationException, IOException {
        runner.setProperty(Notify.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Notify.ATTRIBUTE_CACHE_REGEX, ".*");
        runner.setProperty(Notify.SIGNAL_COUNTER_NAME, "${status}");
        runner.setProperty(Notify.SIGNAL_BUFFER_COUNT, "2");

        final Map<String, String> props1 = new HashMap<>();
        props1.put("releaseSignalAttribute", "someDataProcessing");
        props1.put("key", "data1");
        props1.put("status", "success");
        runner.enqueue(new byte[]{}, props1);

        final Map<String, String> props2 = new HashMap<>();
        props2.put("releaseSignalAttribute", "someDataProcessing");
        props2.put("key", "data2");
        props2.put("status", "success");
        runner.enqueue(new byte[]{}, props2);

        final Map<String, String> props3 = new HashMap<>();
        props3.put("releaseSignalAttribute", "someDataProcessing");
        props3.put("key", "data3");
        props3.put("status", "failure");
        runner.enqueue(new byte[]{}, props3);

        runner.run();

        // Limited by the buffer count
        runner.assertAllFlowFilesTransferred(Notify.REL_SUCCESS, 2);
        runner.getFlowFilesForRelationship(Notify.REL_SUCCESS).forEach(flowFile -> flowFile.assertAttributeEquals(Notify.NOTIFIED_ATTRIBUTE_NAME, "true"));
        runner.clearTransferState();

        Signal signal = new WaitNotifyProtocol(service).getSignal("someDataProcessing");
        Map<String, String> cachedAttributes = signal.getAttributes();
        assertEquals("Same attribute key will be overwritten by the latest signal", "data2", cachedAttributes.get("key"));
        assertTrue(signal.isTotalCountReached(2));
        assertEquals(2, signal.getCount("success"));
        assertEquals(0, signal.getCount("failure"));

        // Run it again, and it should process remaining one flow file.
        runner.run();
        runner.assertAllFlowFilesTransferred(Notify.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(Notify.REL_SUCCESS).forEach(flowFile -> flowFile.assertAttributeEquals(Notify.NOTIFIED_ATTRIBUTE_NAME, "true"));
        runner.clearTransferState();

        signal = new WaitNotifyProtocol(service).getSignal("someDataProcessing");
        cachedAttributes = signal.getAttributes();
        assertEquals("Same attribute key will be overwritten by the latest signal", "data3", cachedAttributes.get("key"));
        assertTrue(signal.isTotalCountReached(3));
        assertEquals(2, signal.getCount("success"));
        assertEquals(1, signal.getCount("failure"));

    }

    @Test
    public void testNotifyCountersUsingDelta() throws InitializationException, IOException {
        runner.setProperty(Notify.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Notify.ATTRIBUTE_CACHE_REGEX, ".*");
        runner.setProperty(Notify.SIGNAL_COUNTER_NAME, "${status}");
        runner.setProperty(Notify.SIGNAL_COUNTER_DELTA, "${record.count}");
        runner.setProperty(Notify.SIGNAL_BUFFER_COUNT, "10");

        final Map<String, String> props1 = new HashMap<>();
        props1.put("releaseSignalAttribute", "someDataProcessing");
        props1.put("key", "data1");
        props1.put("status", "success");
        props1.put("record.count", "1024");
        runner.enqueue(new byte[]{}, props1);

        final Map<String, String> props2 = new HashMap<>();
        props2.put("releaseSignalAttribute", "someDataProcessing");
        props2.put("key", "data2");
        props2.put("status", "success");
        props2.put("record.count", "2048");
        runner.enqueue(new byte[]{}, props2);

        final Map<String, String> props3 = new HashMap<>();
        props3.put("releaseSignalAttribute", "someDataProcessing");
        props3.put("key", "data3");
        props3.put("status", "failure");
        props3.put("record.count", "512");
        runner.enqueue(new byte[]{}, props3);

        runner.run();

        runner.assertAllFlowFilesTransferred(Notify.REL_SUCCESS, 3);
        runner.getFlowFilesForRelationship(Notify.REL_SUCCESS).forEach(flowFile -> flowFile.assertAttributeEquals(Notify.NOTIFIED_ATTRIBUTE_NAME, "true"));
        runner.clearTransferState();

        final Signal signal = new WaitNotifyProtocol(service).getSignal("someDataProcessing");
        Map<String, String> cachedAttributes = signal.getAttributes();
        assertEquals("Same attribute key will be overwritten by the latest signal", "data3", cachedAttributes.get("key"));
        assertTrue(signal.isTotalCountReached(3584));
        assertEquals(3072, signal.getCount("success"));
        assertEquals(512, signal.getCount("failure"));
    }

    @Test
    public void testIllegalDelta() throws InitializationException, IOException {
        runner.setProperty(Notify.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Notify.ATTRIBUTE_CACHE_REGEX, ".*");
        runner.setProperty(Notify.SIGNAL_COUNTER_NAME, "${status}");
        runner.setProperty(Notify.SIGNAL_COUNTER_DELTA, "${record.count}");
        runner.setProperty(Notify.SIGNAL_BUFFER_COUNT, "10");

        final Map<String, String> props1 = new HashMap<>();
        props1.put("releaseSignalAttribute", "someDataProcessing");
        props1.put("key", "data1");
        props1.put("status", "success");
        props1.put("record.count", "1024");
        runner.enqueue(new byte[]{}, props1);

        final Map<String, String> props2 = new HashMap<>();
        props2.put("releaseSignalAttribute", "someDataProcessing");
        props2.put("key", "data2");
        props2.put("status", "success");
        props2.put("record.count", "2048 records");
        runner.enqueue(new byte[]{}, props2);

        final Map<String, String> props3 = new HashMap<>();
        props3.put("releaseSignalAttribute", "someDataProcessing");
        props3.put("key", "data3");
        props3.put("status", "failure");
        props3.put("record.count", "512");
        runner.enqueue(new byte[]{}, props3);

        runner.run();

        // Only failed records should be transferred to failure.
        runner.assertTransferCount(Notify.REL_SUCCESS, 2);
        runner.getFlowFilesForRelationship(Notify.REL_SUCCESS).forEach(flowFile -> flowFile.assertAttributeEquals(Notify.NOTIFIED_ATTRIBUTE_NAME, "true"));
        runner.assertTransferCount(Notify.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(Notify.REL_FAILURE).forEach(flowFile -> flowFile.assertAttributeEquals(Notify.NOTIFIED_ATTRIBUTE_NAME, "false"));
        runner.clearTransferState();

        final Signal signal = new WaitNotifyProtocol(service).getSignal("someDataProcessing");
        Map<String, String> cachedAttributes = signal.getAttributes();
        assertEquals("Same attribute key will be overwritten by the latest signal", "data3", cachedAttributes.get("key"));
        assertTrue(signal.isTotalCountReached(1536));
        assertEquals(1024, signal.getCount("success"));
        assertEquals(512, signal.getCount("failure"));

    }

    @Test
    public void testRegex() throws InitializationException, IOException {
        runner.setProperty(Notify.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");
        runner.setProperty(Notify.ATTRIBUTE_CACHE_REGEX, "key[0-9]*");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "1");
        props.put("key1", "value");
        props.put("other.key1", "value");
        runner.enqueue(new byte[] {},props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Notify.REL_SUCCESS, 1);
        runner.clearTransferState();

        final Signal signal = new WaitNotifyProtocol(service).getSignal("1");
        Map<String, String> cachedAttributes = signal.getAttributes();
        assertEquals("value", cachedAttributes.get("key1"));
        assertNull(cachedAttributes.get("other.key1"));
        assertTrue(signal.isTotalCountReached(1));
    }

    @Test
    public void testEmptyReleaseSignal() throws InitializationException, InterruptedException {
        runner.setProperty(Notify.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");

        final Map<String, String> props = new HashMap<>();
        runner.enqueue(new byte[] {},props);

        runner.run();

        runner.assertAllFlowFilesTransferred(Notify.REL_FAILURE, 1);
        runner.clearTransferState();
    }

    @Test
    public void testFailingCacheService() throws InitializationException, IOException {
        service.setFailOnCalls(true);
        runner.setProperty(Notify.RELEASE_SIGNAL_IDENTIFIER, "${releaseSignalAttribute}");

        final Map<String, String> props = new HashMap<>();
        props.put("releaseSignalAttribute", "2");
        runner.enqueue(new byte[] {}, props);
        try {
            runner.run();
            fail("Processor should throw RuntimeException in case it receives an IO exception from the cache service and yield for a while.");
        } catch (final AssertionError e) {
            assertTrue(e.getCause() instanceof RuntimeException);
        }
        service.setFailOnCalls(false);

    }

    static class MockCacheClient extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {
        private final ConcurrentMap<Object, AtomicCacheEntry<Object, Object, Long>> values = new ConcurrentHashMap<>();
        private boolean failOnCalls = false;

        void setFailOnCalls(boolean failOnCalls){
            this.failOnCalls = failOnCalls;
        }


        private void verifyNotFail() throws IOException {
            if (failOnCalls) {
                throw new IOException("Could not call to remote service because Unit Test marked service unavailable");
            }
        }

        private void unsupported() throws UnsupportedOperationException {
            throw new UnsupportedOperationException("This method shouldn't be used from Notify processor.");
        }

        @Override
        public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            unsupported();
            return false;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
            final Deserializer<V> valueDeserializer) throws IOException {
            unsupported();
            return null;
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            unsupported();
            return false;
        }

        @Override
        public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            unsupported();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();

            final AtomicCacheEntry entry = values.get(key);
            if (entry == null) {
                return null;
            }

            // This mock cache stores String as it is, without serializing, so it needs to convert it to byte[] first here.
            return valueDeserializer.deserialize(((String)entry.getValue()).getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            verifyNotFail();
            return values.remove(key) != null;
        }

        @Override
        public long removeByPattern(String regex) throws IOException {
            verifyNotFail();
            final List<Object> removedRecords = new ArrayList<>();
            Pattern p = Pattern.compile(regex);
            for (Object key : values.keySet()) {
                // Key must be backed by something that can be converted into a String
                Matcher m = p.matcher(key.toString());
                if (m.matches()) {
                    removedRecords.add(values.get(key));
                }
            }
            final long numRemoved = removedRecords.size();
            removedRecords.forEach(values::remove);
            return numRemoved;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> AtomicCacheEntry<K, V, Long> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();

            return (AtomicCacheEntry<K, V, Long>) values.get(key);
        }

        @Override
        public <K, V> boolean replace(AtomicCacheEntry<K, V, Long> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();

            final K key = entry.getKey();
            final AtomicCacheEntry<Object, Object, Long> existing = values.get(key);
            if (existing != null && !existing.getRevision().equals(entry.getRevision())) {
                return false;
            }

            values.put(key, new AtomicCacheEntry<>(key, entry.getValue(), entry.getRevision().orElse(0L) + 1));

            return true;
        }
    }


}