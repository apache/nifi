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

import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient.CacheEntry;
import org.apache.nifi.distributed.cache.client.StandardCacheEntry;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.processors.standard.WaitNotifyProtocol.Signal;
import org.apache.nifi.processors.standard.util.FlowFileAttributesSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.nio.charset.StandardCharsets;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TestWaitNotifyProtocol {

    private final Map<String, CacheEntry<String, String>> cacheEntries = new HashMap<>();

    private AtomicDistributedMapCacheClient cache;
    private final Answer successfulReplace = invocation -> {
        final String key = invocation.getArgumentAt(0, String.class);
        final String value = invocation.getArgumentAt(1, String.class);
        final Long revision = invocation.getArgumentAt(4, Long.class);
        cacheEntries.put(key, new StandardCacheEntry<>(key, value, revision + 1));
        return true;
    };

    @Before
    public void before() throws Exception {
        cacheEntries.clear();

        // Default mock implementations.
        cache = mock(AtomicDistributedMapCacheClient.class);
        doAnswer(invocation -> {
            final CacheEntry<String, String> entry = cacheEntries.get(invocation.getArguments()[0]);
            return entry;
        }).when(cache).fetch(any(), any(), any());
    }

    @Test
    public void testNotifyRetryFailure() throws Exception {

        // replace always return false.
        doAnswer(invocation -> false)
                .when(cache).replace(any(), any(), any(), any(), anyLong());

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";
        try {
            protocol.notify(signalId, "a", 1, null);
            fail("Notify should fail after retrying few times.");
        } catch (ConcurrentModificationException e) {
        }
    }

    @Test
    public void testNotifyFirst() throws Exception {

        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), anyLong());

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";
        final Signal signal = protocol.notify(signalId, "a", 1, null);

        assertNotNull(signal);
        assertEquals(Long.valueOf(1), signal.getCounts().get("a"));
        assertTrue(cacheEntries.containsKey("signal-id"));

        final CacheEntry<String, String> cacheEntry = cacheEntries.get("signal-id");

        assertEquals(0, cacheEntry.getRevision());
        assertEquals("{\"counts\":{\"a\":1},\"attributes\":{}}", cacheEntry.getValue());
    }

    @Test
    public void testNotifyCounters() throws Exception {

        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), anyLong());

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";

        protocol.notify(signalId, "a", 1, null);
        protocol.notify(signalId, "a", 1, null);

        CacheEntry<String, String> cacheEntry = cacheEntries.get("signal-id");
        assertEquals(1, cacheEntry.getRevision());
        assertEquals("{\"counts\":{\"a\":2},\"attributes\":{}}", cacheEntry.getValue());

        protocol.notify(signalId, "a", 10, null);

        cacheEntry = cacheEntries.get("signal-id");
        assertEquals(2, cacheEntry.getRevision());
        assertEquals("{\"counts\":{\"a\":12},\"attributes\":{}}", cacheEntry.getValue());

        protocol.notify(signalId, "b", 2, null);
        protocol.notify(signalId, "c", 3, null);

        cacheEntry = cacheEntries.get("signal-id");
        assertEquals(4, cacheEntry.getRevision());
        assertEquals("{\"counts\":{\"a\":12,\"b\":2,\"c\":3},\"attributes\":{}}", cacheEntry.getValue());

    }

    @Test
    public void testNotifyAttributes() throws Exception {
        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), anyLong());

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";

        final Map<String, String> attributeA1 = new HashMap<>();
        attributeA1.put("p1", "a1");
        attributeA1.put("p2", "a1");

        protocol.notify(signalId, "a", 1, attributeA1);

        CacheEntry<String, String> cacheEntry = cacheEntries.get("signal-id");
        assertEquals(0, cacheEntry.getRevision());
        assertEquals("{\"counts\":{\"a\":1},\"attributes\":{\"p1\":\"a1\",\"p2\":\"a1\"}}", cacheEntry.getValue());

        final Map<String, String> attributeA2 = new HashMap<>();
        attributeA2.put("p2", "a2"); // Update p2
        attributeA2.put("p3", "a2"); // Add p3

        // Notify again
        protocol.notify(signalId, "a", 1, attributeA2);

        cacheEntry = cacheEntries.get("signal-id");
        assertEquals(1, cacheEntry.getRevision());
        assertEquals("Updated attributes should be merged correctly",
                "{\"counts\":{\"a\":2},\"attributes\":{\"p1\":\"a1\",\"p2\":\"a2\",\"p3\":\"a2\"}}", cacheEntry.getValue());

    }

    @Test
    public void testSignalCount() throws Exception {
        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), anyLong());

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";

        Signal signal = protocol.getSignal(signalId);
        assertNull("Should be null since there's no signal yet", signal);

        // First notification.
        protocol.notify(signalId, "success", 1, null);

        signal = protocol.getSignal(signalId);
        assertNotNull(signal);
        assertEquals(1, signal.getCount("success"));
        assertTrue(signal.isCountReached("success", 1));
        assertFalse(signal.isCountReached("success", 2));
        assertTrue(signal.isTotalCountReached(1));
        assertFalse(signal.isTotalCountReached(2));

        // Notify again with different counter name.
        protocol.notify(signalId, "failure", 1, null);

        signal = protocol.getSignal(signalId);
        assertNotNull(signal);
        assertEquals(1, signal.getCount("success"));
        assertEquals(1, signal.getCount("failure"));
        assertTrue(signal.isCountReached("failure", 1));
        assertFalse(signal.isCountReached("failure", 2));
        assertTrue(signal.isTotalCountReached(1));
        assertTrue(signal.isTotalCountReached(2));

    }

    /**
     * Test migration across NiFi version upgrade.
     * Old version of Wait/Notify processors use FlowFileAttributesSerializer for cache entries.
     * New version uses StringSerializer. WaitNotifyProtocol should be able to migrate old cache entries.
     */
    @Test
    public void testNiFiVersionUpgrade() throws Exception {
        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), anyLong());

        // Simulate old cache entry.
        final FlowFileAttributesSerializer attributesSerializer = new FlowFileAttributesSerializer();
        final Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("key1", "value1");
        cachedAttributes.put("key2", "value2");
        cachedAttributes.put("key3", "value3");
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        attributesSerializer.serialize(cachedAttributes, bos);

        final String signalId = "old-entry";
        cacheEntries.put(signalId, new StandardCacheEntry<>(signalId, new String(bos.toByteArray(), StandardCharsets.UTF_8), 0));

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);
        final Signal signal = protocol.getSignal(signalId);

        assertEquals(1, signal.getCount(WaitNotifyProtocol.DEFAULT_COUNT_NAME));
        assertEquals("value1", signal.getAttributes().get("key1"));
        assertEquals("value2", signal.getAttributes().get("key2"));
        assertEquals("value3", signal.getAttributes().get("key3"));

        cacheEntries.put(signalId, new StandardCacheEntry<>(signalId, "UNSUPPORTED_FORMAT", 0));
        try {
            protocol.getSignal(signalId);
            fail("Should fail since cached value was not in expected format.");
        } catch (DeserializationException e) {
        }

    }

}