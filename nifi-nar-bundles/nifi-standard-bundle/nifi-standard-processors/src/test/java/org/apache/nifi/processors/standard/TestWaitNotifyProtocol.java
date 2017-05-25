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
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.processors.standard.WaitNotifyProtocol.Signal;
import org.apache.nifi.processors.standard.util.FlowFileAttributesSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.nifi.processors.standard.WaitNotifyProtocol.DEFAULT_COUNT_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TestWaitNotifyProtocol {

    private final Map<String, String> cacheEntries = new HashMap<>();

    private AtomicDistributedMapCacheClient cache;
    private final Answer successfulReplace = invocation -> {
        // 0 is key, 1 is prevValue, 2 is new value
        final String key = invocation.getArgumentAt(0, String.class);
        final String value = invocation.getArgumentAt(2, String.class);
        cacheEntries.put(key, value);
        return true;
    };

    @Before
    public void before() throws Exception {
        cacheEntries.clear();

        // Default mock implementations.
        cache = mock(AtomicDistributedMapCacheClient.class);
        doAnswer(invocation -> {
            final String value = cacheEntries.get(invocation.getArguments()[0]);
            return value;
        }).when(cache).get(any(), any(), any());
    }

    @Test
    public void testNotifyRetryFailure() throws Exception {

        // replace always return false.
        doAnswer(invocation -> false)
                .when(cache).replace(any(), any(), any(), any(Serializer.class), any(Serializer.class));

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

        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(Serializer.class), any(Serializer.class));

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";
        final Signal signal = protocol.notify(signalId, "a", 1, null);

        assertNotNull(signal);
        assertEquals(Long.valueOf(1), signal.getCounts().get("a"));
        assertTrue(cacheEntries.containsKey("signal-id"));

        final String value = cacheEntries.get("signal-id");

        assertEquals("{\"counts\":{\"a\":1},\"attributes\":{},\"releasableCount\":0}", value);
    }

    @Test
    public void testNotifyCounters() throws Exception {

        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), any());

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";

        protocol.notify(signalId, "a", 1, null);
        protocol.notify(signalId, "a", 1, null);

        String value = cacheEntries.get("signal-id");
        assertEquals("{\"counts\":{\"a\":2},\"attributes\":{},\"releasableCount\":0}", value);

        protocol.notify(signalId, "a", 10, null);

        value = cacheEntries.get("signal-id");
        assertEquals("{\"counts\":{\"a\":12},\"attributes\":{},\"releasableCount\":0}", value);

        protocol.notify(signalId, "b", 2, null);
        protocol.notify(signalId, "c", 3, null);

        value = cacheEntries.get("signal-id");
        assertEquals("{\"counts\":{\"a\":12,\"b\":2,\"c\":3},\"attributes\":{},\"releasableCount\":0}", value);

        final Map<String, Integer> deltas = new HashMap<>();
        deltas.put("a", 10);
        deltas.put("b", 25);
        protocol.notify("signal-id", deltas, null);

        value = cacheEntries.get("signal-id");
        assertEquals("{\"counts\":{\"a\":22,\"b\":27,\"c\":3},\"attributes\":{},\"releasableCount\":0}", value);

        // Zero clear 'b'.
        protocol.notify("signal-id", "b", 0, null);
        value = cacheEntries.get("signal-id");
        assertEquals("{\"counts\":{\"a\":22,\"b\":0,\"c\":3},\"attributes\":{},\"releasableCount\":0}", value);
    }

    @Test
    public void testNotifyAttributes() throws Exception {
        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), any());

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";

        final Map<String, String> attributeA1 = new HashMap<>();
        attributeA1.put("p1", "a1");
        attributeA1.put("p2", "a1");

        protocol.notify(signalId, "a", 1, attributeA1);

        String value = cacheEntries.get("signal-id");
        assertEquals("{\"counts\":{\"a\":1},\"attributes\":{\"p1\":\"a1\",\"p2\":\"a1\"},\"releasableCount\":0}", value);

        final Map<String, String> attributeA2 = new HashMap<>();
        attributeA2.put("p2", "a2"); // Update p2
        attributeA2.put("p3", "a2"); // Add p3

        // Notify again
        protocol.notify(signalId, "a", 1, attributeA2);

        value = cacheEntries.get("signal-id");
        assertEquals("Updated attributes should be merged correctly",
                "{\"counts\":{\"a\":2},\"attributes\":{\"p1\":\"a1\",\"p2\":\"a2\",\"p3\":\"a2\"},\"releasableCount\":0}", value);

    }

    @Test
    public void testSignalCount() throws Exception {
        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), any());

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = "signal-id";

        WaitNotifyProtocol.SignalHolder signalHolder = protocol.getSignal(signalId);
        assertNull("Should be null since there's no signal yet", signalHolder);

        // First notification.
        protocol.notify(signalId, "success", 1, null);

        signalHolder = protocol.getSignal(signalId);
        assertNotNull(signalHolder);
        assertNotNull(signalHolder.getSignal());
        assertEquals(1, signalHolder.getSignal().getCount("success"));
        assertTrue(signalHolder.getSignal().isCountReached("success", 1));
        assertFalse(signalHolder.getSignal().isCountReached("success", 2));
        assertTrue(signalHolder.getSignal().isTotalCountReached(1));
        assertFalse(signalHolder.getSignal().isTotalCountReached(2));

        // Notify again with different counter name.
        protocol.notify(signalId, "failure", 1, null);

        signalHolder = protocol.getSignal(signalId);
        assertNotNull(signalHolder);
        assertNotNull(signalHolder.getSignal());
        assertEquals(1, signalHolder.getSignal().getCount("success"));
        assertEquals(1, signalHolder.getSignal().getCount("failure"));
        assertTrue(signalHolder.getSignal().isCountReached("failure", 1));
        assertFalse(signalHolder.getSignal().isCountReached("failure", 2));
        assertTrue(signalHolder.getSignal().isTotalCountReached(1));
        assertTrue(signalHolder.getSignal().isTotalCountReached(2));

    }

    /**
     * Test migration across NiFi version upgrade.
     * Old version of Wait/Notify processors use FlowFileAttributesSerializer for cache entries.
     * New version uses StringSerializer. WaitNotifyProtocol should be able to migrate old cache entries.
     */
    @Test
    public void testNiFiVersionUpgrade() throws Exception {
        doAnswer(successfulReplace).when(cache).replace(any(), any(), any(), any(), any());

        // Simulate old cache entry.
        final FlowFileAttributesSerializer attributesSerializer = new FlowFileAttributesSerializer();
        final Map<String, String> cachedAttributes = new HashMap<>();
        cachedAttributes.put("key1", "value1");
        cachedAttributes.put("key2", "value2");
        cachedAttributes.put("key3", "value3");
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        attributesSerializer.serialize(cachedAttributes, bos);

        final String signalId = "old-entry";
        cacheEntries.put(signalId, new String(bos.toByteArray(), StandardCharsets.UTF_8));

        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);
        final Signal signal = protocol.getSignal(signalId).getSignal();

        assertEquals(1, signal.getCount(DEFAULT_COUNT_NAME));
        assertEquals("value1", signal.getAttributes().get("key1"));
        assertEquals("value2", signal.getAttributes().get("key2"));
        assertEquals("value3", signal.getAttributes().get("key3"));

        cacheEntries.put(signalId, "UNSUPPORTED_FORMAT");
        try {
            protocol.getSignal(signalId);
            fail("Should fail since cached value was not in expected format.");
        } catch (DeserializationException e) {
        }

    }

    @Test
    public void testReleaseCandidate() throws Exception {
        final List<Integer> candidates = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        final Signal signal = new Signal();
        final List<Integer> released = new ArrayList<>();
        final List<Integer> waiting = new ArrayList<>();

        // Test default name.
        final String counterName = null;

        final BiConsumer<Long, Integer> releaseCandidate = (requiredCountForPass, releasableCandidatePerPass) -> {
            released.clear();
            waiting.clear();
            signal.releaseCandidatese(counterName, requiredCountForPass, releasableCandidatePerPass, candidates,
                    r -> released.addAll(r), w -> waiting.addAll(w));
        };

        final Field releasableCount = Signal.class.getDeclaredField("releasableCount");
        releasableCount.setAccessible(true);

        // No counter, should wait.
        releaseCandidate.accept(3L, 1);
        assertEquals(0, released.size());
        assertEquals(10, waiting.size());
        assertEquals(0, signal.getCount(DEFAULT_COUNT_NAME));
        assertEquals(0, releasableCount.getInt(signal));

        // Counter is not enough yet.
        signal.getCounts().put(DEFAULT_COUNT_NAME, 1L);
        releaseCandidate.accept(3L, 1);
        assertEquals(0, released.size());
        assertEquals(10, waiting.size());
        assertEquals(1, signal.getCount(DEFAULT_COUNT_NAME)); // Counter incremented, but not enough
        assertEquals(0, releasableCount.getInt(signal));

        // Counter reached the target.
        signal.getCounts().put(DEFAULT_COUNT_NAME, 3L);
        releaseCandidate.accept(3L, 1);
        assertEquals(1, released.size());
        assertEquals(9, waiting.size());
        assertEquals(0, signal.getCount(DEFAULT_COUNT_NAME)); // Counter 3 was converted into 1 release
        assertEquals(0, releasableCount.getInt(signal));

        // Counter reached the target for two candidates.
        signal.getCounts().put(DEFAULT_COUNT_NAME, 6L);
        releaseCandidate.accept(3L, 1);
        assertEquals(2, released.size());
        assertEquals(8, waiting.size());
        assertEquals(0, signal.getCount(DEFAULT_COUNT_NAME)); // Counter 3 was converted into 1 release
        assertEquals(0, releasableCount.getInt(signal));

        // Counter reached the target for two candidates, and reminder is 2.
        signal.getCounts().put(DEFAULT_COUNT_NAME, 11L);
        releaseCandidate.accept(3L, 1);
        assertEquals(3, released.size()); // 11 / 3 = 3
        assertEquals(7, waiting.size());
        assertEquals(2, signal.getCount(DEFAULT_COUNT_NAME)); // 11 % 3 = 2
        assertEquals(0, releasableCount.getInt(signal));

        // Counter reached the target for two pass count and each pass can release 2 candidates.
        signal.getCounts().put(DEFAULT_COUNT_NAME, 6L);
        releaseCandidate.accept(3L, 2);
        assertEquals(4, released.size()); // (6 / 3) * 2 = 4
        assertEquals(6, waiting.size());
        assertEquals(0, signal.getCount(DEFAULT_COUNT_NAME)); // 6 % 3 = 0
        assertEquals(0, releasableCount.getInt(signal));

        // If there are counts more than enough to release current candidates, unused releasableCount should remain.
        signal.getCounts().put(DEFAULT_COUNT_NAME, 50L);
        releaseCandidate.accept(3L, 2);
        assertEquals(10, released.size()); // (50 / 3) * 2 = 32. Used 10.
        assertEquals(0, waiting.size());
        assertEquals(2, signal.getCount(DEFAULT_COUNT_NAME)); // 50 % 3 = 2.
        assertEquals(22, releasableCount.getInt(signal)); // 32 - 10 = 22.

    }

}