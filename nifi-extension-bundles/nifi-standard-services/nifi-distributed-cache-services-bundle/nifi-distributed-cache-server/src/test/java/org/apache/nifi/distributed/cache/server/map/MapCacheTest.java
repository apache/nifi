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
package org.apache.nifi.distributed.cache.server.map;

import org.apache.commons.lang3.SerializationException;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.MapCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(5)
public class MapCacheTest {

    private static TestRunner runner = null;
    private static MapCacheServer server = null;
    private static MapCacheClientService client = null;
    private static final Serializer<String> serializer = new StringSerializer();
    private static final Deserializer<String> deserializer = new StringDeserializer();

    @BeforeAll
    public static void startServices() throws Exception {
        runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));

        server = new MapCacheServer();
        runner.addControllerService(server.getClass().getName(), server);
        runner.setProperty(server, MapCacheServer.PORT, "0");
        runner.enableControllerService(server);
        final int port = server.getPort();

        client = new MapCacheClientService();
        runner.addControllerService(client.getClass().getName(), client);
        runner.setProperty(client, MapCacheClientService.HOSTNAME, "localhost");
        runner.setProperty(client, MapCacheClientService.PORT, String.valueOf(port));
        runner.enableControllerService(client);
    }

    @AfterAll
    public static void shutdownServices() {
        runner.disableControllerService(client);
        runner.removeControllerService(client);

        runner.disableControllerService(server);
        runner.removeControllerService(server);
    }

    @Test
    public void testMapPut() throws IOException {
        final String key = "keyPut";
        final String value = "valuePut";
        assertFalse(client.containsKey(key, serializer));
        client.put(key, value, serializer, serializer);
        assertTrue(client.containsKey(key, serializer));
        assertEquals(value, client.get(key, serializer, deserializer));
        assertTrue(client.remove(key, serializer));
        assertFalse(client.containsKey(key, serializer));
    }

    @Test
    public void testMapPutIfAbsent() throws IOException {
        final String key = "keyPutIfAbsent";
        final String value = "valuePutIfAbsent";
        assertFalse(client.containsKey(key, serializer));
        assertTrue(client.putIfAbsent(key, value, serializer, serializer));
        assertTrue(client.containsKey(key, serializer));
        assertFalse(client.putIfAbsent(key, value, serializer, serializer));
        assertEquals(value, client.get(key, serializer, deserializer));
        assertTrue(client.remove(key, serializer));
        assertFalse(client.containsKey(key, serializer));
    }

    @Test
    public void testMapGetAndPutIfAbsent() throws IOException {
        final String key = "keyGetAndPutIfAbsent";
        final String value = "valueGetAndPutIfAbsent";
        assertFalse(client.containsKey(key, serializer));
        assertNull(client.getAndPutIfAbsent(key, value, serializer, serializer, deserializer));
        assertTrue(client.containsKey(key, serializer));
        assertEquals(value, client.getAndPutIfAbsent(key, value, serializer, serializer, deserializer));
        assertEquals(value, client.get(key, serializer, deserializer));
        assertTrue(client.remove(key, serializer));
        assertFalse(client.containsKey(key, serializer));
    }

    @Test
    public void testKeySet() throws IOException {
        final String[] keys = {"keySet1", "keySet2", "keySet3"};
        final String value = "valueKeySet";
        for (String key : keys) {
            client.put(key, value, serializer, serializer);
        }
        final Set<String> keySet = client.keySet(deserializer);
        for (String key : keys) {
            assertTrue(keySet.contains(key));
            assertTrue(client.remove(key, serializer));
        }
    }

    @Test
    public void testSubMap() throws IOException {
        final String key = "keySubMap";
        final String value = "valueSubMap";
        for (int i = 0; (i < 3); ++i) {
            client.put(key + i, value + i, serializer, serializer);
        }
        final Set<String> keys = new LinkedHashSet<>(Arrays.asList("keySubMap0", "keySubMap1", "keySubMap2"));
        final Map<String, String> subMap = client.subMap(keys, serializer, deserializer);
        assertEquals(3, subMap.size());
        for (int i = 0; (i < 3); ++i) {
            assertEquals(value + i, subMap.get(key + i));
        }
    }

    @Test
    public void testReplaceFetchAtomic() throws IOException {
        final String key = "keyReplaceAtomic";
        final String value = "valueReplaceAtomic";
        final AtomicCacheEntry<String, String, Long> entry = new AtomicCacheEntry<>(key, value, null);
        assertTrue(client.replace(entry, serializer, serializer));

        final AtomicCacheEntry<String, String, Long> fetchA = client.fetch(key, serializer, deserializer);
        assertEquals(key, fetchA.getKey());
        assertEquals(Long.valueOf(0L), fetchA.getRevision().orElse(-1L));
        assertTrue(client.replace(fetchA, serializer, serializer));
        assertFalse(client.replace(fetchA, serializer, serializer));

        final AtomicCacheEntry<String, String, Long> fetchB = client.fetch(key, serializer, deserializer);
        assertEquals(key, fetchB.getKey());
        assertEquals(Long.valueOf(1L), fetchB.getRevision().orElse(-1L));
        assertTrue(client.replace(fetchB, serializer, serializer));
        assertFalse(client.replace(fetchB, serializer, serializer));

        assertTrue(client.remove(key, serializer));
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream output) throws SerializationException, IOException {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class StringDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(final byte[] input) throws DeserializationException {
            return input.length == 0 ? null : new String(input, StandardCharsets.UTF_8);
        }
    }
}
