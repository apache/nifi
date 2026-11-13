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
package org.apache.nifi.redis.service;

import org.apache.commons.lang3.SerializationException;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.redis.testcontainers.RedisContainer;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.redis.connection.RedisConnection;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link RedisDistributedMapCacheClientService} against a real Redis, covering the atomic
 * operations {@code getAndPutIfAbsent} (used by DetectDuplicate) and {@code replace} (used by Wait/Notify).
 * A non-zero TTL is configured so the tests exercise the value-plus-expiration paths.
 * The endpoint requires ACL credentials and prohibits WATCH, MULTI, and EXEC.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITRedisDistributedMapCacheClientService {

    private static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
    private static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();

    public final RedisContainer redisContainer = new RedisContainer(RedisContainer.DEFAULT_IMAGE_NAME).withExposedPorts(6379);

    private TestRunner testRunner;
    private RedisConnectionPoolService redisConnectionPool;
    private RedisDistributedMapCacheClientService cacheClient;

    @BeforeAll
    public void startContainer() throws IOException, InterruptedException {
        redisContainer.addConfigurationOption("user default off");
        redisContainer.addConfigurationOption("user nifi on >test-password ~* +@all -watch -multi -exec");
        if ("Cluster".equals(getRedisMode())) {
            redisContainer.addConfigurationOption("cluster-enabled yes");
        }
        redisContainer.start();
        if ("Cluster".equals(getRedisMode())) {
            redisContainer.execInContainer("redis-cli", "--user", "nifi", "-a", "test-password", "CONFIG", "SET",
                    "cluster-announce-ip", redisContainer.getHost(), "cluster-announce-port", redisContainer.getFirstMappedPort().toString());
            redisContainer.execInContainer("redis-cli", "--user", "nifi", "-a", "test-password", "CLUSTER", "ADDSLOTSRANGE", "0", "16383");
            final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            boolean ready = false;
            while (System.nanoTime() < deadline) {
                if (redisContainer.execInContainer("redis-cli", "--user", "nifi", "-a", "test-password", "CLUSTER", "INFO")
                        .getStdout().contains("cluster_state:ok")) {
                    ready = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertTrue(ready, "Redis Cluster must own all slots and be ready before connecting");
        }
    }

    protected String getRedisMode() {
        return "Standalone";
    }

    @AfterAll
    public void stopContainer() {
        redisContainer.stop();
    }

    @BeforeEach
    public void setup() throws InitializationException {
        flushDatabase();

        testRunner = TestRunners.newTestRunner(new FakeRedisProcessor());

        redisConnectionPool = new RedisConnectionPoolService();
        testRunner.addControllerService("redis-pool", redisConnectionPool);
        testRunner.setProperty(redisConnectionPool, RedisUtils.REDIS_MODE, getRedisMode());
        testRunner.setProperty(redisConnectionPool, RedisUtils.CONNECTION_STRING,
                redisContainer.getHost() + ":" + redisContainer.getFirstMappedPort());
        testRunner.setProperty(redisConnectionPool, RedisUtils.USERNAME, "nifi");
        testRunner.setProperty(redisConnectionPool, RedisUtils.PASSWORD, "test-password");
        testRunner.enableControllerService(redisConnectionPool);

        cacheClient = new RedisDistributedMapCacheClientService();
        testRunner.addControllerService("redis-cache", cacheClient);
        testRunner.setProperty(cacheClient, RedisUtils.REDIS_CONNECTION_POOL, "redis-pool");
        // A non-zero TTL exercises the value-plus-expiration path of the atomic operations.
        testRunner.setProperty(cacheClient, RedisUtils.TTL, "60 secs");
        testRunner.enableControllerService(cacheClient);
    }

    @AfterEach
    public void teardown() {
        if (cacheClient != null) {
            testRunner.disableControllerService(cacheClient);
        }
        if (redisConnectionPool != null) {
            testRunner.disableControllerService(redisConnectionPool);
        }
        flushDatabase();
    }

    @Test
    public void testGetAndPutIfAbsentReturnsNullWhenKeyAbsent() throws IOException {
        final String key = "detect-duplicate-absent";

        final String previous = cacheClient.getAndPutIfAbsent(key, "v1",
                STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER);

        assertNull(previous);
        assertEquals("v1", cacheClient.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    public void testGetAndPutIfAbsentReturnsExistingWhenKeyPresent() throws IOException {
        final String key = "detect-duplicate-present";

        // first occurrence: absent, stored, returns null
        assertNull(cacheClient.getAndPutIfAbsent(key, "v1",
                STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER));

        // second occurrence: present -> must return the existing value and NOT overwrite it.
        final String previous = cacheClient.getAndPutIfAbsent(key, "v2",
                STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("v1", previous);
        assertEquals("v1", cacheClient.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    public void testReplaceSucceedsWithMatchingRevision() throws IOException {
        final String key = "replace-match";
        cacheClient.put(key, "v1", STRING_SERIALIZER, STRING_SERIALIZER);

        final AtomicCacheEntry<String, String, byte[]> entry =
                cacheClient.fetch(key, STRING_SERIALIZER, STRING_DESERIALIZER);
        assertEquals("v1", entry.getValue());
        assertTrue(entry.getRevision().isPresent());

        entry.setValue("v2");
        assertTrue(cacheClient.replace(entry, STRING_SERIALIZER, STRING_SERIALIZER));
        assertEquals("v2", cacheClient.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    public void testReplaceFailsWithStaleRevision() throws IOException {
        final String key = "replace-stale";
        cacheClient.put(key, "v1", STRING_SERIALIZER, STRING_SERIALIZER);

        final AtomicCacheEntry<String, String, byte[]> entry =
                cacheClient.fetch(key, STRING_SERIALIZER, STRING_DESERIALIZER);

        // another writer changes the key after the fetch -> the entry's revision is now stale
        cacheClient.put(key, "changed", STRING_SERIALIZER, STRING_SERIALIZER);

        entry.setValue("v2");
        assertFalse(cacheClient.replace(entry, STRING_SERIALIZER, STRING_SERIALIZER));
        assertEquals("changed", cacheClient.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    public void testReplaceCreatesWhenKeyAbsent() throws IOException {
        final String key = "replace-create";

        // no revision -> create-if-absent semantics
        final AtomicCacheEntry<String, String, byte[]> createEntry = new AtomicCacheEntry<>(key, "v1", null);
        assertTrue(cacheClient.replace(createEntry, STRING_SERIALIZER, STRING_SERIALIZER));
        assertEquals("v1", cacheClient.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));

        // a second create against a now-present key must fail
        final AtomicCacheEntry<String, String, byte[]> secondCreate = new AtomicCacheEntry<>(key, "v2", null);
        assertFalse(cacheClient.replace(secondCreate, STRING_SERIALIZER, STRING_SERIALIZER));
        assertEquals("v1", cacheClient.get(key, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    public void testPutIfAbsentSetsExpirationWithoutChangingExistingExpiration() throws IOException {
        assertTrue(cacheClient.putIfAbsent("ttl", "v1", STRING_SERIALIZER, STRING_SERIALIZER));
        try (RedisConnection connection = redisConnectionPool.getConnection()) {
            final byte[] key = "ttl".getBytes(StandardCharsets.UTF_8);
            final long ttl = connection.keyCommands().ttl(key);
            assertTrue(ttl > 0 && ttl <= 60);
            connection.keyCommands().expire(key, 20);
            assertFalse(cacheClient.putIfAbsent("ttl", "v2", STRING_SERIALIZER, STRING_SERIALIZER));
            assertTrue(connection.keyCommands().ttl(key) <= 20);
        }
        assertEquals("v1", cacheClient.get("ttl", STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    public void testGetAndPutIfAbsentDoesNotRefreshExistingExpiration() throws IOException {
        assertNull(cacheClient.getAndPutIfAbsent("ttl", "v1", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER));
        try (RedisConnection connection = redisConnectionPool.getConnection()) {
            final byte[] key = "ttl".getBytes(StandardCharsets.UTF_8);
            assertTrue(connection.keyCommands().ttl(key) > 0);
            connection.keyCommands().expire(key, 20);
            assertEquals("v1", cacheClient.getAndPutIfAbsent("ttl", "v2", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER));
            assertTrue(connection.keyCommands().ttl(key) <= 20);
        }
    }

    @Test
    public void testReplacePreservesExpirationOnConflictAndRefreshesOnSuccess() throws IOException {
        cacheClient.put("ttl", "v1", STRING_SERIALIZER, STRING_SERIALIZER);
        final AtomicCacheEntry<String, String, byte[]> entry = cacheClient.fetch("ttl", STRING_SERIALIZER, STRING_DESERIALIZER);
        try (RedisConnection connection = redisConnectionPool.getConnection()) {
            final byte[] key = "ttl".getBytes(StandardCharsets.UTF_8);
            connection.keyCommands().expire(key, 20);
            assertFalse(cacheClient.replace(new AtomicCacheEntry<>("ttl", "v2", new byte[0]), STRING_SERIALIZER, STRING_SERIALIZER));
            assertTrue(connection.keyCommands().ttl(key) <= 20);
            entry.setValue("v2");
            assertTrue(cacheClient.replace(entry, STRING_SERIALIZER, STRING_SERIALIZER));
            assertTrue(connection.keyCommands().ttl(key) > 20);
        }
    }

    @Test
    public void testBinaryAndEmptyValuesAreDistinctFromMissingKeys() throws IOException {
        final Serializer<byte[]> serializer = (value, output) -> output.write(value);
        final Deserializer<byte[]> deserializer = value -> value;
        final byte[] key = {0, (byte) 255, 1};
        final byte[] empty = new byte[0];
        final byte[] binary = {0, (byte) 255, 2};
        assertNull(cacheClient.getAndPutIfAbsent(key, empty, serializer, serializer, deserializer));
        assertArrayEquals(empty, cacheClient.getAndPutIfAbsent(key, binary, serializer, serializer, deserializer));
        assertFalse(cacheClient.replace(new AtomicCacheEntry<>(key, binary, null), serializer, serializer));
        assertTrue(cacheClient.replace(new AtomicCacheEntry<>(key, binary, empty), serializer, serializer));
        assertArrayEquals(binary, cacheClient.fetch(key, serializer, deserializer).getRevision().orElseThrow());
        assertFalse(cacheClient.replace(new AtomicCacheEntry<>(new byte[]{3}, binary, empty), serializer, serializer));
    }

    @Test
    public void testZeroTtlCreatesPersistentEntries() throws IOException {
        testRunner.disableControllerService(cacheClient);
        testRunner.setProperty(cacheClient, RedisUtils.TTL, "0 secs");
        testRunner.enableControllerService(cacheClient);
        assertTrue(cacheClient.putIfAbsent("put", "v1", STRING_SERIALIZER, STRING_SERIALIZER));
        assertNull(cacheClient.getAndPutIfAbsent("get", "v1", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER));
        assertTrue(cacheClient.replace(new AtomicCacheEntry<>("replace", "v1", null), STRING_SERIALIZER, STRING_SERIALIZER));
        try (RedisConnection connection = redisConnectionPool.getConnection()) {
            for (String key : List.of("put", "get", "replace")) {
                assertEquals(-1L, connection.keyCommands().ttl(key.getBytes(StandardCharsets.UTF_8)));
            }
            // A replacement without TTL must remove an existing expiration, as SET did before this change.
            connection.keyCommands().expire("replace".getBytes(StandardCharsets.UTF_8), 20);
            assertTrue(cacheClient.replace(new AtomicCacheEntry<>("replace", "v2", "v1".getBytes(StandardCharsets.UTF_8)),
                    STRING_SERIALIZER, STRING_SERIALIZER));
            assertEquals(-1L, connection.keyCommands().ttl("replace".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    public void testConcurrentInsertsHaveOneWinner() throws Exception {
        try (var executor = Executors.newFixedThreadPool(8)) {
            final List<Callable<String>> tasks = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                final String value = "value-" + i;
                tasks.add(() -> cacheClient.getAndPutIfAbsent("concurrent", value,
                        STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER));
            }
            int winners = 0;
            final String[] previousValues = new String[tasks.size()];
            final var results = executor.invokeAll(tasks);
            for (int i = 0; i < results.size(); i++) {
                previousValues[i] = results.get(i).get();
                if (previousValues[i] == null) {
                    winners++;
                }
            }
            assertEquals(1, winners);
            final String stored = cacheClient.get("concurrent", STRING_SERIALIZER, STRING_DESERIALIZER);
            for (String previous : previousValues) {
                if (previous != null) {
                    assertEquals(stored, previous);
                }
            }
        }
    }

    @Test
    public void testConcurrentReplacementsHaveOneWinner() throws Exception {
        cacheClient.put("concurrent", "initial", STRING_SERIALIZER, STRING_SERIALIZER);
        try (var executor = Executors.newFixedThreadPool(8)) {
            final List<Callable<Boolean>> tasks = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                final String value = "value-" + i;
                tasks.add(() -> cacheClient.replace(new AtomicCacheEntry<>("concurrent", value, "initial".getBytes(StandardCharsets.UTF_8)),
                        STRING_SERIALIZER, STRING_SERIALIZER));
            }
            int winners = 0;
            for (var result : executor.invokeAll(tasks)) {
                if (result.get()) {
                    winners++;
                }
            }
            assertEquals(1, winners);
        }
    }

    private void flushDatabase() {
        try {
            final Container.ExecResult execResult = redisContainer.execInContainer("redis-cli", "--user", "nifi", "-a", "test-password", "flushall");
            if (execResult.getExitCode() != 0) {
                throw new IllegalStateException(String.format("Failed to flush Redis container: %s%s",
                        execResult.getStdout(), execResult.getStderr()));
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while flushing Redis container", interruptedException);
        } catch (IOException ioException) {
            throw new IllegalStateException("Failed to flush Redis container", ioException);
        }
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
            return input == null || input.length == 0 ? null : new String(input, StandardCharsets.UTF_8);
        }
    }
}
