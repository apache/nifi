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
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link RedisDistributedMapCacheClientService} against a real Redis, covering the atomic
 * operations {@code getAndPutIfAbsent} (used by DetectDuplicate) and {@code replace} (used by Wait/Notify).
 * A non-zero TTL is configured so the tests exercise the value-plus-expiration paths.
 */
public class ITRedisDistributedMapCacheClientService {

    private static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
    private static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();

    public static RedisContainer redisContainer = new RedisContainer(RedisContainer.DEFAULT_IMAGE_NAME).withExposedPorts(6379);

    private TestRunner testRunner;
    private RedisConnectionPoolService redisConnectionPool;
    private RedisDistributedMapCacheClientService cacheClient;

    @BeforeAll
    public static void startContainer() {
        redisContainer.start();
    }

    @AfterAll
    public static void stopContainer() {
        redisContainer.stop();
    }

    @BeforeEach
    public void setup() throws InitializationException {
        flushDatabase();

        testRunner = TestRunners.newTestRunner(new FakeRedisProcessor());

        redisConnectionPool = new RedisConnectionPoolService();
        testRunner.addControllerService("redis-pool", redisConnectionPool);
        testRunner.setProperty(redisConnectionPool, RedisUtils.REDIS_MODE, "Standalone");
        testRunner.setProperty(redisConnectionPool, RedisUtils.CONNECTION_STRING,
                redisContainer.getHost() + ":" + redisContainer.getFirstMappedPort());
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

    private static void flushDatabase() {
        try {
            final Container.ExecResult execResult = redisContainer.execInContainer("redis-cli", "flushall");
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
