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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.testcontainers.RedisContainer;
import org.apache.nifi.redis.testcontainers.RedisReplicaContainer;
import org.apache.nifi.redis.testcontainers.RedisSentinelContainer;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.nifi.redis.util.RedisUtils.REDIS_CONNECTION_POOL;
import static org.apache.nifi.redis.util.RedisUtils.REDIS_MODE_SENTINEL;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is an integration test that is meant to be run against a real Redis instance.
 */
public class ITRedisDistributedMapCacheClientService {

    public static final String CONTAINER_IMAGE_TAG = "redis:7.0.12-alpine";

    private static final String masterName = "redisLeader";
    @TempDir

    private Path testDirectory;

    private final TestRedisProcessor proc = new TestRedisProcessor();
    private final TestRunner testRunner = TestRunners.newTestRunner(proc);

    private final List<RedisContainer> redisContainers = new ArrayList<>();
    private RedisConnectionPoolService redisConnectionPool;

    @Test
    public void testStandaloneRedis() throws InitializationException, IOException {
        int redisPort = setupStandaloneRedis(null,null).port;
        setUpRedisConnectionPool(portsToConnectionString(redisPort), pool -> {
            // uncomment this to test using a different database index than the default 0
            //  testRunner.setProperty(pool, RedisUtils.DATABASE, "1");
        });
        setupRedisMapCacheClientService();

        executeProcessor();
    }

    @Test
    public void testStandaloneRedisWithUsernameAndPasswordAuthentication() throws InitializationException, IOException {
        final String redisUsername = "foo";
        final String redisPassword = "foobared";
        final int redisPort = setupStandaloneRedis(redisUsername, redisPassword).port;
        setUpRedisConnectionPool(portsToConnectionString(redisPort), pool -> {
            testRunner.setProperty(redisConnectionPool, RedisUtils.USERNAME, redisUsername);
            testRunner.setProperty(redisConnectionPool, RedisUtils.PASSWORD, redisPassword);
        });
        setupRedisMapCacheClientService();

        executeProcessor();
    }


    @Test
    public void testStandaloneRedisWithDefaultUserAuthentication() throws InitializationException, IOException {
        final String redisPassword = "foobared";
        final int redisPort = setupStandaloneRedis(null, redisPassword).port;
        setUpRedisConnectionPool(portsToConnectionString(redisPort), pool -> {
            testRunner.setProperty(redisConnectionPool, RedisUtils.PASSWORD, redisPassword);
        });
        setupRedisMapCacheClientService();

        executeProcessor();
    }

    @Test
    public void testSentinelRedis() throws InitializationException, IOException {
        RedisContainer redisMasterContainer = setupStandaloneRedis(null,null);
        String masterHost = "127.0.0.1";
        int masterPort = redisMasterContainer.port;
        setUpRedisReplica(masterHost, masterPort, null, null);
        setUpRedisReplica(masterHost, masterPort, null, null);

        int sentinelAPort = setUpSentinel(masterHost, masterPort, null, null, 2, null, null).port;
        int sentinelBPort = setUpSentinel(masterHost, masterPort, null, null, 2, null, null).port;
        int sentinelCPort = setUpSentinel(masterHost, masterPort, null, null, 2, null, null).port;

        setUpRedisConnectionPool(portsToConnectionString(sentinelAPort, sentinelBPort, sentinelCPort), pool -> {
            testRunner.setProperty(redisConnectionPool, RedisUtils.REDIS_MODE, REDIS_MODE_SENTINEL);
            testRunner.setProperty(redisConnectionPool, RedisUtils.SENTINEL_MASTER, masterName);
        });

        setupRedisMapCacheClientService();

        executeProcessor();
    }

    @Test
    public void testSentinelRedisWithDefaultUserAuthentication() throws InitializationException, IOException {
        String redisPassword = "t0p_53cr35";
        String sentinelPassword = "otherPassword";

        RedisContainer redisMasterContainer = setupStandaloneRedis(null, redisPassword);
        String masterHost = "127.0.0.1";
        int masterPort = redisMasterContainer.port;
        setUpRedisReplica(masterHost, masterPort, null, redisPassword);
        setUpRedisReplica(masterHost, masterPort, null, redisPassword);

        int sentinelAPort = setUpSentinel(masterHost, masterPort, null, redisPassword, 2, null, sentinelPassword).port;
        int sentinelBPort = setUpSentinel(masterHost, masterPort, null, redisPassword, 2, null, sentinelPassword).port;
        int sentinelCPort = setUpSentinel(masterHost, masterPort, null, redisPassword, 2, null, sentinelPassword).port;

        setUpRedisConnectionPool(portsToConnectionString(sentinelAPort, sentinelBPort, sentinelCPort), pool -> {
            testRunner.setProperty(redisConnectionPool, RedisUtils.REDIS_MODE, REDIS_MODE_SENTINEL);
            testRunner.setProperty(redisConnectionPool, RedisUtils.SENTINEL_MASTER, masterName);

            testRunner.setProperty(redisConnectionPool, RedisUtils.PASSWORD, redisPassword);
            testRunner.setProperty(redisConnectionPool, RedisUtils.SENTINEL_PASSWORD, sentinelPassword);
        });
        setupRedisMapCacheClientService();

        executeProcessor();
    }

    @Test
    public void testSentinelRedisWithUsernameAndPasswordAuthentication() throws InitializationException, IOException {
        final String redisUser = "redisUser";
        final String redisPassword = "t0p_53cr35";
        final String sentinelUsername = "sentinelUser";
        final String sentinelPassword = "otherPassword";

        final RedisContainer redisMasterContainer = setupStandaloneRedis(redisUser, redisPassword);
        final String masterHost = "127.0.0.1";
        final int masterPort = redisMasterContainer.port;
        setUpRedisReplica(masterHost, masterPort, redisUser, redisPassword);
        setUpRedisReplica(masterHost, masterPort, redisUser, redisPassword);

        int sentinelAPort = setUpSentinel(masterHost, masterPort, redisUser, redisPassword, 2, sentinelUsername, sentinelPassword).port;
        int sentinelBPort = setUpSentinel(masterHost, masterPort, redisUser, redisPassword, 2, sentinelUsername, sentinelPassword).port;
        int sentinelCPort = setUpSentinel(masterHost, masterPort, redisUser, redisPassword, 2, sentinelUsername, sentinelPassword).port;

        setUpRedisConnectionPool(portsToConnectionString(sentinelAPort, sentinelBPort, sentinelCPort), pool -> {
            testRunner.setProperty(redisConnectionPool, RedisUtils.REDIS_MODE, REDIS_MODE_SENTINEL);
            testRunner.setProperty(redisConnectionPool, RedisUtils.SENTINEL_MASTER, masterName);

            testRunner.setProperty(redisConnectionPool, RedisUtils.USERNAME, redisUser);
            testRunner.setProperty(redisConnectionPool, RedisUtils.PASSWORD, redisPassword);
            testRunner.setProperty(redisConnectionPool, RedisUtils.SENTINEL_USERNAME, sentinelUsername);
            testRunner.setProperty(redisConnectionPool, RedisUtils.SENTINEL_PASSWORD, sentinelPassword);
        });
        setupRedisMapCacheClientService();

        executeProcessor();
    }

    @AfterEach
    public void teardown() {
        if (redisConnectionPool != null) {
            redisConnectionPool.onDisabled();
        }

        redisContainers.forEach(RedisContainer::stop);
    }

    private RedisContainer setupStandaloneRedis(@Nullable final String redisUsername, final @Nullable String redisPassword) throws IOException {
        int redisPort = getAvailablePort();

        RedisContainer redisContainer = new RedisContainer(CONTAINER_IMAGE_TAG);
        redisContainer.mountConfigurationFrom(testDirectory);
        redisContainer.setPort(redisPort);
        redisContainer.addPortBinding(redisPort, redisPort);
        redisContainer.setUsername(redisUsername);
        redisContainer.setPassword(redisPassword);

        redisContainers.add(redisContainer);
        redisContainer.start();

        return redisContainer;
    }

    private RedisReplicaContainer setUpRedisReplica(final @NonNull String masterHost,
                                                    final int masterPort,
                                                    final @Nullable String redisUsername,
                                                    final @Nullable String redisPassword) throws IOException {
        int replicaPort = getAvailablePort();

        RedisReplicaContainer redisReplicaContainer = new RedisReplicaContainer(CONTAINER_IMAGE_TAG);
        redisReplicaContainer.mountConfigurationFrom(testDirectory);
        redisReplicaContainer.setPort(replicaPort);
        redisReplicaContainer.addPortBinding(replicaPort, replicaPort);
        redisReplicaContainer.setReplicaOf(masterHost, masterPort);
        redisReplicaContainer.setUsername(redisUsername);
        redisReplicaContainer.setPassword(redisPassword);

        redisContainers.add(redisReplicaContainer);
        redisReplicaContainer.start();

        return redisReplicaContainer;
    }

    private RedisSentinelContainer setUpSentinel(final @NonNull String masterHost,
                                                 final int masterPort,
                                                 final @Nullable String redisUsername,
                                                 final @Nullable String redisPassword,
                                                 final int quorumSize,
                                                 final @Nullable String sentinelUsername,
                                                 final @Nullable String sentinelPassword) throws IOException {
        int sentinelPort = getAvailablePort();

        RedisSentinelContainer redisSentinelContainer = new RedisSentinelContainer(CONTAINER_IMAGE_TAG);
        redisSentinelContainer.mountConfigurationFrom(testDirectory);
        redisSentinelContainer.setPort(sentinelPort);
        redisSentinelContainer.addPortBinding(sentinelPort, sentinelPort);
        redisSentinelContainer.setMasterHost(masterHost);
        redisSentinelContainer.setMasterPort(masterPort);
        redisSentinelContainer.setMasterName(masterName);
        redisSentinelContainer.setQuorumSize(quorumSize);
        redisSentinelContainer.setUsername(redisUsername);
        redisSentinelContainer.setPassword(redisPassword);
        redisSentinelContainer.setSentinelUsername(sentinelUsername);
        redisSentinelContainer.setSentinelPassword(sentinelPassword);

        redisContainers.add(redisSentinelContainer);
        redisSentinelContainer.start();

        return redisSentinelContainer;
    }

    private int getAvailablePort() throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            socket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socket.bind(new InetSocketAddress("localhost", 0));
            return socket.socket().getLocalPort();
        }
    }

    private String portsToConnectionString(int... ports) {
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < ports.length; i++) {
            if (i > 0) {
                stringBuilder.append(',');
            }
            stringBuilder.append("localhost");
            stringBuilder.append(':');
            stringBuilder.append(ports[i]);
        }

        return stringBuilder.toString();
    }

    public void setUpRedisConnectionPool(String connectionString, Consumer<RedisConnectionPool> initializer) throws InitializationException {
        // create, configure, and enable the RedisConnectionPool service
        redisConnectionPool = new RedisConnectionPoolService();
        testRunner.addControllerService("redis-connection-pool", redisConnectionPool);
        testRunner.setProperty(redisConnectionPool, RedisUtils.CONNECTION_STRING, connectionString);

        if (initializer != null) {
            initializer.accept(redisConnectionPool);
        }

        testRunner.enableControllerService(redisConnectionPool);
    }

    private void setupRedisMapCacheClientService() throws InitializationException {
        // create, configure, and enable the RedisDistributedMapCacheClient service
        RedisDistributedMapCacheClientService redisMapCacheClientService = new RedisDistributedMapCacheClientService();
        testRunner.addControllerService("redis-map-cache-client", redisMapCacheClientService);
        testRunner.setProperty(redisMapCacheClientService, REDIS_CONNECTION_POOL, "redis-connection-pool");
        testRunner.enableControllerService(redisMapCacheClientService);
        testRunner.setProperty(TestRedisProcessor.REDIS_MAP_CACHE, "redis-map-cache-client");
    }

    private void executeProcessor() {
        // queue a flow file to trigger the processor and executeProcessor it
        testRunner.enqueue("trigger");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(TestRedisProcessor.REL_SUCCESS, 1);
    }

    /**
     * Test processor that exercises RedisDistributedMapCacheClient.
     */
    private static class TestRedisProcessor extends AbstractProcessor {

        public static final PropertyDescriptor REDIS_MAP_CACHE = new PropertyDescriptor.Builder()
                .name("redis-map-cache")
                .displayName("Redis Map Cache")
                .identifiesControllerService(AtomicDistributedMapCacheClient.class)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(true)
                .build();

        public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
        public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").build();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.singletonList(REDIS_MAP_CACHE);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            final FlowFile flowFile = session.get();
            if (flowFile == null) {
                return;
            }

            final Serializer<String> stringSerializer = new StringSerializer();
            final Deserializer<String> stringDeserializer = new StringDeserializer();
            final Deserializer<String> stringDeserializerWithoutNullCheck = new StringDeserializerWithoutNullCheck();

            final AtomicDistributedMapCacheClient cacheClient = context.getProperty(REDIS_MAP_CACHE).asControllerService(AtomicDistributedMapCacheClient.class);

            try {
                final long timestamp = System.currentTimeMillis();
                final String key = "test-redis-processor-" + timestamp;
                final String value = "the time is " + timestamp;

                // verify the key doesn't exist, put the key/value, then verify it exists
                assertFalse(cacheClient.containsKey(key, stringSerializer));
                cacheClient.put(key, value, stringSerializer, stringSerializer);
                assertTrue(cacheClient.containsKey(key, stringSerializer));

                // verify get returns the expected value we set above
                final String retrievedValue = cacheClient.get(key, stringSerializer, stringDeserializer);
                assertEquals(value, retrievedValue);

                // verify get returns null for a key that doesn't exist
                final String missingValue = cacheClient.get("does-not-exist", stringSerializer, stringDeserializerWithoutNullCheck);
                assertNull(missingValue);

                // verify remove removes the entry and contains key returns false after
                assertTrue(cacheClient.remove(key, stringSerializer));
                assertFalse(cacheClient.containsKey(key, stringSerializer));

                // verify putIfAbsent works the first time and returns false the second time
                assertTrue(cacheClient.putIfAbsent(key, value, stringSerializer, stringSerializer));
                assertFalse(cacheClient.putIfAbsent(key, "some other value", stringSerializer, stringSerializer));
                assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify that getAndPutIfAbsent returns the existing value and doesn't modify it in the cache
                final String getAndPutIfAbsentResult = cacheClient.getAndPutIfAbsent(key, value, stringSerializer, stringSerializer, stringDeserializer);
                assertEquals(value, getAndPutIfAbsentResult);
                assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify that getAndPutIfAbsent on a key that doesn't exist returns null
                final String keyThatDoesntExist = key + "_DOES_NOT_EXIST";
                assertFalse(cacheClient.containsKey(keyThatDoesntExist, stringSerializer));
                final String getAndPutIfAbsentResultWhenDoesntExist = cacheClient.getAndPutIfAbsent(keyThatDoesntExist, value, stringSerializer, stringSerializer, stringDeserializer);
                assertNull(getAndPutIfAbsentResultWhenDoesntExist);
                assertEquals(value, cacheClient.get(keyThatDoesntExist, stringSerializer, stringDeserializer));

                // verify atomic fetch returns the correct entry
                final AtomicCacheEntry<String, String, byte[]> entry = cacheClient.fetch(key, stringSerializer, stringDeserializer);
                assertEquals(key, entry.getKey());
                assertEquals(value, entry.getValue());
                assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), entry.getRevision().orElse(null));

                final AtomicCacheEntry<String, String, byte[]> notLatestEntry = new AtomicCacheEntry<>(entry.getKey(), entry.getValue(), "not previous".getBytes(StandardCharsets.UTF_8));

                // verify atomic replace does not replace when previous value is not equal
                assertFalse(cacheClient.replace(notLatestEntry, stringSerializer, stringSerializer));
                assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify atomic replace does replace when previous value is equal
                final String replacementValue = "this value has been replaced";
                entry.setValue(replacementValue);
                assertTrue(cacheClient.replace(entry, stringSerializer, stringSerializer));
                assertEquals(replacementValue, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify atomic replace does replace no value previous existed
                final String replaceKeyDoesntExist = key + "_REPLACE_DOES_NOT_EXIST";
                final AtomicCacheEntry<String, String, byte[]> entryDoesNotExist = new AtomicCacheEntry<>(replaceKeyDoesntExist, replacementValue, null);
                assertTrue(cacheClient.replace(entryDoesNotExist, stringSerializer, stringSerializer));
                assertEquals(replacementValue, cacheClient.get(replaceKeyDoesntExist, stringSerializer, stringDeserializer));

                final int numToDelete = 2000;
                for (int i = 0; i < numToDelete; i++) {
                    cacheClient.put(key + "-" + i, value, stringSerializer, stringSerializer);
                }

                Map<String, String> bulk = new HashMap<>();
                bulk.put("bulk-1", "testing1");
                bulk.put("bulk-2", "testing2");
                bulk.put("bulk-3", "testing3");
                bulk.put("bulk-4", "testing4");
                bulk.put("bulk-5", "testing5");

                cacheClient.putAll(bulk, stringSerializer, stringSerializer);
                assertTrue(cacheClient.containsKey("bulk-1", stringSerializer));
                assertTrue(cacheClient.containsKey("bulk-2", stringSerializer));
                assertTrue(cacheClient.containsKey("bulk-3", stringSerializer));
                assertTrue(cacheClient.containsKey("bulk-4", stringSerializer));
                assertTrue(cacheClient.containsKey("bulk-5", stringSerializer));

                session.transfer(flowFile, REL_SUCCESS);
            } catch (final Exception e) {
                getLogger().error("Routing to failure due to: " + e.getMessage(), e);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(String value, OutputStream output) throws SerializationException, IOException {
            if (value != null) {
                output.write(value.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static class StringDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(byte[] input) throws DeserializationException {
            return input == null ? null : new String(input, StandardCharsets.UTF_8);
        }
    }

    private static class StringDeserializerWithoutNullCheck implements Deserializer<String> {
        @Override
        public String deserialize(byte[] input) throws DeserializationException {
            return new String(input, StandardCharsets.UTF_8);
        }
    }
}

