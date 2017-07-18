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
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is an integration test that is meant to be run against a real Redis instance.
 */
public class ITRedisDistributedMapCacheClientService {

    private TestRedisProcessor proc;
    private TestRunner testRunner;
    private RedisServer redisServer;
    private RedisConnectionPoolService redisConnectionPool;
    private RedisDistributedMapCacheClientService redisMapCacheClientService;
    private int redisPort;

    @Before
    public void setup() throws IOException {
        this.redisPort = getAvailablePort();

        this.redisServer = new RedisServer(redisPort);
        redisServer.start();

        proc = new TestRedisProcessor();
        testRunner = TestRunners.newTestRunner(proc);
    }

    private int getAvailablePort() throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            socket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socket.bind(new InetSocketAddress("localhost", 0));
            return socket.socket().getLocalPort();
        }
    }

    @After
    public void teardown() throws IOException {
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Test
    public void testStandaloneRedis() throws InitializationException {
        try {
            // create, configure, and enable the RedisConnectionPool service
            redisConnectionPool = new RedisConnectionPoolService();
            testRunner.addControllerService("redis-connection-pool", redisConnectionPool);
            testRunner.setProperty(redisConnectionPool, RedisUtils.CONNECTION_STRING, "localhost:" + redisPort);

            // uncomment this to test using a different database index than the default 0
            //testRunner.setProperty(redisConnectionPool, RedisUtils.DATABASE, "1");

            // uncomment this to test using a password to authenticate to redis
            //testRunner.setProperty(redisConnectionPool, RedisUtils.PASSWORD, "foobared");

            testRunner.enableControllerService(redisConnectionPool);

            setupRedisMapCacheClientService();
            executeProcessor();
        } finally {
            if (redisConnectionPool != null) {
                redisConnectionPool.onDisabled();
            }
        }
    }

    private void setupRedisMapCacheClientService() throws InitializationException {
        // create, configure, and enable the RedisDistributedMapCacheClient service
        redisMapCacheClientService = new RedisDistributedMapCacheClientService();
        testRunner.addControllerService("redis-map-cache-client", redisMapCacheClientService);
        testRunner.setProperty(redisMapCacheClientService, RedisDistributedMapCacheClientService.REDIS_CONNECTION_POOL, "redis-connection-pool");
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

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final Serializer<String> stringSerializer = new StringSerializer();
            final Deserializer<String> stringDeserializer = new StringDeserializer();

            final AtomicDistributedMapCacheClient cacheClient = context.getProperty(REDIS_MAP_CACHE).asControllerService(AtomicDistributedMapCacheClient.class);

            try {
                final long timestamp = System.currentTimeMillis();
                final String key = "test-redis-processor-" + timestamp;
                final String value = "the time is " + timestamp;

                // verify the key doesn't exists, put the key/value, then verify it exists
                Assert.assertFalse(cacheClient.containsKey(key, stringSerializer));
                cacheClient.put(key, value, stringSerializer, stringSerializer);
                Assert.assertTrue(cacheClient.containsKey(key, stringSerializer));

                // verify get returns the expected value we set above
                final String retrievedValue = cacheClient.get(key, stringSerializer, stringDeserializer);
                Assert.assertEquals(value, retrievedValue);

                // verify remove removes the entry and contains key returns false after
                Assert.assertTrue(cacheClient.remove(key, stringSerializer));
                Assert.assertFalse(cacheClient.containsKey(key, stringSerializer));

                // verify putIfAbsent works the first time and returns false the second time
                Assert.assertTrue(cacheClient.putIfAbsent(key, value, stringSerializer, stringSerializer));
                Assert.assertFalse(cacheClient.putIfAbsent(key, "some other value", stringSerializer, stringSerializer));
                Assert.assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify that getAndPutIfAbsent returns the existing value and doesn't modify it in the cache
                final String getAndPutIfAbsentResult = cacheClient.getAndPutIfAbsent(key, value, stringSerializer, stringSerializer, stringDeserializer);
                Assert.assertEquals(value, getAndPutIfAbsentResult);
                Assert.assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify that getAndPutIfAbsent on a key that doesn't exist returns null
                final String keyThatDoesntExist = key + "_DOES_NOT_EXIST";
                Assert.assertFalse(cacheClient.containsKey(keyThatDoesntExist, stringSerializer));
                final String getAndPutIfAbsentResultWhenDoesntExist = cacheClient.getAndPutIfAbsent(keyThatDoesntExist, value, stringSerializer, stringSerializer, stringDeserializer);
                Assert.assertEquals(null, getAndPutIfAbsentResultWhenDoesntExist);
                Assert.assertEquals(value, cacheClient.get(keyThatDoesntExist, stringSerializer, stringDeserializer));

                // verify atomic fetch returns the correct entry
                final AtomicCacheEntry<String,String,byte[]> entry = cacheClient.fetch(key, stringSerializer, stringDeserializer);
                Assert.assertEquals(key, entry.getKey());
                Assert.assertEquals(value, entry.getValue());
                Assert.assertTrue(Arrays.equals(value.getBytes(StandardCharsets.UTF_8), entry.getRevision().orElse(null)));

                final AtomicCacheEntry<String,String,byte[]> notLatestEntry = new AtomicCacheEntry<>(entry.getKey(), entry.getValue(), "not previous".getBytes(StandardCharsets.UTF_8));

                // verify atomic replace does not replace when previous value is not equal
                Assert.assertFalse(cacheClient.replace(notLatestEntry, stringSerializer, stringSerializer));
                Assert.assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify atomic replace does replace when previous value is equal
                final String replacementValue = "this value has been replaced";
                entry.setValue(replacementValue);
                Assert.assertTrue(cacheClient.replace(entry, stringSerializer, stringSerializer));
                Assert.assertEquals(replacementValue, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify atomic replace does replace no value previous existed
                final String replaceKeyDoesntExist = key + "_REPLACE_DOES_NOT_EXIST";
                final AtomicCacheEntry<String,String,byte[]> entryDoesNotExist = new AtomicCacheEntry<>(replaceKeyDoesntExist, replacementValue, null);
                Assert.assertTrue(cacheClient.replace(entryDoesNotExist, stringSerializer, stringSerializer));
                Assert.assertEquals(replacementValue, cacheClient.get(replaceKeyDoesntExist, stringSerializer, stringDeserializer));

                final int numToDelete = 2000;
                for (int i=0; i < numToDelete; i++) {
                    cacheClient.put(key + "-" + i, value, stringSerializer, stringSerializer);
                }

                Assert.assertTrue(cacheClient.removeByPattern("test-redis-processor-*") >= numToDelete);
                Assert.assertFalse(cacheClient.containsKey(key, stringSerializer));

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
        public String deserialize(byte[] input) throws DeserializationException, IOException {
            return input == null ? null : new String(input, StandardCharsets.UTF_8);
        }
    }
}
