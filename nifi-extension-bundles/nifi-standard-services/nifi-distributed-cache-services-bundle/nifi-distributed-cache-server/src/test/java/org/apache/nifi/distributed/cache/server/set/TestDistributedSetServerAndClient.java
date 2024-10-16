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
package org.apache.nifi.distributed.cache.server.set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.SetCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.server.AbstractCacheServer;
import org.apache.nifi.distributed.cache.server.SetCacheServer;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDistributedSetServerAndClient {

    private final File dataFile = new File("target/cache-data");

    private TestRunner runner;

    private SetCacheServer server;

    @BeforeEach
    public void setRunner() throws InitializationException, IOException {
        if (dataFile.exists()) {
            FileUtils.cleanDirectory(dataFile);
        }

        runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));

        server = new SetCacheServer();
        runner.addControllerService("server", server);

        runner.setProperty(server, SetCacheServer.PORT, "0");
    }

    @AfterEach
    public void shutdownServer() throws IOException {
        server.shutdownServer();
    }

    @Test
    public void testNonPersistentSetServerAndClient() throws InitializationException, IOException {
        runner.enableControllerService(server);

        final SetCacheClientService client = createClient(server.getPort());
        try {
            final Serializer<String> serializer = new StringSerializer();
            final boolean added = client.addIfAbsent("test", serializer);
            assertTrue(added);

            final boolean contains = client.contains("test", serializer);
            assertTrue(contains);

            final boolean addedAgain = client.addIfAbsent("test", serializer);
            assertFalse(addedAgain);

            final boolean removed = client.remove("test", serializer);
            assertTrue(removed);

            final boolean containedAfterRemove = client.contains("test", serializer);
            assertFalse(containedAfterRemove);
        } finally {
            client.close();
        }
    }

    @Test
    public void testPersistentSetServerAndClient() throws InitializationException, IOException {
        runner.setProperty(server, SetCacheServer.PERSISTENCE_PATH, dataFile.getAbsolutePath());
        runner.enableControllerService(server);

        final SetCacheClientService client = createClient(server.getPort());
        try {
            final Serializer<String> serializer = new StringSerializer();
            final boolean added = client.addIfAbsent("test", serializer);
            final boolean added2 = client.addIfAbsent("test2", serializer);
            assertTrue(added);
            assertTrue(added2);

            final boolean contains = client.contains("test", serializer);
            final boolean contains2 = client.contains("test2", serializer);
            assertTrue(contains);
            assertTrue(contains2);

            final boolean addedAgain = client.addIfAbsent("test", serializer);
            assertFalse(addedAgain);

            final boolean removed = client.remove("test", serializer);
            assertTrue(removed);

            final boolean containedAfterRemove = client.contains("test", serializer);
            assertFalse(containedAfterRemove);
        } finally {
            client.close();
        }
    }

    @Test
    public void testPersistentSetServerAndClientWithLFUEvictions() throws InitializationException, IOException {
        runner.setProperty(server, SetCacheServer.PERSISTENCE_PATH, dataFile.getAbsolutePath());
        runner.setProperty(server, SetCacheServer.MAX_CACHE_ENTRIES, "3");
        runner.setProperty(server, SetCacheServer.EVICTION_POLICY, SetCacheServer.EVICTION_STRATEGY_LFU);
        runner.enableControllerService(server);

        final SetCacheClientService client = createClient(server.getPort());
        try {
            final Serializer<String> serializer = new StringSerializer();
            final boolean added = client.addIfAbsent("test", serializer);
            waitABit();
            final boolean added2 = client.addIfAbsent("test2", serializer);
            waitABit();
            final boolean added3 = client.addIfAbsent("test3", serializer);
            waitABit();
            assertTrue(added);
            assertTrue(added2);
            assertTrue(added3);

            final boolean contains = client.contains("test", serializer);
            final boolean contains2 = client.contains("test2", serializer);
            assertTrue(contains);
            assertTrue(contains2);

            final boolean addedAgain = client.addIfAbsent("test", serializer);
            assertFalse(addedAgain);

            final boolean added4 = client.addIfAbsent("test4", serializer);
            assertTrue(added4);

            // ensure that added3 was evicted because it was used least frequently
            assertFalse(client.contains("test3", serializer));
        } finally {
            client.close();
        }
    }

    @Test
    public void testPersistentSetServerAndClientWithFIFOEvictions() throws InitializationException, IOException {
        runner.setProperty(server, SetCacheServer.PERSISTENCE_PATH, dataFile.getAbsolutePath());
        runner.setProperty(server, SetCacheServer.MAX_CACHE_ENTRIES, "3");
        runner.setProperty(server, SetCacheServer.EVICTION_POLICY, SetCacheServer.EVICTION_STRATEGY_FIFO);
        runner.enableControllerService(server);

        final SetCacheClientService client = createClient(server.getPort());
        try {
            final Serializer<String> serializer = new StringSerializer();

            // add 3 entries to the cache. But, if we add too fast, we'll have the same millisecond
            // for the entry time so we don't know which entry will be evicted. So we wait a few millis in between
            final boolean added = client.addIfAbsent("test", serializer);
            waitABit();
            final boolean added2 = client.addIfAbsent("test2", serializer);
            waitABit();
            final boolean added3 = client.addIfAbsent("test3", serializer);
            waitABit();

            assertTrue(added);
            assertTrue(added2);
            assertTrue(added3);

            final boolean contains = client.contains("test", serializer);
            final boolean contains2 = client.contains("test2", serializer);
            assertTrue(contains);
            assertTrue(contains2);

            final boolean addedAgain = client.addIfAbsent("test", serializer);
            assertFalse(addedAgain);

            final boolean added4 = client.addIfAbsent("test4", serializer);
            assertTrue(added4);

            // ensure that added3 was evicted because it was used least frequently
            assertFalse(client.contains("test", serializer));
            assertTrue(client.contains("test3", serializer));
        } finally {
            client.close();
        }
    }

    @Test
    public void testLimitServiceReadSize() throws InitializationException, IOException {
        runner.enableControllerService(server);

        final SetCacheClientService client = createClient(server.getPort());
        try {
            final Serializer<String> serializer = new StringSerializer();

            final String value = "value";
            final int maxReadSize = new MockPropertyValue(AbstractCacheServer.MAX_READ_SIZE.getDefaultValue()).asDataSize(DataUnit.B).intValue();
            final int belowThreshold = maxReadSize / value.length();
            final int aboveThreshold = belowThreshold + 1;
            final String valueBelowThreshold = StringUtils.repeat(value, belowThreshold);
            final String valueAboveThreshold = StringUtils.repeat(value, aboveThreshold);
            assertFalse(client.contains(valueBelowThreshold, serializer));
            assertThrows(IOException.class, () -> client.contains(valueAboveThreshold, serializer));
        } finally {
            client.close();
        }
    }

    private void waitABit() {
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private SetCacheClientService createClient(final int port) throws InitializationException {
        final SetCacheClientService client = new SetCacheClientService();
        final MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(SetCacheClientService.HOSTNAME, "localhost");
        clientProperties.put(SetCacheClientService.PORT, String.valueOf(port));
        final MockConfigurationContext clientContext = new MockConfigurationContext(clientProperties, clientInitContext.getControllerServiceLookup(), null);
        client.onEnabled(clientContext);

        return client;
    }

    private static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream output) throws SerializationException, IOException {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
