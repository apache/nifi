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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.MapCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.distributed.cache.server.CacheServer;
import org.apache.nifi.distributed.cache.server.AbstractCacheServer;
import org.apache.nifi.distributed.cache.server.EvictionPolicy;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.remote.StandardVersionNegotiator;
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

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDistributedMapServerAndClient {

    private final File dataFile = new File("target/cache-data");

    private TestRunner runner;

    private MapCacheServer server;

    @BeforeEach
    public void setRunner() throws InitializationException, IOException {
        if (dataFile.exists()) {
            FileUtils.cleanDirectory(dataFile);
        }

        runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));

        server = new MapCacheServer();
        runner.addControllerService("server", server);

        runner.setProperty(server, MapCacheServer.PORT, "0");
    }

    @AfterEach
    public void shutdownServer() throws IOException {
        server.shutdownServer();
    }

    @Test
    public void testNonPersistentMapServerAndClient() throws InitializationException, IOException {
        runner.enableControllerService(server);

        MapCacheClientService client = new MapCacheClientService();
        try {
            runner.addControllerService("client", client);
            runner.setProperty(client, MapCacheClientService.HOSTNAME, "localhost");
            runner.setProperty(client, MapCacheClientService.PORT, String.valueOf(server.getPort()));
            runner.enableControllerService(client);

            final Serializer<String> valueSerializer = new StringSerializer();
            final Serializer<String> keySerializer = new StringSerializer();
            final Deserializer<String> deserializer = new StringDeserializer();

            final String original = client.getAndPutIfAbsent("testKey", "test", keySerializer, valueSerializer, deserializer);
            assertNull(original);

            final boolean contains = client.containsKey("testKey", keySerializer);
            assertTrue(contains);

            final boolean added = client.putIfAbsent("testKey", "test", keySerializer, valueSerializer);
            assertFalse(added);

            final String originalAfterPut = client.getAndPutIfAbsent("testKey", "test", keySerializer, valueSerializer, deserializer);
            assertEquals("test", originalAfterPut);

            final boolean removed = client.remove("testKey", keySerializer);
            assertTrue(removed);

            client.put("testKey", "testValue", keySerializer, valueSerializer);
            assertTrue(client.containsKey("testKey", keySerializer));
            String removedValue = client.removeAndGet("testKey", keySerializer, deserializer);
            assertEquals("testValue", removedValue);
            removedValue = client.removeAndGet("testKey", keySerializer, deserializer);
            assertNull(removedValue);

            final Set<String> keys = client.keySet(deserializer);
            assertEquals(0, keys.size());

            client.putIfAbsent("testKey", "test", keySerializer, valueSerializer);
            runner.disableControllerService(client);

            assertThrows(Exception.class, () -> client.containsKey("testKey", keySerializer));
        } finally {
            client.close();
        }
    }

    @Test
    public void testOptimisticLock() throws Exception {
        runner.enableControllerService(server);

        MapCacheClientService client1 = new MapCacheClientService();
        MockControllerServiceInitializationContext clientInitContext1 = new MockControllerServiceInitializationContext(client1, "client1");
        client1.initialize(clientInitContext1);

        MapCacheClientService client2 = new MapCacheClientService();
        MockControllerServiceInitializationContext clientInitContext2 = new MockControllerServiceInitializationContext(client2, "client2");
        client2.initialize(clientInitContext2);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(MapCacheClientService.HOSTNAME, "localhost");
        clientProperties.put(MapCacheClientService.PORT, String.valueOf(server.getPort()));
        clientProperties.put(MapCacheClientService.COMMUNICATIONS_TIMEOUT, "360 secs");

        MockConfigurationContext clientContext1 = new MockConfigurationContext(clientProperties, clientInitContext1.getControllerServiceLookup(), null);
        client1.onEnabled(clientContext1);
        MockConfigurationContext clientContext2 = new MockConfigurationContext(clientProperties, clientInitContext2.getControllerServiceLookup(), null);
        client2.onEnabled(clientContext2);

        try {
            final Serializer<String> stringSerializer = new StringSerializer();
            final Deserializer<String> stringDeserializer = new StringDeserializer();

            final String key = "test-optimistic-lock";

            // Ensure there's no existing key
            assertFalse(client1.containsKey(key, stringSerializer));
            assertNull(client1.fetch(key, stringSerializer, stringDeserializer));

            // Client 1 inserts the key.
            client1.put(key, "valueC1-0", stringSerializer, stringSerializer);

            // Client 1 and 2 fetch the key
            AtomicCacheEntry<String, String, Long> c1 = client1.fetch(key, stringSerializer, stringDeserializer);
            AtomicCacheEntry<String, String, Long> c2 = client2.fetch(key, stringSerializer, stringDeserializer);
            assertEquals(Long.valueOf(0), c1.getRevision().orElse(0L));
            assertEquals("valueC1-0", c1.getValue());
            assertEquals(Long.valueOf(0), c2.getRevision().orElse(0L));
            assertEquals("valueC1-0", c2.getValue());

            // Client 1 replace
            c1.setValue("valueC1-1");
            boolean c1Result = client1.replace(c1, stringSerializer, stringSerializer);
            assertTrue(c1Result, "C1 should be able to replace the key");
            // Client 2 replace with the old revision
            c2.setValue("valueC2-1");
            boolean c2Result = client2.replace(c2, stringSerializer, stringSerializer);
            assertFalse(c2Result, "C2 shouldn't be able to replace the key");

            // Client 2 fetch the key again
            c2 = client2.fetch(key, stringSerializer, stringDeserializer);
            assertEquals("valueC1-1", c2.getValue());
            assertEquals(Long.valueOf(1), c2.getRevision().orElse(0L));

            // Now, Client 2 knows the correct revision so it can replace the key
            c2.setValue("valueC2-2");
            c2Result = client2.replace(c2, stringSerializer, stringSerializer);
            assertTrue(c2Result, "C2 should be able to replace the key");

            // Assert the cache
            c2 = client2.fetch(key, stringSerializer, stringDeserializer);
            assertEquals("valueC2-2", c2.getValue());
            assertEquals(Long.valueOf(2), c2.getRevision().orElse(0L));
        } finally {
            client1.close();
            client2.close();
        }
    }

    @Test
    public void testBackwardCompatibility() throws Exception {
        // Create a server that only supports protocol version 1.
        server = new MapCacheServer() {
            @Override
            protected CacheServer createMapCacheServer(int port, int maxSize, SSLContext sslContext, EvictionPolicy evictionPolicy, File persistenceDir, int maxReadSize) throws IOException {
                return new StandardMapCacheServer(getLogger(), getIdentifier(), sslContext, port, maxSize, evictionPolicy, persistenceDir, maxReadSize) {
                    @Override
                    protected StandardVersionNegotiator createVersionNegotiator() {
                        return new StandardVersionNegotiator(ProtocolVersion.V1.value());
                    }
                };
            }
        };
        runner.addControllerService("server", server);
        runner.setProperty(server, MapCacheServer.PORT, "0");
        runner.enableControllerService(server);

        MapCacheClientService client = new MapCacheClientService();
        MockControllerServiceInitializationContext clientInitContext1 = new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext1);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(MapCacheClientService.HOSTNAME, "localhost");
        clientProperties.put(MapCacheClientService.PORT, String.valueOf(server.getPort()));
        clientProperties.put(MapCacheClientService.COMMUNICATIONS_TIMEOUT, "360 secs");

        MockConfigurationContext clientContext = new MockConfigurationContext(clientProperties, clientInitContext1.getControllerServiceLookup(), null);
        client.onEnabled(clientContext);

        try {
            final Serializer<String> stringSerializer = new StringSerializer();
            final Deserializer<String> stringDeserializer = new StringDeserializer();

            final String key = "test-backward-compatibility";

            // Version 1 operations should work
            client.put(key, "value1", stringSerializer, stringSerializer);
            assertEquals("value1", client.get(key, stringSerializer, stringDeserializer));

            assertTrue(client.containsKey(key, stringSerializer));

            assertThrows(UnsupportedOperationException.class, () -> client.fetch(key, stringSerializer, stringDeserializer));

            AtomicCacheEntry<String, String, Long> entry = new AtomicCacheEntry<>(key, "value2", 0L);
            assertThrows(UnsupportedOperationException.class, () -> client.replace(entry, stringSerializer, stringSerializer));

            assertThrows(UnsupportedOperationException.class, () -> client.keySet(stringDeserializer));
            assertThrows(UnsupportedOperationException.class, () -> client.removeAndGet("v.*", stringSerializer, stringDeserializer));
        } finally {
            client.close();
        }
    }

    @Test
    public void testLimitServiceReadSize() throws InitializationException, IOException {
        runner.enableControllerService(server);

        final MapCacheClientService client = createClient(server.getPort());
        try {
            final Serializer<String> serializer = new StringSerializer();

            final String value = "value";
            final int maxReadSize = new MockPropertyValue(AbstractCacheServer.MAX_READ_SIZE.getDefaultValue()).asDataSize(DataUnit.B).intValue();
            final int belowThreshold = maxReadSize / value.length();
            final int aboveThreshold = belowThreshold + 1;
            final String valueBelowThreshold = StringUtils.repeat(value, belowThreshold);
            final String valueAboveThreshold = StringUtils.repeat(value, aboveThreshold);
            assertFalse(client.containsKey(valueBelowThreshold, serializer));
            assertThrows(IOException.class, () -> client.containsKey(valueAboveThreshold, serializer));
        } finally {
            client.close();
        }
    }

    @Test
    public void testIncompleteHandshakeScenario() throws InitializationException, IOException {
        // This is used to simulate a DistributedCacheServer that does not complete the handshake response
        final BlockingQueue<ByteArrayMessage> messages = new LinkedBlockingQueue<>();
        final NettyEventServerFactory serverFactory = getEventServerFactory(0, messages);
        final EventServer eventServer = serverFactory.getEventServer();

        MapCacheClientService client = new MapCacheClientService();

        runner.addControllerService("client", client);
        runner.setProperty(client, MapCacheClientService.HOSTNAME, "localhost");
        runner.setProperty(client, MapCacheClientService.PORT, String.valueOf(eventServer.getListeningPort()));
        runner.setProperty(client, MapCacheClientService.COMMUNICATIONS_TIMEOUT, "250 ms");
        runner.enableControllerService(client);

        final Serializer<String> valueSerializer = new StringSerializer();
        final Serializer<String> keySerializer = new StringSerializer();
        final Deserializer<String> deserializer = new StringDeserializer();

        try {
            assertThrows(IOException.class, () -> client.getAndPutIfAbsent("testKey", "test", keySerializer, valueSerializer, deserializer));
        } finally {
            eventServer.shutdown();
        }
    }

    private NettyEventServerFactory getEventServerFactory(final int port, final BlockingQueue<ByteArrayMessage> messages) throws UnknownHostException {
        final ByteArrayMessageNettyEventServerFactory factory = new ByteArrayMessageNettyEventServerFactory(Mockito.mock(ComponentLog.class),
                InetAddress.getByName("127.0.0.1"), port, TransportProtocol.TCP, "\n".getBytes(), 1024, messages);
        factory.setWorkerThreads(1);
        factory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        factory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        return factory;
    }

    private MapCacheClientService createClient(final int port) throws InitializationException {
        final MapCacheClientService client = new MapCacheClientService();
        final MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(MapCacheClientService.HOSTNAME, "localhost");
        clientProperties.put(MapCacheClientService.PORT, String.valueOf(port));
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

    private static class StringDeserializer implements Deserializer<String> {

        @Override
        public String deserialize(final byte[] input) throws DeserializationException {
            return input.length == 0 ? null : new String(input, StandardCharsets.UTF_8);
        }
    }
}
