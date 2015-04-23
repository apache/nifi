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
package org.apache.nifi.distributed.cache.server;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.distributed.cache.server.DistributedSetCacheServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService;
import org.apache.nifi.distributed.cache.client.DistributedSetCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;

import org.apache.commons.lang3.SerializationException;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServerAndClient {

    private static Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.distributed.cache.server.AbstractCacheServer", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.distributed.cache.client.DistributedMapCacheClientService", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.distributed.cache.server.TestServerAndClient", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.remote.io.socket.ssl.SSLSocketChannel", "trace");
        LOGGER = LoggerFactory.getLogger(TestServerAndClient.class);
    }

    @Ignore("Test fails when in a maven parallel build due to address/port already taken - need to vary these so tests can run in parallel")
    @Test
    public void testNonPersistentSetServerAndClient() throws InitializationException, IOException {
        LOGGER.info("Testing " + Thread.currentThread().getStackTrace()[1].getMethodName());
        // Create server
        final DistributedSetCacheServer server = new DistributedSetCacheServer();
        MockControllerServiceInitializationContext serverInitContext = new MockControllerServiceInitializationContext(server, "server");
        server.initialize(serverInitContext);

        final Map<PropertyDescriptor, String> serverProperties = new HashMap<>();
        final MockConfigurationContext serverContext = new MockConfigurationContext(serverProperties, serverInitContext.getControllerServiceLookup());
        server.startServer(serverContext);

        final DistributedSetCacheClientService client = createClient();
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

        server.shutdownServer();
    }

    @Ignore("Test fails when in a maven parallel build due to address/port already taken - need to vary these so tests can run in parallel")
    @Test
    public void testPersistentSetServerAndClient() throws InitializationException, IOException {
        LOGGER.info("Testing " + Thread.currentThread().getStackTrace()[1].getMethodName());
        // Create server
        final DistributedSetCacheServer server = new DistributedSetCacheServer();
        MockControllerServiceInitializationContext serverInitContext = new MockControllerServiceInitializationContext(server, "server");
        server.initialize(serverInitContext);

        final File dataFile = new File("target/cache-data");
        deleteRecursively(dataFile);

        final Map<PropertyDescriptor, String> serverProperties = new HashMap<>();
        serverProperties.put(DistributedSetCacheServer.PERSISTENCE_PATH, dataFile.getAbsolutePath());
        final MockConfigurationContext serverContext = new MockConfigurationContext(serverProperties, serverInitContext.getControllerServiceLookup());
        server.startServer(serverContext);

        final DistributedSetCacheClientService client = createClient();
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

        server.shutdownServer();

        final DistributedSetCacheServer newServer = new DistributedSetCacheServer();
        MockControllerServiceInitializationContext newServerInitContext = new MockControllerServiceInitializationContext(newServer, "server2");
        newServer.initialize(newServerInitContext);

        final MockConfigurationContext newServerContext = new MockConfigurationContext(serverProperties,
                newServerInitContext.getControllerServiceLookup());
        newServer.startServer(newServerContext);

        assertFalse(client.contains("test", serializer));
        assertTrue(client.contains("test2", serializer));

        newServer.shutdownServer();
    }

    @Ignore("Test fails when in a maven parallel build due to address/port already taken - need to vary these so tests can run in parallel")    
    @Test
    public void testPersistentSetServerAndClientWithLFUEvictions() throws InitializationException, IOException {
        LOGGER.info("Testing " + Thread.currentThread().getStackTrace()[1].getMethodName());
        // Create server
        final DistributedSetCacheServer server = new DistributedSetCacheServer();
        MockControllerServiceInitializationContext serverInitContext = new MockControllerServiceInitializationContext(server, "server");
        server.initialize(serverInitContext);

        final File dataFile = new File("target/cache-data");
        deleteRecursively(dataFile);

        final Map<PropertyDescriptor, String> serverProperties = new HashMap<>();
        serverProperties.put(DistributedSetCacheServer.PERSISTENCE_PATH, dataFile.getAbsolutePath());
        serverProperties.put(DistributedSetCacheServer.MAX_CACHE_ENTRIES, "3");

        final MockConfigurationContext serverContext = new MockConfigurationContext(serverProperties, serverInitContext.getControllerServiceLookup());
        server.startServer(serverContext);

        final DistributedSetCacheClientService client = createClient();
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

        server.shutdownServer();

        final DistributedSetCacheServer newServer = new DistributedSetCacheServer();
        MockControllerServiceInitializationContext newServerInitContext = new MockControllerServiceInitializationContext(newServer, "server2");
        newServer.initialize(newServerInitContext);

        final MockConfigurationContext newServerContext = new MockConfigurationContext(serverProperties,
                newServerInitContext.getControllerServiceLookup());
        newServer.startServer(newServerContext);

        assertTrue(client.contains("test", serializer));
        assertTrue(client.contains("test2", serializer));
        assertFalse(client.contains("test3", serializer));
        assertTrue(client.contains("test4", serializer));

        newServer.shutdownServer();
    }

    @Ignore("Test fails when in a maven parallel build due to address/port already taken - need to vary these so tests can run in parallel")    
    @Test
    public void testPersistentSetServerAndClientWithFIFOEvictions() throws InitializationException, IOException {
        LOGGER.info("Testing " + Thread.currentThread().getStackTrace()[1].getMethodName());
        // Create server
        final DistributedSetCacheServer server = new DistributedSetCacheServer();
        MockControllerServiceInitializationContext serverInitContext = new MockControllerServiceInitializationContext(server, "server");
        server.initialize(serverInitContext);

        final File dataFile = new File("target/cache-data");
        deleteRecursively(dataFile);

        final Map<PropertyDescriptor, String> serverProperties = new HashMap<>();
        serverProperties.put(DistributedSetCacheServer.PERSISTENCE_PATH, dataFile.getAbsolutePath());
        serverProperties.put(DistributedSetCacheServer.MAX_CACHE_ENTRIES, "3");
        serverProperties.put(DistributedSetCacheServer.EVICTION_POLICY, DistributedSetCacheServer.EVICTION_STRATEGY_FIFO);

        final MockConfigurationContext serverContext = new MockConfigurationContext(serverProperties, serverInitContext.getControllerServiceLookup());
        server.startServer(serverContext);

        final DistributedSetCacheClientService client = createClient();
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

        server.shutdownServer();

        final DistributedSetCacheServer newServer = new DistributedSetCacheServer();
        MockControllerServiceInitializationContext newServerInitContext = new MockControllerServiceInitializationContext(newServer, "server2");
        newServer.initialize(newServerInitContext);

        final MockConfigurationContext newServerContext = new MockConfigurationContext(serverProperties,
                newServerInitContext.getControllerServiceLookup());
        newServer.startServer(newServerContext);

        assertFalse(client.contains("test", serializer));
        assertTrue(client.contains("test2", serializer));
        assertTrue(client.contains("test3", serializer));
        assertTrue(client.contains("test4", serializer));

        newServer.shutdownServer();
    }

    @Ignore("Test fails when in a maven parallel build due to address/port already taken - need to vary these so tests can run in parallel")
    @Test
    public void testNonPersistentMapServerAndClient() throws InitializationException, IOException, InterruptedException {
        LOGGER.info("Testing " + Thread.currentThread().getStackTrace()[1].getMethodName());
        // Create server
        final DistributedMapCacheServer server = new DistributedMapCacheServer();
        MockControllerServiceInitializationContext serverInitContext = new MockControllerServiceInitializationContext(server, "server");
        server.initialize(serverInitContext);

        final Map<PropertyDescriptor, String> serverProperties = new HashMap<>();
        final MockConfigurationContext serverContext = new MockConfigurationContext(serverProperties, serverInitContext.getControllerServiceLookup());
        server.startServer(serverContext);

        DistributedMapCacheClientService client = new DistributedMapCacheClientService();
        MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME, "localhost");
        clientProperties.put(DistributedMapCacheClientService.COMMUNICATIONS_TIMEOUT, "360 secs");
        MockConfigurationContext clientContext = new MockConfigurationContext(clientProperties, clientInitContext.getControllerServiceLookup());
        client.cacheConfig(clientContext);
        final Serializer<String> valueSerializer = new StringSerializer();
        final Serializer<String> keySerializer = new StringSerializer();
        final Deserializer<String> deserializer = new StringDeserializer();

        final String original = client.getAndPutIfAbsent("testKey", "test", keySerializer, valueSerializer, deserializer);
        assertEquals(null, original);
        LOGGER.debug("end getAndPutIfAbsent");

        final boolean contains = client.containsKey("testKey", keySerializer);
        assertTrue(contains);
        LOGGER.debug("end containsKey");

        final boolean added = client.putIfAbsent("testKey", "test", keySerializer, valueSerializer);
        assertFalse(added);
        LOGGER.debug("end putIfAbsent");

        final String originalAfterPut = client.getAndPutIfAbsent("testKey", "test", keySerializer, valueSerializer, deserializer);
        assertEquals("test", originalAfterPut);
        LOGGER.debug("end getAndPutIfAbsent");

        final boolean removed = client.remove("testKey", keySerializer);
        assertTrue(removed);
        LOGGER.debug("end remove");

        final boolean containedAfterRemove = client.containsKey("testKey", keySerializer);
        assertFalse(containedAfterRemove);

        client.putIfAbsent("testKey", "test", keySerializer, valueSerializer);
        client.close();
        try {
            client.containsKey("testKey", keySerializer);
            fail("Should be closed and not accessible");
        } catch (Exception e) {

        }
        client = null;
        clientInitContext = null;
        clientContext = null;

        DistributedMapCacheClientService client2 = new DistributedMapCacheClientService();

        MockControllerServiceInitializationContext clientInitContext2 = new MockControllerServiceInitializationContext(client2, "client2");
        client2.initialize(clientInitContext2);

        MockConfigurationContext clientContext2 = new MockConfigurationContext(clientProperties,
                clientInitContext2.getControllerServiceLookup());
        client2.cacheConfig(clientContext2);
        assertFalse(client2.putIfAbsent("testKey", "test", keySerializer, valueSerializer));
        assertTrue(client2.containsKey("testKey", keySerializer));
        server.shutdownServer();
        Thread.sleep(1000);
        try {
            client2.containsKey("testKey", keySerializer);
            fail("Should have blown exception!");
        } catch (ConnectException e) {
            client2 = null;
            clientContext2 = null;
            clientInitContext2 = null;
        }
        Thread.sleep(2000);
        System.gc();
        LOGGER.debug("end testNonPersistentMapServerAndClient");
    }

    @Test
    public void testClientTermination() throws InitializationException, IOException, InterruptedException {

        /**
         * This bypasses the test for build environments in OS X running Java 1.8 due to a JVM bug
         * See:  https://issues.apache.org/jira/browse/NIFI-437
         */
        Assume.assumeFalse("testClientTermination is skipped due to build environment being OS X with JDK 1.8. See https://issues.apache.org/jira/browse/NIFI-437",
                SystemUtils.IS_OS_MAC && SystemUtils.IS_JAVA_1_8);

        LOGGER.info("Testing " + Thread.currentThread().getStackTrace()[1].getMethodName());
        // Create server
        final DistributedMapCacheServer server = new DistributedMapCacheServer();
        MockControllerServiceInitializationContext serverInitContext = new MockControllerServiceInitializationContext(server, "server");
        server.initialize(serverInitContext);

        final Map<PropertyDescriptor, String> serverProperties = new HashMap<>();
        final MockConfigurationContext serverContext = new MockConfigurationContext(serverProperties, serverInitContext.getControllerServiceLookup());
        server.startServer(serverContext);

        DistributedMapCacheClientService client = new DistributedMapCacheClientService();
        MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME, "localhost");
        clientProperties.put(DistributedMapCacheClientService.COMMUNICATIONS_TIMEOUT, "360 secs");
        MockConfigurationContext clientContext = new MockConfigurationContext(clientProperties, clientInitContext.getControllerServiceLookup());
        client.cacheConfig(clientContext);
        final Serializer<String> valueSerializer = new StringSerializer();
        final Serializer<String> keySerializer = new StringSerializer();
        final Deserializer<String> deserializer = new StringDeserializer();

        final String original = client.getAndPutIfAbsent("testKey", "test", keySerializer, valueSerializer, deserializer);
        assertEquals(null, original);

        final boolean contains = client.containsKey("testKey", keySerializer);
        assertTrue(contains);

        final boolean added = client.putIfAbsent("testKey", "test", keySerializer, valueSerializer);
        assertFalse(added);

        final String originalAfterPut = client.getAndPutIfAbsent("testKey", "test", keySerializer, valueSerializer, deserializer);
        assertEquals("test", originalAfterPut);

        final boolean removed = client.remove("testKey", keySerializer);
        assertTrue(removed);

        final boolean containedAfterRemove = client.containsKey("testKey", keySerializer);
        assertFalse(containedAfterRemove);

        client = null;
        clientInitContext = null;
        clientContext = null;
        Thread.sleep(2000);
        System.gc();
        server.shutdownServer();
    }

    @Ignore
    @Test
    public void testSSLWith2RequestsWithServerTimeout() throws InitializationException, IOException, InterruptedException {
        LOGGER.info("Testing " + Thread.currentThread().getStackTrace()[1].getMethodName());
        // Create SSLContext Service
        final StandardSSLContextService sslService = new StandardSSLContextService();
        final MockControllerServiceInitializationContext sslServerInitContext = new MockControllerServiceInitializationContext(sslService,
                "ssl-context");
        sslService.initialize(sslServerInitContext);

        final Map<PropertyDescriptor, String> sslServerProps = new HashMap<>();
        sslServerProps.put(StandardSSLContextService.KEYSTORE, "src/test/resources/localhost-ks.jks");
        sslServerProps.put(StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
        sslServerProps.put(StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        sslServerProps.put(StandardSSLContextService.TRUSTSTORE, "src/test/resources/localhost-ts.jks");
        sslServerProps.put(StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        sslServerProps.put(StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        MockConfigurationContext sslServerContext = new MockConfigurationContext(sslServerProps, sslServerInitContext);
        sslService.onConfigured(sslServerContext);
        sslService.createSSLContext(ClientAuth.REQUIRED);
        // Create server
        final DistributedMapCacheServer server = new DistributedMapCacheServer();
        final MockControllerServiceInitializationContext serverInitContext = new MockControllerServiceInitializationContext(server, "server");
        server.initialize(serverInitContext);

        final Map<PropertyDescriptor, String> serverProperties = new HashMap<>();
        serverProperties.put(DistributedMapCacheServer.SSL_CONTEXT_SERVICE, "ssl-context");
        final MockConfigurationContext serverContext = new MockConfigurationContext(serverProperties, serverInitContext.getControllerServiceLookup());
        server.startServer(serverContext);

        DistributedMapCacheClientService client = new DistributedMapCacheClientService();
        MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME, "localhost");
        clientProperties.put(DistributedMapCacheClientService.COMMUNICATIONS_TIMEOUT, "360 secs");
        clientProperties.put(DistributedMapCacheClientService.SSL_CONTEXT_SERVICE, "ssl-context");
        MockConfigurationContext clientContext = new MockConfigurationContext(clientProperties, clientInitContext.getControllerServiceLookup());
        client.cacheConfig(clientContext);
        final Serializer<String> valueSerializer = new StringSerializer();
        final Serializer<String> keySerializer = new StringSerializer();
        final Deserializer<String> deserializer = new StringDeserializer();

        final String original = client.getAndPutIfAbsent("testKey", "test", keySerializer, valueSerializer, deserializer);
        assertEquals(null, original);

        Thread.sleep(30000);
        try {
            final boolean contains = client.containsKey("testKey", keySerializer);
            assertTrue(contains);
        } catch (IOException e) {
            // this is due to the server timing out in the middle of this request
            assertTrue(e.getMessage().contains("Channel is closed"));
        }

        server.shutdownServer();
    }

    private void waitABit() {
        try {
            Thread.sleep(10L);
        } catch (final InterruptedException e) {
        }
    }

    private DistributedSetCacheClientService createClient() throws InitializationException {
        final DistributedSetCacheClientService client = new DistributedSetCacheClientService();
        MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedSetCacheClientService.HOSTNAME, "localhost");
        final MockConfigurationContext clientContext = new MockConfigurationContext(clientProperties, clientInitContext.getControllerServiceLookup());
        client.onConfigured(clientContext);

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
        public String deserialize(final byte[] input) throws DeserializationException, IOException {
            return (input.length == 0) ? null : new String(input, StandardCharsets.UTF_8);
        }
    }

    private static void deleteRecursively(final File dataFile) throws IOException {
        if (dataFile == null || !dataFile.exists()) {
            return;
        }

        final File[] children = dataFile.listFiles();
        for (final File child : children) {
            if (child.isDirectory()) {
                deleteRecursively(child);
            } else {
                for (int i = 0; i < 100 && child.exists(); i++) {
                    child.delete();
                }

                if (child.exists()) {
                    throw new IOException("Could not delete " + dataFile.getAbsolutePath());
                }
            }
        }
    }
}
