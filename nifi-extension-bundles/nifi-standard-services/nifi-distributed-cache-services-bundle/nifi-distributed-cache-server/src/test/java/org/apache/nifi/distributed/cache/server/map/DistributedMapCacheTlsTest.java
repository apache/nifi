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
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DistributedMapCacheTlsTest {

    private static TestRunner runner = null;
    private static SSLContextService sslContextService = null;
    private static DistributedMapCacheServer server = null;
    private static DistributedMapCacheClientService client = null;
    private static final Serializer<String> serializer = new StringSerializer();
    private static final Deserializer<String> deserializer = new StringDeserializer();

    @BeforeAll
    public static void setServices() throws Exception {
        runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        sslContextService = createSslContextService();
        runner.addControllerService(sslContextService.getIdentifier(), sslContextService);
        runner.enableControllerService(sslContextService);

        server = new DistributedMapCacheServer();
        runner.addControllerService(server.getClass().getName(), server);
        runner.setProperty(server, DistributedMapCacheServer.PORT, "0");
        runner.setProperty(server, DistributedMapCacheServer.SSL_CONTEXT_SERVICE, sslContextService.getIdentifier());
        runner.enableControllerService(server);
        final int listeningPort = server.getPort();

        client = new DistributedMapCacheClientService();
        runner.addControllerService(client.getClass().getName(), client);
        runner.setProperty(client, DistributedMapCacheClientService.HOSTNAME, "localhost");
        runner.setProperty(client, DistributedMapCacheClientService.PORT, String.valueOf(listeningPort));
        runner.setProperty(client, DistributedMapCacheClientService.SSL_CONTEXT_SERVICE, sslContextService.getIdentifier());
        runner.enableControllerService(client);
    }

    @AfterAll
    public static void shutdown() {
        runner.disableControllerService(client);
        runner.removeControllerService(client);

        runner.disableControllerService(server);
        runner.removeControllerService(server);

        runner.disableControllerService(sslContextService);
        runner.removeControllerService(sslContextService);
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

    /**
     * Create a fresh {@link SSLContext} in order to test mutual TLS authentication aspect of the
     * distributed cache protocol.
     *
     * @return a NiFi {@link SSLContextService}, to be used to secure the distributed cache comms
     * @throws GeneralSecurityException on SSLContext generation failure
     */
    private static SSLContextService createSslContextService() throws GeneralSecurityException {
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        final SSLContext sslContext =  SslContextFactory.createSslContext(tlsConfiguration);
        final SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(sslContextService.getClass().getName());
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);
        return sslContextService;
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
