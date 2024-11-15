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
import org.apache.nifi.distributed.cache.client.MapCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class MapCacheServiceTlsTest {

    private static TestRunner runner = null;
    private static SSLContextProvider sslContextProvider = null;
    private static MapCacheServer server = null;
    private static MapCacheClientService client = null;
    private static final Serializer<String> serializer = new StringSerializer();
    private static final Deserializer<String> deserializer = new StringDeserializer();

    @BeforeAll
    public static void setServices() throws Exception {
        runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        sslContextProvider = createSslContextService();
        runner.addControllerService(sslContextProvider.getIdentifier(), sslContextProvider);
        runner.enableControllerService(sslContextProvider);

        server = new MapCacheServer();
        runner.addControllerService(server.getClass().getName(), server);
        runner.setProperty(server, MapCacheServer.PORT, "0");
        runner.setProperty(server, MapCacheServer.SSL_CONTEXT_SERVICE, sslContextProvider.getIdentifier());
        runner.enableControllerService(server);
        final int listeningPort = server.getPort();

        client = new MapCacheClientService();
        runner.addControllerService(client.getClass().getName(), client);
        runner.setProperty(client, MapCacheClientService.HOSTNAME, "localhost");
        runner.setProperty(client, MapCacheClientService.PORT, String.valueOf(listeningPort));
        runner.setProperty(client, MapCacheClientService.SSL_CONTEXT_SERVICE, sslContextProvider.getIdentifier());
        runner.enableControllerService(client);
    }

    @AfterAll
    public static void shutdown() {
        runner.disableControllerService(client);
        runner.removeControllerService(client);

        runner.disableControllerService(server);
        runner.removeControllerService(server);

        runner.disableControllerService(sslContextProvider);
        runner.removeControllerService(sslContextProvider);
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

    private static SSLContextProvider createSslContextService() throws NoSuchAlgorithmException {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        final SSLContext sslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(new char[]{})
                .build();

        final SSLContextProvider sslContextService = Mockito.mock(SSLContextProvider.class);
        when(sslContextService.getIdentifier()).thenReturn(sslContextService.getClass().getName());
        when(sslContextService.createContext()).thenReturn(sslContext);
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
