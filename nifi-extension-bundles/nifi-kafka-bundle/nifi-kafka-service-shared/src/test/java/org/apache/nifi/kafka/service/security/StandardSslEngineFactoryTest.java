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
package org.apache.nifi.kafka.service.security;

import org.apache.kafka.common.KafkaException;
import org.apache.nifi.ssl.SSLContextProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardSslEngineFactoryTest {

    private static final String LOCALHOST_ADDRESS = "127.0.0.1";

    private static final int PEER_PORT = 443;

    @Mock
    private SSLContextProvider sslContextProvider;

    private final StandardSslEngineFactory factory = new StandardSslEngineFactory();

    @Test
    void testConfigureProviderNotFound() {
        final Map<String, Object> configuration = Map.of();
        assertThrows(KafkaException.class, () -> factory.configure(configuration));
    }

    @Test
    void testCreateClientSslEngine() throws Exception {
        final SSLContext sslContext = SSLContext.getDefault();
        when(sslContextProvider.createContext()).thenReturn(sslContext);

        final Map<String, Object> configuration = Map.of(
            StandardSslEngineFactory.SSL_CONTEXT_PROVIDER_PROPERTY, sslContextProvider
        );
        factory.configure(configuration);

        final SSLEngine sslEngine = factory.createClientSslEngine(LOCALHOST_ADDRESS, PEER_PORT, null);

        assertNotNull(sslEngine);
        assertTrue(sslEngine.getUseClientMode());
    }
}
