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
package org.apache.nifi.processors.grpc.ssl;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SslContextProviderTest {
    static final List<String> APPLICATION_PROTOCOLS = Collections.singletonList("h2");

    static TlsConfiguration tlsConfiguration;

    static SSLContext sslContext;

    @Mock
    RestrictedSSLContextService sslContextService;

    @BeforeAll
    static void setTlsConfiguration() throws TlsException {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        sslContext = SslContextFactory.createSslContext(tlsConfiguration);
    }

    @Test
    void testGetClientSslContext() {
        when(sslContextService.createContext()).thenReturn(sslContext);
        when(sslContextService.createTlsConfiguration()).thenReturn(tlsConfiguration);

        final SslContext clientSslContext = SslContextProvider.getSslContext(sslContextService, true);

        assertNotNull(clientSslContext);
        assertTrue(clientSslContext.isClient());
        assertFalse(clientSslContext.isServer());
        assertEquals(APPLICATION_PROTOCOLS, clientSslContext.applicationProtocolNegotiator().protocols());
    }

    @Test
    void testGetServerSslContextClientAuthRequired() {
        when(sslContextService.createContext()).thenReturn(sslContext);
        when(sslContextService.createTlsConfiguration()).thenReturn(tlsConfiguration);

        final SslContext serverSslContext = SslContextProvider.getSslContext(sslContextService, false);

        assertServerStatus(serverSslContext);

        final SSLEngine sslEngine = serverSslContext.newEngine(ByteBufAllocator.DEFAULT);
        assertTrue(sslEngine.getNeedClientAuth());
    }

    @Test
    void testGetServerSslContextClientAuthNone() {
        when(sslContextService.createContext()).thenReturn(sslContext);

        final TlsConfiguration keyStoreConfiguration = new StandardTlsConfiguration();
        when(sslContextService.createTlsConfiguration()).thenReturn(keyStoreConfiguration);

        final SslContext serverSslContext = SslContextProvider.getSslContext(sslContextService, false);

        assertServerStatus(serverSslContext);

        final SSLEngine sslEngine = serverSslContext.newEngine(ByteBufAllocator.DEFAULT);
        assertFalse(sslEngine.getNeedClientAuth());
    }

    private void assertServerStatus(final SslContext configuredSslContext) {
        assertNotNull(configuredSslContext);
        assertFalse(configuredSslContext.isClient());
        assertTrue(configuredSslContext.isServer());
        assertEquals(APPLICATION_PROTOCOLS, configuredSslContext.applicationProtocolNegotiator().protocols());
    }
}
