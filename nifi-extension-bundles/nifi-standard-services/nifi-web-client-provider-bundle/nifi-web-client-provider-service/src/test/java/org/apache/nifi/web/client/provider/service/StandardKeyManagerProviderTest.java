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
package org.apache.nifi.web.client.provider.service;

import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.X509KeyManager;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardKeyManagerProviderTest {
    static TlsConfiguration tlsConfiguration;

    @Mock
    SSLContextService sslContextService;

    StandardKeyManagerProvider provider;

    @BeforeAll
    static void setTlsConfiguration() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
    }

    @BeforeEach
    void setProvider() {
        provider = new StandardKeyManagerProvider();
    }

    @Test
    void testGetKeyManagerNotConfigured() {
        when(sslContextService.isKeyStoreConfigured()).thenReturn(false);

        final Optional<X509KeyManager> keyManager = provider.getKeyManager(sslContextService);

        assertFalse(keyManager.isPresent());
    }

    @Test
    void testGetKeyManager() {
        when(sslContextService.isKeyStoreConfigured()).thenReturn(true);
        when(sslContextService.getKeyStoreType()).thenReturn(tlsConfiguration.getKeystoreType().getType());
        when(sslContextService.getKeyStoreFile()).thenReturn(tlsConfiguration.getKeystorePath());
        when(sslContextService.getKeyStorePassword()).thenReturn(tlsConfiguration.getKeystorePassword());

        final Optional<X509KeyManager> keyManager = provider.getKeyManager(sslContextService);

        assertTrue(keyManager.isPresent());
    }
}
