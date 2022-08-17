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
package org.apache.nifi.security.util;

import org.apache.nifi.util.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SslSocketFactoryTest {
    private static TlsConfiguration tlsConfiguration;

    @BeforeAll
    public static void setTlsConfiguration() {
        tlsConfiguration = new TemporaryKeyStoreBuilder().build();
    }

    @Test
    public void testCreateSslContextNullTlsConfiguration() throws TlsException {
        final SSLContext sslContext = SslContextFactory.createSslContext(null);
        assertNull(sslContext);
    }

    @Test
    public void testCreateSslContextEmptyTlsConfiguration() throws TlsException {
        final SSLContext sslContext = SslContextFactory.createSslContext(new StandardTlsConfiguration());
        assertNull(sslContext);
    }

    @Test
    public void testCreateSslContextEmptyKeyPassword() throws TlsException {
        final TlsConfiguration customTlsConfiguration = new StandardTlsConfiguration(
                tlsConfiguration.getKeystorePath(),
                tlsConfiguration.getKeystorePassword(),
                StringUtils.EMPTY,
                tlsConfiguration.getKeystoreType(),
                tlsConfiguration.getTruststorePath(),
                tlsConfiguration.getTruststorePassword(),
                tlsConfiguration.getTruststoreType(),
                tlsConfiguration.getProtocol()
        );
        final SSLContext sslContext = SslContextFactory.createSslContext(customTlsConfiguration);
        assertNotNull(sslContext);
        assertEquals(customTlsConfiguration.getProtocol(), sslContext.getProtocol());
    }

    @Test
    public void testCreateSslContextEmptyTrustStorePasswordJks() throws TlsException {
        final TlsConfiguration customTlsConfiguration = new TemporaryKeyStoreBuilder()
                .trustStorePassword(StringUtils.EMPTY)
                .trustStoreType(KeystoreType.JKS.getType())
                .build();
        final SSLContext sslContext = SslContextFactory.createSslContext(customTlsConfiguration);
        assertNotNull(sslContext);
        assertEquals(customTlsConfiguration.getProtocol(), sslContext.getProtocol());
    }

    @Test
    public void testCreateSslContext() throws TlsException {
        final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration);
        assertNotNull(sslContext);
        assertEquals(tlsConfiguration.getProtocol(), sslContext.getProtocol());
    }

    @Test
    public void testGetTrustManager() throws TlsException {
        final X509TrustManager trustManager = SslContextFactory.getX509TrustManager(tlsConfiguration);
        assertNotNull(trustManager);
        assertEquals(1, trustManager.getAcceptedIssuers().length);
    }

    @Test
    public void testGetTrustManagers() throws TlsException {
        final TrustManager[] trustManagers = SslContextFactory.getTrustManagers(tlsConfiguration);
        assertNotNull(trustManagers);
        assertEquals(1, trustManagers.length);
    }

    @Test
    public void testGetTrustManagersEmptyTlsConfiguration() throws TlsException {
        final TrustManager[] trustManagers = SslContextFactory.getTrustManagers(new StandardTlsConfiguration());
        assertNull(trustManagers);
    }
}
