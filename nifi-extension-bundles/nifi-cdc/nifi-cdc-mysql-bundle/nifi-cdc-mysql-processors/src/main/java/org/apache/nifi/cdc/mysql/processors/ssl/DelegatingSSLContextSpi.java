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
package org.apache.nifi.cdc.mysql.processors.ssl;

import java.security.SecureRandom;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/**
 * SSLContextSpi implementation that delegates all operations to a pre-initialized SSLContext. Initialization
 * parameters supplied by the caller are ignored because the delegated SSLContext already contains the configured
 * key and trust material. This allows MySQL Connector/J to obtain a fully configured SSLContext from a
 * {@link DelegatingSSLContextProvider} rather than building one from keystore and truststore files.
 */
final class DelegatingSSLContextSpi extends SSLContextSpi {

    private final SSLContext sslContext;

    DelegatingSSLContextSpi(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    protected void engineInit(final KeyManager[] keyManagers, final TrustManager[] trustManagers, final SecureRandom secureRandom) {
        // The delegated SSLContext is already initialized, so the key and trust managers provided by the caller are ignored
    }

    @Override
    protected SSLSocketFactory engineGetSocketFactory() {
        return sslContext.getSocketFactory();
    }

    @Override
    protected SSLServerSocketFactory engineGetServerSocketFactory() {
        return sslContext.getServerSocketFactory();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine() {
        return sslContext.createSSLEngine();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine(final String host, final int port) {
        return sslContext.createSSLEngine(host, port);
    }

    @Override
    protected SSLSessionContext engineGetServerSessionContext() {
        return sslContext.getServerSessionContext();
    }

    @Override
    protected SSLSessionContext engineGetClientSessionContext() {
        return sslContext.getClientSessionContext();
    }
}
