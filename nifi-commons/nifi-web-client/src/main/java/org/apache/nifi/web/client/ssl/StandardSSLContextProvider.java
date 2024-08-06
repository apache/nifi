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
package org.apache.nifi.web.client.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Optional;

public class StandardSSLContextProvider implements SSLContextProvider {
    /**
     * Get SSL Context
     *
     * @param tlsContext TLS Context configured
     * @return SSL Context
     */
    @Override
    public SSLContext getSslContext(final TlsContext tlsContext) {
        Objects.requireNonNull(tlsContext, "TLS Context required");
        final SSLContext sslContext = getSslContextConfigured(tlsContext);

        try {
            final Optional<X509KeyManager> keyManager = tlsContext.getKeyManager();
            final KeyManager[] keyManagers = keyManager.map(x509KeyManager -> new KeyManager[]{x509KeyManager}).orElse(null);

            final X509TrustManager trustManager = tlsContext.getTrustManager();
            final TrustManager[] trustManagers = trustManager == null ? null : new TrustManager[]{trustManager};

            final SecureRandom secureRandom = new SecureRandom();
            sslContext.init(keyManagers, trustManagers, secureRandom);

            return sslContext;
        } catch (final KeyManagementException e) {
            throw new IllegalArgumentException("SSLContext initialization failed", e);
        }
    }

    private SSLContext getSslContextConfigured(final TlsContext tlsContext) {
        final String protocol = Objects.requireNonNull(tlsContext.getProtocol(), "TLS Protocol required");
        try {
            return SSLContext.getInstance(protocol);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(String.format("SSLContext protocol [%s] not supported", protocol), e);
        }
    }
}
