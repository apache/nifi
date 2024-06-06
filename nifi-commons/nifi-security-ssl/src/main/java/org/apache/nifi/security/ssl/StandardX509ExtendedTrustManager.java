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
package org.apache.nifi.security.ssl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standard X.509 Extended Trust Manager delegates to configurable wrapped Trust Manager
 */
public class StandardX509ExtendedTrustManager extends X509ExtendedTrustManager implements TrustManagerListener {
    private final AtomicReference<X509ExtendedTrustManager> trustManagerRef;

    /**
     * Standard X.509 Extended Trust Manager with required delegate Trust Manager
     *
     * @param trustManager Trust Manager
     */
    public StandardX509ExtendedTrustManager(final X509ExtendedTrustManager trustManager) {
        Objects.requireNonNull(trustManager, "Trust Manager required");
        trustManagerRef = new AtomicReference<>(trustManager);
    }

    /**
     * Set X.509 Extended Trust Manager to be used for subsequent operations
     *
     * @param trustManager Trust Manager
     */
    @Override
    public void setTrustManager(final X509ExtendedTrustManager trustManager) {
        Objects.requireNonNull(trustManager, "Trust Manager required");
        trustManagerRef.lazySet(trustManager);
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType, final Socket socket) throws CertificateException {
        trustManagerRef.get().checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType, final Socket socket) throws CertificateException {
        trustManagerRef.get().checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType, final SSLEngine engine) throws CertificateException {
        trustManagerRef.get().checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType, final SSLEngine engine) throws CertificateException {
        trustManagerRef.get().checkServerTrusted(chain, authType, engine);
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
        trustManagerRef.get().checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
        trustManagerRef.get().checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return trustManagerRef.get().getAcceptedIssuers();
    }
}
