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
import javax.net.ssl.X509ExtendedKeyManager;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standard X.509 Extended Key Manager delegates to configurable wrapped Key Manager
 */
public class StandardX509ExtendedKeyManager extends X509ExtendedKeyManager implements KeyManagerListener {
    private final AtomicReference<X509ExtendedKeyManager> keyManagerRef;

    /**
     * Standard X.509 Extended Key Manager with required delegate Key Manager
     *
     * @param keyManager Key Manager
     */
    public StandardX509ExtendedKeyManager(final X509ExtendedKeyManager keyManager) {
        Objects.requireNonNull(keyManager, "Key Manager required");
        this.keyManagerRef = new AtomicReference<>(keyManager);
    }

    /**
     * Set X.509 Extended Key Manager to be used for subsequent operations
     *
     * @param keyManager Key Manager
     */
    @Override
    public void setKeyManager(final X509ExtendedKeyManager keyManager) {
        Objects.requireNonNull(keyManager, "Key Manager required");
        keyManagerRef.lazySet(keyManager);
    }

    @Override
    public String[] getClientAliases(final String keyType, final Principal[] issuers) {
        return keyManagerRef.get().getClientAliases(keyType, issuers);
    }

    @Override
    public String chooseClientAlias(final String[] keyType, final Principal[] issuers, final Socket socket) {
        return keyManagerRef.get().chooseClientAlias(keyType, issuers, socket);
    }

    public String chooseEngineClientAlias(final String[] keyType, final Principal[] issuers, final SSLEngine engine) {
        return keyManagerRef.get().chooseEngineClientAlias(keyType, issuers, engine);
    }

    @Override
    public String[] getServerAliases(final String keyType, final Principal[] issuers) {
        return keyManagerRef.get().getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseServerAlias(final String keyType, final Principal[] issuers, final Socket socket) {
        return keyManagerRef.get().chooseServerAlias(keyType, issuers, socket);
    }

    public String chooseEngineServerAlias(final String keyType, final Principal[] issuers, final SSLEngine engine) {
        return keyManagerRef.get().chooseEngineServerAlias(keyType, issuers, engine);
    }

    @Override
    public X509Certificate[] getCertificateChain(final String alias) {
        return keyManagerRef.get().getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(final String alias) {
        return keyManagerRef.get().getPrivateKey(alias);
    }
}
