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

import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509KeyManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Optional;

/**
 * Standard implementation of Key Manager Provider
 */
class StandardKeyManagerProvider implements KeyManagerProvider {
    /**
     * Get X.509 Key Manager using SSL Context Service configuration properties
     *
     * @param sslContextService SSL Context Service
     * @return X.509 Key Manager or empty when not configured
     */
    @Override
    public Optional<X509KeyManager> getKeyManager(final SSLContextService sslContextService) {
        final X509KeyManager keyManager;

        if (sslContextService.isKeyStoreConfigured()) {
            final KeyManagerFactory keyManagerFactory = getKeyManagerFactory();
            final KeyStore keyStore = getKeyStore(sslContextService);
            final char[] keyPassword = getKeyPassword(sslContextService);
            try {
                keyManagerFactory.init(keyStore, keyPassword);
            } catch (final KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
                throw new IllegalStateException("Key Manager Factory initialization failed", e);
            }

            final KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
            final Optional<KeyManager> firstKeyManager = Arrays.stream(keyManagers).findFirst();
            final KeyManager configuredKeyManager = firstKeyManager.orElse(null);
            keyManager = configuredKeyManager instanceof X509KeyManager ? (X509KeyManager) configuredKeyManager : null;
        } else {
            keyManager = null;
        }

        return Optional.ofNullable(keyManager);
    }

    private KeyStore getKeyStore(final SSLContextService sslContextService) {
        final String keyStoreType = sslContextService.getKeyStoreType();
        final KeyStore keyStore = getKeyStore(keyStoreType);
        final char[] keyStorePassword = sslContextService.getKeyStorePassword().toCharArray();
        final String keyStoreFile = sslContextService.getKeyStoreFile();
        try {
            try (final InputStream inputStream = new FileInputStream(keyStoreFile)) {
                keyStore.load(inputStream, keyStorePassword);
            }
            return keyStore;
        } catch (final IOException e) {
            throw new IllegalStateException(String.format("Key Store File [%s] reading failed", keyStoreFile), e);
        } catch (final NoSuchAlgorithmException | CertificateException e) {
            throw new IllegalStateException(String.format("Key Store File [%s] loading failed", keyStoreFile), e);
        }
    }

    private KeyStore getKeyStore(final String keyStoreType) {
        try {
            return KeyStore.getInstance(keyStoreType);
        } catch (final KeyStoreException e) {
            throw new IllegalStateException(String.format("Key Store Type [%s] creation failed", keyStoreType), e);
        }
    }

    private char[] getKeyPassword(final SSLContextService sslContextService) {
        final String keyPassword = sslContextService.getKeyPassword();
        final String keyStorePassword = sslContextService.getKeyStorePassword();
        final String password = keyPassword == null ? keyStorePassword : keyPassword;
        return password.toCharArray();
    }

    private KeyManagerFactory getKeyManagerFactory() {
        try {
            return KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Key Manager Factory creation failed", e);
        }
    }
}
