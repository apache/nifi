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
package org.apache.nifi.framework.ssl;

import org.apache.nifi.security.ssl.BuilderConfigurationException;
import org.apache.nifi.security.ssl.PemPrivateKeyCertificateKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardKeyManagerBuilder;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardX509ExtendedKeyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.X509ExtendedKeyManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Objects;

/**
 * Framework implementation fo Key Manager Builder capable of reloading a Key Store when building a Key Manager
 */
public class FrameworkKeyManagerBuilder extends StandardKeyManagerBuilder {
    private static final Logger logger = LoggerFactory.getLogger(FrameworkKeyManagerBuilder.class);

    private static final char[] EMPTY_PROTECTION_PARAMETER = new char[]{};

    private final Path keyStorePath;

    private final StandardKeyStoreBuilder keyStoreBuilder;

    private final Path privateKeyPath;

    private final Path certificatePath;

    private final PemPrivateKeyCertificateKeyStoreBuilder pemKeyStoreBuilder;

    public FrameworkKeyManagerBuilder(
            final Path keyStorePath,
            final StandardKeyStoreBuilder keyStoreBuilder,
            final char[] keyPassword
    ) {
        this.keyStorePath = Objects.requireNonNull(keyStorePath, "Key Store Path required");
        this.keyStoreBuilder = Objects.requireNonNull(keyStoreBuilder, "Key Store Builder required");
        keyPassword(Objects.requireNonNull(keyPassword, "Key Password required"));

        this.privateKeyPath = null;
        this.certificatePath = null;
        this.pemKeyStoreBuilder = null;
    }

    public FrameworkKeyManagerBuilder(
            final Path privateKeyPath,
            final Path certificatePath,
            final PemPrivateKeyCertificateKeyStoreBuilder pemKeyStoreBuilder
    ) {
        this.keyStorePath = null;
        this.keyStoreBuilder = null;

        this.privateKeyPath = Objects.requireNonNull(privateKeyPath, "Private Key Path required");
        this.certificatePath = Objects.requireNonNull(certificatePath, "Certificate Path required");
        this.pemKeyStoreBuilder = Objects.requireNonNull(pemKeyStoreBuilder, "PEM Key Store Builder required");
    }

    /**
     * Build X.509 Extended Key Manager after loading Key Store
     *
     * @return X.509 Extended Key Manager
     */
    @Override
    public X509ExtendedKeyManager build() {
        if (privateKeyPath == null) {
            loadKeyStore();
        } else {
            // Set empty key password as placeholder for construction of Key Managers
            keyPassword(EMPTY_PROTECTION_PARAMETER);
            loadPemKeyStore();
        }
        final X509ExtendedKeyManager keyManager = super.build();

        if (privateKeyPath == null) {
            logger.info("Key Manager loaded from Key Store [{}]", keyStorePath);
        } else {
            logger.info("Key Manager loaded from PEM Private Key [{}] and Certificate [{}]", privateKeyPath, certificatePath);
        }

        return new StandardX509ExtendedKeyManager(keyManager);
    }

    private void loadPemKeyStore() {
        try (
                InputStream privateKeyInputStream = Files.newInputStream(privateKeyPath);
                InputStream certificateInputStream = Files.newInputStream(certificatePath)
        ) {
            final KeyStore loadedKeyStore = pemKeyStoreBuilder.privateKeyInputStream(privateKeyInputStream).certificateInputStream(certificateInputStream).build();
            keyStore(loadedKeyStore);
        } catch (final IOException e) {
            throw new BuilderConfigurationException("PEM Key Store loading failed", e);
        }
    }

    private void loadKeyStore() {
        try (InputStream inputStream = Files.newInputStream(keyStorePath)) {
            final KeyStore loadedKeyStore = keyStoreBuilder.inputStream(inputStream).build();
            keyStore(loadedKeyStore);
        } catch (final IOException e) {
            throw new BuilderConfigurationException("Key Store loading failed", e);
        }
    }
}
