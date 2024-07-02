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

    private final Path keyStorePath;

    private final StandardKeyStoreBuilder keyStoreBuilder;

    public FrameworkKeyManagerBuilder(
            final Path keyStorePath,
            final StandardKeyStoreBuilder keyStoreBuilder,
            final char[] keyPassword
    ) {
        this.keyStorePath = Objects.requireNonNull(keyStorePath, "Key Store Path required");
        this.keyStoreBuilder = Objects.requireNonNull(keyStoreBuilder, "Key Store Builder required");
        keyPassword(Objects.requireNonNull(keyPassword, "Key Password required"));
    }

    /**
     * Build X.509 Extended Key Manager after loading Key Store
     *
     * @return X.509 Extended Key Manager
     */
    @Override
    public X509ExtendedKeyManager build() {
        this.loadKeyStore();
        final X509ExtendedKeyManager keyManager = super.build();

        logger.info("Key Manager loaded from Key Store [{}]", keyStorePath);
        return new StandardX509ExtendedKeyManager(keyManager);
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
