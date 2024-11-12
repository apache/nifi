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
import org.apache.nifi.security.ssl.InputStreamKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.security.ssl.StandardX509ExtendedTrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Objects;

/**
 * Framework implementation of Trust Manager Builder capable of reloading a Trust Store when building a Trust Manager
 */
public class FrameworkTrustManagerBuilder extends StandardTrustManagerBuilder {
    private static final Logger logger = LoggerFactory.getLogger(FrameworkTrustManagerBuilder.class);

    private final Path trustStorePath;

    private final InputStreamKeyStoreBuilder trustStoreBuilder;

    public FrameworkTrustManagerBuilder(final Path trustStorePath, final InputStreamKeyStoreBuilder trustStoreBuilder) {
        this.trustStorePath = Objects.requireNonNull(trustStorePath, "Trust Store Path required");
        this.trustStoreBuilder = Objects.requireNonNull(trustStoreBuilder, "Trust Store Builder required");
    }

    @Override
    public X509ExtendedTrustManager build() {
        loadTrustStore();
        final X509ExtendedTrustManager trustManager = super.build();

        logger.info("Trust Manager loaded from Trust Store [{}]", trustStorePath);
        return new StandardX509ExtendedTrustManager(trustManager);
    }

    private void loadTrustStore() {
        try (InputStream inputStream = Files.newInputStream(trustStorePath)) {
            final KeyStore loadedTrustStore = trustStoreBuilder.inputStream(inputStream).build();
            trustStore(loadedTrustStore);
        } catch (final IOException e) {
            throw new BuilderConfigurationException("Trust Store loading failed", e);
        }
    }
}
