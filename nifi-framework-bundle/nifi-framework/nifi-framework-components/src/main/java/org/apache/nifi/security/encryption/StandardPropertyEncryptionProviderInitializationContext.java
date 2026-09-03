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
package org.apache.nifi.security.encryption;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

/**
 * A context object that provides the configuration for a Property Encryption Provider.
 */
public class StandardPropertyEncryptionProviderInitializationContext implements PropertyEncryptionProviderInitializationContext {
    private final Map<String, String> properties;
    private final SSLContext sslContext;
    private final X509TrustManager trustManager;

    public StandardPropertyEncryptionProviderInitializationContext(final Map<String, String> properties, final SSLContext sslContext, final X509TrustManager trustManager) {
        this.properties = Map.copyOf(Objects.requireNonNull(properties, "Properties required"));
        this.sslContext = sslContext;
        this.trustManager = trustManager;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Optional<SSLContext> getSSLContext() {
        return Optional.ofNullable(sslContext);
    }

    @Override
    public Optional<X509TrustManager> getTrustManager() {
        return Optional.ofNullable(trustManager);
    }
}
