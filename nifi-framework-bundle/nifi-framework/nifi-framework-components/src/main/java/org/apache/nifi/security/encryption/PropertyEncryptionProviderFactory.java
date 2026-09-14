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

import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.util.NiFiProperties;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

/**
 * Factory for creating and initializing the Property Encryption Provider configured in application properties.
 * Installations that do not configure an implementation class use the password-based provider, which derives a secret
 * key from the sensitive properties key.
 */
public final class PropertyEncryptionProviderFactory {
    private static final String DEFAULT_IMPLEMENTATION = "org.apache.nifi.security.encryption.password.PasswordBasedPropertyEncryptionProvider";

    private PropertyEncryptionProviderFactory() {
    }

    /**
     * Create and initialize the configured Property Encryption Provider
     *
     * @param extensionManager Extension Manager for locating the provider in loaded NARs
     * @param properties Application properties containing the implementation class and provider properties
     * @param sslContext SSL Context supplied to the provider, which may be null
     * @param trustManager Trust Manager supplied to the provider, which may be null
     * @return Initialized Property Encryption Provider
     */
    public static PropertyEncryptionProvider getPropertyEncryptionProvider(
            final ExtensionManager extensionManager,
            final NiFiProperties properties,
            final SSLContext sslContext,
            final X509TrustManager trustManager
    ) {
        final String configuredClassName = properties.getProperty(NiFiProperties.PROPERTY_ENCRYPTION_PROVIDER_IMPLEMENTATION);
        final String className = configuredClassName == null || configuredClassName.isBlank() ? DEFAULT_IMPLEMENTATION : configuredClassName;

        final PropertyEncryptionProvider provider;
        try {
            provider = NarThreadContextClassLoader.createInstance(extensionManager, className, PropertyEncryptionProvider.class, properties);
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to create PropertyEncryptionProvider with class [%s]".formatted(className), e);
        }

        return initialize(provider, getProviderProperties(properties), sslContext, trustManager);
    }

    private static PropertyEncryptionProvider initialize(
            final PropertyEncryptionProvider provider,
            final Map<String, String> providerProperties,
            final SSLContext sslContext,
            final X509TrustManager trustManager
    ) {
        final PropertyEncryptionProvider wrapped = wrapWithComponentNarLoader(provider);
        try {
            final PropertyEncryptionProviderInitializationContext initializationContext =
                    new StandardPropertyEncryptionProviderInitializationContext(providerProperties, sslContext, trustManager);
            wrapped.initialize(initializationContext);
            return wrapped;
        } catch (final RuntimeException e) {
            try {
                wrapped.close();
            } catch (final Exception closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    /**
     * Wrap the provider so that every callback runs with the Thread Context ClassLoader of the NAR that supplied the
     * implementation, because NarThreadContextClassLoader sets the ClassLoader only while constructing the instance.
     */
    private static PropertyEncryptionProvider wrapWithComponentNarLoader(final PropertyEncryptionProvider provider) {
        final ClassLoader componentClassLoader = provider.getClass().getClassLoader();
        return new PropertyEncryptionProvider() {
            @Override
            public void initialize(final PropertyEncryptionProviderInitializationContext context) {
                try (NarCloseable ignored = NarCloseable.withComponentNarLoader(componentClassLoader)) {
                    provider.initialize(context);
                }
            }

            @Override
            public byte[] encrypt(final byte[] property, final SensitivePropertyContext context) {
                try (NarCloseable ignored = NarCloseable.withComponentNarLoader(componentClassLoader)) {
                    return provider.encrypt(property, context);
                }
            }

            @Override
            public byte[] decrypt(final byte[] encryptedProperty, final SensitivePropertyContext context) {
                try (NarCloseable ignored = NarCloseable.withComponentNarLoader(componentClassLoader)) {
                    return provider.decrypt(encryptedProperty, context);
                }
            }

            @Override
            public void close() throws IOException {
                try (NarCloseable ignored = NarCloseable.withComponentNarLoader(componentClassLoader)) {
                    provider.close();
                }
            }
        };
    }

    private static Map<String, String> getProviderProperties(final NiFiProperties properties) {
        final String prefix = NiFiProperties.PROPERTY_ENCRYPTION_PROVIDER_PREFIX;
        return properties.getPropertiesWithPrefix(prefix)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().substring(prefix.length()), Map.Entry::getValue));
    }
}
