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

package org.apache.nifi.stateless.config;

import org.apache.nifi.security.ssl.StandardKeyManagerBuilder;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.web.client.ssl.TlsContext;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Optional;

public class SslConfigurationUtil {

    public static TlsContext createTlsContext(final SslContextDefinition sslContextDefinition) throws StatelessConfigurationException {
        final X509ExtendedKeyManager keyManager;
        if (sslContextDefinition.getKeystoreFile() == null) {
            keyManager = null;
        } else {
            final KeyStore keyStore = getKeyStore(sslContextDefinition);
            final char[] keyStorePass = sslContextDefinition.getKeystorePass().toCharArray();
            keyManager = new StandardKeyManagerBuilder().keyStore(keyStore).keyPassword(keyStorePass).build();
        }

        final KeyStore trustStore = getTrustStore(sslContextDefinition);
        final X509ExtendedTrustManager trustManager = new StandardTrustManagerBuilder().trustStore(trustStore).build();

        return new TlsContext() {
            @Override
            public String getProtocol() {
                return TlsPlatform.getLatestProtocol();
            }

            @Override
            public X509TrustManager getTrustManager() {
                return trustManager;
            }

            @Override
            public Optional<X509KeyManager> getKeyManager() {
                return Optional.ofNullable(keyManager);
            }
        };
    }

    public static SSLContext createSslContext(final SslContextDefinition sslContextDefinition) throws StatelessConfigurationException {
        if (sslContextDefinition == null) {
            return null;
        }
        if (sslContextDefinition.getTruststoreFile() == null) {
            return null;
        }

        final StandardSslContextBuilder sslContextBuilder = new StandardSslContextBuilder();

        final KeyStore trustStore = getTrustStore(sslContextDefinition);
        final X509ExtendedTrustManager trustManager = new StandardTrustManagerBuilder().trustStore(trustStore).build();
        sslContextBuilder.trustManager(trustManager);

        if (sslContextDefinition.getKeystoreFile() != null) {
            final KeyStore keyStore = getKeyStore(sslContextDefinition);
            final char[] keyStorePass = sslContextDefinition.getKeystorePass().toCharArray();
            final X509ExtendedKeyManager keyManager = new StandardKeyManagerBuilder().keyStore(keyStore).keyPassword(keyStorePass).build();
            sslContextBuilder.keyManager(keyManager);
        }

        return sslContextBuilder.build();
    }

    private static KeyStore getTrustStore(final SslContextDefinition sslContextDefinition) throws StatelessConfigurationException {
        final char[] trustStorePass = sslContextDefinition.getTruststorePass().toCharArray();
        final StandardKeyStoreBuilder builder = new StandardKeyStoreBuilder()
                .type(sslContextDefinition.getTruststoreType())
                .password(trustStorePass);

        final Path trustStorePath = Paths.get(sslContextDefinition.getTruststoreFile());
        try (InputStream inputStream = Files.newInputStream(trustStorePath)) {
            builder.inputStream(inputStream);
            return builder.build();
        } catch (final Exception e) {
            throw new StatelessConfigurationException("Load Trust Store [%s] failed".formatted(trustStorePath), e);
        }
    }

    private static KeyStore getKeyStore(final SslContextDefinition sslContextDefinition) throws StatelessConfigurationException {
        final char[] keyStorePass = sslContextDefinition.getKeystorePass().toCharArray();
        final StandardKeyStoreBuilder builder = new StandardKeyStoreBuilder()
                .type(sslContextDefinition.getKeystoreType())
                .password(keyStorePass);

        final Path keyStorePath = Paths.get(sslContextDefinition.getKeystoreFile());
        try (InputStream inputStream = Files.newInputStream(keyStorePath)) {
            builder.inputStream(inputStream);
            return builder.build();
        } catch (final Exception e) {
            throw new StatelessConfigurationException("Load Key Store [%s] failed".formatted(keyStorePath), e);
        }
    }
}
