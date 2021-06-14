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
package org.apache.nifi.vault.hashicorp;

import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultSslProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class TestStandardHashiCorpVaultCommunicationService {
    public static final String URI_VALUE = "http://127.0.0.1:8200";
    public static final String CIPHER_SUITE_VALUE = "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384";

    private HashiCorpVaultProperties properties;
    private HashiCorpVaultSslProperties sslProperties;
    private File authProps;

    @Before
    public void init() throws IOException {
        authProps = TestHashiCorpVaultConfiguration.writeBasicVaultAuthProperties();

        properties = Mockito.mock(HashiCorpVaultProperties.class);
        sslProperties = Mockito.mock(HashiCorpVaultSslProperties.class);

        Mockito.when(properties.getUri()).thenReturn(URI_VALUE);
        Mockito.when(properties.getAuthPropertiesFilename()).thenReturn(authProps.getAbsolutePath());
        Mockito.when(properties.getSsl()).thenReturn(sslProperties);
    }

    @After
    public void cleanUp() throws IOException {
        Files.deleteIfExists(authProps.toPath());
    }

    private HashiCorpVaultCommunicationService configureService() {
        return new StandardHashiCorpVaultCommunicationService(properties);
    }

    @Test
    public void testBasicConfiguration() {
        this.configureService();

        // Once to check if the URI is https, and once by VaultTemplate
        Mockito.verify(properties, Mockito.times(2)).getUri();

        // Once to check if the property is set, and once to retrieve the value
        Mockito.verify(properties, Mockito.times(2)).getAuthPropertiesFilename();

        // These should not be called because TLS is not configured
        this.ensureTlsPropertiesAccessed(0);
    }

    private void ensureTlsPropertiesAccessed(int numberOfTimes) {
        Mockito.verify(sslProperties, Mockito.times(numberOfTimes)).getKeyStore();
        Mockito.verify(sslProperties, Mockito.times(numberOfTimes)).getKeyStoreType();
        Mockito.verify(sslProperties, Mockito.times(numberOfTimes)).getKeyStorePassword();
        Mockito.verify(sslProperties, Mockito.times(numberOfTimes)).getTrustStore();
        Mockito.verify(sslProperties, Mockito.times(numberOfTimes)).getTrustStoreType();
        Mockito.verify(sslProperties, Mockito.times(numberOfTimes)).getTrustStorePassword();
        Mockito.verify(sslProperties, Mockito.times(numberOfTimes)).getEnabledProtocols();
        Mockito.verify(sslProperties, Mockito.times(numberOfTimes)).getEnabledCipherSuites();
    }

    @Test
    public void testTimeouts() {
        Mockito.when(properties.getConnectionTimeout()).thenReturn(Optional.of("20 secs"));
        Mockito.when(properties.getReadTimeout()).thenReturn(Optional.of("40 secs"));
        this.configureService();
    }

    @Test
    public void testTLS() throws GeneralSecurityException, IOException {
        TlsConfiguration tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();
        try {
            Mockito.when(sslProperties.getKeyStore()).thenReturn(tlsConfiguration.getKeystorePath());
            Mockito.when(sslProperties.getKeyStorePassword()).thenReturn(tlsConfiguration.getKeystorePassword());
            Mockito.when(sslProperties.getKeyStoreType()).thenReturn(tlsConfiguration.getKeystoreType().getType());
            Mockito.when(sslProperties.getTrustStore()).thenReturn(tlsConfiguration.getTruststorePath());
            Mockito.when(sslProperties.getTrustStorePassword()).thenReturn(tlsConfiguration.getTruststorePassword());
            Mockito.when(sslProperties.getTrustStoreType()).thenReturn(tlsConfiguration.getTruststoreType().getType());
            Mockito.when(sslProperties.getEnabledProtocols()).thenReturn(Arrays.stream(tlsConfiguration.getEnabledProtocols())
                    .collect(Collectors.joining(",")));
            Mockito.when(sslProperties.getEnabledCipherSuites()).thenReturn(CIPHER_SUITE_VALUE);

            Mockito.when(properties.getUri()).thenReturn(URI_VALUE.replace("http", "https"));
            this.configureService();

            this.ensureTlsPropertiesAccessed(1);
        } finally {
            Files.deleteIfExists(Paths.get(tlsConfiguration.getKeystorePath()));
            Files.deleteIfExists(Paths.get(tlsConfiguration.getTruststorePath()));
        }
    }
}
