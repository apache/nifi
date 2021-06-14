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

import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultConfiguration;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultPropertySource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.vault.authentication.ClientAuthentication;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.support.SslConfiguration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestHashiCorpVaultConfiguration {
    public static final String VAULT_AUTHENTICATION = "vault.authentication";
    public static final String VAULT_TOKEN = "vault.token";

    private static final String TEST_TOKEN_VALUE = "test-token";
    private static final String TOKEN_VALUE = "TOKEN";
    private static final String URI_VALUE = "http://localhost:8200";
    private static final String KEYSTORE_PASSWORD_VALUE = "keystorePassword";
    private static final String KEYSTORE_TYPE_VALUE = "keystoreType";
    private static final String TRUSTSTORE_PASSWORD_VALUE = "truststorePassword";
    private static final String TRUSTSTORE_TYPE_VALUE = "truststoreType";
    public static final String TLS_V_1_3_VALUE = "TLSv1.3";
    public static final String TEST_CIPHER_SUITE_VALUE = "Test cipher suite";

    private static Path keystoreFile;
    private static Path truststoreFile;

    private HashiCorpVaultProperties.HashiCorpVaultPropertiesBuilder propertiesBuilder;
    private static File authProps;

    private HashiCorpVaultConfiguration config;

    @BeforeClass
    public static void initClass() throws IOException {
        keystoreFile = Files.createTempFile("test", ".jks");
        truststoreFile = Files.createTempFile("test", ".jks");
        authProps = writeBasicVaultAuthProperties();
    }

    @AfterClass
    public static void cleanUpClass() throws IOException {
        Files.deleteIfExists(keystoreFile);
        Files.deleteIfExists(truststoreFile);
        Files.deleteIfExists(authProps.toPath());
    }

    @Before
    public void init() throws IOException {
        propertiesBuilder = new HashiCorpVaultProperties.HashiCorpVaultPropertiesBuilder()
                .setUri(URI_VALUE)
                .setAuthPropertiesFilename(authProps.getAbsolutePath());

    }

    public static File writeVaultAuthProperties(final Map<String, String> properties) throws IOException {
        File authProps = File.createTempFile("vault-", ".properties");
        writeProperties(properties, authProps);
        return authProps;
    }

    /**
     * Writes a new temp vault authentication properties file with the following properties:
     * vault.authentication=TOKEN
     * vault.token=test-token
     * @return The created temp file
     * @throws IOException If the file could not be written
     */
    public static File writeBasicVaultAuthProperties() throws IOException {
        Map<String, String> properties = new HashMap<>();
        properties.put(VAULT_AUTHENTICATION, TOKEN_VALUE);
        properties.put(VAULT_TOKEN, TEST_TOKEN_VALUE);
        return writeVaultAuthProperties(properties);
    }

    public static void writeProperties(Map<String, String> props, File authProps) throws IOException {
        Properties properties = new Properties();

        for (Map.Entry<String, String> entry : props.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        try (Writer writer = new FileWriter(authProps)) {
            properties.store(writer, "Vault test authentication properties");
        }
    }

    public void runTest(final String expectedScheme) {
        config = new HashiCorpVaultConfiguration(new HashiCorpVaultPropertySource(propertiesBuilder.build()));

        VaultEndpoint endpoint = config.vaultEndpoint();
        Assert.assertEquals("localhost", endpoint.getHost());
        Assert.assertEquals(8200, endpoint.getPort());
        Assert.assertEquals(expectedScheme, endpoint.getScheme());

        ClientAuthentication clientAuthentication = config.clientAuthentication();
        Assert.assertNotNull(clientAuthentication);
    }

    @Test
    public void testBasicProperties() {
        this.runTest("http");
    }

    @Test
    public void testTlsProperties() throws IOException {
        propertiesBuilder.setKeyStore(keystoreFile.toFile().getAbsolutePath());
        propertiesBuilder.setKeyStorePassword(KEYSTORE_PASSWORD_VALUE);
        propertiesBuilder.setKeyStoreType(KEYSTORE_TYPE_VALUE);
        propertiesBuilder.setTrustStore(truststoreFile.toFile().getAbsolutePath());
        propertiesBuilder.setTrustStorePassword(TRUSTSTORE_PASSWORD_VALUE);
        propertiesBuilder.setTrustStoreType(TRUSTSTORE_TYPE_VALUE);
        propertiesBuilder.setEnabledTlsProtocols(TLS_V_1_3_VALUE);
        propertiesBuilder.setEnabledTlsCipherSuites(TEST_CIPHER_SUITE_VALUE);
        propertiesBuilder.setUri(URI_VALUE.replace("http", "https"));

        this.runTest("https");

        SslConfiguration sslConfiguration = config.sslConfiguration();
        Assert.assertEquals(keystoreFile.toFile().getAbsolutePath(), sslConfiguration.getKeyStoreConfiguration().getResource().getFile().getAbsolutePath());
        Assert.assertEquals(KEYSTORE_PASSWORD_VALUE, new String(sslConfiguration.getKeyStoreConfiguration().getStorePassword()));
        Assert.assertEquals(KEYSTORE_TYPE_VALUE, sslConfiguration.getKeyStoreConfiguration().getStoreType());
        Assert.assertEquals(truststoreFile.toFile().getAbsolutePath(), sslConfiguration.getTrustStoreConfiguration().getResource().getFile().getAbsolutePath());
        Assert.assertEquals(TRUSTSTORE_PASSWORD_VALUE, new String(sslConfiguration.getTrustStoreConfiguration().getStorePassword()));
        Assert.assertEquals(TRUSTSTORE_TYPE_VALUE, sslConfiguration.getTrustStoreConfiguration().getStoreType());
        Assert.assertEquals(Arrays.asList(TLS_V_1_3_VALUE), sslConfiguration.getEnabledProtocols());
        Assert.assertEquals(Arrays.asList(TEST_CIPHER_SUITE_VALUE), sslConfiguration.getEnabledCipherSuites());
    }

    @Test
    public void testInvalidTLS() {
        propertiesBuilder.setUri(URI_VALUE.replace("http", "https"));
        Assert.assertThrows(NullPointerException.class, () -> this.runTest("https"));
    }

    @Test
    public void testMissingAuthToken() throws IOException {
        File authProperties = null;
        try {
            final Map<String, String> props = new HashMap<>();
            props.put(VAULT_AUTHENTICATION, TOKEN_VALUE);
            authProperties = writeVaultAuthProperties(props);
            propertiesBuilder.setAuthPropertiesFilename(authProperties.getAbsolutePath());

            Assert.assertThrows(IllegalArgumentException.class, () -> this.runTest("http"));
        } finally {
            if (authProperties != null) {
                Files.deleteIfExists(authProperties.toPath());
            }
        }
    }

    @Test
    public void testMissingAuthType() throws IOException {
        File authProperties = null;
        try {
            final Map<String, String> props = new HashMap<>();
            authProperties = writeVaultAuthProperties(props);
            propertiesBuilder.setAuthPropertiesFilename(authProperties.getAbsolutePath());

            Assert.assertThrows(IllegalArgumentException.class, () -> this.runTest("http"));
        } finally {
            if (authProperties != null) {
                Files.deleteIfExists(authProperties.toPath());
            }
        }
    }
}
