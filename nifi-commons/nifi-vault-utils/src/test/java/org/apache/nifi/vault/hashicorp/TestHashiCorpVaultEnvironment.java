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

import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultEnvironment;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestHashiCorpVaultEnvironment {
    public static final String VAULT_AUTHENTICATION = "vault.authentication";
    public static final String VAULT_TOKEN = "vault.token";

    private static final String NONEXISTANT_PROPERTY = "nonexistant.property";

    private static final String TEST_TOKEN_VALUE = "test-token";
    private static final String TOKEN_VALUE = "TOKEN";
    private static final String URI_VALUE = "http://localhost:8200";
    private static final String DEFAULT_URI_VALUE = "http://default:8200";
    private static final String KEYSTORE_VALUE = "keystore";
    private static final String KEYSTORE_PASSWORD_VALUE = "keystorePassword";
    private static final String KEYSTORE_TYPE_VALUE = "keystoreType";
    private static final String TRUSTSTORE_VALUE = "truststore";
    private static final String TRUSTSTORE_PASSWORD_VALUE = "truststorePassword";
    private static final String TRUSTSTORE_TYPE_VALUE = "truststoreType";
    public static final String TLS_V_1_3_VALUE = "TLSv1.3";
    public static final String TEST_CIPHER_SUITE_VALUE = "Test cipher suite";

    private HashiCorpVaultProperties.VaultPropertiesBuilder propertiesBuilder;
    private File authProps;

    private HashiCorpVaultEnvironment env;

    @Before
    public void init() throws IOException {
        authProps = writeBasicVaultAuthProperties();
        propertiesBuilder = new HashiCorpVaultProperties.VaultPropertiesBuilder()
                .setUri(URI_VALUE)
                .setAuthPropertiesFilename(authProps.getAbsolutePath());

    }

    /**
     * Writes a new temp vault authentication properties file with the following properties:
     * vault.authentication=TOKEN
     * vault.token=test-token
     * @return The created temp file
     * @throws IOException If the file could not be written
     */
    public static File writeBasicVaultAuthProperties() throws IOException {
        File authProps = File.createTempFile("vault-", ".properties");
        Map<String, String> properties = new HashMap<>();
        properties.put(VAULT_AUTHENTICATION, TOKEN_VALUE);
        properties.put(VAULT_TOKEN, TEST_TOKEN_VALUE);
        writeProperties(properties, authProps);
        return authProps;
    }

    @After
    public void cleanUp() throws IOException {
        Files.deleteIfExists(authProps.toPath());
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

    public void runSuccessfulTest() {
        env = new HashiCorpVaultEnvironment(propertiesBuilder.build());

        Assert.assertEquals(TOKEN_VALUE, env.getProperty(VAULT_AUTHENTICATION));
        Assert.assertEquals(TEST_TOKEN_VALUE, env.getProperty(VAULT_TOKEN));
        Assert.assertEquals(URI_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_URI));

        Assert.assertEquals(URI.create(URI_VALUE), env.getProperty(HashiCorpVaultEnvironment.VAULT_URI, URI.class));
        Assert.assertEquals(URI.create(URI_VALUE), env.getProperty(HashiCorpVaultEnvironment.VAULT_URI, URI.class, URI.create(DEFAULT_URI_VALUE)));

        Assert.assertEquals(URI_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_URI, DEFAULT_URI_VALUE));
        Assert.assertEquals(DEFAULT_URI_VALUE, env.getProperty(NONEXISTANT_PROPERTY, DEFAULT_URI_VALUE));
        Assert.assertEquals(URI.create(DEFAULT_URI_VALUE), env.getProperty(NONEXISTANT_PROPERTY, URI.class, URI.create(DEFAULT_URI_VALUE)));
    }

    @Test
    public void testBasicProperties() {
        this.runSuccessfulTest();
    }

    @Test
    public void testTlsProperties() {
        propertiesBuilder.setKeystore(KEYSTORE_VALUE);
        propertiesBuilder.setKeystorePassword(KEYSTORE_PASSWORD_VALUE);
        propertiesBuilder.setKeystoreType(KEYSTORE_TYPE_VALUE);
        propertiesBuilder.setTruststore(TRUSTSTORE_VALUE);
        propertiesBuilder.setTruststorePassword(TRUSTSTORE_PASSWORD_VALUE);
        propertiesBuilder.setTruststoreType(TRUSTSTORE_TYPE_VALUE);
        propertiesBuilder.setEnabledTlsProtocols(TLS_V_1_3_VALUE);
        propertiesBuilder.setEnabledTlsCipherSuites(TEST_CIPHER_SUITE_VALUE);

        this.runSuccessfulTest();

        Assert.assertEquals(KEYSTORE_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_SSL_KEYSTORE));
        Assert.assertEquals(KEYSTORE_PASSWORD_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_SSL_KEYSTORE_PASSWORD));
        Assert.assertEquals(KEYSTORE_TYPE_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_SSL_KEYSTORE_TYPE));
        Assert.assertEquals(TRUSTSTORE_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_SSL_TRUSTSTORE));
        Assert.assertEquals(TRUSTSTORE_PASSWORD_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_SSL_TRUSTSTORE_PASSWORD));
        Assert.assertEquals(TRUSTSTORE_TYPE_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_SSL_TRUSTSTORE_TYPE));
        Assert.assertEquals(TLS_V_1_3_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_SSL_ENABLED_PROTOCOLS));
        Assert.assertEquals(TEST_CIPHER_SUITE_VALUE, env.getProperty(HashiCorpVaultEnvironment.VAULT_SSL_ENABLED_CIPHER_SUITES));
    }

    @Test(expected = NullPointerException.class)
    public void testInvalidTLS() {
        propertiesBuilder.setUri(URI_VALUE.replace("http", "https"));
        this.runSuccessfulTest();
    }
}
