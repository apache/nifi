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
package org.apache.nifi.properties;

import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import org.apache.nifi.properties.configuration.AzureCryptographyClientProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * For the purposes of running this test, if you have a key, first make sure you have configured
 * credential information as in the following link
 * https://docs.microsoft.com/en-us/java/api/overview/azure/security-keyvault-keys-readme?view=azure-java-stable
 *
 * Then simply set the system property -Dazure.keyvault.key.id to the key identifier
 * when running the integration test.
 * The key identifier takes the form: "https://keyvault-name.vault.azure.net/keys/key-name/keyID"
 *
 * Otherwise, to create a key, it will be simplest through the Azure web portal. First create a resource group,
 * then a key vault tied to that resource group. Then you can create a key through the key vault.
 *
 * Then simply set the system property -Dazure.keyvault.key.id to the key identifier
 * when running the integration test.
 * The key identifier takes the form: "https://keyvault-name.vault.azure.net/keys/key-name/keyID"
 *
 * Another system property that needs to be set is the algorithm: set it as
 * -Dazure.keyvault.encryption.algorithm=algorithm
 * Available algorithms can be found in:
 * https://docs.microsoft.com/en-us/java/api/com.azure.security.keyvault.keys.cryptography.models.encryptionalgorithm
 */

public class AzureKeyVaultKeySensitivePropertyProviderIT {
    private static final String SAMPLE_PLAINTEXT = "AzureKeyVaultKeySensitivePropertyProviderIT SAMPLE-PLAINTEXT";
    private static final String KEYVAULT_KEY_PROPS_NAME = "azure.keyvault.key.id";
    private static final String ENCRYPTION_ALGORITHM_PROPS_NAME = "azure.keyvault.encryption.algorithm";

    private static final String BOOTSTRAP_AZURE_FILE_PROPS_NAME = "nifi.bootstrap.protection.azure.keyvault.conf";

    private static final String EMPTY_PROPERTY = "";

    private static AzureKeyVaultKeySensitivePropertyProvider spp;

    private static BootstrapProperties props;

    private static Path mockBootstrapConf, mockAzureBootstrapConf;

    private static final Logger logger = LoggerFactory.getLogger(AzureKeyVaultKeySensitivePropertyProviderIT.class);

    private static void initializeBootstrapProperties() throws IOException {
        mockBootstrapConf = Files.createTempFile("bootstrap", ".conf").toAbsolutePath();
        mockAzureBootstrapConf = Files.createTempFile("bootstrap-azure", ".conf").toAbsolutePath();
        IOUtil.writeText(BOOTSTRAP_AZURE_FILE_PROPS_NAME + "=" + mockAzureBootstrapConf.toAbsolutePath(), mockBootstrapConf.toFile());

        final Properties bootstrapProperties = new Properties();
        try (final InputStream inputStream = Files.newInputStream(mockBootstrapConf)) {
            bootstrapProperties.load(inputStream);
            props = new BootstrapProperties("nifi", bootstrapProperties, mockBootstrapConf);
        }

        String keyId = System.getProperty(KEYVAULT_KEY_PROPS_NAME, EMPTY_PROPERTY);
        String algorithm = System.getProperty(ENCRYPTION_ALGORITHM_PROPS_NAME, EMPTY_PROPERTY);

        StringBuilder bootstrapConfText = new StringBuilder();
        String lineSeparator = System.getProperty("line.separator");
        bootstrapConfText.append(KEYVAULT_KEY_PROPS_NAME + "=" + keyId);
        bootstrapConfText.append(lineSeparator + ENCRYPTION_ALGORITHM_PROPS_NAME + "=" + algorithm);
        IOUtil.writeText(bootstrapConfText.toString(), mockAzureBootstrapConf.toFile());
    }

    @BeforeAll
    public static void initOnce() throws IOException {
        initializeBootstrapProperties();
        assertNotNull(props);
        final AzureCryptographyClientProvider provider = new AzureCryptographyClientProvider();
        final Properties properties = provider.getClientProperties(props).orElse(null);
        final CryptographyClient cryptographyClient = provider.getClient(properties).orElse(null);
        spp = new AzureKeyVaultKeySensitivePropertyProvider(cryptographyClient, properties);
        assertNotNull(spp);
    }

    @AfterAll
    public static void tearDownOnce() throws IOException {
        Files.deleteIfExists(mockBootstrapConf);
        Files.deleteIfExists(mockAzureBootstrapConf);

        spp.cleanUp();
    }

    @Test
    public void testEncryptDecrypt() {
        logger.info("Running testEncryptDecrypt of Azure Key Vault Key SPP integration test");
        runEncryptDecryptTest();
        logger.info("testEncryptDecrypt of Azure Key Vault Key SPP integration test completed");
    }

    private static void runEncryptDecryptTest() {
        logger.info("Plaintext: " + SAMPLE_PLAINTEXT);
        String protectedValue = spp.protect(SAMPLE_PLAINTEXT, ProtectedPropertyContext.defaultContext("property"));
        logger.info("Protected Value: " + protectedValue);
        String unprotectedValue = spp.unprotect(protectedValue, ProtectedPropertyContext.defaultContext("property"));
        logger.info("Unprotected Value: " + unprotectedValue);

        assertEquals(SAMPLE_PLAINTEXT, unprotectedValue);
        assertNotEquals(SAMPLE_PLAINTEXT, protectedValue);
        assertNotEquals(protectedValue, unprotectedValue);
    }
}
