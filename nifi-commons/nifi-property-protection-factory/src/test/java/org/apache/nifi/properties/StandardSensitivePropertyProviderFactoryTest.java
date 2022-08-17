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

import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.properties.scheme.StandardProtectionScheme;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardSensitivePropertyProviderFactoryTest {

    private SensitivePropertyProviderFactory factory;

    private static final String BOOTSTRAP_KEY_HEX = "0123456789ABCDEFFEDCBA9876543210";

    private static final ProtectionScheme AES_GCM = new StandardProtectionScheme("aes/gcm");
    private static final ProtectionScheme AES_GCM_128 = new StandardProtectionScheme("aes/gcm/128");
    private static final ProtectionScheme HASHICORP_VAULT_TRANSIT = new StandardProtectionScheme("hashicorp/vault/transit/testing");
    private static final ProtectionScheme HASHICORP_VAULT_KV = new StandardProtectionScheme("hashicorp/vault/kv/testing");

    private static Path tempConfDir;
    private static Path bootstrapConf;
    private static Path hashicorpVaultBootstrapConf;
    private static Path nifiProperties;
    private static Path azureKeyVaultConf;
    private static String defaultBootstrapContents;

    @BeforeAll
    public static void initOnce() throws IOException {
        tempConfDir = Files.createTempDirectory("conf");
        bootstrapConf = Files.createTempFile("bootstrap", ".conf").toAbsolutePath();
        azureKeyVaultConf = Files.createTempFile("bootstrap-azure-keyvault", ".conf").toAbsolutePath();
        hashicorpVaultBootstrapConf = Files.createTempFile("bootstrap-hashicorp-vault", ".conf").toAbsolutePath();

        nifiProperties = Files.createTempFile("nifi", ".properties").toAbsolutePath();

        nifiProperties = Files.move(nifiProperties, tempConfDir.resolve("nifi.properties"));

        defaultBootstrapContents = String.format("%s=%s\n%s=%s\n%s=%s",
                "nifi.bootstrap.sensitive.key", BOOTSTRAP_KEY_HEX,
                "nifi.bootstrap.protection.azure.keyvault.conf", FilenameUtils.separatorsToUnix(azureKeyVaultConf.toString()),
                "nifi.bootstrap.protection.hashicorp.vault.conf", FilenameUtils.separatorsToUnix(hashicorpVaultBootstrapConf.toString()));
        bootstrapConf = writeDefaultBootstrapConf();
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, FilenameUtils.separatorsToUnix(nifiProperties.toString()));
    }

    @AfterAll
    public static void tearDownOnce() throws IOException {
        Files.deleteIfExists(bootstrapConf);
        Files.deleteIfExists(azureKeyVaultConf);
        Files.deleteIfExists(hashicorpVaultBootstrapConf);
        Files.deleteIfExists(nifiProperties);
        Files.deleteIfExists(tempConfDir);
        System.clearProperty(NiFiProperties.PROPERTIES_FILE_PATH);
    }

    @BeforeEach
    public void setFactory() {
        factory = StandardSensitivePropertyProviderFactory.withDefaults();
    }

    @Test
    public void testGetPropertyContextNotConfigured() {
        assertEquals("default/prop", factory.getPropertyContext("ldap-provider", "prop").getContextKey());
    }

    @Test
    public void testGetPropertyContext() throws IOException {
        writeBootstrapConf(defaultBootstrapContents + "\n" +
                "nifi.bootstrap.protection.context.mapping.ldap=ldap-.*");
        try {
            assertEquals("ldap/prop", factory.getPropertyContext("ldap-provider", "prop").getContextKey());
            assertEquals("ldap/prop", factory.getPropertyContext("ldap-user-group-provider", "prop").getContextKey());
        } finally {
            writeDefaultBootstrapConf();
        }
    }

    @Test
    public void testGetSupportedProviders() {
        final Collection<SensitivePropertyProvider> providers = factory.getSupportedProviders();
        assertFalse(providers.isEmpty());

        final boolean aesProviderFound = providers.stream()
                .anyMatch(provider -> provider instanceof AesGcmSensitivePropertyProvider);
        assertTrue(aesProviderFound);
    }

    @Test
    public void testAzureKeyVaultSecret() throws IOException {
        final Properties properties = new Properties();
        properties.put("azure.keyvault.uri", "https://testing.vault.azure.net");
        configureAzureKeyVault(properties);

        final SensitivePropertyProvider provider = factory.getProvider(new StandardProtectionScheme("azure/keyvault/secret"));
        assertTrue(provider.isSupported());
        assertEquals(AzureKeyVaultSecretSensitivePropertyProvider.class, provider.getClass());
    }

    @Test
    public void testHashiCorpVaultKeyVaultSupported() throws IOException {
        final Properties properties = new Properties();
        properties.put("vault.kv.path", "testing");
        properties.put("vault.uri", "http://localhost:8200");
        properties.put("vault.token", "test-token");
        configureHashicorpVault(properties);

        final SensitivePropertyProvider provider = factory.getProvider(HASHICORP_VAULT_KV);
        assertTrue(provider.isSupported());
        assertEquals(HashiCorpVaultKeyValueSensitivePropertyProvider.class, provider.getClass());
    }

    @Test
    public void testHashiCorpVaultTransitSupported() throws IOException {
        final Properties properties = new Properties();
        properties.put("vault.transit.path", "testing");
        properties.put("vault.uri", "http://localhost:8200");
        properties.put("vault.token", "test-token");
        configureHashicorpVault(properties);

        final SensitivePropertyProvider provider = factory.getProvider(HASHICORP_VAULT_TRANSIT);
        assertTrue(provider.isSupported());
        assertEquals(HashiCorpVaultTransitSensitivePropertyProvider.class, provider.getClass());
    }

    @Test
    public void testHashiCorpVaultTransitExceptionWhenMissingProperties() throws IOException {
        final Properties properties = new Properties();
        properties.put("vault.uri", "http://localhost:8200");
        configureHashicorpVault(properties);

        assertThrows(SensitivePropertyProtectionException.class, () -> factory.getProvider(HASHICORP_VAULT_TRANSIT));
    }

    @Test
    public void testAesGcmWithoutKeySizeSupported() {
        final SensitivePropertyProvider provider = factory.getProvider(AES_GCM);
        assertEquals(AesGcmSensitivePropertyProvider.class, provider.getClass());
        assertTrue(provider.isSupported());
    }

    @Test
    public void testAesGcm128Supported() {
        final SensitivePropertyProvider provider = factory.getProvider(AES_GCM_128);
        assertEquals(AesGcmSensitivePropertyProvider.class, provider.getClass());
        assertTrue(provider.isSupported());
    }


    private static Path writeDefaultBootstrapConf() throws IOException {
        return writeBootstrapConf(defaultBootstrapContents);
    }

    private static Path writeBootstrapConf(final String contents) throws IOException {
        final Path tempBootstrapConf = Files.createTempFile("bootstrap", ".conf").toAbsolutePath();
        final Path bootstrapConf = Files.move(tempBootstrapConf, tempConfDir.resolve("bootstrap.conf"), StandardCopyOption.REPLACE_EXISTING);

        Files.write(bootstrapConf, contents.getBytes(StandardCharsets.UTF_8));
        return bootstrapConf;
    }

    private void configureHashicorpVault(final Properties properties) throws IOException {
        try (OutputStream out = new FileOutputStream(hashicorpVaultBootstrapConf.toFile())) {
            properties.store(out, hashicorpVaultBootstrapConf.toString());
        }
    }

    private void configureAzureKeyVault(final Properties properties) throws IOException {
        try (OutputStream out = new FileOutputStream(azureKeyVaultConf.toFile())) {
            properties.store(out, azureKeyVaultConf.toString());
        }
    }
}
