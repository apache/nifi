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
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.io.IOUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StandardSensitivePropertyProviderFactoryTest {

    private static final String AES_GCM_128 = "aes/gcm/128";
    private SensitivePropertyProviderFactory factory;

    private static final String BOOTSTRAP_KEY_HEX = "0123456789ABCDEFFEDCBA9876543210";
    private static final String AD_HOC_KEY_HEX = "123456789ABCDEFFEDCBA98765432101";

    private static Path tempConfDir;
    private static Path bootstrapConf;
    private static Path hashicorpVaultBootstrapConf;
    private static Path nifiProperties;

    private static NiFiProperties niFiProperties;

    @BeforeClass
    public static void initOnce() throws IOException {
        Security.addProvider(new BouncyCastleProvider());
        tempConfDir = Files.createTempDirectory("conf");
        bootstrapConf = Files.createTempFile("bootstrap", ".conf").toAbsolutePath();
        hashicorpVaultBootstrapConf = Files.createTempFile("bootstrap-hashicorp-vault", ".conf").toAbsolutePath();

        nifiProperties = Files.createTempFile("nifi", ".properties").toAbsolutePath();

        bootstrapConf = Files.move(bootstrapConf, tempConfDir.resolve("bootstrap.conf"));
        nifiProperties = Files.move(nifiProperties, tempConfDir.resolve("nifi.properties"));

        final String bootstrapConfText = String.format("%s=%s\n%s=%s",
                "nifi.bootstrap.sensitive.key", BOOTSTRAP_KEY_HEX,
                "nifi.bootstrap.protection.hashicorp.vault.conf", FilenameUtils.separatorsToUnix(hashicorpVaultBootstrapConf.toString()));
        IOUtil.writeText(bootstrapConfText, bootstrapConf.toFile());
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, FilenameUtils.separatorsToUnix(nifiProperties.toString()));

        niFiProperties = new NiFiProperties();
    }

    @AfterClass
    public static void tearDownOnce() throws IOException {
        Files.deleteIfExists(bootstrapConf);
        Files.deleteIfExists(hashicorpVaultBootstrapConf);
        Files.deleteIfExists(nifiProperties);
        Files.deleteIfExists(tempConfDir);
        System.clearProperty(NiFiProperties.PROPERTIES_FILE_PATH);
    }

    /**
     * Configures the factory using the default bootstrap location.
     */
    private void configureDefaultFactory() {
        factory = StandardSensitivePropertyProviderFactory.withDefaults();
    }

    /**
     * Configures the factory using an ad hoc key hex.
     */
    private void configureAdHocKeyFactory() {
        factory = StandardSensitivePropertyProviderFactory.withKey(AD_HOC_KEY_HEX);
    }

    /**
     * Configures the factory using an ad hoc key hex and bootstrap.conf properties.  The key should override
     * the on in the bootstrap.conf.
     */
    private void configureAdHocKeyAndPropertiesFactory() throws IOException {
        factory = StandardSensitivePropertyProviderFactory.withKeyAndBootstrapSupplier(AD_HOC_KEY_HEX, mockBootstrapProperties());
    }

    private Supplier<BootstrapProperties> mockBootstrapProperties() throws IOException {
        final Properties bootstrapProperties = new Properties();
        try (final InputStream inputStream = Files.newInputStream(bootstrapConf)) {
            bootstrapProperties.load(inputStream);
            return () -> new BootstrapProperties("nifi", bootstrapProperties, bootstrapConf);
        }
    }

    private void configureHashicorpVault(final Properties properties) throws IOException {
        try (OutputStream out = new FileOutputStream(hashicorpVaultBootstrapConf.toFile())) {
            properties.store(out, "HashiCorpVault test");
        }
    }

    @Test
    public void testHashicorpVaultTransit() throws IOException {
        configureDefaultFactory();
        final Properties properties = new Properties();
        properties.put("vault.transit.path", "nifi-transit");
        configureHashicorpVault(properties);

        final SensitivePropertyProvider spp = factory.getProvider(PropertyProtectionScheme.HASHICORP_VAULT_TRANSIT);
    }

    @Test
    public void testHashicorpVaultTransit_isSupported() throws IOException {
        configureDefaultFactory();
        final Properties properties = new Properties();
        properties.put("vault.transit.path", "nifi-transit");
        properties.put("vault.uri", "http://localhost:8200");
        properties.put("vault.token", "test-token");
        configureHashicorpVault(properties);

        SensitivePropertyProvider spp = factory.getProvider(PropertyProtectionScheme.HASHICORP_VAULT_TRANSIT);
        assertTrue(spp.isSupported());

        properties.remove("vault.uri");
        configureHashicorpVault(properties);
        configureDefaultFactory();
        spp = factory.getProvider(PropertyProtectionScheme.HASHICORP_VAULT_TRANSIT);
        assertFalse(spp.isSupported());

        properties.put("vault.uri", "http://localhost:8200");
        properties.remove("vault.transit.path");
        spp = factory.getProvider(PropertyProtectionScheme.HASHICORP_VAULT_TRANSIT);
        assertFalse(spp.isSupported());
    }

    @Test
    public void testHashicorpVaultTransit_invalidCharacters() throws IOException {
        configureDefaultFactory();
        final Properties properties = new Properties();
        properties.put("vault.transit.path", "invalid/characters");
        configureHashicorpVault(properties);

        assertThrows(SensitivePropertyProtectionException.class, () -> factory.getProvider(PropertyProtectionScheme.HASHICORP_VAULT_TRANSIT));
    }

    @Test
    public void testAES_GCM() throws IOException {
        configureDefaultFactory();

        final SensitivePropertyProvider spp = factory.getProvider(PropertyProtectionScheme.AES_GCM);
        assertNotNull(spp);
        assertTrue(spp.isSupported());

        final String cleartext = "test";
        assertEquals(cleartext, spp.unprotect(spp.protect(cleartext)));
        assertNotEquals(cleartext, spp.protect(cleartext));
        assertEquals(AES_GCM_128, spp.getIdentifierKey());

        // Key is now different
        configureAdHocKeyFactory();
        final SensitivePropertyProvider sppAdHocKey = factory.getProvider(PropertyProtectionScheme.AES_GCM);
        assertNotNull(sppAdHocKey);
        assertTrue(sppAdHocKey.isSupported());
        assertEquals(AES_GCM_128, sppAdHocKey.getIdentifierKey());

        assertNotEquals(spp.protect(cleartext), sppAdHocKey.protect(cleartext));
        assertEquals(cleartext, sppAdHocKey.unprotect(sppAdHocKey.protect(cleartext)));

        // This should use the same keyHex as the second one
        configureAdHocKeyAndPropertiesFactory();
        final SensitivePropertyProvider sppKeyProperties = factory.getProvider(PropertyProtectionScheme.AES_GCM);
        assertNotNull(sppKeyProperties);
        assertTrue(sppKeyProperties.isSupported());
        assertEquals(AES_GCM_128, sppKeyProperties.getIdentifierKey());

        assertEquals(cleartext, sppKeyProperties.unprotect(sppKeyProperties.protect(cleartext)));
    }
}
