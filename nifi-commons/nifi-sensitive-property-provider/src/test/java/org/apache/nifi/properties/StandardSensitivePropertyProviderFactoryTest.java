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

import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StandardSensitivePropertyProviderFactoryTest {

    private static final String AES_GCM_128 = "aes/gcm/128";
    private SensitivePropertyProviderFactory factory;

    private static final String BOOTSTRAP_KEY_HEX = "0123456789ABCDEFFEDCBA9876543210";
    private static final String AD_HOC_KEY_HEX = "123456789ABCDEFFEDCBA98765432101";

    private static Path tempConfDir;
    private static Path mockBootstrapConf;
    private static Path mockNifiProperties;

    private static NiFiProperties niFiProperties;

    @BeforeClass
    public static void initOnce() throws IOException {
        Security.addProvider(new BouncyCastleProvider());
        tempConfDir = Files.createTempDirectory("conf");
        mockBootstrapConf = Files.createTempFile("bootstrap", ".conf").toAbsolutePath();

        mockNifiProperties = Files.createTempFile("nifi", ".properties").toAbsolutePath();

        mockBootstrapConf = Files.move(mockBootstrapConf, tempConfDir.resolve("bootstrap.conf"));
        mockNifiProperties = Files.move(mockNifiProperties, tempConfDir.resolve("nifi.properties"));

        IOUtil.writeText("nifi.bootstrap.sensitive.key=" + BOOTSTRAP_KEY_HEX, mockBootstrapConf.toFile());
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, mockNifiProperties.toString());

        niFiProperties = new NiFiProperties();
    }

    @AfterClass
    public static void tearDownOnce() throws IOException {
        Files.deleteIfExists(mockBootstrapConf);
        Files.deleteIfExists(mockNifiProperties);
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
        try (final InputStream inputStream = Files.newInputStream(mockBootstrapConf)) {
            bootstrapProperties.load(inputStream);
            return () -> new BootstrapProperties("nifi", bootstrapProperties, mockBootstrapConf);
        }
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
