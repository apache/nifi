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
package org.apache.nifi.bootstrap.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.util.IPAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSecureNiFiConfigUtil {
    public static final String TEST_RESOURCE_DIR = "src/test/resources/";
    private final Logger logger = LoggerFactory.getLogger("org.apache.nifi.bootstrap.util.TestSecureNiFiConfigUtil");

    private static final String PROPERTIES_PREFIX = "nifi-properties";
    private static final boolean EXPECT_STORES_TO_EXIST = true;

    private Path nifiPropertiesFile;
    private Path keystorePath;
    private Path truststorePath;
    private final Path existingKeystorePath = getTestFilePath("existing-keystore.p12");
    private final Path existingTruststorePath = getTestFilePath("existing-truststore.p12");

    private NiFiProperties configureSecureNiFiProperties(Path testPropertiesFile) throws IOException {
        Files.copy(testPropertiesFile, nifiPropertiesFile, StandardCopyOption.REPLACE_EXISTING);
        SecureNiFiConfigUtil.configureSecureNiFiProperties(nifiPropertiesFile.toString(), logger);

        return new NiFiPropertiesLoader().load(nifiPropertiesFile.toFile());
    }

    private static Path getPathFromClasspath(String filename) {
        try {
            return Paths.get(TestSecureNiFiConfigUtil.class.getClassLoader().getResource(filename).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static Path getTestFilePath(String filename) {
        return Paths.get(TEST_RESOURCE_DIR + filename);
    }

    private static String getFileHash(Path filepath) throws IOException {
        return DigestUtils.sha256Hex(Files.readAllBytes(filepath));
    }

    @BeforeEach
    public void init() throws IOException {
        nifiPropertiesFile = Files.createTempFile(PROPERTIES_PREFIX, ".properties");
        TlsConfiguration tlsConfig = new TemporaryKeyStoreBuilder().build();
        Files.move(Paths.get(tlsConfig.getKeystorePath()), existingKeystorePath, StandardCopyOption.REPLACE_EXISTING);
        Files.move(Paths.get(tlsConfig.getTruststorePath()), existingTruststorePath, StandardCopyOption.REPLACE_EXISTING);
    }

    @AfterEach
    public void cleanUp() throws IOException {
        deleteIfExists(nifiPropertiesFile);

        deleteIfExists(keystorePath);
        deleteIfExists(truststorePath);
        deleteIfExists(existingKeystorePath);
        deleteIfExists(existingTruststorePath);
    }

    private static void deleteIfExists(Path path) throws IOException {
        if (path != null && StringUtils.isNotEmpty(path.toString())) {
            Files.deleteIfExists(path);
        }
    }

    private void runTestWithExpectedSuccess(String testPropertiesFile, List<String> expectedSANs) throws IOException, GeneralSecurityException {
        Path testPropertiesFilePath = getPathFromClasspath(testPropertiesFile);
        NiFiProperties niFiProperties = this.configureSecureNiFiProperties(testPropertiesFilePath);

        keystorePath = Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE));
        assertTrue(keystorePath.toFile().exists());
        assertFalse(niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).isEmpty());
        assertEquals("PKCS12", niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE));

        char[] keyPassword = niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).toCharArray();
        KeyStore keyStore = KeyStoreUtils.loadKeyStore(keystorePath.toString(),
                niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).toCharArray(),
                niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE));
        String alias = keyStore.aliases().nextElement();
        assertTrue(keyStore.isKeyEntry(alias));
        Key key = keyStore.getKey(alias, keyPassword);
        assertNotNull(key);

        truststorePath = Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE));
        assertTrue(truststorePath.toFile().exists());
        assertFalse(niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD).isEmpty());
        assertEquals("PKCS12", niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE));

        KeyStore trustStore = KeyStoreUtils.loadKeyStore(truststorePath.toString(),
                niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD).toCharArray(),
                niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE));
        String trustAlias = trustStore.aliases().nextElement();
        assertTrue(trustStore.isCertificateEntry(trustAlias));
        Certificate certificate = trustStore.getCertificate(trustAlias);
        certificate.verify(certificate.getPublicKey());

        if (!expectedSANs.isEmpty()) {
            Collection<List<?>> sans = ((X509Certificate)certificate).getSubjectAlternativeNames();
            Set<String> foundSands = new HashSet<>();
            for(List<?> list : sans) {
                String san = (String) list.get(1);
                if (IPAddress.isValid(san)) {
                    assertEquals(GeneralName.iPAddress, list.get(0));
                } else {
                    assertEquals(GeneralName.dNSName, list.get(0));
                }
                foundSands.add((String) list.get(1));
            }
            for(String expectedSAN : expectedSANs) {
                assertTrue(foundSands.contains(expectedSAN));
            }
        }
    }

    private void runTestWithNoExpectedUpdates(String testPropertiesFile, boolean expectBothStoresToExist) throws IOException {
       this.runTestWithNoExpectedUpdates(testPropertiesFile, expectBothStoresToExist, expectBothStoresToExist);
    }

    private void runTestWithNoExpectedUpdates(String testPropertiesFile, boolean expectKeystoreToExist, boolean expectTruststoreToExist)
            throws IOException {
        Path testPropertiesFilePath = getPathFromClasspath(testPropertiesFile);
        NiFiProperties niFiProperties = this.configureSecureNiFiProperties(testPropertiesFilePath);
        keystorePath = Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE));
        truststorePath = Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE));

        assertEquals(expectKeystoreToExist, Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE)).toFile().exists());
        assertEquals(expectTruststoreToExist, Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE)).toFile().exists());

        // Show that nifi.properties was not updated
        assertEquals(getFileHash(nifiPropertiesFile), getFileHash(testPropertiesFilePath));
    }

    @Test
    public void testSuccessfulConfiguration() throws IOException, GeneralSecurityException {
        runTestWithExpectedSuccess("nifi.properties.success", Collections.emptyList());
    }

    @Test
    public void testSuccessfulDNSSANs() throws IOException, GeneralSecurityException {
        runTestWithExpectedSuccess("nifi.properties.dns-sans",
                Arrays.asList("test-host", "remote-host", "proxy-host", "cluster-host"));
    }

    @Test
    public void testNoHttps() throws IOException {
        runTestWithNoExpectedUpdates("nifi.properties.no-https", !EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testNoKeystores() throws IOException {
        runTestWithNoExpectedUpdates("nifi.properties.no-keystores", !EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testTruststorePasswordSet() throws IOException {
        runTestWithNoExpectedUpdates("nifi.properties.truststore-password", !EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testKeystorePasswordSet() throws IOException {
        runTestWithNoExpectedUpdates("nifi.properties.keystore-password", !EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void test_keystoreAndTruststoreAlreadyExist() throws IOException {
        runTestWithNoExpectedUpdates("nifi.properties.stores-exist", EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testNoKeystoresTypes() throws IOException, GeneralSecurityException {
        runTestWithExpectedSuccess("nifi.properties.no-keystore-types", Collections.emptyList());
    }

    @Test
    public void testFailure_onlyTruststoreExists() throws IOException {
        runTestWithNoExpectedUpdates("nifi.properties.only-truststore", !EXPECT_STORES_TO_EXIST, EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testFailure_onlyKeystoreExists() throws IOException {
        runTestWithNoExpectedUpdates("nifi.properties.only-keystore", EXPECT_STORES_TO_EXIST, !EXPECT_STORES_TO_EXIST);
    }
}
