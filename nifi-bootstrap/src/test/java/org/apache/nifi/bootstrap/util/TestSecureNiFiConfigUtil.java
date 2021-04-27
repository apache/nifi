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
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.util.IPAddress;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

public class TestSecureNiFiConfigUtil {
    public static final String TEST_RESOURCE_DIR = "src/test/resources/";
    private Logger logger = LoggerFactory.getLogger("org.apache.nifi.bootstrap.util.TestSecureNiFiConfigUtil");

    private static final String PROPERTIES_PREFIX = "nifi-properties";
    private static final boolean EXPECT_STORES_TO_EXIST = true;

    private Path nifiPropertiesFile;
    private Path keystorePath;
    private Path truststorePath;
    private Path existingKeystorePath = getTestFilePath("existing-keystore.p12");
    private Path existingTruststorePath = getTestFilePath("existing-truststore.p12");

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

    @Before
    public void init() throws IOException, GeneralSecurityException {
        nifiPropertiesFile = Files.createTempFile(PROPERTIES_PREFIX, ".properties");
        TlsConfiguration tlsConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(new StandardTlsConfiguration());
        Files.move(Paths.get(tlsConfig.getKeystorePath()), existingKeystorePath, StandardCopyOption.REPLACE_EXISTING);
        Files.move(Paths.get(tlsConfig.getTruststorePath()), existingTruststorePath, StandardCopyOption.REPLACE_EXISTING);
    }

    @After
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
        Assert.assertTrue(keystorePath.toFile().exists());
        Assert.assertFalse(niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).isEmpty());
        Assert.assertEquals("PKCS12", niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE));

        char[] keyPassword = niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).toCharArray();
        KeyStore keyStore = KeyStoreUtils.loadKeyStore(keystorePath.toString(),
                niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).toCharArray(),
                niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE));
        String alias = keyStore.aliases().nextElement();
        Assert.assertTrue(keyStore.isKeyEntry(alias));
        Key key = keyStore.getKey(alias, keyPassword);
        Assert.assertNotNull(key);

        truststorePath = Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE));
        Assert.assertTrue(truststorePath.toFile().exists());
        Assert.assertFalse(niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD).isEmpty());
        Assert.assertEquals("PKCS12", niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE));

        KeyStore trustStore = KeyStoreUtils.loadKeyStore(truststorePath.toString(),
                niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD).toCharArray(),
                niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE));
        String trustAlias = trustStore.aliases().nextElement();
        Assert.assertTrue(trustStore.isCertificateEntry(trustAlias));
        Certificate certificate = trustStore.getCertificate(trustAlias);
        certificate.verify(certificate.getPublicKey());

        if (!expectedSANs.isEmpty()) {
            Collection<List<?>> sans = ((X509Certificate)certificate).getSubjectAlternativeNames();
            Set<String> foundSands = new HashSet<>();
            for(List<?> list : sans) {
                String san = (String) list.get(1);
                if (IPAddress.isValid(san)) {
                    Assert.assertEquals(GeneralName.iPAddress, list.get(0));
                } else {
                    Assert.assertEquals(GeneralName.dNSName, list.get(0));
                }
                foundSands.add((String) list.get(1));
            }
            for(String expectedSAN : expectedSANs) {
                Assert.assertTrue(foundSands.contains(expectedSAN));
            }
        }
    }

    private void runTestWithNoExpectedUpdates(String testPropertiesFile, boolean expectBothStoresToExist) throws IOException, GeneralSecurityException {
       this.runTestWithNoExpectedUpdates(testPropertiesFile, expectBothStoresToExist, expectBothStoresToExist);
    }

    private void runTestWithNoExpectedUpdates(String testPropertiesFile, boolean expectKeystoreToExist, boolean expectTruststoreToExist)
            throws IOException, GeneralSecurityException {
        Path testPropertiesFilePath = getPathFromClasspath(testPropertiesFile);
        NiFiProperties niFiProperties = this.configureSecureNiFiProperties(testPropertiesFilePath);
        keystorePath = Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE));
        truststorePath = Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE));

        Assert.assertEquals(expectKeystoreToExist, Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE)).toFile().exists());
        Assert.assertEquals(expectTruststoreToExist, Paths.get(niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE)).toFile().exists());

        // Show that nifi.properties was not updated
        Assert.assertEquals(getFileHash(nifiPropertiesFile), getFileHash(testPropertiesFilePath));
    }

    @Test
    public void testSuccessfulConfiguration() throws IOException, GeneralSecurityException {
        runTestWithExpectedSuccess("nifi.properties.success", Collections.EMPTY_LIST);
    }

    @Test
    public void testSuccessfulDNSSANs() throws IOException, GeneralSecurityException {
        runTestWithExpectedSuccess("nifi.properties.dns-sans",
                Arrays.asList(new String[] {"test-host", "remote-host", "proxy-host", "cluster-host"}));
    }

    @Test
    public void testNoHttps() throws IOException, GeneralSecurityException {
        runTestWithNoExpectedUpdates("nifi.properties.no-https", !EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testNoKeystores() throws IOException, GeneralSecurityException {
        runTestWithNoExpectedUpdates("nifi.properties.no-keystores", !EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testTruststorePasswordSet() throws IOException, GeneralSecurityException {
        runTestWithNoExpectedUpdates("nifi.properties.truststore-password", !EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testKeystorePasswordSet() throws IOException, GeneralSecurityException {
        runTestWithNoExpectedUpdates("nifi.properties.keystore-password", !EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void test_keystoreAndTruststoreAlreadyExist() throws IOException, GeneralSecurityException {
        runTestWithNoExpectedUpdates("nifi.properties.stores-exist", EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testNoKeystoresTypes() throws IOException, GeneralSecurityException {
        runTestWithExpectedSuccess("nifi.properties.no-keystore-types", Collections.EMPTY_LIST);
    }

    @Test
    public void testFailure_onlyTruststoreExists() throws IOException, GeneralSecurityException {
        runTestWithNoExpectedUpdates("nifi.properties.only-truststore", !EXPECT_STORES_TO_EXIST, EXPECT_STORES_TO_EXIST);
    }

    @Test
    public void testFailure_onlyKeystoreExists() throws IOException, GeneralSecurityException {
        runTestWithNoExpectedUpdates("nifi.properties.only-keystore", EXPECT_STORES_TO_EXIST, !EXPECT_STORES_TO_EXIST);
    }
}
