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

package org.apache.nifi.toolkit.tls.standalone;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.toolkit.tls.commandLine.BaseCommandLine;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.util.TlsHelperTest;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Permission;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TlsToolkitStandaloneTest {
    public static final String NIFI_FAKE_PROPERTY = "nifi.fake.property";
    public static final String FAKE_VALUE = "fake value";
    public static final String TEST_NIFI_PROPERTIES = "src/test/resources/localhost/nifi.properties";
    private SecurityManager originalSecurityManager;

    private File tempDir;

    @Before
    public void setup() throws IOException {
        tempDir = File.createTempFile("tls-test", UUID.randomUUID().toString());
        if (!tempDir.delete()) {
            throw new IOException("Couldn't delete " + tempDir);
        }

        if (!tempDir.mkdirs()) {
            throw new IOException("Couldn't make directory " + tempDir);
        }

        originalSecurityManager = System.getSecurityManager();
        // [see http://stackoverflow.com/questions/309396/java-how-to-test-methods-that-call-system-exit#answer-309427]
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {
                // Noop
            }

            @Override
            public void checkPermission(Permission perm, Object context) {
                // Noop
            }

            @Override
            public void checkExit(int status) {
                super.checkExit(status);
                throw new ExitException(status);
            }
        });
    }

    @After
    public void teardown() throws IOException {
        System.setSecurityManager(originalSecurityManager);
        FileUtils.deleteDirectory(tempDir);
    }

    @Test
    public void testBadParse() {
        runAndAssertExitCode(ExitCode.ERROR_PARSING_COMMAND_LINE.ordinal(), "--unknownArgument");
    }

    @Test
    public void testHelp() {
        runAndAssertExitCode(ExitCode.HELP.ordinal(), "-h");
        runAndAssertExitCode(ExitCode.HELP.ordinal(), "--help");
    }

    @Test
    public void testDirOutput() throws Exception {
        runAndAssertExitCode(0, "-o", tempDir.getAbsolutePath());
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertNull(nifiProperties.get("nifi.fake.property"));
        assertEquals(nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD), nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD));
    }

    @Test
    public void testDifferentArg() throws Exception {
        runAndAssertExitCode(0, "-o", tempDir.getAbsolutePath(), "-g");
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertNull(nifiProperties.get("nifi.fake.property"));
        assertNotEquals(nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD), nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD));
    }

    @Test
    public void testFileArg() throws Exception {
        runAndAssertExitCode(0, "-o", tempDir.getAbsolutePath(), "-f", TEST_NIFI_PROPERTIES);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertEquals(FAKE_VALUE, nifiProperties.get(NIFI_FAKE_PROPERTY));
    }

    @Test
    public void testHostnamesArgument() throws Exception {
        String nifi1 = "nifi1";
        String nifi2 = "nifi2";
        String nifi3 = "nifi3";

        runAndAssertExitCode(0, "-o", tempDir.getAbsolutePath(), "-n", nifi1 + "," + nifi2 + "," + nifi3);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        checkHostDirAndReturnNifiProperties(nifi1, x509Certificate);
        checkHostDirAndReturnNifiProperties(nifi2, x509Certificate);
        checkHostDirAndReturnNifiProperties(nifi3, x509Certificate);
    }

    @Test
    public void testKeyPasswordArg() throws Exception {
        String testKey = "testKey";
        runAndAssertExitCode(0, "-o", tempDir.getAbsolutePath(), "-K", testKey);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertEquals(testKey, nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD));
    }

    @Test
    public void testKeyStorePasswordArg() throws Exception {
        String testKeyStore = "testKeyStore";
        runAndAssertExitCode(0, "-o", tempDir.getAbsolutePath(), "-S", testKeyStore);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertEquals(testKeyStore, nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD));
    }

    @Test
    public void testTrustStorePasswordArg() throws Exception {
        String testTrustStore = "testTrustStore";
        runAndAssertExitCode(0, "-o", tempDir.getAbsolutePath(), "-P", testTrustStore);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertEquals(testTrustStore, nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD));
    }

    private X509Certificate checkLoadCertPrivateKey(String algorithm) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, CertificateException {
        KeyPair keyPair = TlsHelperTest.loadKeyPair(new File(tempDir, TlsToolkitStandalone.NIFI_KEY + ".key"));

        assertEquals(algorithm, keyPair.getPrivate().getAlgorithm());
        assertEquals(algorithm, keyPair.getPublic().getAlgorithm());

        X509Certificate x509Certificate = TlsHelperTest.loadCertificate(new File(tempDir, TlsToolkitStandalone.NIFI_CERT + ".pem"));
        assertEquals(keyPair.getPublic(), x509Certificate.getPublicKey());
        return x509Certificate;
    }

    private Properties checkHostDirAndReturnNifiProperties(String hostname, X509Certificate rootCert) throws Exception {
        File hostDir = new File(tempDir, hostname);
        Properties nifiProperties = new Properties();
        try (InputStream inputStream = new FileInputStream(new File(hostDir, TlsToolkitStandalone.NIFI_PROPERTIES))) {
            nifiProperties.load(inputStream);
        }

        String trustStoreType = nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE);
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        try (InputStream inputStream = new FileInputStream(new File(hostDir, "truststore." + trustStoreType))) {
            trustStore.load(inputStream, nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD).toCharArray());
        }

        Certificate certificate = trustStore.getCertificate(TlsToolkitStandalone.NIFI_CERT);
        assertEquals(rootCert, certificate);

        String keyStoreType = nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE);
        String keyStoreFilename = BaseCommandLine.KEYSTORE + keyStoreType;
        File keyStoreFile = new File(hostDir, keyStoreFilename);
        assertEquals(keyStoreFilename, nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE));

        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        try (InputStream inputStream = new FileInputStream(keyStoreFile)) {
            keyStore.load(inputStream, nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).toCharArray());
        }

        char[] keyPassword = nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD).toCharArray();

        KeyStore.Entry entry = keyStore.getEntry(TlsToolkitStandalone.NIFI_KEY, new KeyStore.PasswordProtection(keyPassword));
        assertEquals(KeyStore.PrivateKeyEntry.class, entry.getClass());

        KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry) entry;

        Certificate[] certificateChain = privateKeyEntry.getCertificateChain();

        assertEquals(2, certificateChain.length);
        assertEquals(rootCert, certificateChain[1]);
        certificateChain[1].verify(rootCert.getPublicKey());
        certificateChain[0].verify(rootCert.getPublicKey());
        return nifiProperties;
    }

    private void runAndAssertExitCode(int exitCode, String... args) {
        try {
            TlsToolkitStandaloneCommandLine.main(args);
            fail("Expecting exit code: " + exitCode);
        } catch (ExitException e) {
            assertEquals(exitCode, e.getExitCode());
        }
    }

    private static class ExitException extends SecurityException {
        private final int exitCode;

        public ExitException(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }
}
