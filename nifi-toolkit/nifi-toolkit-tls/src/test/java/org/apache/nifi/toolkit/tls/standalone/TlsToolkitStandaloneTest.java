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
import org.apache.commons.io.IOUtils;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.toolkit.tls.SystemExitCapturer;
import org.apache.nifi.toolkit.tls.commandLine.BaseTlsToolkitCommandLine;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.service.TlsCertificateAuthorityTest;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.toolkit.tls.util.TlsHelperTest;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class TlsToolkitStandaloneTest {
    public static final String NIFI_FAKE_PROPERTY = "nifi.fake.property";
    public static final String FAKE_VALUE = "fake value";
    public static final String TEST_NIFI_PROPERTIES = "src/test/resources/localhost/nifi.properties";
    private SystemExitCapturer systemExitCapturer;

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
        systemExitCapturer = new SystemExitCapturer();
    }

    @After
    public void teardown() throws IOException {
        systemExitCapturer.close();
        FileUtils.deleteDirectory(tempDir);
    }

    @Test
    public void testBadParse() {
        runAndAssertExitCode(ExitCode.ERROR_PARSING_COMMAND_LINE, "--unknownArgument");
    }

    @Test
    public void testHelp() {
        runAndAssertExitCode(ExitCode.HELP, "-h");
        runAndAssertExitCode(ExitCode.HELP, "--help");
    }

    @Test
    public void testDirOutput() throws Exception {
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-n", TlsConfig.DEFAULT_HOSTNAME);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertNull(nifiProperties.get("nifi.fake.property"));
        assertEquals(nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD), nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD));
    }

    @Test
    public void testDifferentArg() throws Exception {
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-g", "-n", TlsConfig.DEFAULT_HOSTNAME);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertNull(nifiProperties.get("nifi.fake.property"));
        assertNotEquals(nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD), nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD));
    }

    @Test
    public void testFileArg() throws Exception {
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-f", TEST_NIFI_PROPERTIES, "-n", TlsConfig.DEFAULT_HOSTNAME);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertEquals(FAKE_VALUE, nifiProperties.get(NIFI_FAKE_PROPERTY));
    }

    @Test
    public void testHostnamesArgumentOverwrite() throws Exception {
        String nifi1 = "nifi1";
        String nifi2 = "nifi2";
        String nifi3 = "nifi3";

        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-n", nifi1 + "," + nifi2);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-n", nifi3);

        checkHostDirAndReturnNifiProperties(nifi1, x509Certificate);
        checkHostDirAndReturnNifiProperties(nifi2, x509Certificate);
        checkHostDirAndReturnNifiProperties(nifi3, x509Certificate);
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-O", "-n", nifi3);
        checkHostDirAndReturnNifiProperties(nifi3, x509Certificate);
    }

    @Test
    public void testHostnamesArgumentNoOverwrite() throws Exception {
        String nifi = "nifi";

        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-n", nifi);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        checkHostDirAndReturnNifiProperties(nifi, x509Certificate);
        runAndAssertExitCode(ExitCode.ERROR_GENERATING_CONFIG, "-o", tempDir.getAbsolutePath(), "-n", nifi);
    }

    @Test
    public void testKeyPasswordArg() throws Exception {
        String testKey = "testKey";
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-K", testKey, "-n", TlsConfig.DEFAULT_HOSTNAME);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertEquals(testKey, nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD));
    }

    @Test
    public void testKeyStorePasswordArg() throws Exception {
        String testKeyStore = "testKeyStore";
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-S", testKeyStore, "-n", TlsConfig.DEFAULT_HOSTNAME);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertEquals(testKeyStore, nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD));
    }

    @Test
    public void testTrustStorePasswordArg() throws Exception {
        String testTrustStore = "testTrustStore";
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-P", testTrustStore, "-n", TlsConfig.DEFAULT_HOSTNAME);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        Properties nifiProperties = checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
        assertEquals(testTrustStore, nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD));
    }

    @Test
    public void testDnArgs() throws Exception {
        String nifiDnPrefix = "O=apache, CN=";
        String nifiDnSuffix = ", OU=nifi";
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-n", TlsConfig.DEFAULT_HOSTNAME,
                "--" + TlsToolkitStandaloneCommandLine.NIFI_DN_PREFIX_ARG, nifiDnPrefix, "--" + TlsToolkitStandaloneCommandLine.NIFI_DN_SUFFIX_ARG, nifiDnSuffix);
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, nifiDnPrefix, nifiDnSuffix, x509Certificate);
    }

    @Test
    public void testKeyStoreTypeArg() throws Exception {
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-n", TlsConfig.DEFAULT_HOSTNAME, "-T", KeystoreType.PKCS12.toString().toLowerCase(),
                "-K", "change", "-S", "change", "-P", "change");
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);
        checkHostDirAndReturnNifiProperties(TlsConfig.DEFAULT_HOSTNAME, x509Certificate);
    }

    @Test
    public void testClientDnsArg() throws Exception {
        String clientDn = "OU=NIFI,CN=testuser";
        String clientDn2 = "OU=NIFI,CN=testuser2";
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-C", clientDn, "-C", clientDn2, "-B", "pass1", "-P", "pass2");
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        checkClientCert(clientDn, x509Certificate);
        checkClientCert(clientDn2, x509Certificate);

        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-O", "-C", clientDn2, "-B", "pass3");
        checkClientCert(clientDn2, x509Certificate);
    }

    @Test
    public void testClientDnsArgNoOverwrite() throws Exception {
        String clientDn = "OU=NIFI,CN=testuser";
        runAndAssertExitCode(ExitCode.SUCCESS, "-o", tempDir.getAbsolutePath(), "-C", clientDn, "-B", "passwor");
        X509Certificate x509Certificate = checkLoadCertPrivateKey(TlsConfig.DEFAULT_KEY_PAIR_ALGORITHM);

        checkClientCert(clientDn, x509Certificate);

        runAndAssertExitCode(ExitCode.ERROR_GENERATING_CONFIG, "-o", tempDir.getAbsolutePath(), "-C", clientDn);
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
        return checkHostDirAndReturnNifiProperties(hostname, TlsConfig.DEFAULT_DN_PREFIX, TlsConfig.DEFAULT_DN_SUFFIX, rootCert);
    }

    private Properties checkHostDirAndReturnNifiProperties(String hostname, String dnPrefix, String dnSuffix, X509Certificate rootCert) throws Exception {
        File hostDir = new File(tempDir, hostname);
        Properties nifiProperties = new Properties();
        try (InputStream inputStream = new FileInputStream(new File(hostDir, TlsToolkitStandalone.NIFI_PROPERTIES))) {
            nifiProperties.load(inputStream);
        }

        String trustStoreType = nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE);
        assertEquals(KeystoreType.JKS.toString().toLowerCase(), trustStoreType.toLowerCase());
        KeyStore trustStore = KeyStoreUtils.getTrustStore(trustStoreType);
        try (InputStream inputStream = new FileInputStream(new File(hostDir, "truststore." + trustStoreType))) {
            trustStore.load(inputStream, nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD).toCharArray());
        }

        String trustStoreFilename = BaseTlsToolkitCommandLine.TRUSTSTORE + trustStoreType;
        assertEquals("./conf/" + trustStoreFilename, nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE));

        Certificate certificate = trustStore.getCertificate(TlsToolkitStandalone.NIFI_CERT);
        assertEquals(rootCert, certificate);

        String keyStoreType = nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE);
        String keyStoreFilename = BaseTlsToolkitCommandLine.KEYSTORE + keyStoreType;
        File keyStoreFile = new File(hostDir, keyStoreFilename);
        assertEquals("./conf/" + keyStoreFilename, nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE));

        KeyStore keyStore = KeyStoreUtils.getKeyStore(keyStoreType);
        char[] keyStorePassword = nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD).toCharArray();
        try (InputStream inputStream = new FileInputStream(keyStoreFile)) {
            keyStore.load(inputStream, keyStorePassword);
        }

        char[] keyPassword = nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD).toCharArray();
        if (keyPassword == null || keyPassword.length == 0) {
            keyPassword = keyStorePassword;
        }

        KeyStore.Entry entry = keyStore.getEntry(TlsToolkitStandalone.NIFI_KEY, new KeyStore.PasswordProtection(keyPassword));
        assertEquals(KeyStore.PrivateKeyEntry.class, entry.getClass());

        KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry) entry;

        Certificate[] certificateChain = privateKeyEntry.getCertificateChain();

        assertEquals(2, certificateChain.length);
        assertEquals(rootCert, certificateChain[1]);
        certificateChain[1].verify(rootCert.getPublicKey());
        certificateChain[0].verify(rootCert.getPublicKey());
        TlsConfig tlsConfig = new TlsConfig();
        tlsConfig.setDnPrefix(dnPrefix);
        tlsConfig.setDnSuffix(dnSuffix);
        assertEquals(tlsConfig.calcDefaultDn(hostname), CertificateUtils.convertAbstractX509Certificate(certificateChain[0]).getSubjectX500Principal().getName());
        TlsCertificateAuthorityTest.assertPrivateAndPublicKeyMatch(privateKeyEntry.getPrivateKey(), certificateChain[0].getPublicKey());
        return nifiProperties;
    }

    private void checkClientCert(String clientDn, X509Certificate rootCert) throws Exception {
        String clientDnFile = TlsHelper.escapeFilename(CertificateUtils.reorderDn(clientDn));
        String password;
        try (FileReader fileReader = new FileReader(new File(tempDir, clientDnFile + ".password"))) {
            List<String> lines = IOUtils.readLines(fileReader);
            assertEquals(1, lines.size());
            password = lines.get(0);
        }

        KeyStore keyStore = KeyStoreUtils.getKeyStore(KeystoreType.PKCS12.toString());
        try (FileInputStream fileInputStream = new FileInputStream(new File(tempDir, clientDnFile + ".p12"))) {
            keyStore.load(fileInputStream, password.toCharArray());
        }
        PrivateKey privateKey = (PrivateKey) keyStore.getKey(TlsToolkitStandalone.NIFI_KEY, new char[0]);
        Certificate[] certificateChain = keyStore.getCertificateChain(TlsToolkitStandalone.NIFI_KEY);
        assertEquals(2, certificateChain.length);
        assertEquals(rootCert, certificateChain[1]);
        certificateChain[1].verify(rootCert.getPublicKey());
        certificateChain[0].verify(rootCert.getPublicKey());
        PublicKey publicKey = certificateChain[0].getPublicKey();
        TlsCertificateAuthorityTest.assertPrivateAndPublicKeyMatch(privateKey, publicKey);

    }

    private void runAndAssertExitCode(ExitCode exitCode, String... args) {
        systemExitCapturer.runAndAssertExitCode(() -> TlsToolkitStandaloneCommandLine.main(args), exitCode);
    }
}
