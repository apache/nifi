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

package org.apache.nifi.toolkit.tls.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.service.client.TlsCertificateAuthorityClient;
import org.apache.nifi.toolkit.tls.service.client.TlsCertificateAuthorityClientCommandLine;
import org.apache.nifi.toolkit.tls.service.server.TlsCertificateAuthorityService;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone;
import org.apache.nifi.toolkit.tls.util.InputStreamFactory;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableEntryException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TlsCertificateAuthorityTest {
    private File serverConfigFile;
    private File clientConfigFile;
    private OutputStreamFactory outputStreamFactory;
    private InputStreamFactory inputStreamFactory;
    private TlsConfig serverConfig;
    private TlsClientConfig clientConfig;
    private ObjectMapper objectMapper;
    private ByteArrayOutputStream serverKeyStoreOutputStream;
    private ByteArrayOutputStream clientKeyStoreOutputStream;
    private ByteArrayOutputStream clientTrustStoreOutputStream;
    private ByteArrayOutputStream serverConfigFileOutputStream;
    private ByteArrayOutputStream clientConfigFileOutputStream;
    private String subjectAlternativeName;

    @Before
    public void setup() throws FileNotFoundException {
        objectMapper = new ObjectMapper();
        serverConfigFile = new File("fake.server.config");
        clientConfigFile = new File("fake.client.config");
        String serverKeyStore = "serverKeyStore";
        String clientKeyStore = "clientKeyStore";
        String clientTrustStore = "clientTrustStore";
        serverKeyStoreOutputStream = new ByteArrayOutputStream();
        clientKeyStoreOutputStream = new ByteArrayOutputStream();
        clientTrustStoreOutputStream = new ByteArrayOutputStream();
        serverConfigFileOutputStream = new ByteArrayOutputStream();
        clientConfigFileOutputStream = new ByteArrayOutputStream();
        subjectAlternativeName = "nifi.apache.org";

        String myTestTokenUseSomethingStronger = "myTestTokenUseSomethingStronger";
        int port = availablePort();

        serverConfig = new TlsConfig();
        serverConfig.setCaHostname("localhost");
        serverConfig.setToken(myTestTokenUseSomethingStronger);
        serverConfig.setKeyStore(serverKeyStore);
        serverConfig.setPort(port);
        serverConfig.setDays(5);
        serverConfig.setKeySize(2048);
        serverConfig.initDefaults();

        clientConfig = new TlsClientConfig();
        clientConfig.setCaHostname("localhost");
        clientConfig.setDn("OU=NIFI,CN=otherHostname");
        clientConfig.setKeyStore(clientKeyStore);
        clientConfig.setTrustStore(clientTrustStore);
        clientConfig.setToken(myTestTokenUseSomethingStronger);
        clientConfig.setDomainAlternativeNames(Arrays.asList(subjectAlternativeName));
        clientConfig.setPort(port);
        clientConfig.setKeySize(2048);
        clientConfig.initDefaults();

        outputStreamFactory = mock(OutputStreamFactory.class);
        mockReturnOutputStream(outputStreamFactory, new File(serverKeyStore), serverKeyStoreOutputStream);
        mockReturnOutputStream(outputStreamFactory, new File(clientKeyStore), clientKeyStoreOutputStream);
        mockReturnOutputStream(outputStreamFactory, new File(clientTrustStore), clientTrustStoreOutputStream);
        mockReturnOutputStream(outputStreamFactory, serverConfigFile, serverConfigFileOutputStream);
        mockReturnOutputStream(outputStreamFactory, clientConfigFile, clientConfigFileOutputStream);

        inputStreamFactory = mock(InputStreamFactory.class);
        mockReturnProperties(inputStreamFactory, serverConfigFile, serverConfig);
        mockReturnProperties(inputStreamFactory, clientConfigFile, clientConfig);
    }

    private void mockReturnProperties(InputStreamFactory inputStreamFactory, File file, TlsConfig tlsConfig) throws FileNotFoundException {
        when(inputStreamFactory.create(eq(file))).thenAnswer(invocation -> {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            objectMapper.writeValue(byteArrayOutputStream, tlsConfig);
            return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        });
    }

    private void mockReturnOutputStream(OutputStreamFactory outputStreamFactory, File file, OutputStream outputStream) throws FileNotFoundException {
        when(outputStreamFactory.create(or(eq(file), eq(new File(file.getAbsolutePath()))))).thenReturn(outputStream);
    }

    @Test
    public void testClientGetCertDifferentPasswordsForKeyAndKeyStore() throws Exception {
        TlsCertificateAuthorityService tlsCertificateAuthorityService = null;
        try {
            tlsCertificateAuthorityService = new TlsCertificateAuthorityService(outputStreamFactory);
            tlsCertificateAuthorityService.start(serverConfig, serverConfigFile.getAbsolutePath(), true);
            TlsCertificateAuthorityClient tlsCertificateAuthorityClient = new TlsCertificateAuthorityClient(outputStreamFactory);
            tlsCertificateAuthorityClient.generateCertificateAndGetItSigned(clientConfig, null, clientConfigFile.getAbsolutePath(), true);
            validate();
        } finally {
            if (tlsCertificateAuthorityService != null) {
                tlsCertificateAuthorityService.shutdown();
            }
        }
    }

    @Test
    public void testClientGetCertSamePasswordsForKeyAndKeyStore() throws Exception {
        TlsCertificateAuthorityService tlsCertificateAuthorityService = null;
        try {
            tlsCertificateAuthorityService = new TlsCertificateAuthorityService(outputStreamFactory);
            tlsCertificateAuthorityService.start(serverConfig, serverConfigFile.getAbsolutePath(), false);
            TlsCertificateAuthorityClient tlsCertificateAuthorityClient = new TlsCertificateAuthorityClient(outputStreamFactory);
            tlsCertificateAuthorityClient.generateCertificateAndGetItSigned(clientConfig, null, clientConfigFile.getAbsolutePath(), false);
            validate();
        } finally {
            if (tlsCertificateAuthorityService != null) {
                tlsCertificateAuthorityService.shutdown();
            }
        }
    }

    @Test
    public void testClientPkcs12() throws Exception {
        serverConfig.setKeyStoreType(KeystoreType.PKCS12.toString());
        clientConfig.setKeyStoreType(KeystoreType.PKCS12.toString());
        TlsCertificateAuthorityService tlsCertificateAuthorityService = null;
        try {
            tlsCertificateAuthorityService = new TlsCertificateAuthorityService(outputStreamFactory);
            tlsCertificateAuthorityService.start(serverConfig, serverConfigFile.getAbsolutePath(), false);
            TlsCertificateAuthorityClient tlsCertificateAuthorityClient = new TlsCertificateAuthorityClient(outputStreamFactory);
            new TlsCertificateAuthorityClientCommandLine(inputStreamFactory);
            tlsCertificateAuthorityClient.generateCertificateAndGetItSigned(clientConfig, null, clientConfigFile.getAbsolutePath(), true);
            validate();
        } finally {
            if (tlsCertificateAuthorityService != null) {
                tlsCertificateAuthorityService.shutdown();
            }
        }
    }

    @Test
    public void testTokenMismatch() throws Exception {
        serverConfig.setToken("a different token...");
        try {
            testClientGetCertSamePasswordsForKeyAndKeyStore();
            fail("Expected error with mismatching token");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("forbidden"));
        }
    }

    private void validate() throws CertificateException, InvalidKeyException, NoSuchAlgorithmException, KeyStoreException, SignatureException,
            NoSuchProviderException, UnrecoverableEntryException, IOException {
        Certificate caCertificate = validateServerKeyStore();
        validateClient(caCertificate);
    }

    private Certificate validateServerKeyStore() throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableEntryException,
            InvalidKeyException, NoSuchProviderException, SignatureException {
        serverConfig = objectMapper.readValue(new ByteArrayInputStream(serverConfigFileOutputStream.toByteArray()), TlsConfig.class);

        KeyStore serverKeyStore = KeyStoreUtils.getKeyStore(serverConfig.getKeyStoreType());
        serverKeyStore.load(new ByteArrayInputStream(serverKeyStoreOutputStream.toByteArray()), serverConfig.getKeyStorePassword().toCharArray());
        String keyPassword = serverConfig.getKeyPassword();
        KeyStore.Entry serverKeyEntry = serverKeyStore.getEntry(TlsToolkitStandalone.NIFI_KEY,
                new KeyStore.PasswordProtection(keyPassword == null ? serverConfig.getKeyStorePassword().toCharArray() : keyPassword.toCharArray()));

        assertTrue(serverKeyEntry instanceof KeyStore.PrivateKeyEntry);
        KeyStore.PrivateKeyEntry privateKeyEntry = (KeyStore.PrivateKeyEntry) serverKeyEntry;
        Certificate[] certificateChain = privateKeyEntry.getCertificateChain();
        assertEquals(1, certificateChain.length);
        Certificate caCertificate = certificateChain[0];
        caCertificate.verify(caCertificate.getPublicKey());
        assertPrivateAndPublicKeyMatch(privateKeyEntry.getPrivateKey(), caCertificate.getPublicKey());
        return caCertificate;
    }

    private void validateClient(Certificate caCertificate) throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException,
            UnrecoverableEntryException, InvalidKeyException, NoSuchProviderException, SignatureException {
        clientConfig = objectMapper.readValue(new ByteArrayInputStream(clientConfigFileOutputStream.toByteArray()), TlsClientConfig.class);

        KeyStore clientKeyStore = KeyStoreUtils.getKeyStore(clientConfig.getKeyStoreType());
        clientKeyStore.load(new ByteArrayInputStream(clientKeyStoreOutputStream.toByteArray()), clientConfig.getKeyStorePassword().toCharArray());
        String keyPassword = clientConfig.getKeyPassword();
        KeyStore.Entry clientKeyStoreEntry = clientKeyStore.getEntry(TlsToolkitStandalone.NIFI_KEY,
                new KeyStore.PasswordProtection(keyPassword == null ? clientConfig.getKeyStorePassword().toCharArray() : keyPassword.toCharArray()));

        assertTrue(clientKeyStoreEntry instanceof KeyStore.PrivateKeyEntry);
        KeyStore.PrivateKeyEntry clientPrivateKeyEntry = (KeyStore.PrivateKeyEntry) clientKeyStoreEntry;
        Certificate[] certificateChain = clientPrivateKeyEntry.getCertificateChain();
        assertEquals(2, certificateChain.length);
        assertEquals(caCertificate, certificateChain[1]);
        certificateChain[0].verify(caCertificate.getPublicKey());
        assertPrivateAndPublicKeyMatch(clientPrivateKeyEntry.getPrivateKey(), certificateChain[0].getPublicKey());

        // Does the certificate contain the SAN we defined in the client config?
        assert(isSANPresent(certificateChain[0]));

        KeyStore clientTrustStore = KeyStoreUtils.getKeyStore(KeystoreType.JKS.toString());
        clientTrustStore.load(new ByteArrayInputStream(clientTrustStoreOutputStream.toByteArray()), clientConfig.getTrustStorePassword().toCharArray());
        assertEquals(caCertificate, clientTrustStore.getCertificate(TlsToolkitStandalone.NIFI_CERT));
    }

    public static void assertPrivateAndPublicKeyMatch(PrivateKey privateKey, PublicKey publicKey) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        Signature signature = Signature.getInstance(TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        signature.initSign(privateKey);
        byte[] bytes = "test string".getBytes(StandardCharsets.UTF_8);
        signature.update(bytes);

        Signature verify = Signature.getInstance(TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        verify.initVerify(publicKey);
        verify.update(bytes);
        verify.verify(signature.sign());
    }

    private boolean isSANPresent(Certificate cert) {
        Iterator<List<?>> iterator = null;
        try {
            iterator = ((X509Certificate) cert).getSubjectAlternativeNames().iterator();
        } catch (CertificateParsingException e) {
            e.printStackTrace();
        }
        boolean containsSAN = false;
        while(iterator.hasNext()) {
            if(iterator.next().contains(subjectAlternativeName)) {
                containsSAN = true;
            }
        }
        return containsSAN;
    }

    /**
     * Will determine the available port used by ca server
     */
    private int availablePort() {
        ServerSocket s = null;
        try {
            s = new ServerSocket(0);
            s.setReuseAddress(true);
            return s.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to discover available port.", e);
        } finally {
            try {
                s.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
