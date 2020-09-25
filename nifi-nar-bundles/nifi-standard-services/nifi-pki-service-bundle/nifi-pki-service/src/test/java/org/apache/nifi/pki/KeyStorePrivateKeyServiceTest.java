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
package org.apache.nifi.pki;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;

public class KeyStorePrivateKeyServiceTest {
    private static final String SERVICE_ID = UUID.randomUUID().toString();

    private static final char[] PASSWORD = SERVICE_ID.toCharArray();

    private static final String KEY_ALGORITHM = "RSA";

    private static final String SUBJECT_DN = "CN=subject";

    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private static final int ONE_DAY = 1;

    private static final String P12_EXTENSION = ".p12";

    private static final String PKCS12 = "PKCS12";

    private static final String ALIAS = KeyStorePrivateKeyService.class.getSimpleName();

    private static final String PRIVATE_KEY_NOT_FOUND = "Private Key not found";

    private static final String PRIVATE_KEY_FOUND = "Private Key found";

    private static final String DELETE_FAILED = "Delete Key Store [%s] failed";

    private static final String UNKNOWN = "UNKNOWN";

    private TestRunner testRunner;

    private KeyStorePrivateKeyService service;

    private File keyStoreFile;

    @Before
    public void init() throws Exception {
        service = new KeyStorePrivateKeyService();
        final Processor processor = Mockito.mock(Processor.class);
        testRunner = TestRunners.newTestRunner(processor);

        keyStoreFile = File.createTempFile(KeyStorePrivateKeyServiceTest.class.getSimpleName(), P12_EXTENSION);
        keyStoreFile.deleteOnExit();
    }

    @After
    public void tearDown() throws IOException {
        if (!keyStoreFile.delete()) {
            final String message = String.format(DELETE_FAILED, keyStoreFile.getAbsolutePath());
            throw new IOException(message);
        }
    }

    @Test(expected = InitializationException.class)
    public void testOnEnabledFailed() throws InitializationException {
        final ConfigurationContext context = Mockito.mock(ConfigurationContext.class);
        final PropertyValue typeProperty = Mockito.mock(PropertyValue.class);
        Mockito.when(typeProperty.evaluateAttributeExpressions()).thenReturn(typeProperty);
        Mockito.when(typeProperty.getValue()).thenReturn(UNKNOWN);
        Mockito.when(context.getProperty(eq(KeyStorePrivateKeyService.KEY_STORE_TYPE))).thenReturn(typeProperty);
        Mockito.when(context.getProperty(eq(KeyStorePrivateKeyService.KEY_STORE_PATH))).thenReturn(typeProperty);
        Mockito.when(context.getProperty(eq(KeyStorePrivateKeyService.KEY_STORE_PASSWORD))).thenReturn(typeProperty);
        Mockito.when(context.getProperty(eq(KeyStorePrivateKeyService.KEY_PASSWORD))).thenReturn(typeProperty);
        service.onEnabled(context);
    }

    @Test
    public void testFindPrivateKey() throws Exception {
        final X509Certificate certificate = setServiceProperties();

        final BigInteger serialNumber = certificate.getSerialNumber();
        final X500Principal issuer = certificate.getIssuerX500Principal();
        final Optional<PrivateKey> privateKeyFound = service.findPrivateKey(serialNumber, issuer);
        assertTrue(PRIVATE_KEY_NOT_FOUND, privateKeyFound.isPresent());
    }

    @Test
    public void testFindPrivateKeyUnmatched() throws Exception {
        final X509Certificate certificate = setServiceProperties();

        final X500Principal issuer = certificate.getIssuerX500Principal();
        final Optional<PrivateKey> privateKeyFound = service.findPrivateKey(BigInteger.ZERO, issuer);
        assertFalse(PRIVATE_KEY_FOUND, privateKeyFound.isPresent());
    }

    private X509Certificate setServiceProperties() throws Exception {
        final KeyStore trustStore = KeyStore.getInstance(PKCS12);
        trustStore.load(null);

        final KeyPair keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();
        final X509Certificate certificate = CertificateUtils.generateSelfSignedX509Certificate(keyPair, SUBJECT_DN, SIGNING_ALGORITHM, ONE_DAY);
        final X509Certificate[] certificates = new X509Certificate[]{certificate};
        trustStore.setKeyEntry(ALIAS, keyPair.getPrivate(), PASSWORD, certificates);

        try (final OutputStream outputStream = new FileOutputStream(keyStoreFile)) {
            trustStore.store(outputStream, PASSWORD);
        }

        testRunner.addControllerService(SERVICE_ID, service);

        final String keyStorePath = keyStoreFile.getAbsolutePath();
        testRunner.setProperty(service, KeyStorePrivateKeyService.KEY_STORE_PATH, keyStorePath);
        testRunner.setProperty(service, KeyStorePrivateKeyService.KEY_STORE_TYPE, PKCS12);
        testRunner.setProperty(service, KeyStorePrivateKeyService.KEY_STORE_PASSWORD, SERVICE_ID);
        testRunner.setProperty(service, KeyStorePrivateKeyService.KEY_PASSWORD, SERVICE_ID);

        testRunner.assertValid(service);
        testRunner.enableControllerService(service);
        return certificate;
    }
}
