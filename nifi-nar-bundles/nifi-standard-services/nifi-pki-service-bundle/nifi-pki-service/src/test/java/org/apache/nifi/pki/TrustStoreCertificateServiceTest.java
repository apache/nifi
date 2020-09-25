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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;

public class TrustStoreCertificateServiceTest {
    private static final String SERVICE_ID = UUID.randomUUID().toString();

    private static final char[] PASSWORD = SERVICE_ID.toCharArray();

    private static final String KEY_ALGORITHM = "RSA";

    private static final String SUBJECT_DN = "CN=subject";

    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private static final int ONE_DAY = 1;

    private static final String P12_EXTENSION = ".p12";

    private static final String PKCS12 = "PKCS12";

    private static final String WILDCARD_SEARCH = ".*";

    private static final String UNMATCHED_SEARCH = "^$";

    private static final String ALIAS = TrustStoreCertificateService.class.getSimpleName();

    private static final String CERTIFICATES_NOT_FOUND = "Certificates not found";

    private static final String DELETE_FAILED = "Delete Trust Store [%s] failed";

    private static final String UNKNOWN = "UNKNOWN";

    private TestRunner testRunner;

    private TrustStoreCertificateService service;

    private File trustStoreFile;

    @Before
    public void init() throws Exception {
        service = new TrustStoreCertificateService();
        final Processor processor = Mockito.mock(Processor.class);
        testRunner = TestRunners.newTestRunner(processor);

        trustStoreFile = File.createTempFile(TrustStoreCertificateServiceTest.class.getSimpleName(), P12_EXTENSION);
        trustStoreFile.deleteOnExit();
    }

    @After
    public void tearDown() throws IOException {
        if (!trustStoreFile.delete()) {
            final String message = String.format(DELETE_FAILED, trustStoreFile.getAbsolutePath());
            throw new IOException(message);
        }
    }

    @Test(expected = InitializationException.class)
    public void testOnEnabledFailed() throws InitializationException {
        final ConfigurationContext context = Mockito.mock(ConfigurationContext.class);
        final PropertyValue typeProperty = Mockito.mock(PropertyValue.class);
        Mockito.when(typeProperty.evaluateAttributeExpressions()).thenReturn(typeProperty);
        Mockito.when(typeProperty.getValue()).thenReturn(UNKNOWN);
        Mockito.when(context.getProperty(eq(TrustStoreCertificateService.TRUST_STORE_TYPE))).thenReturn(typeProperty);
        Mockito.when(context.getProperty(eq(TrustStoreCertificateService.TRUST_STORE_PATH))).thenReturn(typeProperty);
        Mockito.when(context.getProperty(eq(TrustStoreCertificateService.TRUST_STORE_PASSWORD))).thenReturn(typeProperty);
        service.onEnabled(context);
    }

    @Test
    public void testFindCertificates() throws Exception {
        final X509Certificate certificate = setServiceProperties();

        final List<X509Certificate> certificates = service.findCertificates(WILDCARD_SEARCH);
        final Iterator<X509Certificate> certificatesFound = certificates.iterator();
        assertTrue(CERTIFICATES_NOT_FOUND, certificatesFound.hasNext());

        final X509Certificate certificateFound = certificatesFound.next();
        assertEquals(certificate, certificateFound);
    }

    @Test
    public void testFindCertificatesUnmatched() throws Exception {
        setServiceProperties();

        final List<X509Certificate> certificates = service.findCertificates(UNMATCHED_SEARCH);
        final Iterator<X509Certificate> certificatesFound = certificates.iterator();
        assertFalse(CERTIFICATES_NOT_FOUND, certificatesFound.hasNext());
    }

    private X509Certificate setServiceProperties() throws Exception{
        final KeyStore trustStore = KeyStore.getInstance(PKCS12);
        trustStore.load(null);

        final KeyPair keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();
        final X509Certificate certificate = CertificateUtils.generateSelfSignedX509Certificate(keyPair, SUBJECT_DN, SIGNING_ALGORITHM, ONE_DAY);
        trustStore.setCertificateEntry(ALIAS, certificate);

        try (final OutputStream outputStream = new FileOutputStream(trustStoreFile)) {
            trustStore.store(outputStream, PASSWORD);
        }

        testRunner.addControllerService(SERVICE_ID, service);

        final String trustStorePath = trustStoreFile.getAbsolutePath();
        testRunner.setProperty(service, TrustStoreCertificateService.TRUST_STORE_PATH, trustStorePath);
        testRunner.setProperty(service, TrustStoreCertificateService.TRUST_STORE_TYPE, PKCS12);
        testRunner.setProperty(service, TrustStoreCertificateService.TRUST_STORE_PASSWORD, SERVICE_ID);

        testRunner.assertValid(service);
        testRunner.enableControllerService(service);
        return certificate;
    }
}
