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
package org.apache.nifi.security.ssl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardPemCertificateReaderTest {
    private static final String JAVA_HOME_PROPERTY = "java.home";

    private static final String TRUST_STORE_PATH = "lib/security/cacerts";

    private static final String LINE_LENGTH_PATTERN = "(?<=\\G.{64})";

    private static final char LINE_FEED = 10;

    private static Certificate firstCertificate;

    private static byte[] firstCertificatePem;

    private static Certificate secondCertificate;

    private static byte[] certificatesPem;

    private StandardPemCertificateReader reader;

    @BeforeAll
    static void setTrustStore() throws Exception {
        final KeyStore trustStore = loadDefaultTrustStore();

        final Enumeration<String> aliases = trustStore.aliases();

        final String firstAlias = aliases.nextElement();
        firstCertificate = trustStore.getCertificate(firstAlias);
        final String firstCertificateFormatted = getCertificateFormatted(firstCertificate);
        firstCertificatePem = firstCertificateFormatted.getBytes(StandardCharsets.US_ASCII);

        final String secondAlias = aliases.nextElement();
        secondCertificate = trustStore.getCertificate(secondAlias);
        final String secondCertificateFormatted = getCertificateFormatted(secondCertificate);

        String certificatesCombined = firstCertificateFormatted + LINE_FEED + secondCertificateFormatted;
        certificatesPem = certificatesCombined.getBytes(StandardCharsets.US_ASCII);
    }

    @BeforeEach
    void setReader() {
        reader = new StandardPemCertificateReader();
    }

    @Test
    void testReadCertificates() {
        final List<Certificate> certificates = reader.readCertificates(new ByteArrayInputStream(certificatesPem));

        assertNotNull(certificates);
        assertFalse(certificates.isEmpty());

        final Iterator<Certificate> certificatesRead = certificates.iterator();

        assertTrue(certificatesRead.hasNext());
        final Certificate firstCertificateRead = certificatesRead.next();
        assertEquals(firstCertificate, firstCertificateRead);

        assertTrue(certificatesRead.hasNext());
        final Certificate secondCertificateRead = certificatesRead.next();
        assertEquals(secondCertificate, secondCertificateRead);

        assertFalse(certificatesRead.hasNext());
    }

    @Test
    void testReadCertificatesOneFound() {
        final List<Certificate> certificates = reader.readCertificates(new ByteArrayInputStream(firstCertificatePem));

        assertNotNull(certificates);
        assertFalse(certificates.isEmpty());

        final Iterator<Certificate> certificatesRead = certificates.iterator();

        assertTrue(certificatesRead.hasNext());
        final Certificate firstCertificateRead = certificatesRead.next();
        assertEquals(firstCertificate, firstCertificateRead);

        assertFalse(certificatesRead.hasNext());
    }

    @Test
    void testReadCertificatesEmpty() {
        final List<Certificate> certificates = reader.readCertificates(new ByteArrayInputStream(new byte[]{}));

        assertNotNull(certificates);
        assertTrue(certificates.isEmpty());
    }

    @Test
    void testReadCertificatesHeaderException() {
        final byte[] encoded = StandardPemCertificateReader.CERTIFICATE_HEADER.getBytes(StandardCharsets.US_ASCII);

        assertThrows(ReadEntityException.class, () -> reader.readCertificates(new ByteArrayInputStream(encoded)));
    }

    static String getCertificateFormatted(final Certificate certificate) throws CertificateEncodingException {
        final byte[] certificateBinary = certificate.getEncoded();

        final String certificateEncoded = Base64.getEncoder().encodeToString(certificateBinary);
        final String[] certificateLines = certificateEncoded.split(LINE_LENGTH_PATTERN);

        final StringBuilder certificateBuilder = new StringBuilder();
        certificateBuilder.append(StandardPemCertificateReader.CERTIFICATE_HEADER);
        certificateBuilder.append(LINE_FEED);

        for (final String line : certificateLines) {
            certificateBuilder.append(line);
            certificateBuilder.append(LINE_FEED);
        }

        certificateBuilder.append(StandardPemCertificateReader.CERTIFICATE_FOOTER);
        certificateBuilder.append(LINE_FEED);

        return certificateBuilder.toString();
    }

    static KeyStore loadDefaultTrustStore() throws GeneralSecurityException, IOException {
        final String javaHomeProperty = System.getProperty(JAVA_HOME_PROPERTY);
        final Path javaHomeDirectory = Paths.get(javaHomeProperty);
        final Path trustStorePath = javaHomeDirectory.resolve(TRUST_STORE_PATH);

        final String trustStoreType = KeyStore.getDefaultType();
        final KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        try (InputStream inputStream = Files.newInputStream(trustStorePath)) {
            trustStore.load(inputStream, null);
        }

        return trustStore;
    }
}
