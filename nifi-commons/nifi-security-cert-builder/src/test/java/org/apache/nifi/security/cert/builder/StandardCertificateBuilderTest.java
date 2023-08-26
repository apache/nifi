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
package org.apache.nifi.security.cert.builder;

import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.security.auth.x500.X500Principal;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardCertificateBuilderTest {
    private static final String KEY_PAIR_ALGORITHM = "RSA";

    private static final X500Principal ISSUER = new X500Principal("CN=issuer, OU=nifi, OU=apache, OU=org");

    private static final X500Principal SUBJECT = new X500Principal("CN=subject, OU=nifi, OU=apache, OU=org");

    private static final String SUBJECT_COMMON_NAME = "subject";

    private static final String ISSUER_COMMON_NAME = "issuer";

    private static final Duration VALIDITY_PERIOD = Duration.ofDays(30);

    private static final int AUTHORITY_CONSTRAINTS = 2147483647;

    private static final int STANDARD_CONSTRAINTS = -1;

    private static final List<String> EXTENDED_KEY_USAGE = Arrays.asList(KeyPurposeId.id_kp_clientAuth.toString(), KeyPurposeId.id_kp_serverAuth.toString());

    private static final boolean[] AUTHORITY_KEY_USAGE = new boolean[]{true, true, true, true, true, true, true, false, false};

    private static final boolean[] STANDARD_KEY_USAGE = new boolean[]{true, true, true, true, true, false, false, false, false};

    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";

    private static final String DNS_NAME = "localhost.local";

    private static final int NAME_TYPE_INDEX = 0;

    private static final int NAME_INDEX = 1;

    private static final int DNS_NAME_TYPE = 2;

    private static KeyPair issuerKeyPair;

    private static KeyPair subjectKeyPair;

    @BeforeAll
    static void setIssuerKeyPair() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_PAIR_ALGORITHM);
        issuerKeyPair = keyPairGenerator.generateKeyPair();
        subjectKeyPair = keyPairGenerator.generateKeyPair();
    }

    @Test
    void testBuildSelfSigned() throws CertificateParsingException {
        final StandardCertificateBuilder builder = new StandardCertificateBuilder(issuerKeyPair, ISSUER, VALIDITY_PERIOD);

        final X509Certificate certificate = builder.build();

        assertPropertiesFound(certificate);

        assertEquals(ISSUER, certificate.getSubjectX500Principal());
        assertEquals(issuerKeyPair.getPublic(), certificate.getPublicKey());
        assertEquals(AUTHORITY_CONSTRAINTS, certificate.getBasicConstraints());
        assertArrayEquals(AUTHORITY_KEY_USAGE, certificate.getKeyUsage());
        assertFirstSubjectAlternativeNameEquals(ISSUER_COMMON_NAME, certificate);
    }

    @Test
    void testBuildSelfSignedSubjectAlternativeNames() throws CertificateParsingException {
        final StandardCertificateBuilder builder = new StandardCertificateBuilder(issuerKeyPair, ISSUER, VALIDITY_PERIOD);

        final X509Certificate certificate = builder.setDnsSubjectAlternativeNames(Collections.singletonList(DNS_NAME)).build();

        assertPropertiesFound(certificate);

        assertEquals(ISSUER, certificate.getSubjectX500Principal());
        assertEquals(issuerKeyPair.getPublic(), certificate.getPublicKey());
        assertEquals(AUTHORITY_CONSTRAINTS, certificate.getBasicConstraints());
        assertArrayEquals(AUTHORITY_KEY_USAGE, certificate.getKeyUsage());
        assertFirstSubjectAlternativeNameEquals(ISSUER_COMMON_NAME, certificate);
        assertSecondSubjectAlternativeNameEquals(certificate);
    }

    @Test
    void testBuildIssued() throws CertificateParsingException {
        final StandardCertificateBuilder builder = new StandardCertificateBuilder(issuerKeyPair, ISSUER, VALIDITY_PERIOD);
        final PublicKey subjectPublicKey = subjectKeyPair.getPublic();

        final X509Certificate certificate = builder.setSubject(SUBJECT).setSubjectPublicKey(subjectPublicKey).build();
        assertPropertiesFound(certificate);

        assertEquals(SUBJECT, certificate.getSubjectX500Principal());
        assertEquals(subjectPublicKey, certificate.getPublicKey());
        assertEquals(STANDARD_CONSTRAINTS, certificate.getBasicConstraints());
        assertArrayEquals(STANDARD_KEY_USAGE, certificate.getKeyUsage());
        assertFirstSubjectAlternativeNameEquals(SUBJECT_COMMON_NAME, certificate);
    }

    private void assertPropertiesFound(final X509Certificate certificate) throws CertificateParsingException {
        assertNotNull(certificate);

        assertEquals(ISSUER, certificate.getIssuerX500Principal());

        final Date notBefore = certificate.getNotBefore();
        final Date notAfterExpected = Date.from(notBefore.toInstant().plus(VALIDITY_PERIOD));
        assertEquals(notAfterExpected, certificate.getNotAfter());

        assertEquals(SIGNATURE_ALGORITHM, certificate.getSigAlgName());
        final List<String> extendedKeyUsage = certificate.getExtendedKeyUsage();
        assertEquals(EXTENDED_KEY_USAGE, extendedKeyUsage);
    }

    private void assertFirstSubjectAlternativeNameEquals(final String nameExpected, final X509Certificate certificate) throws CertificateParsingException {
        final Collection<?> subjectAlternativeNames = certificate.getSubjectAlternativeNames();
        assertNotNull(subjectAlternativeNames);

        final Optional<?> firstName = subjectAlternativeNames.stream().findFirst();
        assertTrue(firstName.isPresent());

        final List<?> subjectAlternativeName = (List<?>) firstName.get();
        assertSubjectAlternativeNameEquals(nameExpected, subjectAlternativeName);
    }

    private void assertSecondSubjectAlternativeNameEquals(final X509Certificate certificate) throws CertificateParsingException {
        final Collection<?> subjectAlternativeNames = certificate.getSubjectAlternativeNames();
        assertNotNull(subjectAlternativeNames);

        final Iterator<?> names = subjectAlternativeNames.iterator();
        assertNotNull(names.next());

        final List<?> subjectAlternativeName = (List<?>) names.next();
        assertSubjectAlternativeNameEquals(DNS_NAME, subjectAlternativeName);
    }

    private void assertSubjectAlternativeNameEquals(final String nameExpected, final List<?> subjectAlternativeName) {
        final String name = subjectAlternativeName.get(NAME_INDEX).toString();
        assertEquals(nameExpected, name);

        final Object nameType = subjectAlternativeName.get(NAME_TYPE_INDEX);
        assertEquals(DNS_NAME_TYPE, nameType);
    }
}
