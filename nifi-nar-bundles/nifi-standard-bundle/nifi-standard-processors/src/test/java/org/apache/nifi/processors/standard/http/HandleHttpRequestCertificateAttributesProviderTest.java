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
package org.apache.nifi.processors.standard.http;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.security.auth.x500.X500Principal;
import jakarta.servlet.http.HttpServletRequest;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HandleHttpRequestCertificateAttributesProviderTest {
    private static final String SUBJECT_FORMATTED = "CN=subject, OU=users";

    private static final X500Principal SUBJECT_PRINCIPAL = new X500Principal(SUBJECT_FORMATTED);

    private static final String ISSUER_FORMATTED = "CN=issuer, OU=authorities";

    private static final X500Principal ISSUER_PRINCIPAL = new X500Principal(ISSUER_FORMATTED);

    private static final String RFC_822_NAME_GENERAL_NAME = "rfc822Name";

    private static final String DNS_NAME_GENERAL_NAME = "dNSName";

    private static final Integer RFC_822_NAME_TYPE = 1;

    private static final String EMAIL_ADDRESS = "username@localhost.localdomain";

    private static final Integer DNS_NAME_TYPE = 2;

    private static final String DNS_NAME = "localhost.localdomain";

    private static final String FIRST_SAN_NAME_ATTRIBUTE_KEY = "http.certificate.sans.0.name";

    private static final String FIRST_SAN_NAME_TYPE_ATTRIBUTE_KEY = "http.certificate.sans.0.nameType";

    private static final String SECOND_SAN_NAME_ATTRIBUTE_KEY = "http.certificate.sans.1.name";

    private static final String SECOND_SAN_NAME_TYPE_ATTRIBUTE_KEY = "http.certificate.sans.1.nameType";

    private static final String PARSING_EXCEPTION_MESSAGE = "SAN parsing failed";

    @Mock
    private HttpServletRequest request;

    @Mock
    private X509Certificate certificate;

    private HandleHttpRequestCertificateAttributesProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new HandleHttpRequestCertificateAttributesProvider();
    }

    @Test
    void testCertificatesNotFound() {
        final Map<String, String> attributes = provider.getCertificateAttributes(request);

        assertTrue(attributes.isEmpty());
    }

    @Test
    void testCertificatesFound() {
        final X509Certificate[] certificates = new X509Certificate[]{certificate};
        when(request.getAttribute(eq(HandleHttpRequestCertificateAttributesProvider.REQUEST_CERTIFICATES_ATTRIBUTE_NAME))).thenReturn(certificates);

        when(certificate.getSubjectX500Principal()).thenReturn(SUBJECT_PRINCIPAL);
        when(certificate.getIssuerX500Principal()).thenReturn(ISSUER_PRINCIPAL);

        final Map<String, String> attributes = provider.getCertificateAttributes(request);

        assertSubjectIssuerFound(attributes);
    }

    @Test
    void testCertificatesFoundParsingException() throws CertificateParsingException {
        final X509Certificate[] certificates = new X509Certificate[]{certificate};
        when(request.getAttribute(eq(HandleHttpRequestCertificateAttributesProvider.REQUEST_CERTIFICATES_ATTRIBUTE_NAME))).thenReturn(certificates);

        when(certificate.getSubjectX500Principal()).thenReturn(SUBJECT_PRINCIPAL);
        when(certificate.getIssuerX500Principal()).thenReturn(ISSUER_PRINCIPAL);

        when(certificate.getSubjectAlternativeNames()).thenThrow(new CertificateParsingException(PARSING_EXCEPTION_MESSAGE));

        final Map<String, String> attributes = provider.getCertificateAttributes(request);

        assertSubjectIssuerFound(attributes);
    }

    @Test
    void testCertificateSubjectAlternativeNamesFound() throws CertificateParsingException {
        final X509Certificate[] certificates = new X509Certificate[]{certificate};
        when(request.getAttribute(eq(HandleHttpRequestCertificateAttributesProvider.REQUEST_CERTIFICATES_ATTRIBUTE_NAME))).thenReturn(certificates);

        when(certificate.getSubjectX500Principal()).thenReturn(SUBJECT_PRINCIPAL);
        when(certificate.getIssuerX500Principal()).thenReturn(ISSUER_PRINCIPAL);

        final Collection<List<?>> subjectAlternativeNames = new ArrayList<>();

        final List<?> emailAddressName = Arrays.asList(RFC_822_NAME_TYPE, EMAIL_ADDRESS);
        subjectAlternativeNames.add(emailAddressName);

        final List<?> dnsName = Arrays.asList(DNS_NAME_TYPE, DNS_NAME);
        subjectAlternativeNames.add(dnsName);

        when(certificate.getSubjectAlternativeNames()).thenReturn(subjectAlternativeNames);

        final Map<String, String> attributes = provider.getCertificateAttributes(request);

        assertSubjectIssuerFound(attributes);

        assertEquals(attributes.get(FIRST_SAN_NAME_ATTRIBUTE_KEY), EMAIL_ADDRESS);
        assertEquals(attributes.get(FIRST_SAN_NAME_TYPE_ATTRIBUTE_KEY), RFC_822_NAME_GENERAL_NAME);

        assertEquals(attributes.get(SECOND_SAN_NAME_ATTRIBUTE_KEY), DNS_NAME);
        assertEquals(attributes.get(SECOND_SAN_NAME_TYPE_ATTRIBUTE_KEY), DNS_NAME_GENERAL_NAME);
    }

    private void assertSubjectIssuerFound(final Map<String, String> attributes) {
        assertEquals(SUBJECT_FORMATTED, attributes.get(CertificateAttribute.HTTP_SUBJECT_DN.getName()));
        assertEquals(ISSUER_FORMATTED, attributes.get(CertificateAttribute.HTTP_ISSUER_DN.getName()));
    }
}
