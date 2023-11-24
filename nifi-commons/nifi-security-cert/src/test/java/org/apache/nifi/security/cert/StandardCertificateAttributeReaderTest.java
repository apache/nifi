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
package org.apache.nifi.security.cert;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardCertificateAttributeReaderTest {
    private static final String DNS_NAME = "localhost.local";

    private static final byte[] REGISTERED_ID = new byte[]{1, 2, 3};

    @Mock
    private X509Certificate certificate;

    private StandardCertificateAttributeReader reader;

    @BeforeEach
    void setReader() {
        reader = new StandardCertificateAttributeReader();
    }

    @Test
    void testGetSubjectAlternativeNamesEmpty() throws CertificateParsingException {
        when(certificate.getSubjectAlternativeNames()).thenReturn(Collections.emptyList());
        final List<SubjectAlternativeName> subjectAlternativeNames = reader.getSubjectAlternativeNames(certificate);

        assertNotNull(subjectAlternativeNames);
        assertTrue(subjectAlternativeNames.isEmpty());
    }

    @Test
    void testGetSubjectAlternativeNamesNull() throws CertificateParsingException {
        when(certificate.getSubjectAlternativeNames()).thenReturn(null);
        final List<SubjectAlternativeName> subjectAlternativeNames = reader.getSubjectAlternativeNames(certificate);

        assertNotNull(subjectAlternativeNames);
        assertTrue(subjectAlternativeNames.isEmpty());
    }

    @Test
    void testGetSubjectAlternativeNamesException() throws CertificateParsingException {
        when(certificate.getSubjectAlternativeNames()).thenThrow(new CertificateParsingException());
        final List<SubjectAlternativeName> subjectAlternativeNames = reader.getSubjectAlternativeNames(certificate);

        assertNotNull(subjectAlternativeNames);
        assertTrue(subjectAlternativeNames.isEmpty());
    }

    @Test
    void testGetSubjectAlternativeNamesDnsName() throws CertificateParsingException {
        final List<Object> generalName = new ArrayList<>();
        generalName.add(GeneralNameType.DNS_NAME.getNameType());
        generalName.add(DNS_NAME);

        when(certificate.getSubjectAlternativeNames()).thenReturn(Collections.singletonList(generalName));
        final List<SubjectAlternativeName> subjectAlternativeNames = reader.getSubjectAlternativeNames(certificate);

        assertNotNull(subjectAlternativeNames);

        final Iterator<SubjectAlternativeName> names = subjectAlternativeNames.iterator();
        assertTrue(names.hasNext());
        final SubjectAlternativeName subjectAlternativeName = names.next();
        assertEquals(GeneralNameType.DNS_NAME, subjectAlternativeName.getGeneralNameType());
        assertEquals(String.class, subjectAlternativeName.getGeneralNameType().getValueClass());
        assertEquals(DNS_NAME, subjectAlternativeName.getName());
    }

    @Test
    void testGetSubjectAlternativeNamesRegisteredId() throws CertificateParsingException {
        final List<Object> generalName = new ArrayList<>();
        generalName.add(GeneralNameType.REGISTERED_ID.getNameType());
        generalName.add(REGISTERED_ID);

        when(certificate.getSubjectAlternativeNames()).thenReturn(Collections.singletonList(generalName));
        final List<SubjectAlternativeName> subjectAlternativeNames = reader.getSubjectAlternativeNames(certificate);

        assertNotNull(subjectAlternativeNames);

        final Iterator<SubjectAlternativeName> names = subjectAlternativeNames.iterator();
        assertTrue(names.hasNext());
        final SubjectAlternativeName subjectAlternativeName = names.next();
        assertEquals(GeneralNameType.REGISTERED_ID, subjectAlternativeName.getGeneralNameType());
        assertEquals(byte[].class, subjectAlternativeName.getGeneralNameType().getValueClass());

        final String registeredIdEncoded = Base64.getEncoder().encodeToString(REGISTERED_ID);
        assertEquals(registeredIdEncoded, subjectAlternativeName.getName());
    }
}
