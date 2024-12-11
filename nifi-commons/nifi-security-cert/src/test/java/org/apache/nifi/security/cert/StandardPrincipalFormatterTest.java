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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.security.auth.x500.X500Principal;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardPrincipalFormatterTest {

    private static final String SUBJECT_CANONICAL = "CN=Subject,O=Organization,C=US";

    private static final String SUBJECT_FORMATTED = "CN=Subject, O=Organization, C=US";

    private static final X500Principal SUBJECT_PRINCIPAL = new X500Principal(SUBJECT_CANONICAL);

    private static final String SUBJECT_LDAP_ELEMENTS = "UID=Subject,DC=Apache,DC=NiFi";

    private static final String SUBJECT_LDAP_FORMATTED = "UID=Subject, DC=Apache, DC=NiFi";

    private static final X500Principal SUBJECT_LDAP_PRINCIPAL = new X500Principal(SUBJECT_LDAP_ELEMENTS);

    private static final String ISSUER_CANONICAL = "CN=Certificate Authority,O=Organization,C=US";

    private static final String ISSUER_FORMATTED = "CN=Certificate Authority, O=Organization, C=US";

    private static final X500Principal ISSUER_PRINCIPAL = new X500Principal(ISSUER_CANONICAL);

    @Mock
    private X509Certificate certificate;

    @Test
    void testGetSubject() {
        when(certificate.getSubjectX500Principal()).thenReturn(SUBJECT_PRINCIPAL);

        final String subject = StandardPrincipalFormatter.getInstance().getSubject(certificate);

        assertEquals(SUBJECT_FORMATTED, subject);
    }

    @Test
    void testGetSubjectLdapElements() {
        when(certificate.getSubjectX500Principal()).thenReturn(SUBJECT_LDAP_PRINCIPAL);

        final String subject = StandardPrincipalFormatter.getInstance().getSubject(certificate);

        assertEquals(SUBJECT_LDAP_FORMATTED, subject);
    }

    @Test
    void testGetIssuer() {
        when(certificate.getIssuerX500Principal()).thenReturn(ISSUER_PRINCIPAL);

        final String issuer = StandardPrincipalFormatter.getInstance().getIssuer(certificate);

        assertEquals(ISSUER_FORMATTED, issuer);
    }
}
