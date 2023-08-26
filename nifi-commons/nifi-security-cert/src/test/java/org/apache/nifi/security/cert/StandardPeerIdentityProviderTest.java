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

import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardPeerIdentityProviderTest {
    private static final String DNS_NAME = "localhost.local";

    @Mock
    private X509Certificate certificate;

    private StandardPeerIdentityProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new StandardPeerIdentityProvider();
    }

    @Test
    void testGetIdentitiesNullCertificates() {
        assertThrows(SSLPeerUnverifiedException.class, () -> provider.getIdentities(null));
    }

    @Test
    void testGetIdentitiesEmptyCertificates() {
        assertThrows(SSLPeerUnverifiedException.class, () -> provider.getIdentities(new Certificate[]{}));
    }

    @Test
    void testGetIdentitiesInvalidCertificates() throws CertificateNotYetValidException, CertificateExpiredException {
        doThrow(new CertificateExpiredException()).when(certificate).checkValidity();

        assertThrows(SSLPeerUnverifiedException.class, () -> provider.getIdentities(new Certificate[]{certificate}));
    }

    @Test
    void testGetIdentitiesDnsNameFound() throws CertificateParsingException, SSLPeerUnverifiedException {
        final List<Object> generalName = new ArrayList<>();
        generalName.add(GeneralNameType.DNS_NAME.getNameType());
        generalName.add(DNS_NAME);

        when(certificate.getSubjectAlternativeNames()).thenReturn(Collections.singletonList(generalName));

        final Set<String> identities = provider.getIdentities(new Certificate[]{certificate});

        assertNotNull(identities);
        final Iterator<String> names = identities.iterator();
        assertTrue(names.hasNext());
        final String identity = names.next();
        assertEquals(DNS_NAME, identity);
    }
}
