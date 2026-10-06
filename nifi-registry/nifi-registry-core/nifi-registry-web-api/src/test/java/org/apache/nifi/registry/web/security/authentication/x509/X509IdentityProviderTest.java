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
package org.apache.nifi.registry.web.security.authentication.x509;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.exception.InvalidCredentialsException;
import org.apache.nifi.registry.security.util.ProxiedEntitiesUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class X509IdentityProviderTest {

    private static final String HTTP_METHOD = "GET";

    private static final String CERTIFICATE_PRINCIPAL = "CN=node";

    private static final String ALICE = "alice";

    private static final String BOB = "bob";

    private X509IdentityProvider provider;

    private X509PrincipalExtractor principalExtractor;

    private X509Certificate certificate;

    @BeforeEach
    void setup() {
        principalExtractor = mock(X509PrincipalExtractor.class);
        certificate = mock(X509Certificate.class);
        final X509CertificateExtractor certificateExtractor = mock(X509CertificateExtractor.class);
        when(certificateExtractor.extractClientCertificate(any(HttpServletRequest.class))).thenReturn(new X509Certificate[] {certificate});
        provider = new X509IdentityProvider(principalExtractor, certificateExtractor);
    }

    @Test
    void testMultipleProxiedEntitiesChainValuesRejected() {
        final HttpServletRequest request = secureRequest(formatEntities(ALICE), formatEntities(BOB));

        assertThrows(InvalidCredentialsException.class, () -> provider.extractCredentials(request));
    }

    @Test
    void testSingleProxiedEntitiesChainPassedThrough() {
        when(principalExtractor.extractPrincipal(certificate)).thenReturn(CERTIFICATE_PRINCIPAL);
        final String chain = formatEntities(ALICE);
        final HttpServletRequest request = secureRequest(chain);

        final AuthenticationRequest authenticationRequest = provider.extractCredentials(request);

        final X509AuthenticationRequestDetails details = assertInstanceOf(X509AuthenticationRequestDetails.class, authenticationRequest.getDetails());
        assertEquals(chain, details.getProxiedEntitiesChain());
        assertEquals(HTTP_METHOD, details.getHttpMethod());
    }

    private HttpServletRequest secureRequest(final String... chainValues) {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.isSecure()).thenReturn(true);
        when(request.getMethod()).thenReturn(HTTP_METHOD);
        when(request.getHeaders(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN)).thenReturn(Collections.enumeration(List.of(chainValues)));
        when(request.getHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN)).thenReturn(chainValues[0]);
        return request;
    }

    private static String formatEntities(final String... entities) {
        final StringBuilder formattedEntities = new StringBuilder();
        for (final String entity : entities) {
            formattedEntities.append('<').append(entity).append('>');
        }
        return formattedEntities.toString();
    }
}
