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
package org.apache.nifi.web.security.x509;

import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiWebAuthenticationDetails;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class X509AuthenticationFilterTest {

    private static final String CLIENT_CERTIFICATE_ATTRIBUTE = "jakarta.servlet.request.X509Certificate";

    private static final String ALICE = "alice";

    private static final String BOB = "bob";

    private static final String ANALYSTS = "analysts";

    private static final String OPERATORS = "operators";

    private X509AuthenticationFilter filter;

    @BeforeEach
    void setup() {
        filter = new X509AuthenticationFilter();
        filter.setCertificateExtractor(new X509CertificateExtractor());
        filter.setPrincipalExtractor(mock(X509PrincipalExtractor.class));
        filter.setAuthenticationDetailsSource(NiFiWebAuthenticationDetails::new);
    }

    @Test
    void testMultipleProxiedEntitiesChainValuesRejected() {
        final MockHttpServletRequest request = secureRequestWithCertificate();
        request.addHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, formatEntities(ALICE));
        request.addHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, formatEntities(BOB));

        assertThrows(InvalidAuthenticationException.class, () -> filter.attemptAuthentication(request));
    }

    @Test
    void testMultipleProxiedEntityGroupsValuesRejected() {
        final MockHttpServletRequest request = secureRequestWithCertificate();
        request.addHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, formatEntities(ALICE));
        request.addHeader(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS, formatEntities(ANALYSTS));
        request.addHeader(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS, formatEntities(OPERATORS));

        assertThrows(InvalidAuthenticationException.class, () -> filter.attemptAuthentication(request));
    }

    @Test
    void testSingleProxiedEntityHeaderPassedThrough() {
        final MockHttpServletRequest request = secureRequestWithCertificate();
        request.addHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, formatEntities(ALICE));
        request.addHeader(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS, formatEntities(ANALYSTS));

        final Authentication authentication = filter.attemptAuthentication(request);

        final X509AuthenticationRequestToken token = assertInstanceOf(X509AuthenticationRequestToken.class, authentication);
        assertEquals(formatEntities(ALICE), token.getProxiedEntitiesChain());
        assertEquals(formatEntities(ANALYSTS), token.getProxiedEntityGroups());
    }

    private static MockHttpServletRequest secureRequestWithCertificate() {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.setSecure(true);
        request.setAttribute(CLIENT_CERTIFICATE_ATTRIBUTE, new X509Certificate[] {mock(X509Certificate.class)});
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
