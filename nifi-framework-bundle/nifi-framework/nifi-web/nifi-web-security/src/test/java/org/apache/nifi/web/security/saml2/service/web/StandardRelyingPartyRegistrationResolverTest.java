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
package org.apache.nifi.web.security.saml2.service.web;

import org.apache.nifi.web.security.saml2.registration.Saml2RegistrationProperty;
import org.apache.nifi.web.util.WebUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrationRepository;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardRelyingPartyRegistrationResolverTest {
    private static final String SERVICE_LOCATION = "{baseUrl}/login/saml2/sso/{registrationId}";

    private static final String SINGLE_LOGOUT_LOCATION = "{baseUrl}/saml2/slo/{registrationId}";

    private static final String CONTEXT_PATH = "/nifi-api";

    private static final String REQUEST_URI = "/nifi-api/access";

    private static final String FORWARDED_PATH = "/forwarded";

    private static final int SERVER_PORT = 8080;

    private static final String EXPECTED_CONSUMER_SERVICE_LOCATION = "http://localhost:8080/nifi-api/login/saml2/sso/consumer";

    private static final String EXPECTED_FORWARDED_CONSUMER_SERVICE_LOCATION = "http://localhost:8080/forwarded/nifi-api/login/saml2/sso/consumer";

    private static final String EXPECTED_SINGLE_LOGOUT_SERVICE_LOCATION = "http://localhost:8080/forwarded/nifi-api/saml2/slo/consumer";

    private static final String REGISTRATION_ID = Saml2RegistrationProperty.REGISTRATION_ID.getProperty();

    @Mock
    RelyingPartyRegistrationRepository repository;

    MockHttpServletRequest request;

    @BeforeEach
    void setResolver() {
        request = new MockHttpServletRequest();
        request.setServerPort(SERVER_PORT);
        request.setRequestURI(REQUEST_URI);
        request.setPathInfo(REQUEST_URI);
        request.setContextPath(CONTEXT_PATH);
    }

    @Test
    void testResolveNotFound() {
        final StandardRelyingPartyRegistrationResolver resolver = new StandardRelyingPartyRegistrationResolver(repository, Collections.emptyList());

        final RelyingPartyRegistration registration = resolver.resolve(request, REGISTRATION_ID);

        assertNull(registration);
    }

    @Test
    void testResolveFound() {
        final StandardRelyingPartyRegistrationResolver resolver = new StandardRelyingPartyRegistrationResolver(repository, Collections.emptyList());

        final RelyingPartyRegistration registration = getRegistrationBuilder().build();
        when(repository.findByRegistrationId(eq(REGISTRATION_ID))).thenReturn(registration);

        final RelyingPartyRegistration resolved = resolver.resolve(request, REGISTRATION_ID);

        assertNotNull(resolved);
        assertEquals(EXPECTED_CONSUMER_SERVICE_LOCATION, resolved.getAssertionConsumerServiceLocation());
    }

    @Test
    void testResolveSingleLogoutForwardedPathFound() {
        final StandardRelyingPartyRegistrationResolver resolver = new StandardRelyingPartyRegistrationResolver(repository, Collections.singletonList(FORWARDED_PATH));

        final RelyingPartyRegistration registration = getSingleLogoutRegistration();
        when(repository.findByRegistrationId(eq(REGISTRATION_ID))).thenReturn(registration);

        request.addHeader(WebUtils.PROXY_CONTEXT_PATH_HTTP_HEADER, FORWARDED_PATH);

        final RelyingPartyRegistration resolved = resolver.resolve(request, REGISTRATION_ID);

        assertNotNull(resolved);
        assertEquals(EXPECTED_FORWARDED_CONSUMER_SERVICE_LOCATION, resolved.getAssertionConsumerServiceLocation());
        assertEquals(EXPECTED_SINGLE_LOGOUT_SERVICE_LOCATION, resolved.getSingleLogoutServiceLocation());
        assertEquals(EXPECTED_SINGLE_LOGOUT_SERVICE_LOCATION, resolved.getSingleLogoutServiceResponseLocation());
    }

    private RelyingPartyRegistration.Builder getRegistrationBuilder() {
        return RelyingPartyRegistration.withRegistrationId(REGISTRATION_ID)
                .entityId(REGISTRATION_ID)
                .assertionConsumerServiceLocation(SERVICE_LOCATION)
                .assertingPartyDetails(assertingPartyDetails -> {
                    assertingPartyDetails.entityId(REGISTRATION_ID);
                    assertingPartyDetails.singleSignOnServiceLocation(SERVICE_LOCATION);
                });
    }

    private RelyingPartyRegistration getSingleLogoutRegistration() {
        return getRegistrationBuilder()
                .singleLogoutServiceLocation(SINGLE_LOGOUT_LOCATION)
                .singleLogoutServiceResponseLocation(SINGLE_LOGOUT_LOCATION)
                .build();
    }
}
