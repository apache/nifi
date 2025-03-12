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
package org.apache.nifi.web.security.saml2.web.authentication.logout;

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.saml2.registration.Saml2RegistrationProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.Cache;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.saml2.provider.service.authentication.logout.Saml2LogoutRequest;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;

import jakarta.servlet.http.Cookie;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardSaml2LogoutRequestRepositoryTest {
    private static final String REQUEST_IDENTIFIER = UUID.randomUUID().toString();

    private static final String RELAY_STATE = Saml2LogoutRequest.class.getSimpleName();

    private static final String LOCATION = "http://localhost/nifi-api";

    private static final String SAML_REQUEST = "<LoginRequest/>";

    @Mock
    Cache cache;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    private StandardSaml2LogoutRequestRepository repository;

    @BeforeEach
    void setRepository() {
        repository = new StandardSaml2LogoutRequestRepository(cache);
        httpServletRequest = new MockHttpServletRequest();
        httpServletResponse = new MockHttpServletResponse();
    }

    @Test
    void testLoadLogoutRequestCookieNotFound() {
        final Saml2LogoutRequest request = repository.loadLogoutRequest(httpServletRequest);

        assertNull(request);
    }

    @Test
    void testLoadLogoutRequestCacheNotFound() {
        final Cookie cookie = new Cookie(ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER);
        httpServletRequest.setCookies(cookie);

        final Saml2LogoutRequest request = repository.loadLogoutRequest(httpServletRequest);

        assertNull(request);
    }

    @Test
    void testLoadLogoutRequestFound() {
        final Cookie cookie = new Cookie(ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER);
        httpServletRequest.setCookies(cookie);

        final Saml2LogoutRequest cachedRequest = getRequest();
        when(cache.get(eq(REQUEST_IDENTIFIER), eq(Saml2LogoutRequest.class))).thenReturn(cachedRequest);

        final Saml2LogoutRequest request = repository.loadLogoutRequest(httpServletRequest);

        assertNotNull(request);
    }

    @Test
    void testSaveLogoutRequest() {
        httpServletRequest.setRequestURI(LOCATION);
        final Saml2LogoutRequest request = getRequest();

        repository.saveLogoutRequest(request, httpServletRequest, httpServletResponse);

        final Cookie cookie = httpServletResponse.getCookie(ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName());
        assertNotNull(cookie);
    }

    @Test
    void testRemoveLogoutRequestCookieNotFound() {
        final Saml2LogoutRequest request = repository.removeLogoutRequest(httpServletRequest, httpServletResponse);

        assertNull(request);
    }

    @Test
    void testRemoveLogoutRequestFound() {
        final Cookie cookie = new Cookie(ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER);
        httpServletRequest.setCookies(cookie);
        httpServletRequest.setRequestURI(LOCATION);

        final Saml2LogoutRequest cachedRequest = getRequest();
        when(cache.get(eq(REQUEST_IDENTIFIER), eq(Saml2LogoutRequest.class))).thenReturn(cachedRequest);

        final Saml2LogoutRequest request = repository.removeLogoutRequest(httpServletRequest, httpServletResponse);

        assertNotNull(request);
    }

    private Saml2LogoutRequest getRequest() {
        final RelyingPartyRegistration registration = RelyingPartyRegistration.withRegistrationId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty())
                .entityId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty())
                .assertingPartyMetadata(assertingPartyMetadata -> {
                    assertingPartyMetadata.entityId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty());
                    assertingPartyMetadata.singleSignOnServiceLocation(LOCATION);
                })
                .build();
        return Saml2LogoutRequest.withRelyingPartyRegistration(registration).samlRequest(SAML_REQUEST).relayState(RELAY_STATE).build();
    }
}
