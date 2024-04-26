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
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;

import jakarta.servlet.http.Cookie;
import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class Saml2LogoutSuccessHandlerTest {
    private static final String REQUEST_IDENTIFIER = UUID.randomUUID().toString();

    private static final String USER_IDENTITY = LogoutRequest.class.getSimpleName();

    private static final String REQUEST_URI = "/nifi-api";

    private static final int SERVER_PORT = 8080;

    private static final String REDIRECTED_URL = "http://localhost:8080/nifi/logout-complete";

    @Mock
    Authentication authentication;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    LogoutRequestManager logoutRequestManager;

    Saml2LogoutSuccessHandler handler;

    @BeforeEach
    void setHandler() {
        logoutRequestManager = new LogoutRequestManager();
        handler = new Saml2LogoutSuccessHandler(logoutRequestManager);
        httpServletRequest = new MockHttpServletRequest();
        httpServletRequest.setServerPort(SERVER_PORT);
        httpServletResponse = new MockHttpServletResponse();
    }

    @Test
    void testOnLogoutSuccessRequestNotFound() throws IOException {
        final Cookie cookie = new Cookie(ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER);
        httpServletRequest.setCookies(cookie);
        httpServletRequest.setRequestURI(REQUEST_URI);

        handler.onLogoutSuccess(httpServletRequest, httpServletResponse, authentication);

        final String redirectedUrl = httpServletResponse.getRedirectedUrl();

        assertEquals(REDIRECTED_URL, redirectedUrl);
    }

    @Test
    void testOnLogoutSuccessRequestFound() throws IOException {
        final Cookie cookie = new Cookie(ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER);
        httpServletRequest.setCookies(cookie);
        httpServletRequest.setRequestURI(REQUEST_URI);

        final LogoutRequest logoutRequest = new LogoutRequest(REQUEST_IDENTIFIER, USER_IDENTITY);
        logoutRequestManager.start(logoutRequest);

        handler.onLogoutSuccess(httpServletRequest, httpServletResponse, authentication);

        final String redirectedUrl = httpServletResponse.getRedirectedUrl();

        assertEquals(REDIRECTED_URL, redirectedUrl);
    }
}
