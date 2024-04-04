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
import org.apache.nifi.web.security.saml2.SamlUrlPath;
import org.apache.nifi.web.security.token.LogoutAuthenticationToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import java.io.IOException;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class Saml2SingleLogoutFilterTest {
    private static final String REQUEST_IDENTIFIER = UUID.randomUUID().toString();

    private static final String USER_IDENTITY = LogoutRequest.class.getSimpleName();

    @Mock
    LogoutSuccessHandler logoutSuccessHandler;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    MockFilterChain filterChain;

    LogoutRequestManager logoutRequestManager;

    Saml2SingleLogoutFilter filter;

    @BeforeEach
    void setFilter() {
        logoutRequestManager = new LogoutRequestManager();
        filter = new Saml2SingleLogoutFilter(logoutRequestManager, logoutSuccessHandler);
        httpServletRequest = new MockHttpServletRequest();
        httpServletResponse = new MockHttpServletResponse();
        filterChain = new MockFilterChain();
    }

    @Test
    void testDoFilterInternalNotMatched() throws ServletException, IOException {
        filter.doFilterInternal(httpServletRequest, httpServletResponse, filterChain);

        verifyNoInteractions(logoutSuccessHandler);
    }

    @Test
    void testDoFilterInternal() throws ServletException, IOException {
        httpServletRequest.setPathInfo(SamlUrlPath.SINGLE_LOGOUT_REQUEST.getPath());

        final Cookie cookie = new Cookie(ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER);
        httpServletRequest.setCookies(cookie);

        final LogoutRequest logoutRequest = new LogoutRequest(REQUEST_IDENTIFIER, USER_IDENTITY);
        logoutRequestManager.start(logoutRequest);

        filter.doFilterInternal(httpServletRequest, httpServletResponse, filterChain);

        verify(logoutSuccessHandler).onLogoutSuccess(eq(httpServletRequest), eq(httpServletResponse), isA(LogoutAuthenticationToken.class));
    }
}
