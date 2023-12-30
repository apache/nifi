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

import org.apache.nifi.web.security.saml2.SamlUrlPath;
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
import java.io.IOException;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class Saml2LocalLogoutFilterTest {
    @Mock
    LogoutSuccessHandler logoutSuccessHandler;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    MockFilterChain filterChain;

    Saml2LocalLogoutFilter filter;

    @BeforeEach
    void setFilter() {
        filter = new Saml2LocalLogoutFilter(logoutSuccessHandler);
        httpServletRequest = new MockHttpServletRequest();
        httpServletResponse = new MockHttpServletResponse();
        filterChain = new MockFilterChain();
    }

    @Test
    void testDoFilterInternalNotMatched() throws ServletException, IOException {
        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        verifyNoInteractions(logoutSuccessHandler);
    }

    @Test
    void testDoFilterInternal() throws ServletException, IOException {
        httpServletRequest.setPathInfo(SamlUrlPath.LOCAL_LOGOUT_REQUEST.getPath());
        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        verify(logoutSuccessHandler).onLogoutSuccess(eq(httpServletRequest), eq(httpServletResponse), isNull());
    }
}
