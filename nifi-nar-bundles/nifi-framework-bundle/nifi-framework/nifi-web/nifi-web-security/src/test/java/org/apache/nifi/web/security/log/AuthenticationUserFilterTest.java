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
package org.apache.nifi.web.security.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AuthenticationUserFilterTest {
    private static final String USERNAME = "User";

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain filterChain;

    @Mock
    private Authentication authentication;

    @Captor
    private ArgumentCaptor<String> usernameCaptor;

    @Test
    public void testDoFilterInternalAuthenticationFound() throws ServletException, IOException {
        final AuthenticationUserFilter filter = new AuthenticationUserFilter();

        when(authentication.getName()).thenReturn(USERNAME);
        final SecurityContext securityContext = new SecurityContextImpl(authentication);
        SecurityContextHolder.setContext(securityContext);

        filter.doFilterInternal(request, response, filterChain);

        verify(request).setAttribute(eq(AuthenticationUserAttribute.USERNAME.getName()), usernameCaptor.capture());
        assertEquals(USERNAME, usernameCaptor.getValue());
    }

    @Test
    public void testDoFilterInternalAuthenticationNotFound() throws ServletException, IOException {
        final AuthenticationUserFilter filter = new AuthenticationUserFilter();

        final SecurityContext securityContext = new SecurityContextImpl();
        SecurityContextHolder.setContext(securityContext);

        filter.doFilterInternal(request, response, filterChain);

        verify(request).getRemoteAddr();
        verifyNoMoreInteractions(request);
    }
}
