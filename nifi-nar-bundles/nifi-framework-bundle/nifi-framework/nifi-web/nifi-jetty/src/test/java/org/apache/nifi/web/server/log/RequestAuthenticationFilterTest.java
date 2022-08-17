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
package org.apache.nifi.web.server.log;

import org.apache.nifi.web.security.log.AuthenticationUserAttribute;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpInput;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.UserIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.Principal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class RequestAuthenticationFilterTest {
    private static final String USERNAME = "User";

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain filterChain;

    @Mock
    private HttpChannel channel;

    @Mock
    private HttpInput input;

    @Test
    public void testDoFilterAuthenticationUsernameFound() throws ServletException, IOException {
        final RequestAuthenticationFilter filter = new RequestAuthenticationFilter();

        final Request request = new Request(channel, input);
        request.setAttribute(AuthenticationUserAttribute.USERNAME.getName(), USERNAME);

        filter.doFilter(request, response, filterChain);

        final UserAuthentication authentication = (UserAuthentication) request.getAuthentication();
        assertNotNull(authentication);

        final UserIdentity userIdentity = authentication.getUserIdentity();
        assertNotNull(userIdentity);

        final Principal principal = userIdentity.getUserPrincipal();
        assertNotNull(principal);

        assertEquals(USERNAME, principal.getName());
    }

    @Test
    public void testDoFilterAuthenticationUsernameNotFound() throws ServletException, IOException {
        final RequestAuthenticationFilter filter = new RequestAuthenticationFilter();

        final Request request = new Request(channel, input);

        filter.doFilter(request, response, filterChain);

        final Authentication authentication = request.getAuthentication();
        assertNull(authentication);
    }
}
