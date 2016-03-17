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
package org.apache.nifi.web.security.kerberos;

import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.apache.nifi.web.security.token.NiFiAuthorizationRequestToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

/**
 */
public class KerberosAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(KerberosAuthenticationFilter.class);

    public static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    public static final String AUTHENTICATION_HEADER_NAME = "WWW-Authenticate";
    public static final String AUTHORIZATION_NEGOTIATE = "Negotiate";

    private KerberosService kerberosService;

    @Override
    public NiFiAuthorizationRequestToken attemptAuthentication(final HttpServletRequest request) {
        try {
            Authentication authentication = kerberosService.validateKerberosTicket(request);
            return new NiFiAuthorizationRequestToken(Arrays.asList(authentication.getName()));
        } catch (AuthenticationException e) {
            throw new InvalidAuthenticationException(e.getMessage(), e);
        }
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        final HttpServletResponse httpResponse = (HttpServletResponse) response;
        super.doFilter(request, response, chain);
        if (requiresAuthentication(httpRequest)) {
            httpResponse.addHeader(AUTHENTICATION_HEADER_NAME, AUTHORIZATION_NEGOTIATE);
            httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    @Override
    protected void unsuccessfulAuthorization(HttpServletRequest request, HttpServletResponse response, AuthenticationException ae) throws IOException {
        response.addHeader(AUTHENTICATION_HEADER_NAME, AUTHORIZATION_NEGOTIATE);
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    public void setKerberosServiceAuthenticationProvider(KerberosServiceAuthenticationProvider kerberosServiceAuthenticationProvider) {
        kerberosService.setKerberosServiceAuthenticationProvider(kerberosServiceAuthenticationProvider);
    }
}
