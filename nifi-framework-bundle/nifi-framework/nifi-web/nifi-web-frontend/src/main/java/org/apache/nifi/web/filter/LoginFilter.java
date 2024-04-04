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
package org.apache.nifi.web.filter;

import org.apache.nifi.web.util.RequestUriBuilder;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;

/**
 * Filter for determining appropriate login location.
 */
public class LoginFilter implements Filter {
    private static final String OAUTH2_AUTHORIZATION_PATH = "/nifi-api/oauth2/authorization/consumer";

    private static final String SAML2_AUTHENTICATE_FILTER_PATH = "/nifi-api/saml2/authenticate/consumer";

    private static final String KNOX_REQUEST_PATH = "/nifi-api/access/knox/request";

    private static final String NIFI_LOGIN_PATH = "/nf/";
    private static final String NIFI_LOGIN_FRAGMENT = "/login";

    private ServletContext servletContext;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        servletContext = filterConfig.getServletContext();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        final boolean supportsOidc = Boolean.parseBoolean(servletContext.getInitParameter("oidc-supported"));
        final boolean supportsKnoxSso = Boolean.parseBoolean(servletContext.getInitParameter("knox-supported"));
        final boolean supportsSAML = Boolean.parseBoolean(servletContext.getInitParameter("saml-supported"));

        final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        final RequestUriBuilder requestUriBuilder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest);

        if  (supportsKnoxSso) {
            final URI redirectUri = requestUriBuilder.path(KNOX_REQUEST_PATH).build();
            sendRedirect(response, redirectUri);
        } else if (supportsOidc) {
            final URI redirectUri = requestUriBuilder.path(OAUTH2_AUTHORIZATION_PATH).build();
            // Redirect to authorization URL defined in Spring Security OAuth2AuthorizationRequestRedirectFilter
            sendRedirect(response, redirectUri);
        } else if (supportsSAML) {
            final URI redirectUri = requestUriBuilder.path(SAML2_AUTHENTICATE_FILTER_PATH).build();
            // Redirect to request consumer URL defined in Spring Security OpenSamlAuthenticationRequestResolver.requestMatcher
            sendRedirect(response, redirectUri);
        } else {
            final URI redirectUri = requestUriBuilder.path(NIFI_LOGIN_PATH).fragment(NIFI_LOGIN_FRAGMENT).build();
            sendRedirect(response, redirectUri);
        }
    }

    @Override
    public void destroy() {
    }

    private void sendRedirect(final ServletResponse response, final URI redirectUri) throws IOException {
        final HttpServletResponse httpServletResponse = (HttpServletResponse) response;
        httpServletResponse.sendRedirect(redirectUri.toString());
    }
}
