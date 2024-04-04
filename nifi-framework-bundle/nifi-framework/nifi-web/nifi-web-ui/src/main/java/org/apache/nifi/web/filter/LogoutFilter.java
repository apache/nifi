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
 * Filter for determining appropriate logout location.
 */
public class LogoutFilter implements Filter {

    private static final String OIDC_LOGOUT_URL = "/nifi-api/access/oidc/logout";

    private static final String SAML_LOCAL_LOGOUT_URL = "/nifi-api/access/saml/local-logout/request";

    private static final String SAML_SINGLE_LOGOUT_URL = "/nifi-api/access/saml/single-logout/request";

    private static final String KNOX_LOGOUT_URL = "/nifi-api/access/knox/logout";

    private static final String LOGOUT_COMPLETE_URL = "/nifi-api/access/logout/complete";

    private ServletContext servletContext;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        servletContext = filterConfig.getServletContext();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException {
        final boolean supportsOidc = Boolean.parseBoolean(servletContext.getInitParameter("oidc-supported"));
        final boolean supportsKnoxSso = Boolean.parseBoolean(servletContext.getInitParameter("knox-supported"));
        final boolean supportsSaml = Boolean.parseBoolean(servletContext.getInitParameter("saml-supported"));
        final boolean supportsSamlSingleLogout = Boolean.parseBoolean(servletContext.getInitParameter("saml-single-logout-supported"));

        // NOTE: This filter runs in the web-ui module and is bound to /nifi/logout. Currently the front-end first makes an ajax call
        // to issue a DELETE to /nifi-api/access/logout. After successful completion it sets the browser location to /nifi/logout
        // which triggers this filter. Since this request was made from setting window.location, the JWT will never be sent which
        // means there will be no logged in user or Authorization header when forwarding to any of the URLs below. Instead the
        // /access/logout end-point sets a Cookie with a logout request identifier which can be used by the end-points below
        // to retrieve information about the user logging out.

        if (supportsOidc) {
            sendRedirect(OIDC_LOGOUT_URL, request, response);
        } else if (supportsKnoxSso) {
            sendRedirect(KNOX_LOGOUT_URL, request, response);
        } else if (supportsSaml) {
            final String logoutUrl = supportsSamlSingleLogout ? SAML_SINGLE_LOGOUT_URL : SAML_LOCAL_LOGOUT_URL;
            sendRedirect(logoutUrl, request, response);
        } else {
            sendRedirect(LOGOUT_COMPLETE_URL, request, response);
        }
    }

    @Override
    public void destroy() {
    }

    private void sendRedirect(final String logoutUrl, final ServletRequest request, final ServletResponse response) throws IOException {
        final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        final URI targetUri = RequestUriBuilder.fromHttpServletRequest(httpServletRequest).path(logoutUrl).build();
        final HttpServletResponse httpServletResponse = (HttpServletResponse) response;
        httpServletResponse.sendRedirect(targetUri.toString());
    }
}
