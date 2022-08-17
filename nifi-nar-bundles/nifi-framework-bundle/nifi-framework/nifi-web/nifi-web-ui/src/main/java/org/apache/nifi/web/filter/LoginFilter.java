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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;

/**
 * Filter for determining appropriate login location.
 */
public class LoginFilter implements Filter {
    private static final String SAML2_AUTHENTICATE_FILTER_PATH = "/nifi-api/saml2/authenticate/consumer";

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

        if (supportsOidc) {
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            apiContext.getRequestDispatcher("/access/oidc/request").forward(request, response);
        } else if (supportsKnoxSso) {
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            apiContext.getRequestDispatcher("/access/knox/request").forward(request, response);
        } else if (supportsSAML) {
            final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
            final URI authenticateUri = RequestUriBuilder.fromHttpServletRequest(httpServletRequest).path(SAML2_AUTHENTICATE_FILTER_PATH).build();
            // Redirect to request consumer URL defined in Spring Security OpenSamlAuthenticationRequestResolver.requestMatcher
            final HttpServletResponse httpServletResponse = (HttpServletResponse) response;
            httpServletResponse.sendRedirect(authenticateUri.toString());
        } else {
            filterChain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {
    }
}
