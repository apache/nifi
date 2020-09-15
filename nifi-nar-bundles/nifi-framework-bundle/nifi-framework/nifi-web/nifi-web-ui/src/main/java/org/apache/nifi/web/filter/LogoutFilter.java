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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * Filter for determining appropriate logout location.
 */
public class LogoutFilter implements Filter {

    private ServletContext servletContext;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        servletContext = filterConfig.getServletContext();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
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
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            apiContext.getRequestDispatcher("/access/oidc/logout").forward(request, response);
        } else if (supportsKnoxSso) {
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            apiContext.getRequestDispatcher("/access/knox/logout").forward(request, response);
        } else if (supportsSaml) {
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            if (supportsSamlSingleLogout) {
                apiContext.getRequestDispatcher("/access/saml/single-logout/request").forward(request, response);
            } else {
                apiContext.getRequestDispatcher("/access/saml/local-logout").forward(request, response);
            }
        } else {
            final ServletContext apiContext = servletContext.getContext("/nifi-api");
            apiContext.getRequestDispatcher("/access/logout/complete").forward(request, response);
        }
    }

    @Override
    public void destroy() {
    }
}
