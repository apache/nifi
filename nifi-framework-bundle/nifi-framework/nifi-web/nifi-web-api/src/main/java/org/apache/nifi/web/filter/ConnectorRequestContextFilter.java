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

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.components.connector.ConnectorRequestContext;
import org.apache.nifi.components.connector.ConnectorRequestContextHolder;
import org.apache.nifi.components.connector.StandardConnectorRequestContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Servlet filter that populates the {@link ConnectorRequestContextHolder} thread-local
 * with the current HTTP request headers and authenticated {@link NiFiUser}.
 *
 * <p>This filter must be registered after the Spring Security filter chain so that the
 * authenticated {@link NiFiUser} is available via the security context. The context is
 * always cleared in the {@code finally} block to prevent thread-local memory leaks.</p>
 */
public class ConnectorRequestContextFilter implements Filter {

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {
        try {
            if (request instanceof HttpServletRequest httpServletRequest) {
                final ConnectorRequestContext context = createContext(httpServletRequest);
                ConnectorRequestContextHolder.setContext(context);
            }
            chain.doFilter(request, response);
        } finally {
            ConnectorRequestContextHolder.clearContext();
        }
    }

    private ConnectorRequestContext createContext(final HttpServletRequest request) {
        final NiFiUser nifiUser = NiFiUserUtils.getNiFiUser();
        final Map<String, List<String>> headers = captureHeaders(request);
        return new StandardConnectorRequestContext(nifiUser, headers);
    }

    private Map<String, List<String>> captureHeaders(final HttpServletRequest request) {
        final Map<String, List<String>> headers = new HashMap<>();
        final Enumeration<String> headerNames = request.getHeaderNames();
        if (headerNames != null) {
            while (headerNames.hasMoreElements()) {
                final String name = headerNames.nextElement();
                final List<String> values = Collections.list(request.getHeaders(name));
                headers.put(name, Collections.unmodifiableList(values));
            }
        }
        return headers;
    }

    @Override
    public void init(final FilterConfig filterConfig) {
    }

    @Override
    public void destroy() {
    }
}
