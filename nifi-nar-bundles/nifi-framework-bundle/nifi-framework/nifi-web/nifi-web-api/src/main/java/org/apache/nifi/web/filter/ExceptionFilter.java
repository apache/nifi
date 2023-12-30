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

import java.io.IOException;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.web.firewall.RequestRejectedException;

/**
 * A filter to catch exceptions that aren't handled by the Jetty error-page.
 *
 */
public class ExceptionFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(ExceptionFilter.class);

    @Override
    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain filterChain)
            throws IOException, ServletException {

        try {
            filterChain.doFilter(req, resp);
        } catch (final RequestRejectedException e) {
            logger.warn("Client request rejected", e);

            final HttpServletResponse filteredResponse = (HttpServletResponse) resp;
            filteredResponse.setStatus(500);
            filteredResponse.getWriter().write("Client request rejected");
        }
    }

    @Override
    public void init(final FilterConfig config) {
    }

    @Override
    public void destroy() {
    }

}
