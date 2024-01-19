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
package org.apache.nifi.web.server.filter;

import org.eclipse.jetty.ee10.servlets.DoSFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Denial-of-Service Filter extended to exclude Data Transfer operations
 */
public class DataTransferExcludedDoSFilter extends DoSFilter {
    protected static final String DATA_TRANSFER_URI_ATTRIBUTE = "nifi-api-data-transfer-uri";

    private static final String DATA_TRANSFER_PATH = "/nifi-api/data-transfer";

    /**
     * Handle Filter Chain and override service filter for Data Transfer requests
     *
     * @param filterChain Filter Chain
     * @param request HTTP Servlet Request to be evaluated
     * @param response HTTP Servlet Response
     * @throws ServletException Thrown on FilterChain.doFilter() failures
     * @throws IOException Thrown on FilterChain.doFilter() failures
     */
    @Override
    protected void doFilterChain(final FilterChain filterChain, final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        final String requestUri = request.getRequestURI();
        if (requestUri.startsWith(DATA_TRANSFER_PATH)) {
            request.setAttribute(DATA_TRANSFER_URI_ATTRIBUTE, requestUri);
            filterChain.doFilter(request, response);
        } else {
            super.doFilterChain(filterChain, request, response);
        }
    }
}
