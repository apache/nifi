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
package org.apache.nifi.web.security.csrf;

import org.springframework.security.web.csrf.CsrfFilter;
import org.springframework.security.web.util.matcher.AndRequestMatcher;
import org.springframework.security.web.util.matcher.NegatedRequestMatcher;
import org.springframework.security.web.util.matcher.RequestHeaderRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Skip Replicated Cross-Site Request Forgery Filter disables subsequent filtering for matched requests
 */
public class SkipReplicatedCsrfFilter extends OncePerRequestFilter {
    /** Replication HTTP Header applied to replicated cluster requests */
    protected static final String REPLICATED_REQUEST_HEADER = "request-transaction-id";

    /** Requests containing replicated header and not containing authorization cookies will be skipped */
    private static final RequestMatcher REQUEST_MATCHER = new AndRequestMatcher(
            new RequestHeaderRequestMatcher(REPLICATED_REQUEST_HEADER),
            new NegatedRequestMatcher(new CsrfCookieRequestMatcher())
    );

    /**
     * Set request attribute to disable standard CSRF Filter when request matches
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param filterChain Filter Chain
     * @throws ServletException Thrown on FilterChain.doFilter()
     * @throws IOException Thrown on FilterChain.doFilter()
     */
    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response, final FilterChain filterChain) throws ServletException, IOException {
        if (REQUEST_MATCHER.matches(request)) {
            CsrfFilter.skipRequest(request);
        }
        filterChain.doFilter(request, response);
    }
}
