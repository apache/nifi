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

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.nifi.web.security.saml2.SamlUrlPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.web.csrf.CsrfFilter;
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher;
import org.springframework.security.web.util.matcher.AndRequestMatcher;
import org.springframework.security.web.util.matcher.NegatedRequestMatcher;
import org.springframework.security.web.util.matcher.OrRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Fetch Site Request Filter rejects state-changing requests that browsers identify as cross-site using the Sec-Fetch-Site header
 */
public class FetchSiteRequestFilter extends OncePerRequestFilter {
    protected static final String FETCH_SITE_HEADER = "Sec-Fetch-Site";

    private static final String CROSS_SITE = "cross-site";

    private static final String HEADER_VALUE_SEPARATOR = ",";

    /** SAML 2.0 HTTP-POST binding delivers Asserting Party messages to consumer paths using cross-site form submissions */
    private static final RequestMatcher SAML_CONSUMER_REQUEST_MATCHER = new OrRequestMatcher(
            PathPatternRequestMatcher.withDefaults().matcher(SamlUrlPath.LOGIN_RESPONSE.getPath()),
            PathPatternRequestMatcher.withDefaults().matcher(SamlUrlPath.SINGLE_LOGOUT_RESPONSE.getPath())
    );

    /** State-changing requests other than SAML 2.0 consumer requests will be evaluated */
    private static final RequestMatcher REQUEST_MATCHER = new AndRequestMatcher(
            CsrfFilter.DEFAULT_CSRF_MATCHER,
            new NegatedRequestMatcher(SAML_CONSUMER_REQUEST_MATCHER)
    );

    private static final Logger logger = LoggerFactory.getLogger(FetchSiteRequestFilter.class);

    /**
     * Skip filtering for requests that do not change state and for SAML 2.0 consumer requests
     *
     * @param request HTTP Servlet Request
     * @return Skip filtering status
     */
    @Override
    protected boolean shouldNotFilter(final HttpServletRequest request) {
        return !REQUEST_MATCHER.matches(request);
    }

    /**
     * Reject requests containing multiple Sec-Fetch-Site header values or a cross-site header value
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param filterChain Filter Chain
     * @throws ServletException Thrown on FilterChain.doFilter()
     * @throws IOException Thrown on FilterChain.doFilter() or HttpServletResponse.sendError()
     */
    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response, final FilterChain filterChain) throws ServletException, IOException {
        final List<String> fetchSiteValues = getFetchSiteValues(request);

        if (fetchSiteValues.size() > 1) {
            logger.debug("Rejected {} {} with multiple {} header values", request.getMethod(), request.getRequestURI(), FETCH_SITE_HEADER);
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
        } else if (fetchSiteValues.size() == 1 && CROSS_SITE.equalsIgnoreCase(fetchSiteValues.getFirst())) {
            logger.debug("Rejected {} {} with {} header value [{}]", request.getMethod(), request.getRequestURI(), FETCH_SITE_HEADER, CROSS_SITE);
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
        } else {
            filterChain.doFilter(request, response);
        }
    }

    private List<String> getFetchSiteValues(final HttpServletRequest request) {
        final List<String> fetchSiteValues = new ArrayList<>();

        final Enumeration<String> headers = request.getHeaders(FETCH_SITE_HEADER);
        while (headers.hasMoreElements()) {
            final String header = headers.nextElement();
            Collections.addAll(fetchSiteValues, StringUtils.tokenizeToStringArray(header, HEADER_VALUE_SEPARATOR));
        }

        return fetchSiteValues;
    }
}
