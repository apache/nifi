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
package org.apache.nifi.web;

import org.apache.nifi.web.security.jwt.NiFiBearerTokenResolver;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.web.util.WebUtils;
import javax.servlet.http.HttpServletRequest;

/**
 * Request Matcher checks for the existence of a cookie with the configured name
 */
public class CsrfCookieRequestMatcher implements RequestMatcher {
    private static final String DEFAULT_CSRF_COOKIE_NAME = NiFiBearerTokenResolver.JWT_COOKIE_NAME;

    /**
     * Matches request based on the presence of a cookie found using the configured name
     *
     * @param httpServletRequest HTTP Servlet Request
     * @return Request matching status
     */
    @Override
    public boolean matches(final HttpServletRequest httpServletRequest) {
        return WebUtils.getCookie(httpServletRequest, DEFAULT_CSRF_COOKIE_NAME) != null;
    }
}