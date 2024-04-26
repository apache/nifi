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
package org.apache.nifi.web.security.jwt.resolver;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.security.http.SecurityCookieName;
import org.apache.nifi.web.security.http.SecurityHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.web.util.WebUtils;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;

/**
 * Bearer Token Resolver prefers the HTTP Authorization Header and then evaluates the Authorization Cookie when found
 */
public class StandardBearerTokenResolver implements BearerTokenResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardBearerTokenResolver.class);
    private static final String BEARER_PREFIX = "Bearer ";

    /**
     * Resolve Bearer Token from HTTP Request checking Authorization Header then Authorization Cookie when found
     *
     * @param request HTTP Servlet Request
     * @return Bearer Token or null when not found
     */
    @Override
    public String resolve(final HttpServletRequest request) {
        String bearerToken = null;

        final String header = request.getHeader(SecurityHeader.AUTHORIZATION.getHeader());
        if (StringUtils.startsWithIgnoreCase(header, BEARER_PREFIX)) {
            bearerToken = StringUtils.removeStartIgnoreCase(header, BEARER_PREFIX);
        } else {
            final Cookie cookie = WebUtils.getCookie(request, SecurityCookieName.AUTHORIZATION_BEARER.getName());
            if (cookie == null) {
                LOGGER.trace("Bearer Token not found in Header or Cookie");
            } else {
                bearerToken = cookie.getValue();
            }
        }

        return bearerToken;
    }
}
