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
package org.apache.nifi.web.security.jwt;

import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.WebUtils;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NiFiBearerTokenResolver implements BearerTokenResolver {
    private static final Logger logger = LoggerFactory.getLogger(NiFiBearerTokenResolver.class);
    private static final Pattern BEARER_HEADER_PATTERN = Pattern.compile("^Bearer (\\S*\\.\\S*\\.\\S*){1}$");
    private static final Pattern JWT_PATTERN = Pattern.compile("^(\\S*\\.\\S*\\.\\S*)$");
    public static final String AUTHORIZATION = "Authorization";
    public static final String JWT_COOKIE_NAME = "__Host-Authorization-Bearer";

    @Override
    public String resolve(HttpServletRequest request) {
        final String authorizationHeader = request.getHeader(AUTHORIZATION);
        final Cookie cookieHeader = WebUtils.getCookie(request, JWT_COOKIE_NAME);

        if (StringUtils.isNotBlank(authorizationHeader) && validAuthorizationHeaderFormat(authorizationHeader)) {
            return getTokenFromHeader(authorizationHeader);
        } else if(cookieHeader != null && validJwtFormat(cookieHeader.getValue())) {
            return cookieHeader.getValue();
        } else {
            logger.debug("Authorization header was not present or not in a valid format.");
            return null;
        }
    }

    private boolean validAuthorizationHeaderFormat(String authorizationHeader) {
        Matcher matcher = BEARER_HEADER_PATTERN.matcher(authorizationHeader);
        return matcher.matches();
    }

    private boolean validJwtFormat(String jwt) {
        Matcher matcher = JWT_PATTERN.matcher(jwt);
        return matcher.matches();
    }

    private String getTokenFromHeader(String authenticationHeader) {
        Matcher matcher = BEARER_HEADER_PATTERN.matcher(authenticationHeader);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new InvalidAuthenticationException("JWT did not match expected pattern.");
        }
    }
}
