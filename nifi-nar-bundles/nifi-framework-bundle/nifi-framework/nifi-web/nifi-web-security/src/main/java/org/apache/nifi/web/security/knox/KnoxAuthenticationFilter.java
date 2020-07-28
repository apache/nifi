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
package org.apache.nifi.web.security.knox;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.springframework.security.core.Authentication;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

/**
 */
public class KnoxAuthenticationFilter extends NiFiAuthenticationFilter {

    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request) {
        // only support knox login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // ensure knox sso support is enabled
        final NiFiProperties properties = getProperties();
        if (!properties.isKnoxSsoEnabled()) {
            return null;
        }

        // get the principal out of the user token
        final String knoxJwt = getJwtFromCookie(request, properties.getKnoxCookieName());

        // if there is no cookie, return null to attempt another authentication
        if (knoxJwt == null) {
            return null;
        } else {
            // otherwise create the authentication request token
            return new KnoxAuthenticationRequestToken(knoxJwt, request.getRemoteAddr());
        }
    }

    public String getJwtFromCookie(final HttpServletRequest request, final String cookieName) {
        String jwt = null;

        final Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookieName.equals(cookie.getName())) {
                    jwt = cookie.getValue();
                    break;
                }
            }
        }

        return jwt;
    }

}
