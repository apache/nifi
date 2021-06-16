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
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

import javax.servlet.http.HttpServletRequest;

public class JwtAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    // The Authorization header contains authentication credentials
    private static NiFiBearerTokenResolver bearerTokenResolver = new NiFiBearerTokenResolver();

    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request) {
        // Only support JWT login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // Get JWT from Authorization header or cookie value
        final String headerToken = bearerTokenResolver.resolve(request);

        if (StringUtils.isNotBlank(headerToken)) {
            return new JwtAuthenticationRequestToken(headerToken, request.getRemoteAddr());
        } else {
            return null;
        }
    }
}
