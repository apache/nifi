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

import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.token.NewAccountAuthenticationRequestToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationRequestToken;
import org.apache.nifi.web.security.user.NewAccountRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

/**
 */
public class JwtAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    private JwtService jwtService;

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response) {
        // only suppport jwt login when running securely
        if (!request.isSecure()) {
            return null;
        }

        final String principal = jwtService.getAuthentication(request);
        if (principal == null) {
            return null;
        }

        final List<String> proxyChain = ProxiedEntitiesUtils.buildProxyChain(request, principal);
        if (isNewAccountRequest(request)) {
            return new NewAccountAuthenticationRequestToken(new NewAccountRequest(proxyChain, getJustification(request)));
        } else {
            return new NiFiAuthenticationRequestToken(proxyChain);
        }
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

}
