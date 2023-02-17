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
package org.apache.nifi.web.security.kerberos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationDetailsSource;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.codec.Base64;
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider;
import org.springframework.security.kerberos.authentication.KerberosServiceRequestToken;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class KerberosService {

    private static final Logger logger = LoggerFactory.getLogger(KerberosService.class);

    public static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    public static final String AUTHENTICATION_CHALLENGE_HEADER_NAME = "WWW-Authenticate";
    public static final String AUTHORIZATION_NEGOTIATE = "Negotiate";

    private KerberosServiceAuthenticationProvider kerberosServiceAuthenticationProvider;
    private AuthenticationDetailsSource<HttpServletRequest, ?> authenticationDetailsSource = new WebAuthenticationDetailsSource();

    public void setKerberosServiceAuthenticationProvider(KerberosServiceAuthenticationProvider kerberosServiceAuthenticationProvider) {
        this.kerberosServiceAuthenticationProvider = kerberosServiceAuthenticationProvider;
    }

    public Authentication validateKerberosTicket(HttpServletRequest request) {
        // Only support Kerberos login when running securely
        if (!request.isSecure()) {
            return null;
        }

        String header = request.getHeader(AUTHORIZATION_HEADER_NAME);

        if (isValidKerberosHeader(header)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Received Negotiate Header for request " + request.getRequestURL() + ": " + header);
            }
            byte[] base64Token = header.substring(header.indexOf(" ") + 1).getBytes(StandardCharsets.UTF_8);
            byte[] kerberosTicket = Base64.decode(base64Token);
            KerberosServiceRequestToken authenticationRequest = new KerberosServiceRequestToken(kerberosTicket);
            authenticationRequest.setDetails(authenticationDetailsSource.buildDetails(request));

            return kerberosServiceAuthenticationProvider.authenticate(authenticationRequest);
        } else {
            return null;
        }
    }

    public boolean isValidKerberosHeader(String header) {
        return header != null && (header.startsWith("Negotiate ") || header.startsWith("Kerberos "));
    }
}
