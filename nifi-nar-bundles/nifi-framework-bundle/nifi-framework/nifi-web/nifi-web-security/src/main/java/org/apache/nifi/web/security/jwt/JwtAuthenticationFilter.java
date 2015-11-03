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

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.token.NewAccountAuthenticationRequestToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationRequestToken;
import org.apache.nifi.web.security.user.NewAccountRequest;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 */
public class JwtAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    private X509CertificateExtractor certificateExtractor;
    private X509PrincipalExtractor principalExtractor;
    private JwtService jwtService;

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response) {
        // only suppport jwt login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // get the principal out of the user token
        final String jwtPrincipal = jwtService.getAuthentication(request);
        if (jwtPrincipal == null) {
            return null;
        }

        // look for a certificate
        final X509Certificate certificate = certificateExtractor.extractClientCertificate(request);

        final List<String> chain;
        if (certificate == null) {
            // without a certificate, this is not a proxied request
            chain = Arrays.asList(jwtPrincipal);
        } else {
            // TODO - certificate validation

            // extract the principal
            Object certificatePrincipal = principalExtractor.extractPrincipal(certificate);
            final String principal = ProxiedEntitiesUtils.formatProxyDn(certificatePrincipal.toString());

            // get the proxy chain and verify the principal is found
            chain = ProxiedEntitiesUtils.buildProxyChain(request, principal);

            // ensure the chain contains the jwt principal
            if (!chain.contains(jwtPrincipal)) {
                throw new BadCredentialsException("Principal in user token not found in the proxy chain.");
            }
        }

        if (isNewAccountRequest(request)) {
            return new NewAccountAuthenticationRequestToken(new NewAccountRequest(chain, getJustification(request)));
        } else {
            return new NiFiAuthenticationRequestToken(chain);
        }
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    public void setPrincipalExtractor(X509PrincipalExtractor principalExtractor) {
        this.principalExtractor = principalExtractor;
    }

}
