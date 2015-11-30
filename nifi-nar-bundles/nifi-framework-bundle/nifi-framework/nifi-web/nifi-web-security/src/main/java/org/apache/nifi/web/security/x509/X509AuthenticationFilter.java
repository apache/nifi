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
package org.apache.nifi.web.security.x509;

import java.security.cert.X509Certificate;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.token.NewAccountAuthenticationRequestToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationRequestToken;
import org.apache.nifi.web.security.user.NewAccountRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom X509 filter that will inspect the HTTP headers for a proxied user before extracting the user details from the client certificate.
 */
public class X509AuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(X509AuthenticationFilter.class);

    private X509CertificateExtractor certificateExtractor;
    private X509IdentityProvider certificateIdentityProvider;

    @Override
    public NiFiAuthenticationRequestToken attemptAuthentication(final HttpServletRequest request) {
        // only suppport x509 login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // look for a client certificate
        final X509Certificate[] certificates = certificateExtractor.extractClientCertificate(request);
        if (certificates == null) {
            return null;
        }

        // attempt to authenticate if certificates were found
        final AuthenticationResponse authenticationResponse;
        try {
            authenticationResponse = certificateIdentityProvider.authenticate(certificates);
        } catch (final IllegalArgumentException iae) {
            throw new InvalidAuthenticationException(iae.getMessage(), iae);
        }

        final List<String> proxyChain = ProxiedEntitiesUtils.buildProxiedEntitiesChain(request, authenticationResponse.getIdentity());
        if (isNewAccountRequest(request)) {
            return new NewAccountAuthenticationRequestToken(new NewAccountRequest(proxyChain, getJustification(request)));
        } else {
            return new NiFiAuthenticationRequestToken(proxyChain);
        }
    }

    /* setters */
    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    public void setCertificateIdentityProvider(X509IdentityProvider certificateIdentityProvider) {
        this.certificateIdentityProvider = certificateIdentityProvider;
    }

}
