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

import org.apache.nifi.web.security.x509.ocsp.OcspCertificateValidator;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
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
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * Custom X509 filter that will inspect the HTTP headers for a proxied user before extracting the user details from the client certificate.
 */
public class X509AuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(X509AuthenticationFilter.class);

    private X509PrincipalExtractor principalExtractor;
    private X509CertificateExtractor certificateExtractor;
    private OcspCertificateValidator certificateValidator;

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response) {
        // only suppport x509 login when running securely
        if (!request.isSecure()) {
            return null;
        }
        
        // extract the cert
        X509Certificate certificate = certificateExtractor.extractClientCertificate(request);

        // ensure the cert was found
        if (certificate == null) {
            return null;
        }

        // extract the principal
        Object certificatePrincipal = principalExtractor.extractPrincipal(certificate);
        final String principal = ProxiedEntitiesUtils.formatProxyDn(certificatePrincipal.toString());

        try {
            // ensure the cert is valid
            certificate.checkValidity();
        } catch (CertificateExpiredException cee) {
            final String message = String.format("Client certificate for (%s) is expired.", principal);
            logger.info(message, cee);
            if (logger.isDebugEnabled()) {
                logger.debug("", cee);
            }
            return null;
        } catch (CertificateNotYetValidException cnyve) {
            final String message = String.format("Client certificate for (%s) is not yet valid.", principal);
            logger.info(message, cnyve);
            if (logger.isDebugEnabled()) {
                logger.debug("", cnyve);
            }
            return null;
        }

        // validate the certificate in question
        try {
            certificateValidator.validate(request);
        } catch (final Exception e) {
            logger.info(e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("", e);
            }
            return null;
        }

        final List<String> proxyChain = ProxiedEntitiesUtils.buildProxyChain(request, principal);
        if (isNewAccountRequest(request)) {
            return new NewAccountAuthenticationRequestToken(new NewAccountRequest(proxyChain, getJustification(request)));
        } else {
            return new NiFiAuthenticationRequestToken(proxyChain);
        }
    }

    /* setters */
    public void setCertificateValidator(OcspCertificateValidator certificateValidator) {
        this.certificateValidator = certificateValidator;
    }

    public void setPrincipalExtractor(X509PrincipalExtractor principalExtractor) {
        this.principalExtractor = principalExtractor;
    }

    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

}
