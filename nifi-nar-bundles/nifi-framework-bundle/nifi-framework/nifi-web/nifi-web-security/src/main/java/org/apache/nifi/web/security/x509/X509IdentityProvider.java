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

import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * Identity provider for extract the authenticating a ServletRequest with a X509Certificate.
 */
public class X509IdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(X509IdentityProvider.class);

    private final String issuer = getClass().getSimpleName();

    private X509CertificateValidator certificateValidator;
    private X509PrincipalExtractor principalExtractor;

    /**
     * Authenticates the specified request by checking certificate validity.
     *
     * @param certificates the client certificates
     * @return an authentication response
     * @throws IllegalArgumentException the request did not contain a valid certificate (or no certificate)
     */
    public AuthenticationResponse authenticate(final X509Certificate[] certificates) throws IllegalArgumentException {
        // ensure the cert was found
        if (certificates == null || certificates.length == 0) {
            throw new IllegalArgumentException("The specified request does not contain a client certificate.");
        }

        // extract the principal
        final Object certificatePrincipal = principalExtractor.extractPrincipal(certificates[0]);
        final String principal = certificatePrincipal.toString();

        try {
            certificateValidator.validateClientCertificate(certificates);
        } catch (CertificateExpiredException cee) {
            final String message = String.format("Client certificate for (%s) is expired.", principal);
            logger.info(message, cee);
            if (logger.isDebugEnabled()) {
                logger.debug("", cee);
            }
            throw new IllegalArgumentException(message, cee);
        } catch (CertificateNotYetValidException cnyve) {
            final String message = String.format("Client certificate for (%s) is not yet valid.", principal);
            logger.info(message, cnyve);
            if (logger.isDebugEnabled()) {
                logger.debug("", cnyve);
            }
            throw new IllegalArgumentException(message, cnyve);
        } catch (final Exception e) {
            logger.info(e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("", e);
            }
            throw new IllegalArgumentException(e.getMessage(), e);
        }

        // build the authentication response
        return new AuthenticationResponse(principal, principal, TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS), issuer);
    }

    /* setters */
    public void setCertificateValidator(X509CertificateValidator certificateValidator) {
        this.certificateValidator = certificateValidator;
    }

    public void setPrincipalExtractor(X509PrincipalExtractor principalExtractor) {
        this.principalExtractor = principalExtractor;
    }

}
