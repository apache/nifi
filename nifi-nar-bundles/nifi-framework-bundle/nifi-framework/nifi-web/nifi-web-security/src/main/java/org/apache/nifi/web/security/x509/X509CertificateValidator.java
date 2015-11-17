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
import org.apache.nifi.web.security.x509.ocsp.CertificateStatusException;
import org.apache.nifi.web.security.x509.ocsp.OcspCertificateValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts client certificates from Http requests.
 */
public class X509CertificateValidator {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private OcspCertificateValidator ocspValidator;

    /**
     * Extract the client certificate from the specified HttpServletRequest or null if none is specified.
     *
     * @param certificates the client certificates
     * @throws java.security.cert.CertificateExpiredException cert is expired
     * @throws java.security.cert.CertificateNotYetValidException cert is not yet valid
     * @throws org.apache.nifi.web.security.x509.ocsp.CertificateStatusException ocsp validation issue
     */
    public void validateClientCertificate(final X509Certificate[] certificates)
            throws CertificateExpiredException, CertificateNotYetValidException, CertificateStatusException {

        // ensure the cert is valid
        certificates[0].checkValidity();

        // perform ocsp validator if necessary
        ocspValidator.validate(certificates);
    }

    public void setOcspValidator(OcspCertificateValidator ocspValidator) {
        this.ocspValidator = ocspValidator;
    }

}
