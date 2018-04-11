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

package org.apache.nifi.web.util;

import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.nifi.security.util.CertificateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NiFiHostnameVerifier implements HostnameVerifier {
    private static final Logger logger = LoggerFactory.getLogger(NiFiHostnameVerifier.class);

    @Override
    public boolean verify(final String hostname, final SSLSession ssls) {
        try {
            for (final Certificate peerCertificate : ssls.getPeerCertificates()) {
                if (peerCertificate instanceof X509Certificate) {
                    final X509Certificate x509Cert = (X509Certificate) peerCertificate;
                    final String dn = x509Cert.getSubjectDN().getName();
                    final String commonName = CertificateUtils.extractUsername(dn);
                    if (commonName.equals(hostname)) {
                        return true;
                    }

                    final List<String> subjectAltNames = CertificateUtils.getSubjectAlternativeNames(x509Cert);
                    if (subjectAltNames.contains(hostname.toLowerCase())) {
                        return true;
                    }
                }
            }
        } catch (final SSLPeerUnverifiedException | CertificateParsingException ex) {
            logger.warn("Hostname Verification encountered exception verifying hostname due to: " + ex, ex);
        }

        return false;
    }

}
