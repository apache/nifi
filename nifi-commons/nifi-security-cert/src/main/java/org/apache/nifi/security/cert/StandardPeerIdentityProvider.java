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
package org.apache.nifi.security.cert;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Standard implementation of Peer Identity Provider reads the first certificate as an X.509 certificate with Subject Alternative Names
 */
public class StandardPeerIdentityProvider implements PeerIdentityProvider {
    private final CertificateAttributeReader certificateAttributeReader = new StandardCertificateAttributeReader();

    /**
     * Get Peer Identities from DNS Subject Alternative Names found on validated X.509 certificate provided
     *
     * @param peerCertificates Peer certificates
     * @return Set of identities or empty when no DNS Subject Alternative Names found
     * @throws SSLPeerUnverifiedException Thrown on peer certificates not found or not valid
     */
    @Override
    public Set<String> getIdentities(final Certificate[] peerCertificates) throws SSLPeerUnverifiedException {
        if (peerCertificates == null || peerCertificates.length == 0) {
            throw new SSLPeerUnverifiedException("Peer certificates not found");
        }

        final Certificate peerCertificate = peerCertificates[0];
        if (peerCertificate instanceof X509Certificate) {
            final X509Certificate certificate = (X509Certificate) peerCertificate;
            try {
                certificate.checkValidity();
            } catch (final CertificateException e) {
                final String message = String.format("X.509 Peer certificate not valid: %s", e.getMessage());
                throw new SSLPeerUnverifiedException(message);
            }

            return getIdentities(certificate);
        } else {
            throw new SSLPeerUnverifiedException("X.509 Peer certificate not found");
        }
    }

    private Set<String> getIdentities(final X509Certificate certificate) {
        final Set<String> identities = new LinkedHashSet<>();

        final List<SubjectAlternativeName> subjectAlternativeNames = certificateAttributeReader.getSubjectAlternativeNames(certificate);
        for (final SubjectAlternativeName subjectAlternativeName : subjectAlternativeNames) {
            final GeneralNameType generalNameType = subjectAlternativeName.getGeneralNameType();
            if (GeneralNameType.DNS_NAME == generalNameType) {
                final String name = subjectAlternativeName.getName();
                identities.add(name);
            }
        }

        return identities;
    }
}
