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
package org.apache.nifi.processors.standard.http;

import org.apache.nifi.security.cert.CertificateAttributeReader;
import org.apache.nifi.security.cert.PrincipalFormatter;
import org.apache.nifi.security.cert.StandardCertificateAttributeReader;
import org.apache.nifi.security.cert.StandardPrincipalFormatter;
import org.apache.nifi.security.cert.SubjectAlternativeName;

import jakarta.servlet.http.HttpServletRequest;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Certificate Attributes Provider for HandleHttpRequest reads the first X.509 Certificate presented
 */
public class HandleHttpRequestCertificateAttributesProvider implements CertificateAttributesProvider {
    protected static final String REQUEST_CERTIFICATES_ATTRIBUTE_NAME = "jakarta.servlet.request.X509Certificate";

    private static final String SAN_NAME_TYPE_FORMAT = "%s.%d.nameType";

    private static final String SAN_NAME_FORMAT = "%s.%d.name";

    private final CertificateAttributeReader certificateAttributeReader = new StandardCertificateAttributeReader();

    @Override
    public Map<String, String> getCertificateAttributes(final HttpServletRequest request) {
        Objects.requireNonNull(request, "HTTP Servlet Request required");

        final Map<String, String> attributes;

        final Object requestCertificates = request.getAttribute(REQUEST_CERTIFICATES_ATTRIBUTE_NAME);
        if (requestCertificates instanceof X509Certificate[]) {
            final X509Certificate[] certificates = (X509Certificate[]) requestCertificates;
            if (certificates.length == 0) {
                attributes = Collections.emptyMap();
            } else {
                final X509Certificate clientCertificate = certificates[0];
                attributes = getCertificateAttributes(clientCertificate);
            }
        } else {
            attributes = Collections.emptyMap();
        }

        return attributes;
    }

    private Map<String, String> getCertificateAttributes(final X509Certificate certificate) {
        final Map<String, String> attributes = new LinkedHashMap<>();

        final PrincipalFormatter principalFormatter = StandardPrincipalFormatter.getInstance();
        final String subjectPrincipal = principalFormatter.getSubject(certificate);
        final String issuerPrincipal = principalFormatter.getIssuer(certificate);

        attributes.put(CertificateAttribute.HTTP_SUBJECT_DN.getName(), subjectPrincipal);
        attributes.put(CertificateAttribute.HTTP_ISSUER_DN.getName(), issuerPrincipal);

        final Map<String, String> subjectAlternativeNameAttributes = getSubjectAlternativeNameAttributes(certificate);
        attributes.putAll(subjectAlternativeNameAttributes);

        return attributes;
    }

    private Map<String, String> getSubjectAlternativeNameAttributes(final X509Certificate certificate) {
        final Map<String, String> attributes = new LinkedHashMap<>();

        int subjectAlternativeNameIndex = 0;

        final List<SubjectAlternativeName> subjectAlternativeNames = certificateAttributeReader.getSubjectAlternativeNames(certificate);
        for (final SubjectAlternativeName subjectAlternativeName : subjectAlternativeNames) {
            final String nameTypeAttributeKey = String.format(SAN_NAME_TYPE_FORMAT, CertificateAttribute.HTTP_CERTIFICATE_SANS.getName(), subjectAlternativeNameIndex);
            final String generalNameType = subjectAlternativeName.getGeneralNameType().getGeneralName();
            attributes.put(nameTypeAttributeKey, generalNameType);

            final String nameAttributeKey = String.format(SAN_NAME_FORMAT, CertificateAttribute.HTTP_CERTIFICATE_SANS.getName(), subjectAlternativeNameIndex);
            final String serializedName = subjectAlternativeName.getName();
            attributes.put(nameAttributeKey, serializedName);

            subjectAlternativeNameIndex++;
        }

        return attributes;
    }
}
