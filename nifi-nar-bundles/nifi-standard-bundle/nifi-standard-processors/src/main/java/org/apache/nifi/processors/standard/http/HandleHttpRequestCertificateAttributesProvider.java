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

import javax.servlet.http.HttpServletRequest;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Certificate Attributes Provider for HandleHttpRequest reads the first X.509 Certificate presented
 */
public class HandleHttpRequestCertificateAttributesProvider implements CertificateAttributesProvider {
    protected static final String REQUEST_CERTIFICATES_ATTRIBUTE_NAME = "javax.servlet.request.X509Certificate";

    private static final String SAN_NAME_TYPE_FORMAT = "%s.%d.nameType";

    private static final String SAN_NAME_FORMAT = "%s.%d.name";

    private static final Map<String, String> GENERAL_NAME_TYPES = new LinkedHashMap<>();

    static {
        // General Name types defined in RFC 3280 Section 4.2.1.7 */
        GENERAL_NAME_TYPES.put("0", "otherName");
        GENERAL_NAME_TYPES.put("1", "rfc822Name");
        GENERAL_NAME_TYPES.put("2", "dNSName");
        GENERAL_NAME_TYPES.put("3", "x400Address");
        GENERAL_NAME_TYPES.put("4", "directoryName");
        GENERAL_NAME_TYPES.put("5", "ediPartyName");
        GENERAL_NAME_TYPES.put("6", "uniformResourceIdentifier");
        GENERAL_NAME_TYPES.put("7", "iPAddress");
        GENERAL_NAME_TYPES.put("8", "registeredID");
    }

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

        final String subjectPrincipal = certificate.getSubjectX500Principal().getName();
        final String issuerPrincipal = certificate.getIssuerX500Principal().getName();

        attributes.put(CertificateAttribute.HTTP_SUBJECT_DN.getName(), subjectPrincipal);
        attributes.put(CertificateAttribute.HTTP_ISSUER_DN.getName(), issuerPrincipal);

        try {
            final Collection<List<?>> subjectAlternativeNames = certificate.getSubjectAlternativeNames();

            if (subjectAlternativeNames != null) {
                final Map<String, String> subjectAlternativeNameAttributes = getSubjectAlternativeNameAttributes(subjectAlternativeNames);
                attributes.putAll(subjectAlternativeNameAttributes);
            }
        } catch (final CertificateParsingException e) {
            attributes.put(CertificateAttribute.HTTP_CERTIFICATE_PARSING_EXCEPTION.getName(), e.getMessage());
        }

        return attributes;
    }

    private Map<String, String> getSubjectAlternativeNameAttributes(final Collection<List<?>> subjectAlternativeNames) {
        final Map<String, String> attributes = new LinkedHashMap<>();

        int subjectAlternativeNameIndex = 0;
        for (final List<?> subjectAlternativeTypeName : subjectAlternativeNames) {
            final String nameTypeAttributeKey = String.format(SAN_NAME_TYPE_FORMAT, CertificateAttribute.HTTP_CERTIFICATE_SANS.getName(), subjectAlternativeNameIndex);
            final String nameType = subjectAlternativeTypeName.get(0).toString();
            final String generalNameType = GENERAL_NAME_TYPES.getOrDefault(nameType, nameType);
            attributes.put(nameTypeAttributeKey, generalNameType);

            final String nameAttributeKey = String.format(SAN_NAME_FORMAT, CertificateAttribute.HTTP_CERTIFICATE_SANS.getName(), subjectAlternativeNameIndex);
            final Object name = subjectAlternativeTypeName.get(1);
            final String serializedName = getSerializedName(name);
            attributes.put(nameAttributeKey, serializedName);

            subjectAlternativeNameIndex++;
        }

        return attributes;
    }

    private String getSerializedName(final Object name) {
        final String serializedName;
        if (name instanceof byte[]) {
            final byte[] encodedName = (byte[]) name;
            serializedName = Base64.getEncoder().encodeToString(encodedName);
        } else {
            serializedName = name.toString();
        }
        return serializedName;
    }
}
