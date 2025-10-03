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

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Standard implementation of Certificate Attribute Reader using standard X.509 Certificate methods
 */
public class StandardCertificateAttributeReader implements CertificateAttributeReader {
    private static final int NAME_TYPE_INDEX = 0;

    private static final int NAME_INDEX = 1;

    private static final int NAME_TYPE_UNKNOWN = 0;

    /**
     * Get Subject Alternative Names from X.509 certificate when found
     *
     * @param certificate X.509 certificate to be read
     * @return List of Subject Alternative Names or empty when not found
     */
    @Override
    public List<SubjectAlternativeName> getSubjectAlternativeNames(final X509Certificate certificate) {
        Objects.requireNonNull(certificate, "Certificate required");

        final List<SubjectAlternativeName> names = new ArrayList<>();

        final Collection<List<?>> subjectAlternativeNames = readSubjectAlternativeNames(certificate);
        for (final List<?> subjectAlternativeTypeName : subjectAlternativeNames) {
            final Object nameType = subjectAlternativeTypeName.get(NAME_TYPE_INDEX);
            final GeneralNameType generalNameType = getGeneralNameType(nameType);

            final Object name = subjectAlternativeTypeName.get(NAME_INDEX);

            final SubjectAlternativeName subjectAlternativeName;
            if (name instanceof byte[]) {
                subjectAlternativeName = new StandardSubjectAlternativeName(generalNameType, (byte[]) name);
            } else {
                subjectAlternativeName = new StandardSubjectAlternativeName(generalNameType, name.toString());
            }
            names.add(subjectAlternativeName);
        }

        return names;
    }

    private Collection<List<?>> readSubjectAlternativeNames(final X509Certificate certificate) {
        try {
            final Collection<List<?>> names = certificate.getSubjectAlternativeNames();
            return names == null ? Collections.emptyList() : names;
        } catch (final CertificateParsingException e) {
            return Collections.emptyList();
        }
    }

    private GeneralNameType getGeneralNameType(final Object nameType) {
        GeneralNameType generalNameTypeFound = GeneralNameType.OTHER_NAME;

        final int resolvedNameType = getResolvedNameType(nameType);

        for (final GeneralNameType generalNameType : GeneralNameType.values()) {
            if (generalNameType.getNameType() == resolvedNameType) {
                generalNameTypeFound = generalNameType;
                break;
            }
        }

        return generalNameTypeFound;
    }

    private int getResolvedNameType(final Object nameType) {
        final int resolvedNameType;

        if (nameType instanceof Integer) {
            resolvedNameType = (Integer) nameType;
        } else {
            resolvedNameType = NAME_TYPE_UNKNOWN;
        }

        return resolvedNameType;
    }
}
