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

import javax.security.auth.x500.X500Principal;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Objects;

/**
 * Standard Principal Formatter implementation returns Subject and Issuer formatted according to RFC 1779 following the convention of getSubjectDN and getIssuerDN methods
 */
public class StandardPrincipalFormatter implements PrincipalFormatter {
    private static final PrincipalFormatter INSTANCE = new StandardPrincipalFormatter();

    /** Map of Object Identifiers to Names not included in the standard set from X500Principal.RFC1779 */
    private static final Map<String, String> OBJECT_IDENTIFIER_NAMES = Map.of(
            "0.9.2342.19200300.100.1.1", "UID",
            "0.9.2342.19200300.100.1.25", "DC"
    );

    private StandardPrincipalFormatter() {

    }

    /**
     * Get singleton instance of Principal Formatter
     *
     * @return Standard Principal Formatter
     */
    public static PrincipalFormatter getInstance() {
        return INSTANCE;
    }

    /**
     * Get Subject Distinguished Name formatted as a string according to RFC 1779 with spaces between elements
     *
     * @param certificate X.509 Certificate
     * @return Subject Distinguished Name formatted according to RFC 1779
     */
    @Override
    public String getSubject(final X509Certificate certificate) {
        Objects.requireNonNull(certificate, "Certificate required");
        return getFormatted(certificate.getSubjectX500Principal());
    }

    /**
     * Get Issuer Distinguished Name formatted as a string according to RFC 1779 with spaces between elements
     *
     * @param certificate X.509 Certificate
     * @return Issuer Distinguished Name formatted according to RFC 1779
     */
    @Override
    public String getIssuer(final X509Certificate certificate) {
        Objects.requireNonNull(certificate, "Certificate required");
        return getFormatted(certificate.getIssuerX500Principal());
    }

    private String getFormatted(final X500Principal principal) {
        return principal.getName(X500Principal.RFC1779, OBJECT_IDENTIFIER_NAMES);
    }
}
