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
package org.apache.nifi.web.security.x509.ocsp;

import java.security.cert.X509Certificate;
import java.util.Objects;

/**
 * A request to verify/validate the specified subject certificate via OCSP.
 */
public class OcspRequest {

    private final X509Certificate subjectCertificate;
    private final X509Certificate issuerCertificate;

    public OcspRequest(X509Certificate subjectCertificate, X509Certificate issuerCertificate) {
        this.subjectCertificate = subjectCertificate;
        this.issuerCertificate = issuerCertificate;
    }

    public X509Certificate getSubjectCertificate() {
        return subjectCertificate;
    }

    public X509Certificate getIssuerCertificate() {
        return issuerCertificate;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + Objects.hashCode(this.subjectCertificate);
        hash = 67 * hash + Objects.hashCode(this.issuerCertificate);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final OcspRequest other = (OcspRequest) obj;
        if (!Objects.equals(this.subjectCertificate, other.subjectCertificate)) {
            return false;
        }
        if (!Objects.equals(this.issuerCertificate, other.issuerCertificate)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return new StringBuilder("NiFi OCSP Request: ")
                .append("Subject DN: ").append(subjectCertificate != null ? subjectCertificate.getSubjectDN().getName() : "<null>")
                .append(" issued by ")
                .append("Issuer DN: ").append(issuerCertificate != null ? issuerCertificate.getSubjectDN().getName() : "<null>").toString();

    }

}
