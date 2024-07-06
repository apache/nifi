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

/**
 * X.509 Client Certificate FlowFile Attribute Names
 */
public enum CertificateAttribute {
    /** Certificate Subject Distinguished Name */
    HTTP_SUBJECT_DN("http.subject.dn"),

    /** Certificate Issuer Distinguished Name */
    HTTP_ISSUER_DN("http.issuer.dn"),

    /** Certificate Subject Alternative Names */
    HTTP_CERTIFICATE_SANS("http.certificate.sans");

    private final String name;

    CertificateAttribute(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
