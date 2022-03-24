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

package org.apache.nifi.toolkit.tls.service.dto;

import org.apache.nifi.util.StringUtils;

public class TlsCertificateAuthorityResponse {
    private byte[] hmac;
    private String pemEncodedCertificate;
    private String error;

    public TlsCertificateAuthorityResponse() {
    }

    public TlsCertificateAuthorityResponse(byte[] hmac, String pemEncodedCertificate) {
        this.hmac = hmac;
        this.pemEncodedCertificate = pemEncodedCertificate;
    }

    public TlsCertificateAuthorityResponse(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public byte[] getHmac() {
        return hmac;
    }

    public void setHmac(byte[] hmac) {
        this.hmac = hmac;
    }

    public String getPemEncodedCertificate() {
        return pemEncodedCertificate;
    }

    public void setPemEncodedCertificate(String pemEncodedCertificate) {
        this.pemEncodedCertificate = pemEncodedCertificate;
    }

    public boolean hasCertificate() {
        return !StringUtils.isEmpty(pemEncodedCertificate);
    }

    public boolean hasHmac() {
        return hmac != null && hmac.length > 0;
    }
}
