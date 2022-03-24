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

public class TlsCertificateAuthorityRequest {
    private byte[] hmac;
    private String csr;

    public TlsCertificateAuthorityRequest() {
    }

    public TlsCertificateAuthorityRequest(byte[] hmac, String csr) {
        this.hmac = hmac;
        this.csr = csr;
    }

    public byte[] getHmac() {
        return hmac;
    }

    public void setHmac(byte[] hmac) {
        this.hmac = hmac;
    }

    public boolean hasHmac() {
        return hmac != null && hmac.length > 0;
    }

    public String getCsr() {
        return csr;
    }

    public void setCsr(String csr) {
        this.csr = csr;
    }

    public boolean hasCsr() {
        return !StringUtils.isEmpty(csr);
    }
}
