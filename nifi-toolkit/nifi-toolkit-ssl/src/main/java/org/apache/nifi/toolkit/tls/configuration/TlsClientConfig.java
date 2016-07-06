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

package org.apache.nifi.toolkit.tls.configuration;

import org.apache.nifi.toolkit.tls.service.TlsCertificateSigningRequestPerformer;

import java.security.NoSuchAlgorithmException;

public class TlsClientConfig extends TlsConfig {
    public static final String NIFI_TOOLKIT_TLS_CLIENT_CA_HOSTNAME = "nifi.toolkit.tls.client.caHostname";
    public static final String NIFI_TOOLKIT_TLS_CLIENT_TRUST_STORE = "nifi.toolkit.tls.client.trustStore";
    public static final String NIFI_TOOLKIT_TLS_CLIENT_TRUST_STORE_PASSWORD = "nifi.toolkit.tls.client.trustStorePassword";
    public static final String NIFI_TOOLKIT_TLS_CLIENT_TRUST_STORE_TYPE = "nifi.toolkit.tls.client.trustStoreType";
    private String caHostname;
    private String trustStore;
    private String trustStorePassword;
    private String trustStoreType = DEFAULT_KEY_STORE_TYPE;

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public String getTrustStore() {
        return trustStore;
    }

    public void setTrustStore(String trustStore) {
        this.trustStore = trustStore;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public String getCaHostname() {
        return caHostname;
    }

    public void setCaHostname(String caHostname) {
        this.caHostname = caHostname;
    }

    public TlsCertificateSigningRequestPerformer createCertificateSigningRequestPerformer() throws NoSuchAlgorithmException {
        return new TlsCertificateSigningRequestPerformer(this);
    }
}
