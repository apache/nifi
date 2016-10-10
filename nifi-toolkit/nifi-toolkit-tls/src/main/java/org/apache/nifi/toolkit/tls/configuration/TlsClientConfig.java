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

import org.apache.nifi.toolkit.tls.service.client.TlsCertificateSigningRequestPerformer;
import org.apache.nifi.util.StringUtils;

import java.security.NoSuchAlgorithmException;

/**
 * Configuration object for CA client
 */
public class TlsClientConfig extends TlsConfig {
    private String trustStore;
    private String trustStorePassword;
    private String trustStoreType = DEFAULT_KEY_STORE_TYPE;

    public TlsClientConfig() {
    }

    public TlsClientConfig(TlsConfig tlsConfig) {
        setToken(tlsConfig.getToken());
        setCaHostname(tlsConfig.getCaHostname());
        setPort(tlsConfig.getPort());
        setKeyStoreType(tlsConfig.getKeyStoreType());
        setTrustStoreType(tlsConfig.getKeyStoreType());
        setKeyPairAlgorithm(tlsConfig.getKeyPairAlgorithm());
        setKeySize(tlsConfig.getKeySize());
        setSigningAlgorithm(tlsConfig.getSigningAlgorithm());
    }


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

    public TlsCertificateSigningRequestPerformer createCertificateSigningRequestPerformer() throws NoSuchAlgorithmException {
        return new TlsCertificateSigningRequestPerformer(this);
    }

    @Override
    public void initDefaults() {
        super.initDefaults();
        if (StringUtils.isEmpty(trustStoreType)) {
            trustStoreType = DEFAULT_KEY_STORE_TYPE;
        }
    }
}
