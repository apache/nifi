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
package org.apache.nifi.vault.hashicorp.config;

public class HashiCorpVaultSslProperties {
    private final String keyStore;
    private final String keyStoreType;
    private final String keyStorePassword;
    private final String trustStore;
    private final String trustStoreType;
    private final String trustStorePassword;
    private final String enabledCipherSuites;
    private final String enabledProtocols;

    public HashiCorpVaultSslProperties(String keyStore, String keyStoreType, String keyStorePassword, String trustStore,
                                       String trustStoreType, String trustStorePassword,
                                       String enabledCipherSuites, String enabledProtocols) {
        this.keyStore = keyStore;
        this.keyStoreType = keyStoreType;
        this.keyStorePassword = keyStorePassword;
        this.trustStore = trustStore;
        this.trustStoreType = trustStoreType;
        this.trustStorePassword = trustStorePassword;
        this.enabledCipherSuites = enabledCipherSuites;
        this.enabledProtocols = enabledProtocols;
    }

    @HashiCorpVaultProperty
    public String getKeyStore() {
        return keyStore;
    }

    @HashiCorpVaultProperty
    public String getKeyStoreType() {
        return keyStoreType;
    }

    @HashiCorpVaultProperty
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    @HashiCorpVaultProperty
    public String getTrustStore() {
        return trustStore;
    }

    @HashiCorpVaultProperty
    public String getTrustStoreType() {
        return trustStoreType;
    }

    @HashiCorpVaultProperty
    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    @HashiCorpVaultProperty
    public String getEnabledCipherSuites() {
        return enabledCipherSuites;
    }

    @HashiCorpVaultProperty
    public String getEnabledProtocols() {
        return enabledProtocols;
    }
}
