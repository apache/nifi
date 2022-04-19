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
package org.apache.nifi.c2.client;

import org.apache.nifi.security.util.KeystoreType;

import javax.net.ssl.HostnameVerifier;

/**
 * Configuration for a C2 Client.
 */
public class C2ClientConfig {

    private final String c2Url;
    private final String c2AckUrl;
    private final String agentClass;
    private final String agentIdentifier;
    private final String keystoreFilename;
    private final String keystorePass;
    private final String keyPass;
    private final KeystoreType keystoreType;
    private final String truststoreFilename;
    private final String truststorePass;
    private final KeystoreType truststoreType;
    private final HostnameVerifier hostnameVerifier;
    private final Integer readTimeout;
    private final Integer connectTimeout;


    private C2ClientConfig(final Builder builder) {
        this.c2Url = builder.c2Url;
        this.c2AckUrl = builder.c2AckUrl;
        this.agentClass = builder.agentClass;
        this.agentIdentifier = builder.agentIdentifier;
        this.keystoreFilename = builder.keystoreFilename;
        this.keystorePass = builder.keystorePass;
        this.keyPass = builder.keyPass;
        this.keystoreType = builder.keystoreType;
        this.truststoreFilename = builder.truststoreFilename;
        this.truststorePass = builder.truststorePass;
        this.truststoreType = builder.truststoreType;
        this.hostnameVerifier = builder.hostnameVerifier;
        this.readTimeout = builder.readTimeout;
        this.connectTimeout = builder.connectTimeout;
    }

    public String getC2Url() {
        return c2Url;
    }

    public String getC2AckUrl() {
        return c2AckUrl;
    }

    public String getAgentClass() {
        return agentClass;
    }

    public String getAgentIdentifier() {
        return agentIdentifier;
    }

    public String getKeystoreFilename() {
        return keystoreFilename;
    }

    public String getKeystorePass() {
        return keystorePass;
    }

    public String getKeyPass() {
        return keyPass;
    }

    public KeystoreType getKeystoreType() {
        return keystoreType;
    }

    public String getTruststoreFilename() {
        return truststoreFilename;
    }

    public String getTruststorePass() {
        return truststorePass;
    }

    public KeystoreType getTruststoreType() {
        return truststoreType;
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Builder for client configuration.
     */
    public static class Builder {

        private String c2Url;
        private String c2AckUrl;
        private String agentClass;
        private String agentIdentifier;
        private String keystoreFilename;
        private String keystorePass;
        private String keyPass;
        private KeystoreType keystoreType;
        private String truststoreFilename;
        private String truststorePass;
        private KeystoreType truststoreType;
        private HostnameVerifier hostnameVerifier;
        private Integer readTimeout;
        private Integer connectTimeout;

        public Builder c2Url(final String c2Url) {
            this.c2Url = c2Url;
            return this;
        }

        public Builder c2AckUrl(final String c2AckUrl) {
            this.c2AckUrl = c2AckUrl;
            return this;
        }

        public Builder agentClass(final String agentClass) {
            this.agentClass = agentClass;
            return this;
        }

        public Builder agentIdentifier(final String agentIdentifier) {
            this.agentIdentifier = agentIdentifier;
            return this;
        }

        public Builder keystoreFilename(final String keystoreFilename) {
            this.keystoreFilename = keystoreFilename;
            return this;
        }

        public Builder keystorePassword(final String keystorePass) {
            this.keystorePass = keystorePass;
            return this;
        }

        public Builder keyPassword(final String keyPass) {
            this.keyPass = keyPass;
            return this;
        }

        public Builder keystoreType(final KeystoreType keystoreType) {
            this.keystoreType = keystoreType;
            return this;
        }

        public Builder truststoreFilename(final String truststoreFilename) {
            this.truststoreFilename = truststoreFilename;
            return this;
        }

        public Builder truststorePassword(final String truststorePass) {
            this.truststorePass = truststorePass;
            return this;
        }

        public Builder truststoreType(final KeystoreType truststoreType) {
            this.truststoreType = truststoreType;
            return this;
        }

        public Builder hostnameVerifier(final HostnameVerifier hostnameVerifier) {
            this.hostnameVerifier = hostnameVerifier;
            return this;
        }

        public Builder readTimeout(final Integer readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder connectTimeout(final Integer connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public C2ClientConfig build() {
            return new C2ClientConfig(this);
        }
    }
}
