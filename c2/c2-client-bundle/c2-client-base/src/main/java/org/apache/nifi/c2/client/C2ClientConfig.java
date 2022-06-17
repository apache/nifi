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

/**
 * Configuration for a C2 Client.
 */
public class C2ClientConfig {

    private final String c2Url;
    private final String c2AckUrl;
    private final String agentClass;
    private final String agentIdentifier;
    private final String confDirectory;
    private final String runtimeManifestIdentifier;
    private final String runtimeType;
    private final long heartbeatPeriod;
    private final String keystoreFilename;
    private final String keystorePass;
    private final String keyPass;
    private final String keystoreType;
    private final String truststoreFilename;
    private final String truststorePass;
    private final String truststoreType;
    private final long callTimeout;
    private final long readTimeout;
    private final long connectTimeout;


    private C2ClientConfig(final Builder builder) {
        this.c2Url = builder.c2Url;
        this.c2AckUrl = builder.c2AckUrl;
        this.agentClass = builder.agentClass;
        this.agentIdentifier = builder.agentIdentifier;
        this.confDirectory = builder.confDirectory;
        this.runtimeManifestIdentifier = builder.runtimeManifestIdentifier;
        this.runtimeType = builder.runtimeType;
        this.heartbeatPeriod = builder.heartbeatPeriod;
        this.callTimeout = builder.callTimeout;
        this.keystoreFilename = builder.keystoreFilename;
        this.keystorePass = builder.keystorePass;
        this.keyPass = builder.keyPass;
        this.keystoreType = builder.keystoreType;
        this.truststoreFilename = builder.truststoreFilename;
        this.truststorePass = builder.truststorePass;
        this.truststoreType = builder.truststoreType;
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

    public String getConfDirectory() {
        return confDirectory;
    }

    public String getRuntimeManifestIdentifier() {
        return runtimeManifestIdentifier;
    }

    public String getRuntimeType() {
        return runtimeType;
    }

    public long getHeartbeatPeriod() {
        return heartbeatPeriod;
    }

    public long getCallTimeout() {
        return callTimeout;
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

    public String getKeystoreType() {
        return keystoreType;
    }

    public String getTruststoreFilename() {
        return truststoreFilename;
    }

    public String getTruststorePass() {
        return truststorePass;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public long getReadTimeout() {
        return readTimeout;
    }

    public long getConnectTimeout() {
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
        private String confDirectory;
        private String runtimeManifestIdentifier;
        private String runtimeType;
        private long heartbeatPeriod;
        private long callTimeout;
        private String keystoreFilename;
        private String keystorePass;
        private String keyPass;
        private String keystoreType;
        private String truststoreFilename;
        private String truststorePass;
        private String truststoreType;
        private long readTimeout;
        private long connectTimeout;

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

        public Builder confDirectory(final String confDirectory) {
            this.confDirectory = confDirectory;
            return this;
        }

        public Builder runtimeManifestIdentifier(final String runtimeManifestIdentifier) {
            this.runtimeManifestIdentifier = runtimeManifestIdentifier;
            return this;
        }

        public Builder runtimeType(final String runtimeType) {
            this.runtimeType = runtimeType;
            return this;
        }

        public Builder heartbeatPeriod(final long heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        public Builder callTimeout(final long callTimeout) {
            this.callTimeout = callTimeout;
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

        public Builder keystoreType(final String keystoreType) {
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

        public Builder truststoreType(final String truststoreType) {
            this.truststoreType = truststoreType;
            return this;
        }

        public Builder readTimeout(final long readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder connectTimeout(final long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public C2ClientConfig build() {
            return new C2ClientConfig(this);
        }
    }
}
