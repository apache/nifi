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

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.util.Arrays;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Configuration for a C2 Client.
 */
public class C2ClientConfig {

    private final String c2Url;
    private final String c2AckUrl;
    private final String c2RestPathBase;
    private final String c2RestPathHeartbeat;
    private final String c2RestPathAcknowledge;
    private final String agentClass;
    private final String agentIdentifier;
    private final boolean fullHeartbeat;
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
    private final Map<String, String> httpHeaders;
    private final long connectTimeout;
    private final int maxIdleConnections;
    private final long keepAliveDuration;
    private final String c2RequestCompression;
    private final String c2AssetDirectory;
    private final long bootstrapAcknowledgeTimeout;
    private final int c2FlowInfoProcessorBulletinLimit;
    private final boolean c2FlowInfoProcessorStatusEnabled;

    private C2ClientConfig(final Builder builder) {
        this.c2Url = builder.c2Url;
        this.c2AckUrl = builder.c2AckUrl;
        this.c2RestPathBase = builder.c2RestPathBase;
        this.c2RestPathHeartbeat = builder.c2RestPathHeartbeat;
        this.c2RestPathAcknowledge = builder.c2RestPathAcknowledge;
        this.agentClass = builder.agentClass;
        this.agentIdentifier = builder.agentIdentifier;
        this.fullHeartbeat = builder.fullHeartbeat;
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
        this.httpHeaders = builder.httpHeaders;
        this.maxIdleConnections = builder.maxIdleConnections;
        this.keepAliveDuration = builder.keepAliveDuration;
        this.c2RequestCompression = builder.c2RequestCompression;
        this.c2AssetDirectory = builder.c2AssetDirectory;
        this.bootstrapAcknowledgeTimeout = builder.bootstrapAcknowledgeTimeout;
        this.c2FlowInfoProcessorBulletinLimit = builder.c2FlowInfoProcessorBulletinLimit;
        this.c2FlowInfoProcessorStatusEnabled = builder.c2FlowInfoProcessorStatusEnabled;
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

    public String getC2RestPathBase() {
        return c2RestPathBase;
    }

    public String getC2RestPathHeartbeat() {
        return c2RestPathHeartbeat;
    }

    public String getC2RestPathAcknowledge() {
        return c2RestPathAcknowledge;
    }

    public boolean isFullHeartbeat() {
        return fullHeartbeat;
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

    public Map<String, String> getHttpHeaders() {
        return httpHeaders;
    }

    public String getC2RequestCompression() {
        return c2RequestCompression;
    }

    public String getC2AssetDirectory() {
        return c2AssetDirectory;
    }

    public int getMaxIdleConnections() {
        return maxIdleConnections;
    }

    public long getKeepAliveDuration() {
        return keepAliveDuration;
    }

    public long getBootstrapAcknowledgeTimeout() {
        return bootstrapAcknowledgeTimeout;
    }

    public int getC2FlowInfoProcessorBulletinLimit() {
        return c2FlowInfoProcessorBulletinLimit;
    }

    public boolean isC2FlowInfoProcessorStatusEnabled() {
        return c2FlowInfoProcessorStatusEnabled;
    }
    /**
     * Builder for client configuration.
     */
    public static class Builder {

        private static final String HTTP_HEADERS_SEPARATOR = "#";
        private static final String HTTP_HEADER_KEY_VALUE_SEPARATOR = ":";

        private String c2Url;
        private String c2AckUrl;
        private String c2RestPathBase;
        private String c2RestPathHeartbeat;
        private String c2RestPathAcknowledge;
        private String agentClass;
        private String agentIdentifier;
        private boolean fullHeartbeat;
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
        private Map<String, String> httpHeaders;
        private int maxIdleConnections;
        private long keepAliveDuration;
        private String c2RequestCompression;
        private String c2AssetDirectory;
        private long bootstrapAcknowledgeTimeout;
        private int c2FlowInfoProcessorBulletinLimit;
        private boolean c2FlowInfoProcessorStatusEnabled;

        public Builder c2Url(String c2Url) {
            this.c2Url = c2Url;
            return this;
        }

        public Builder c2AckUrl(String c2AckUrl) {
            this.c2AckUrl = c2AckUrl;
            return this;
        }

        public Builder c2RestPathBase(String c2RestPathBase) {
            this.c2RestPathBase = c2RestPathBase;
            return this;
        }

        public Builder c2RestPathHeartbeat(String c2RestPathHeartbeat) {
            this.c2RestPathHeartbeat = c2RestPathHeartbeat;
            return this;
        }

        public Builder c2RestPathAcknowledge(String c2RestPathAcknowledge) {
            this.c2RestPathAcknowledge = c2RestPathAcknowledge;
            return this;
        }

        public Builder agentClass(String agentClass) {
            this.agentClass = agentClass;
            return this;
        }

        public Builder agentIdentifier(String agentIdentifier) {
            this.agentIdentifier = agentIdentifier;
            return this;
        }

        public Builder fullHeartbeat(boolean fullHeartbeat) {
            this.fullHeartbeat = fullHeartbeat;
            return this;
        }

        public Builder confDirectory(String confDirectory) {
            this.confDirectory = confDirectory;
            return this;
        }

        public Builder runtimeManifestIdentifier(String runtimeManifestIdentifier) {
            this.runtimeManifestIdentifier = runtimeManifestIdentifier;
            return this;
        }

        public Builder runtimeType(String runtimeType) {
            this.runtimeType = runtimeType;
            return this;
        }

        public Builder heartbeatPeriod(long heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        public Builder callTimeout(long callTimeout) {
            this.callTimeout = callTimeout;
            return this;
        }

        public Builder keystoreFilename(String keystoreFilename) {
            this.keystoreFilename = keystoreFilename;
            return this;
        }

        public Builder keystorePassword(String keystorePass) {
            this.keystorePass = keystorePass;
            return this;
        }

        public Builder keyPassword(String keyPass) {
            this.keyPass = keyPass;
            return this;
        }

        public Builder keystoreType(String keystoreType) {
            this.keystoreType = keystoreType;
            return this;
        }

        public Builder truststoreFilename(String truststoreFilename) {
            this.truststoreFilename = truststoreFilename;
            return this;
        }

        public Builder truststorePassword(String truststorePass) {
            this.truststorePass = truststorePass;
            return this;
        }

        public Builder truststoreType(String truststoreType) {
            this.truststoreType = truststoreType;
            return this;
        }

        public Builder readTimeout(long readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder connectTimeout(long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder httpHeaders(String httpHeaders) {
            this.httpHeaders = ofNullable(httpHeaders)
                .filter(StringUtils::isNotBlank)
                .map(headers -> headers.split(HTTP_HEADERS_SEPARATOR))
                .stream()
                .flatMap(Arrays::stream)
                .map(String::trim)
                .map(header -> header.split(HTTP_HEADER_KEY_VALUE_SEPARATOR))
                .filter(split -> split.length == 2)
                .collect(toUnmodifiableMap(
                    split -> ofNullable(split[0]).map(String::trim).orElse(EMPTY),
                    split -> ofNullable(split[1]).map(String::trim).orElse(EMPTY)));
            return this;
        }

        public Builder maxIdleConnections(int maxIdleConnections) {
            this.maxIdleConnections = maxIdleConnections;
            return this;
        }

        public Builder keepAliveDuration(long keepAliveDuration) {
            this.keepAliveDuration = keepAliveDuration;
            return this;
        }

        public Builder c2RequestCompression(String c2RequestCompression) {
            this.c2RequestCompression = c2RequestCompression;
            return this;
        }

        public Builder c2AssetDirectory(String c2AssetDirectory) {
            this.c2AssetDirectory = c2AssetDirectory;
            return this;
        }

        public Builder bootstrapAcknowledgeTimeout(long bootstrapAcknowledgeTimeout) {
            this.bootstrapAcknowledgeTimeout = bootstrapAcknowledgeTimeout;
            return this;
        }

        public Builder c2FlowInfoProcessorBulletinLimit(int c2FlowInfoProcessorBulletinLimit) {
            this.c2FlowInfoProcessorBulletinLimit = c2FlowInfoProcessorBulletinLimit;
            return this;
        }

        public Builder c2FlowInfoProcessorStatusEnabled(boolean c2FlowInfoProcessorStatusEnabled) {
            this.c2FlowInfoProcessorStatusEnabled = c2FlowInfoProcessorStatusEnabled;
            return this;
        }

        public C2ClientConfig build() {
            return new C2ClientConfig(this);
        }
    }
}
