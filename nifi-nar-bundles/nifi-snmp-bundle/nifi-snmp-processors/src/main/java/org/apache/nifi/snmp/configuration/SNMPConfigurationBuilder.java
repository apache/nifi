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
package org.apache.nifi.snmp.configuration;

public class SNMPConfigurationBuilder {
    private String agentHost;
    private String agentPort;
    private int retries;
    private int timeout;
    private int version;
    private String authProtocol;
    private String authPassphrase;
    private String privacyProtocol;
    private String privacyPassphrase;
    private String securityName;
    private String securityLevel;
    private String communityString;

    public SNMPConfigurationBuilder setAgentHost(final String agentHost) {
        this.agentHost = agentHost;
        return this;
    }

    public SNMPConfigurationBuilder setAgentPort(final String agentPort) {
        this.agentPort = agentPort;
        return this;
    }

    public SNMPConfigurationBuilder setRetries(final int retries) {
        this.retries = retries;
        return this;
    }

    public SNMPConfigurationBuilder setTimeout(final int timeout) {
        this.timeout = timeout;
        return this;
    }

    public SNMPConfigurationBuilder setVersion(final int version) {
        this.version = version;
        return this;
    }

    public SNMPConfigurationBuilder setAuthProtocol(final String authProtocol) {
        this.authProtocol = authProtocol;
        return this;
    }

    public SNMPConfigurationBuilder setAuthPassphrase(final String authPassphrase) {
        this.authPassphrase = authPassphrase;
        return this;
    }

    public SNMPConfigurationBuilder setPrivacyProtocol(final String privacyProtocol) {
        this.privacyProtocol = privacyProtocol;
        return this;
    }

    public SNMPConfigurationBuilder setPrivacyPassphrase(final String privacyPassphrase) {
        this.privacyPassphrase = privacyPassphrase;
        return this;
    }

    public SNMPConfigurationBuilder setSecurityName(final String securityName) {
        this.securityName = securityName;
        return this;
    }

    public SNMPConfigurationBuilder setSecurityLevel(final String securityLevel) {
        this.securityLevel = securityLevel;
        return this;
    }

    public SNMPConfigurationBuilder setCommunityString(final String communityString) {
        this.communityString = communityString;
        return this;
    }

    public SNMPConfiguration build() {
        final boolean isValid = agentHost != null && agentPort != null;
        if (!isValid) {
            throw new IllegalStateException("Required properties are not set.");
        }
        return new SNMPConfiguration(agentHost, agentPort, retries, timeout, version, authProtocol, authPassphrase, privacyProtocol, privacyPassphrase, securityName, securityLevel, communityString);
    }
}