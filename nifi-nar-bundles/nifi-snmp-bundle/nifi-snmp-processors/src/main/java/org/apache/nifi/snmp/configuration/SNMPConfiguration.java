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

public class SNMPConfiguration {

    private final String agentHost;
    private final String agentPort;
    private final int retries;
    private final int timeout;
    private final int version;
    private final String authProtocol;
    private final String authPassphrase;
    private final String privacyProtocol;
    private final String privacyPassphrase;
    private final String securityName;
    private final String securityLevel;
    private final String communityString;

    SNMPConfiguration(final String agentHost,
                      final String agentPort,
                      final int retries,
                      final int timeout,
                      final int version,
                      final String authProtocol,
                      final String authPassphrase,
                      final String privacyProtocol,
                      final String privacyPassphrase,
                      final String securityName,
                      final String securityLevel,
                      final String communityString) {
        this.agentHost = agentHost;
        this.agentPort = agentPort;
        this.retries = retries;
        this.timeout = timeout;
        this.version = version;
        this.authProtocol = authProtocol;
        this.authPassphrase = authPassphrase;
        this.privacyProtocol = privacyProtocol;
        this.privacyPassphrase = privacyPassphrase;
        this.securityName = securityName;
        this.securityLevel = securityLevel;
        this.communityString = communityString;
    }

    public String getAgentHost() {
        return agentHost;
    }

    public String getAgentPort() {
        return agentPort;
    }

    public int getRetries() {
        return retries;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getVersion() {
        return version;
    }

    public String getAuthProtocol() {
        return authProtocol;
    }

    public String getAuthPassphrase() {
        return authPassphrase;
    }

    public String getPrivacyProtocol() {
        return privacyProtocol;
    }

    public String getPrivacyPassphrase() {
        return privacyPassphrase;
    }

    public String getSecurityName() {
        return securityName;
    }

    public String getSecurityLevel() {
        return securityLevel;
    }

    public String getCommunityString() {
        return communityString;
    }
}
