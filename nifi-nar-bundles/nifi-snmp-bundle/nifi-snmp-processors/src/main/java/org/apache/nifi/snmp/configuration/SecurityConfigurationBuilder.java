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

public class SecurityConfigurationBuilder {
    private String version;
    private String authProtocol;
    private String authPassword;
    private String privacyProtocol;
    private String privacyPassword;
    private String securityName;
    private String securityLevel;
    private String communityString;

    public SecurityConfigurationBuilder setVersion(String version) {
        this.version = version;
        return this;
    }

    public SecurityConfigurationBuilder setAuthProtocol(String authProtocol) {
        this.authProtocol = authProtocol;
        return this;
    }

    public SecurityConfigurationBuilder setAuthPassword(String authPassword) {
        this.authPassword = authPassword;
        return this;
    }

    public SecurityConfigurationBuilder setPrivacyProtocol(String privacyProtocol) {
        this.privacyProtocol = privacyProtocol;
        return this;
    }

    public SecurityConfigurationBuilder setPrivacyPassword(String privacyPassword) {
        this.privacyPassword = privacyPassword;
        return this;
    }

    public SecurityConfigurationBuilder setSecurityName(String securityName) {
        this.securityName = securityName;
        return this;
    }

    public SecurityConfigurationBuilder setSecurityLevel(String securityLevel) {
        this.securityLevel = securityLevel;
        return this;
    }

    public SecurityConfigurationBuilder setCommunityString(String communityString) {
        this.communityString = communityString;
        return this;
    }

    public SecurityConfiguration createSecurityConfiguration() {
        return new SecurityConfiguration(version, authProtocol, authPassword, privacyProtocol, privacyPassword, securityName, securityLevel, communityString);
    }
}