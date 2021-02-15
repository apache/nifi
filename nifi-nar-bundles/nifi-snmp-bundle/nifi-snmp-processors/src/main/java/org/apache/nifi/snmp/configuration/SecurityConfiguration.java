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

public class SecurityConfiguration {

    private final String version;
    private final String authProtocol;
    private final String authPassword;
    private final String privacyProtocol;
    private final String privacyPassword;
    private final String securityName;
    private final String securityLevel;
    private final String communityString;

    public SecurityConfiguration(final String version,
                                 final String authProtocol,
                                 final String authPassword,
                                 final String privacyProtocol,
                                 final String privacyPassword,
                                 final String securityName,
                                 final String securityLevel,
                                 final String communityString) {
        this.version = version;
        this.authProtocol = authProtocol;
        this.authPassword = authPassword;
        this.privacyProtocol = privacyProtocol;
        this.privacyPassword = privacyPassword;
        this.securityName = securityName;
        this.securityLevel = securityLevel;
        this.communityString = communityString;
    }

    public String getVersion() {
        return version;
    }

    public String getAuthProtocol() {
        return authProtocol;
    }

    public String getAuthPassword() {
        return authPassword;
    }

    public String getPrivacyProtocol() {
        return privacyProtocol;
    }

    public String getPrivacyPassword() {
        return privacyPassword;
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
