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
package org.apache.nifi.snmp.helper.configurations;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.snmp4j.mp.SnmpConstants;

public class SNMPV3ConfigurationFactory implements SNMPConfigurationFactory {

    // V3 security (users are set in test agents)
    public static final String SECURITY_LEVEL = "authPriv";
    public static final String SECURITY_NAME = "SHAAES128";
    public static final String AUTH_PROTOCOL = "HMAC384SHA512";
    public static final String AUTH_PASSPHRASE = "SHAAES128AuthPassphrase";
    public static final String PRIV_PROTOCOL = "AES128";
    public static final String PRIV_PASSPHRASE = "SHAAES128PrivPassphrase";

    @Override
    public SNMPConfiguration createSnmpGetSetConfiguration(final int agentPort) {
        return SNMPConfiguration.builder()
                .setTargetHost(LOCALHOST)
                .setTargetPort(String.valueOf(agentPort))
                .setCommunityString(COMMUNITY_STRING)
                .setVersion(SnmpConstants.version3)
                .setSecurityLevel(SECURITY_LEVEL)
                .setSecurityName(SECURITY_NAME)
                .setAuthProtocol(AUTH_PROTOCOL)
                .setAuthPassphrase(AUTH_PASSPHRASE)
                .setPrivacyProtocol(PRIV_PROTOCOL)
                .setPrivacyPassphrase(PRIV_PASSPHRASE)
                .build();
    }

    @Override
    public SNMPConfiguration createSnmpGetSetConfigWithCustomHost(final String host, final int agentPort) {
        return SNMPConfiguration.builder()
                .setTargetHost(host)
                .setTargetPort(String.valueOf(agentPort))
                .setCommunityString(COMMUNITY_STRING)
                .setVersion(SnmpConstants.version3)
                .setSecurityLevel(SECURITY_LEVEL)
                .setSecurityName(SECURITY_NAME)
                .setAuthProtocol(AUTH_PROTOCOL)
                .setAuthPassphrase(AUTH_PASSPHRASE)
                .setPrivacyProtocol(PRIV_PROTOCOL)
                .setPrivacyPassphrase(PRIV_PASSPHRASE)
                .build();
    }

    @Override
    public SNMPConfiguration createSnmpListenTrapConfig(final int managerPort) {
        return SNMPConfiguration.builder()
                .setManagerPort(managerPort)
                .setVersion(SnmpConstants.version3)
                .build();
    }
}
