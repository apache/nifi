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

import org.junit.jupiter.api.Test;
import org.snmp4j.mp.SnmpConstants;

import static org.apache.nifi.snmp.helper.configurations.SNMPConfigurationFactory.COMMUNITY_STRING;
import static org.apache.nifi.snmp.helper.configurations.SNMPConfigurationFactory.LOCALHOST;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.AUTH_PASSPHRASE;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.AUTH_PROTOCOL;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.PRIV_PASSPHRASE;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.PRIV_PROTOCOL;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.SECURITY_LEVEL;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.SECURITY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SNMPConfigurationTest {

    private static final int MANAGER_PORT = 0;
    private static final String TARGET_PORT = "55556";
    private static final int RETRIES = 3;
    private static final int VERSION = SnmpConstants.version3;
    private static final int TIMEOUT_IN_MS = 3000;

    @Test
    void testMembersAreSetCorrectly() {
        final SNMPConfiguration snmpConfiguration = SNMPConfiguration.builder()
                .setManagerPort(MANAGER_PORT)
                .setTargetHost(LOCALHOST)
                .setTargetPort(TARGET_PORT)
                .setRetries(RETRIES)
                .setTimeoutInMs(TIMEOUT_IN_MS)
                .setVersion(VERSION)
                .setCommunityString(COMMUNITY_STRING)
                .setSecurityLevel(SECURITY_LEVEL)
                .setSecurityName(SECURITY_NAME)
                .setAuthProtocol(AUTH_PROTOCOL)
                .setAuthPassphrase(AUTH_PASSPHRASE)
                .setPrivacyProtocol(PRIV_PROTOCOL)
                .setPrivacyPassphrase(PRIV_PASSPHRASE)
                .build();

        assertEquals(MANAGER_PORT, snmpConfiguration.getManagerPort());
        assertEquals(LOCALHOST, snmpConfiguration.getTargetHost());
        assertEquals(TARGET_PORT, snmpConfiguration.getTargetPort());
        assertEquals(RETRIES, snmpConfiguration.getRetries());
        assertEquals(TIMEOUT_IN_MS, snmpConfiguration.getTimeoutInMs());
        assertEquals(VERSION, snmpConfiguration.getVersion());
        assertEquals(COMMUNITY_STRING, snmpConfiguration.getCommunityString());
        assertEquals(SECURITY_LEVEL, snmpConfiguration.getSecurityLevel());
        assertEquals(SECURITY_NAME, snmpConfiguration.getSecurityName());
        assertEquals(AUTH_PROTOCOL, snmpConfiguration.getAuthProtocol());
        assertEquals(AUTH_PASSPHRASE, snmpConfiguration.getAuthPassphrase());
        assertEquals(PRIV_PROTOCOL, snmpConfiguration.getPrivacyProtocol());
        assertEquals(PRIV_PASSPHRASE, snmpConfiguration.getPrivacyPassphrase());

    }

}
