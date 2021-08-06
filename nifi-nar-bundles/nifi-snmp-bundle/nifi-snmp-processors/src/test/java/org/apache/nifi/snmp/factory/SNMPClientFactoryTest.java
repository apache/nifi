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
package org.apache.nifi.snmp.factory;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.configuration.SNMPConfigurationBuilder;
import org.junit.Test;
import org.snmp4j.Snmp;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SNMPClientFactoryTest {

    private final SNMPConfigurationBuilder configurationBuilder = new SNMPConfigurationBuilder()
            .setAgentHost("127.0.0.1")
            .setAgentPort("12345")
            .setRetries(1)
            .setTimeout(1000)
            .setSecurityLevel("noAuthNoPriv")
            .setSecurityName("userName")
            .setAuthProtocol("SHA")
            .setAuthPassphrase("authPassword")
            .setPrivacyProtocol("DES")
            .setPrivacyPassphrase("privacyPassword")
            .setCommunityString("public");

    @Test
    public void testSnmpV3ClientWithoutCorrespondingAgentDoesNotHaveUSM() {
        final SNMPConfiguration configuration = configurationBuilder
                .setVersion(SnmpConstants.version3)
                .build();


        final SNMPFactory snmpFactory = new CompositeSNMPFactory();
        final Snmp snmpManager = snmpFactory.createSnmpManagerInstance(configuration);
        final UsmUser user = snmpManager.getUSM().getUserTable().getUser(new OctetString("userName")).getUsmUser();

        final OID usmHMACSHAAuthProtocol = new OID("1.3.6.1.6.3.10.1.1.3");
        final OID usmDESPrivProtocol = new OID("1.3.6.1.6.3.10.1.2.2");

        assertThat("userName", is(equalTo(user.getSecurityName().toString())));
        assertThat(usmHMACSHAAuthProtocol, is(equalTo(user.getAuthenticationProtocol())));
        assertThat("authPassword", is(equalTo(user.getAuthenticationPassphrase().toString())));
        assertThat(usmDESPrivProtocol, is(equalTo(user.getPrivacyProtocol())));
        assertThat("privacyPassword", is(equalTo(user.getPrivacyPassphrase().toString())));
        assertThat(3, is(equalTo(user.getSecurityModel())));
    }
}
