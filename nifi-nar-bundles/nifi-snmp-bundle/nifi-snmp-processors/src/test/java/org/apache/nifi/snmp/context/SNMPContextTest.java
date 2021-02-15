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
package org.apache.nifi.snmp.context;

import org.apache.nifi.snmp.configuration.BasicConfiguration;
import org.apache.nifi.snmp.configuration.SecurityConfiguration;
import org.apache.nifi.snmp.configuration.SecurityConfigurationBuilder;
import org.apache.nifi.snmp.helper.SNMPTestUtil;
import org.apache.nifi.snmp.testagents.TestSNMPV3Agent;
import org.junit.Test;
import org.snmp4j.CommunityTarget;
import org.snmp4j.TransportMapping;
import org.snmp4j.UserTarget;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class SNMPContextTest {

    private static final BasicConfiguration basicConfiguration = new BasicConfiguration(SNMPTestUtil.availablePort(), "localhost", SNMPTestUtil.availablePort(), 1, 1000);

    @Test
    public void testSnmpV1CreatesCommunityTarget() {
        SNMPContext snmpContext = SNMPContext.newInstance();
        SecurityConfiguration securityConfiguration = new SecurityConfigurationBuilder()
                .setVersion("SNMPv1")
                .setSecurityLevel("noAuthNoPriv")
                .setSecurityName("userName")
                .setAuthProtocol("SHA")
                .setAuthPassword("authPassword")
                .setPrivacyProtocol("DES")
                .setPrivacyPassword("privacyPassword")
                .setCommunityString("public")
                .createSecurityConfiguration();

        snmpContext.init(basicConfiguration, securityConfiguration);

        assertThat(snmpContext.getTarget(), instanceOf(CommunityTarget.class));

        snmpContext.close();
    }

    @Test
    public void testSnmpV2cCreatesCommunityTarget() {
        SNMPContext snmpContext = SNMPContext.newInstance();
        SecurityConfiguration securityConfiguration = new SecurityConfigurationBuilder()
                .setVersion("SNMPv2c")
                .setSecurityLevel("noAuthNoPriv")
                .setSecurityName("userName")
                .setAuthProtocol("SHA")
                .setAuthPassword("authPassword")
                .setPrivacyProtocol("DES")
                .setPrivacyPassword("privacyPassword")
                .setCommunityString("public")
                .createSecurityConfiguration();

        snmpContext.init(basicConfiguration, securityConfiguration);

        assertThat(snmpContext.getTarget(), instanceOf(CommunityTarget.class));

        snmpContext.close();
    }

    @Test
    public void testSnmpV3CreatesUserTarget() throws IOException {

        TestSNMPV3Agent snmpV3Agent = new TestSNMPV3Agent("0.0.0.0");
        snmpV3Agent.start();

        SNMPContext snmpContext = SNMPContext.newInstance();
        SecurityConfiguration securityConfiguration = new SecurityConfigurationBuilder()
                .setVersion("SNMPv3")
                .setSecurityLevel("authNoPriv")
                .setSecurityName("SHA")
                .setAuthProtocol("SHA")
                .setAuthPassword("authPassword")
                .setPrivacyProtocol("DES")
                .setPrivacyPassword("privacyPassword")
                .setCommunityString("public")
                .createSecurityConfiguration();

        snmpContext.init(basicConfiguration, securityConfiguration);

        assertThat(snmpContext.getTarget(), instanceOf(UserTarget.class));

        snmpV3Agent.stop();

        snmpContext.close();
    }

    @Test
    public void testResourcesClosed() {
        SNMPContext snmpContext = SNMPContext.newInstance();
        SecurityConfiguration securityConfiguration = new SecurityConfigurationBuilder()
                .setVersion("SNMPv2c")
                .setSecurityLevel("noAuthNoPriv")
                .setSecurityName("userName")
                .setAuthProtocol("SHA")
                .setAuthPassword("authPassword")
                .setPrivacyProtocol("DES")
                .setPrivacyPassword("privacyPassword")
                .setCommunityString("public")
                .createSecurityConfiguration();

        snmpContext.init(basicConfiguration, securityConfiguration);
        snmpContext.close();

        final Collection<TransportMapping> transportMappings = snmpContext.getSnmp().getMessageDispatcher().getTransportMappings();

        boolean isAllClosed = transportMappings.stream().noneMatch(TransportMapping::isListening);

        assertTrue(isAllClosed);

        snmpContext.close();
    }
}
