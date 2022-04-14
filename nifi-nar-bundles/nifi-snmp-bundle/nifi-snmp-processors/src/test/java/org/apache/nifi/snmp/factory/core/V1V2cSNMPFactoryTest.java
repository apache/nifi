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
package org.apache.nifi.snmp.factory.core;

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.util.StringUtils;
import org.junit.Test;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.security.SecurityLevel;

import java.util.regex.Pattern;

import static org.apache.nifi.snmp.helper.configurations.SNMPConfigurationFactory.LOCALHOST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class V1V2cSNMPFactoryTest {

    private static final int RETRIES = 3;

    @Test
    public void testFactoryCreatesV1V2Configuration() {
        final V1V2cSNMPFactory snmpFactory = new V1V2cSNMPFactory();
        final int managerPort = NetworkUtils.getAvailableUdpPort();
        final String targetPort = String.valueOf(NetworkUtils.getAvailableUdpPort());
        final SNMPConfiguration snmpConfiguration = getSnmpConfiguration(managerPort, targetPort);

        final Target target = snmpFactory.createTargetInstance(snmpConfiguration);

        assertThat(target, instanceOf(CommunityTarget.class));
        assertEquals(LOCALHOST + "/" + targetPort, target.getAddress().toString());
        assertEquals(RETRIES, target.getRetries());
        assertEquals(1, target.getSecurityLevel());
        assertEquals(StringUtils.EMPTY, target.getSecurityName().toString());
    }

    @Test
    public void testFactoryCreatesSnmpManager() {
        final V1V2cSNMPFactory snmpFactory = new V1V2cSNMPFactory();
        final int managerPort = NetworkUtils.getAvailableUdpPort();
        final String targetPort = String.valueOf(NetworkUtils.getAvailableUdpPort());
        final SNMPConfiguration snmpConfiguration = getSnmpConfiguration(managerPort, targetPort);

        final Snmp snmpManager = snmpFactory.createSnmpManagerInstance(snmpConfiguration);

        final String address = snmpManager.getMessageDispatcher().getTransportMappings().iterator().next().getListenAddress().toString();
        assertTrue(Pattern.compile("0.+?0/" + managerPort).matcher(address).matches());
    }

    @Test
    public void testFactoryCreatesResourceHandler() {
        final V1V2cSNMPFactory snmpFactory = spy(V1V2cSNMPFactory.class);
        final int managerPort = NetworkUtils.getAvailableUdpPort();
        final String targetPort = String.valueOf(NetworkUtils.getAvailableUdpPort());
        final SNMPConfiguration snmpConfiguration = getSnmpConfiguration(managerPort, targetPort);

        snmpFactory.createSNMPResourceHandler(snmpConfiguration);

        verify(snmpFactory).createTargetInstance(snmpConfiguration);
        verify(snmpFactory).createSnmpManagerInstance(snmpConfiguration);
    }

    private SNMPConfiguration getSnmpConfiguration(int managerPort, String targetPort) {
        return new SNMPConfiguration.Builder()
                .setRetries(RETRIES)
                .setManagerPort(managerPort)
                .setTargetHost(LOCALHOST)
                .setTargetPort(targetPort)
                .setSecurityLevel(SecurityLevel.noAuthNoPriv.name())
                .build();
    }
}
