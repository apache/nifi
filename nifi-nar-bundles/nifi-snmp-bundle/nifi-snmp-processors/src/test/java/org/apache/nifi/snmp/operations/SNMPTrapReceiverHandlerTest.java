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
package org.apache.nifi.snmp.operations;

import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.utils.JsonFileUsmReader;
import org.apache.nifi.util.MockComponentLog;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.snmp4j.Snmp;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SNMPTrapReceiverHandlerTest {

    public static final String USERS_JSON = "src/test/resources/users.json";

    @Test
    public void testTrapReceiverCreatesCommandResponder() {
        final SNMPConfiguration snmpConfiguration = mock(SNMPConfiguration.class);
        final ProcessSessionFactory mockProcessSessionFactory = mock(ProcessSessionFactory.class);
        final MockComponentLog mockComponentLog = new MockComponentLog("componentId", new Object());
        final Snmp mockSnmpManager = mock(Snmp.class);
        when(snmpConfiguration.getManagerPort()).thenReturn(NetworkUtils.getAvailableUdpPort());
        when(snmpConfiguration.getVersion()).thenReturn(SnmpConstants.version1);

        final SNMPTrapReceiverHandler trapReceiverHandler = new SNMPTrapReceiverHandler(snmpConfiguration, null);
        trapReceiverHandler.setSnmpManager(mockSnmpManager);
        trapReceiverHandler.createTrapReceiver(mockProcessSessionFactory, mockComponentLog);


        verify(mockSnmpManager).addCommandResponder(any(SNMPTrapReceiver.class));

        assertTrue(trapReceiverHandler.isStarted());
    }

    @Test
    public void testCloseTrapReceiverCleansUpResources() throws IOException {
        final SNMPConfiguration snmpConfiguration = mock(SNMPConfiguration.class);
        final ProcessSessionFactory mockProcessSessionFactory = mock(ProcessSessionFactory.class);
        final MockComponentLog mockComponentLog = new MockComponentLog("componentId", new Object());
        final USM mockUsm = mock(USM.class);
        final Snmp mockSnmpManager = mock(Snmp.class);

        when(mockSnmpManager.getUSM()).thenReturn(mockUsm);
        when(snmpConfiguration.getManagerPort()).thenReturn(NetworkUtils.getAvailableUdpPort());
        when(snmpConfiguration.getVersion()).thenReturn(SnmpConstants.version1);

        final SNMPTrapReceiverHandler trapReceiverHandler = new SNMPTrapReceiverHandler(snmpConfiguration, null);
        trapReceiverHandler.setSnmpManager(mockSnmpManager);
        trapReceiverHandler.createTrapReceiver(mockProcessSessionFactory, mockComponentLog);
        trapReceiverHandler.close();

        verify(mockUsm).removeAllUsers();
        verify(mockSnmpManager).close();

        assertFalse(trapReceiverHandler.isStarted());
    }

    @Test
    public void testAddUsmUsers() {
        final List<UsmUser> usmUsers = new JsonFileUsmReader(USERS_JSON).readUsm();

        final SNMPConfiguration snmpConfiguration = SNMPConfiguration.builder()
                .setManagerPort(NetworkUtils.getAvailableUdpPort())
                .setVersion(SnmpConstants.version3)
                .build();

        final Snmp mockSnmpManager = mock(Snmp.class, RETURNS_DEEP_STUBS);
        final ArgumentCaptor<UsmUser> usmUserCaptor = ArgumentCaptor.forClass(UsmUser.class);

        final SNMPTrapReceiverHandler trapReceiverHandler = new SNMPTrapReceiverHandler(snmpConfiguration, usmUsers);
        trapReceiverHandler.setSnmpManager(mockSnmpManager);
        trapReceiverHandler.createTrapReceiver(null, null);

        verify(mockSnmpManager.getUSM(), times(2)).addUser(usmUserCaptor.capture());
        verify(mockSnmpManager).addCommandResponder(any(SNMPTrapReceiver.class));

        assertTrue(trapReceiverHandler.isStarted());
        assertEquals(usmUsers, usmUserCaptor.getAllValues());
    }
}
