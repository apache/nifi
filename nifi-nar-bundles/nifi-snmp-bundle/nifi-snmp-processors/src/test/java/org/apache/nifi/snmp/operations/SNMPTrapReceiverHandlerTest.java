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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.dto.UserDetails;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.apache.nifi.util.MockComponentLog;
import org.junit.Test;
import org.snmp4j.Snmp;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
    public void testAddUsmUsers() throws JsonProcessingException, FileNotFoundException {
        final Set<String> expectedUserAttributes = new HashSet<>();
        try (Scanner scanner = new Scanner(new File(USERS_JSON))) {
            final String content = scanner.useDelimiter("\\Z").next();
            final ObjectMapper mapper = new ObjectMapper();
            final List<UserDetails> userDetails = mapper.readValue(content, new TypeReference<List<UserDetails>>() {
            });
            userDetails
                    .forEach(user -> {
                        expectedUserAttributes.add(user.getSecurityName());
                        expectedUserAttributes.add(SNMPUtils.getAuth(user.getAuthProtocol()).toString());
                        expectedUserAttributes.add(user.getAuthPassphrase());
                        expectedUserAttributes.add(SNMPUtils.getPriv(user.getPrivProtocol()).toString());
                        expectedUserAttributes.add(user.getPrivPassphrase());
                    });
        }
        final Set<String> usmAttributes = new HashSet<>();
        final SNMPConfiguration snmpConfiguration = mock(SNMPConfiguration.class);
        final ProcessSessionFactory mockProcessSessionFactory = mock(ProcessSessionFactory.class);
        final MockComponentLog mockComponentLog = new MockComponentLog("componentId", new Object());
        final Snmp mockSnmpManager = mock(Snmp.class);
        final USM mockUsm = mock(USM.class);

        when(snmpConfiguration.getManagerPort()).thenReturn(NetworkUtils.getAvailableUdpPort());
        when(snmpConfiguration.getVersion()).thenReturn(SnmpConstants.version3);
        doAnswer(invocation -> {
            UsmUser usmUser = (UsmUser) invocation.getArgument(0);
            usmAttributes.add(usmUser.getSecurityName().toString());
            usmAttributes.add(usmUser.getAuthenticationProtocol().toString());
            usmAttributes.add(usmUser.getAuthenticationPassphrase().toString());
            usmAttributes.add(usmUser.getPrivacyProtocol().toString());
            usmAttributes.add(usmUser.getPrivacyPassphrase().toString());
            return null;
        }).when(mockUsm).addUser(any(UsmUser.class));
        when(mockSnmpManager.getUSM()).thenReturn(mockUsm);

        final SNMPTrapReceiverHandler trapReceiverHandler = new SNMPTrapReceiverHandler(snmpConfiguration, USERS_JSON);
        trapReceiverHandler.setSnmpManager(mockSnmpManager);
        trapReceiverHandler.createTrapReceiver(mockProcessSessionFactory, mockComponentLog);


        verify(mockSnmpManager).addCommandResponder(any(SNMPTrapReceiver.class));

        assertTrue(trapReceiverHandler.isStarted());

        assertEquals(expectedUserAttributes, usmAttributes);
    }

}
