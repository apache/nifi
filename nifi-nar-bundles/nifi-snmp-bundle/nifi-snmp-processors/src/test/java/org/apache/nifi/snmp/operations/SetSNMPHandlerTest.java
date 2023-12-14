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

import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.exception.RequestTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.util.DefaultPDUFactory;
import org.snmp4j.util.PDUFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.snmp.processors.AbstractSNMPProcessor.REQUEST_TIMEOUT_EXCEPTION_TEMPLATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SetSNMPHandlerTest {

    private static final PDUFactory defaultSetPduFactory = new DefaultPDUFactory(PDU.SET);

    private Target mockTarget;
    private Snmp mockSnmpManager;
    private PDU mockPdu;
    private PDU mockResponsePdu;
    private ResponseEvent mockResponseEvent;

    private SetSNMPHandler setSNMPHandler;

    @BeforeEach
    public void init() {
        mockTarget = mock(Target.class);
        mockSnmpManager = mock(Snmp.class);
        PDUFactory mockPduFactory = mock(PDUFactory.class);
        mockPdu = mock(PDU.class);
        mockResponsePdu = mock(PDU.class);
        mockResponseEvent = mock(ResponseEvent.class);

        when(mockPduFactory.createPDU(mockTarget)).thenReturn(mockPdu);

        setSNMPHandler = new SetSNMPHandler(mockSnmpManager);
        SetSNMPHandler.setSetPduFactory(mockPduFactory);
    }

    @AfterEach
    public void tearDown() {
        SetSNMPHandler.setSetPduFactory(defaultSetPduFactory);
    }

    @Test
    void testSetSnmpValidResponse() throws IOException {
        final String flowFileOid = "1.3.6.1.2.1.1.1.0";
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put("snmp$" + flowFileOid, "OID value");

        when(mockResponseEvent.getResponse()).thenReturn(mockResponsePdu);
        when(mockSnmpManager.set(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        setSNMPHandler.set(flowFileAttributes, mockTarget);

        verify(mockSnmpManager).set(mockPdu, mockTarget);
    }

    @Test
    void testSetSnmpTimeoutThrowsException() throws IOException {
        final String flowFileOid = "1.3.6.1.2.1.1.1.0";
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put("snmp$" + flowFileOid, "OID value");

        when(mockSnmpManager.set(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final RequestTimeoutException requestTimeoutException = assertThrows(
                RequestTimeoutException.class,
                () -> setSNMPHandler.set(flowFileAttributes, mockTarget)
        );

        assertEquals(String.format(REQUEST_TIMEOUT_EXCEPTION_TEMPLATE, "write"), requestTimeoutException.getMessage());
    }

    @Test
    void testSetSnmpWithInvalidPduThrowsException() throws IOException {
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put("invalid key", "invalid value");

        when(mockSnmpManager.set(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final Optional<SNMPSingleResponse> optionalResponse = setSNMPHandler.set(flowFileAttributes, mockTarget);

        assertFalse(optionalResponse.isPresent());
    }
}
