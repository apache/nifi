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
import org.apache.nifi.snmp.dto.SNMPTreeResponse;
import org.apache.nifi.snmp.exception.RequestTimeoutException;
import org.apache.nifi.snmp.exception.SNMPWalkException;
import org.apache.nifi.snmp.processors.AbstractSNMPProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.TreeEvent;
import org.snmp4j.util.TreeUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.snmp.operations.GetSNMPHandler.EMPTY_SUBTREE_EXCEPTION_MESSAGE;
import static org.apache.nifi.snmp.operations.GetSNMPHandler.LEAF_ELEMENT_EXCEPTION_MESSAGE;
import static org.apache.nifi.snmp.operations.GetSNMPHandler.SNMP_ERROR_EXCEPTION_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetSNMPHandlerTest {

    private static final String OID = "1.3.6.1.4.1.343";

    private Target mockTarget;
    private Snmp mockSnmpManager;

    @BeforeEach
    public void init() {
        mockTarget = mock(Target.class);
        mockSnmpManager = mock(Snmp.class);
    }

    @Test
    void testGetSnmpWithInvalidFlowFile() throws IOException {
        final Map<String, String> invalidFlowFileAttributes = new HashMap<>();
        invalidFlowFileAttributes.put("invalid", "flowfile attribute");

        final ResponseEvent mockResponseEvent = mock(ResponseEvent.class);
        final PDU mockPdu = mock(PDU.class);
        when(mockResponseEvent.getResponse()).thenReturn(mockPdu);
        when(mockSnmpManager.get(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);
        final Optional<SNMPSingleResponse> optionalResponse = getSNMPHandler.get(invalidFlowFileAttributes, mockTarget);

        assertFalse(optionalResponse.isPresent());
    }

    @Test
    void testGetSnmpWithValidFlowFile() throws IOException {
        final Map<String, String> flowFileAttributes = getFlowFileAttributesWithSingleOID();

        final ResponseEvent mockResponseEvent = mock(ResponseEvent.class);
        final PDU mockPdu = mock(PDU.class);
        when(mockResponseEvent.getResponse()).thenReturn(mockPdu);
        when(mockSnmpManager.get(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);
        getSNMPHandler.get(flowFileAttributes, mockTarget);

        ArgumentCaptor<PDU> captor = ArgumentCaptor.forClass(PDU.class);
        Mockito.verify(mockSnmpManager).get(captor.capture(), any(Target.class));

        final PDU pdu = captor.getValue();
        assertEquals(1, pdu.getVariableBindings().size());
        assertEquals(OID, pdu.getVariableBindings().get(0).getOid().toString());
        assertEquals("Null", pdu.getVariableBindings().get(0).getVariable().toString());
    }

    @Test
    void testGetSnmpWhenTimeout() throws IOException {
        final Map<String, String> flowFileAttributes = getFlowFileAttributesWithSingleOID();
        final ResponseEvent mockResponseEvent = mock(ResponseEvent.class);
        when(mockResponseEvent.getResponse()).thenReturn(null);
        when(mockSnmpManager.get(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);

        final RequestTimeoutException requestTimeoutException = assertThrows(
                RequestTimeoutException.class,
                () -> getSNMPHandler.get(flowFileAttributes, mockTarget)
        );

        assertEquals(String.format(AbstractSNMPProcessor.REQUEST_TIMEOUT_EXCEPTION_TEMPLATE, "read"),
                requestTimeoutException.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWalkSnmpWithEmptyFlowFile() {
        final Map<String, String> flowFileAttributes = getFlowFileAttributesWithSingleOID();
        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final TreeEvent mockTreeEvent = mock(TreeEvent.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        final VariableBinding[] variableBindings = new VariableBinding[1];
        variableBindings[0] = new VariableBinding(new OID(OID), new OctetString("OID value"));
        when(mockTreeEvent.getVariableBindings()).thenReturn(variableBindings);
        when(mockSubtree.get(0)).thenReturn(mockTreeEvent);
        when(mockTreeUtils.walk(mockTarget, new OID[] {new OID(OID)})).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);
        getSNMPHandler.setTreeUtils(mockTreeUtils);
        getSNMPHandler.walk(flowFileAttributes, mockTarget);

        ArgumentCaptor<OID[]> captor = ArgumentCaptor.forClass(OID[].class);
        Mockito.verify(mockTreeUtils).walk(any(Target.class), captor.capture());

        assertEquals(OID, captor.getValue()[0].toString());
    }

    @Test
    void testWalkSnmpWithInvalidFlowFile() {
        final Map<String, String> invalidFlowFileAttributes = new HashMap<>();
        invalidFlowFileAttributes.put("invalid", "flowfile attribute");

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);
        final Optional<SNMPTreeResponse> optionalResponse = getSNMPHandler.walk(invalidFlowFileAttributes, mockTarget);

        assertFalse(optionalResponse.isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWalkSnmpWithValidFlowFile() {
        final Map<String, String> flowFileAttributes = getFlowFileAttributesWithSingleOID();

        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final TreeEvent mockTreeEvent = mock(TreeEvent.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        final VariableBinding[] variableBindings = new VariableBinding[1];
        variableBindings[0] = new VariableBinding(new OID(OID), new OctetString("OID value"));
        when(mockTreeEvent.getVariableBindings()).thenReturn(variableBindings);
        when(mockSubtree.get(0)).thenReturn(mockTreeEvent);
        when(mockSubtree.isEmpty()).thenReturn(false);
        when(mockTreeUtils.walk(any(Target.class), any(OID[].class))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);
        getSNMPHandler.setTreeUtils(mockTreeUtils);
        getSNMPHandler.walk(flowFileAttributes, mockTarget);

        ArgumentCaptor<OID[]> captor = ArgumentCaptor.forClass(OID[].class);
        Mockito.verify(mockTreeUtils).walk(any(Target.class), captor.capture());

        assertEquals(OID, captor.getValue()[0].toString());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWalkSnmpWithEmptySubtreeThrowsException() {
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put("snmp$" + OID, "");
        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        when(mockSubtree.isEmpty()).thenReturn(true);
        when(mockTreeUtils.getSubtree(any(Target.class), any(org.snmp4j.smi.OID.class))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);
        getSNMPHandler.setTreeUtils(mockTreeUtils);

        final SNMPWalkException snmpWalkException = assertThrows(
                SNMPWalkException.class,
                () -> getSNMPHandler.walk(flowFileAttributes, mockTarget)
        );

        assertEquals(String.format(EMPTY_SUBTREE_EXCEPTION_MESSAGE, "[" + OID + "]"), snmpWalkException.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWalkSnmpWithSubtreeErrorThrowsException() {
        final Map<String, String> flowFileAttributes = getFlowFileAttributesWithSingleOID();
        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final TreeEvent mockTreeEvent = mock(TreeEvent.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        when(mockSubtree.get(0)).thenReturn(mockTreeEvent);
        when(mockSubtree.isEmpty()).thenReturn(false);
        when(mockSubtree.size()).thenReturn(1);
        when(mockTreeUtils.walk(any(Target.class), any(org.snmp4j.smi.OID[].class))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);
        getSNMPHandler.setTreeUtils(mockTreeUtils);

        final SNMPWalkException snmpWalkException = assertThrows(
                SNMPWalkException.class,
                () -> getSNMPHandler.walk(flowFileAttributes, mockTarget)
        );

        assertEquals(SNMP_ERROR_EXCEPTION_MESSAGE, snmpWalkException.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWalkSnmpWithLeafElementSubtreeThrowsException() {
        final Map<String, String> flowFileAttributes = getFlowFileAttributesWithSingleOID();
        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final TreeEvent mockTreeEvent = mock(TreeEvent.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        final VariableBinding[] variableBindings = new VariableBinding[0];
        when(mockTreeEvent.getVariableBindings()).thenReturn(variableBindings);
        when(mockSubtree.get(0)).thenReturn(mockTreeEvent);
        when(mockSubtree.isEmpty()).thenReturn(false);
        when(mockSubtree.size()).thenReturn(1);
        when(mockTreeUtils.walk(any(Target.class), any(org.snmp4j.smi.OID[].class))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(mockSnmpManager);
        getSNMPHandler.setTreeUtils(mockTreeUtils);

        final SNMPWalkException snmpWalkException = assertThrows(
                SNMPWalkException.class,
                () -> getSNMPHandler.walk(flowFileAttributes, mockTarget)
        );

        assertEquals(String.format(LEAF_ELEMENT_EXCEPTION_MESSAGE, "[" + OID + "]"), snmpWalkException.getMessage());
    }

    private static Map<String, String> getFlowFileAttributesWithSingleOID() {
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put("snmp$" + OID, "OID value");
        return flowFileAttributes;
    }
}
