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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetSNMPHandlerTest {

    private static final String OID = "1.3.6.1.4.1.343";

    private Target mockTarget;
    private Snmp mockSnmpManager;
    private SNMPResourceHandler snmpResourceHandler;

    @Before
    public void init() {
        mockTarget = mock(Target.class);
        mockSnmpManager = mock(Snmp.class);
        snmpResourceHandler = new SNMPResourceHandler(mockSnmpManager, mockTarget);
    }

    @Test
    public void testGetSnmpWithEmptyFlowFile() throws IOException {
        final ResponseEvent mockResponseEvent = mock(ResponseEvent.class);
        final PDU mockPdu = mock(PDU.class);
        when(mockResponseEvent.getResponse()).thenReturn(mockPdu);
        when(mockSnmpManager.get(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        getSNMPHandler.get(OID);

        ArgumentCaptor<PDU> captor = ArgumentCaptor.forClass(PDU.class);
        Mockito.verify(mockSnmpManager).get(captor.capture(), any(Target.class));

        final PDU pdu = captor.getValue();
        assertEquals(1, pdu.getVariableBindings().size());
        assertEquals(OID, pdu.getVariableBindings().get(0).getOid().toString());
    }

    @Test
    public void testGetSnmpWithInvalidFlowFile() throws IOException {
        final Map<String, String> invalidFlowFileAttributes = new HashMap<>();
        invalidFlowFileAttributes.put("invalid", "flowfile attribute");

        final ResponseEvent mockResponseEvent = mock(ResponseEvent.class);
        final PDU mockPdu = mock(PDU.class);
        when(mockResponseEvent.getResponse()).thenReturn(mockPdu);
        when(mockSnmpManager.get(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        final Optional<SNMPSingleResponse> optionalResponse = getSNMPHandler.get(invalidFlowFileAttributes);

        assertFalse(optionalResponse.isPresent());
    }

    @Test
    public void testGetSnmpWithValidFlowFile() throws IOException {
        final String flowFileOid = "1.3.6.1.2.1.1.1.0";
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put("snmp$" + flowFileOid, "OID value");

        final ResponseEvent mockResponseEvent = mock(ResponseEvent.class);
        final PDU mockPdu = mock(PDU.class);
        when(mockResponseEvent.getResponse()).thenReturn(mockPdu);
        when(mockSnmpManager.get(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        getSNMPHandler.get(flowFileAttributes);

        ArgumentCaptor<PDU> captor = ArgumentCaptor.forClass(PDU.class);
        Mockito.verify(mockSnmpManager).get(captor.capture(), any(Target.class));

        final PDU pdu = captor.getValue();
        assertEquals(1, pdu.getVariableBindings().size());
        assertEquals(flowFileOid, pdu.getVariableBindings().get(0).getOid().toString());
        assertEquals("Null", pdu.getVariableBindings().get(0).getVariable().toString());
    }

    @Test
    public void testGetSnmpWhenTimeout() throws IOException {
        final ResponseEvent mockResponseEvent = mock(ResponseEvent.class);
        when(mockResponseEvent.getResponse()).thenReturn(null);
        when(mockSnmpManager.get(any(PDU.class), any(Target.class))).thenReturn(mockResponseEvent);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);

        final RequestTimeoutException requestTimeoutException = Assert.assertThrows(
                RequestTimeoutException.class,
                () -> getSNMPHandler.get(OID)
        );

        assertEquals(String.format(SNMPResourceHandler.REQUEST_TIMEOUT_EXCEPTION_TEMPLATE, "read"),
                requestTimeoutException.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWalkSnmpWithEmptyFlowFile() {
        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final TreeEvent mockTreeEvent = mock(TreeEvent.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        final VariableBinding[] variableBindings = new VariableBinding[1];
        variableBindings[0] = new VariableBinding(new OID(OID), new OctetString("OID value"));
        when(mockTreeEvent.getVariableBindings()).thenReturn(variableBindings);
        when(mockSubtree.get(0)).thenReturn(mockTreeEvent);
        when(mockTreeUtils.getSubtree(mockTarget, new OID(OID))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        getSNMPHandler.setTreeUtils(mockTreeUtils);
        getSNMPHandler.walk(OID);

        ArgumentCaptor<OID> captor = ArgumentCaptor.forClass(OID.class);
        Mockito.verify(mockTreeUtils).getSubtree(any(Target.class), captor.capture());

        assertEquals(OID, captor.getValue().toString());
    }

    @Test
    public void testWalkSnmpWithInvalidFlowFile() {
        final Map<String, String> invalidFlowFileAttributes = new HashMap<>();
        invalidFlowFileAttributes.put("invalid", "flowfile attribute");

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        final Optional<SNMPTreeResponse> optionalResponse = getSNMPHandler.walk(invalidFlowFileAttributes);

        assertFalse(optionalResponse.isPresent());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWalkSnmpWithValidFlowFile() {
        final String flowFileOid = "1.3.6.1.2.1.1.1.0";
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put("snmp$" + flowFileOid, "OID value");

        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final TreeEvent mockTreeEvent = mock(TreeEvent.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        final VariableBinding[] variableBindings = new VariableBinding[1];
        variableBindings[0] = new VariableBinding(new OID(OID), new OctetString("OID value"));
        when(mockTreeEvent.getVariableBindings()).thenReturn(variableBindings);
        when(mockSubtree.get(0)).thenReturn(mockTreeEvent);
        when(mockSubtree.isEmpty()).thenReturn(false);
        when(mockTreeUtils.walk(any(Target.class), any(OID[].class))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        getSNMPHandler.setTreeUtils(mockTreeUtils);
        getSNMPHandler.walk(flowFileAttributes);

        ArgumentCaptor<OID[]> captor = ArgumentCaptor.forClass(OID[].class);
        Mockito.verify(mockTreeUtils).walk(any(Target.class), captor.capture());

        assertEquals(flowFileOid, captor.getValue()[0].toString());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWalkSnmpWithEmptySubtreeThrowsException() {
        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        when(mockSubtree.isEmpty()).thenReturn(true);
        when(mockTreeUtils.getSubtree(any(Target.class), any(org.snmp4j.smi.OID.class))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        getSNMPHandler.setTreeUtils(mockTreeUtils);

        final SNMPWalkException snmpWalkException = Assert.assertThrows(
                SNMPWalkException.class,
                () -> getSNMPHandler.walk(OID)
        );

        assertEquals(String.format(EMPTY_SUBTREE_EXCEPTION_MESSAGE, OID), snmpWalkException.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWalkSnmpWithSubtreeErrorThrowsException() {
        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final TreeEvent mockTreeEvent = mock(TreeEvent.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        when(mockSubtree.get(0)).thenReturn(mockTreeEvent);
        when(mockSubtree.isEmpty()).thenReturn(false);
        when(mockSubtree.size()).thenReturn(1);
        when(mockTreeUtils.getSubtree(any(Target.class), any(org.snmp4j.smi.OID.class))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        getSNMPHandler.setTreeUtils(mockTreeUtils);

        final SNMPWalkException snmpWalkException = Assert.assertThrows(
                SNMPWalkException.class,
                () -> getSNMPHandler.walk(OID)
        );

        assertEquals(SNMP_ERROR_EXCEPTION_MESSAGE, snmpWalkException.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWalkSnmpWithLeafElementSubtreeThrowsException() {
        final TreeUtils mockTreeUtils = mock(TreeUtils.class);
        final TreeEvent mockTreeEvent = mock(TreeEvent.class);
        final List<TreeEvent> mockSubtree = (List<TreeEvent>) mock(List.class);

        final VariableBinding[] variableBindings = new VariableBinding[0];
        when(mockTreeEvent.getVariableBindings()).thenReturn(variableBindings);
        when(mockSubtree.get(0)).thenReturn(mockTreeEvent);
        when(mockSubtree.isEmpty()).thenReturn(false);
        when(mockSubtree.size()).thenReturn(1);
        when(mockTreeUtils.getSubtree(any(Target.class), any(org.snmp4j.smi.OID.class))).thenReturn(mockSubtree);

        final GetSNMPHandler getSNMPHandler = new GetSNMPHandler(snmpResourceHandler);
        getSNMPHandler.setTreeUtils(mockTreeUtils);

        final SNMPWalkException snmpWalkException = Assert.assertThrows(
                SNMPWalkException.class,
                () -> getSNMPHandler.walk(OID)
        );

        assertEquals(String.format(LEAF_ELEMENT_EXCEPTION_MESSAGE, OID), snmpWalkException.getMessage());
    }
}
