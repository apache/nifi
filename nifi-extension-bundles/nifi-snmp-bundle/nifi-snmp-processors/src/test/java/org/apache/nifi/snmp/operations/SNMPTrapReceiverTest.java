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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.snmp.processors.ListenTrapSNMP;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.VariableBinding;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SNMPTrapReceiverTest {

    private static final Object MOCK_COMPONENT_ID = new Object();

    private ProcessSessionFactory mockProcessSessionFactory;
    private MockProcessSession mockProcessSession;
    private MockComponentLog mockComponentLog;
    private PDU mockPdu;
    private SNMPTrapReceiver snmpTrapReceiver;


    @BeforeEach
    public void init() {
        mockProcessSessionFactory = mock(ProcessSessionFactory.class);
        mockComponentLog = new MockComponentLog("componentId", MOCK_COMPONENT_ID);
        mockPdu = mock(PDU.class);
        snmpTrapReceiver = new SNMPTrapReceiver(mockProcessSessionFactory, mockComponentLog);
        final ListenTrapSNMP listenTrapSNMP = new ListenTrapSNMP();
        mockProcessSession = new MockProcessSession(new SharedSessionState(listenTrapSNMP, new AtomicLong(0L)), listenTrapSNMP);
    }

    @Test
    void testReceiveTrapWithNullPduLogsError() {
        CommandResponderEvent mockEvent = mock(CommandResponderEvent.class);

        snmpTrapReceiver.processPdu(mockEvent);

        final LogMessage logMessage = mockComponentLog.getErrorMessages().getFirst();

        assertEquals(String.format("%s Request timed out or parameters are incorrect.", MOCK_COMPONENT_ID), logMessage.getMsg());
    }

    @Test
    void testReceiveTrapWithInvalidPduTypeLogsError() {
        final CommandResponderEvent mockEvent = mock(CommandResponderEvent.class);

        when(mockPdu.getType()).thenReturn(PDU.REPORT);
        when(mockEvent.getPDU()).thenReturn(mockPdu);
        snmpTrapReceiver.processPdu(mockEvent);

        final LogMessage logMessage = mockComponentLog.getErrorMessages().getFirst();

        assertEquals(String.format("%s Request timed out or parameters are incorrect.", MOCK_COMPONENT_ID), logMessage.getMsg());
    }

    @Test
    void testTrapReceiverCreatesTrapPduV1FlowFile() {
        final CommandResponderEvent mockEvent = mock(CommandResponderEvent.class);
        final PDUv1 mockV1Pdu = mock(PDUv1.class);

        when(mockV1Pdu.getType()).thenReturn(PDU.V1TRAP);
        when(mockV1Pdu.getEnterprise()).thenReturn(new OID("1.3.6.1.2.1.1.1.0"));
        when(mockV1Pdu.getSpecificTrap()).thenReturn(4);

        final Address mockAddress = mock(Address.class);
        when(mockAddress.toString()).thenReturn("127.0.0.1/62");
        when(mockAddress.isValid()).thenReturn(true);

        final Vector<VariableBinding> vbs = new Vector<>();
        doReturn(vbs).when(mockV1Pdu).getVariableBindings();
        when(mockEvent.getPDU()).thenReturn(mockV1Pdu);
        when(mockEvent.getPeerAddress()).thenReturn(mockAddress);
        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);

        snmpTrapReceiver.processPdu(mockEvent);

        final List<MockFlowFile> flowFiles = mockProcessSession.getFlowFilesForRelationship(ListenTrapSNMP.REL_SUCCESS);
        final FlowFile flowFile = flowFiles.getFirst();

        assertEquals("1.3.6.1.2.1.1.1.0", flowFile.getAttribute("snmp$enterprise"));
        assertEquals(String.valueOf(4), flowFile.getAttribute("snmp$specificTrapType"));
        assertEquals("127.0.0.1/62", flowFile.getAttribute("snmp$peerAddress"));

    }

    @Test
    void testTrapReceiverCreatesTrapPduV2FlowFile() {
        final CommandResponderEvent mockEvent = mock(CommandResponderEvent.class);

        when(mockPdu.getType()).thenReturn(PDU.TRAP);
        when(mockPdu.getErrorIndex()).thenReturn(123);
        when(mockPdu.getErrorStatusText()).thenReturn("test error status text");
        final Vector<VariableBinding> vbs = new Vector<>();

        final Address mockAddress = mock(Address.class);
        when(mockAddress.toString()).thenReturn("127.0.0.1/62");
        when(mockAddress.isValid()).thenReturn(true);

        doReturn(vbs).when(mockPdu).getVariableBindings();
        when(mockEvent.getPDU()).thenReturn(mockPdu);
        when(mockEvent.getPeerAddress()).thenReturn(mockAddress);

        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);

        snmpTrapReceiver.processPdu(mockEvent);

        final List<MockFlowFile> flowFiles = mockProcessSession.getFlowFilesForRelationship(ListenTrapSNMP.REL_SUCCESS);
        final FlowFile flowFile = flowFiles.getFirst();

        assertEquals(String.valueOf(123), flowFile.getAttribute("snmp$errorIndex"));
        assertEquals("test error status text", flowFile.getAttribute("snmp$errorStatusText"));
        assertEquals("127.0.0.1/62", flowFile.getAttribute("snmp$peerAddress"));
    }

    @Test
    void testReceiveTrapWithErrorGetsTransferredToFailure() {
        final CommandResponderEvent mockEvent = mock(CommandResponderEvent.class);

        when(mockPdu.getType()).thenReturn(PDU.TRAP);
        when(mockPdu.getErrorStatus()).thenReturn(PDU.badValue);

        final Address mockAddress = mock(Address.class);
        when(mockAddress.isValid()).thenReturn(false);

        final Vector<VariableBinding> vbs = new Vector<>();
        doReturn(vbs).when(mockPdu).getVariableBindings();
        when(mockEvent.getPDU()).thenReturn(mockPdu);
        when(mockEvent.getPeerAddress()).thenReturn(mockAddress);
        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);

        snmpTrapReceiver.processPdu(mockEvent);

        final List<MockFlowFile> flowFiles = mockProcessSession.getFlowFilesForRelationship(ListenTrapSNMP.REL_FAILURE);

        assertFalse(flowFiles.isEmpty());
        final FlowFile flowFile = flowFiles.getFirst();

        assertEquals(String.valueOf(PDU.badValue), flowFile.getAttribute("snmp$errorStatus"));
        assertNull(flowFile.getAttribute("snmp$peerAddress"));
    }
}
