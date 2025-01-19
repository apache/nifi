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
package org.apache.nifi.snmp.processors;

import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPValue;
import org.apache.nifi.snmp.helper.testrunners.SNMPV1TestRunnerFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.snmp4j.mp.SnmpConstants;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractSNMPProcessorTest {

    private static final String TEST_OID = "1.3.6.1.4.1.32437.1.5.1.4.2.0";
    private static final String UNSUPPORTED_SECURITY_LEVEL = "1.3.6.1.6.3.15.1.1.1";

    private GetSNMP getSNMP;
    private MockProcessContext mockProcessContext;
    private MockProcessSession mockProcessSession;
    private MockFlowFile mockFlowFile;
    private SNMPSingleResponse mockResponse;
    private TestRunner getTestRunner;

    @BeforeEach
    public void init() {
        getTestRunner = new SNMPV1TestRunnerFactory().createSnmpGetTestRunner(0, TEST_OID, "GET");
        getSNMP = (GetSNMP) getTestRunner.getProcessor();
        mockProcessContext = new MockProcessContext(getSNMP);
        mockProcessSession = new MockProcessSession(new SharedSessionState(getSNMP, new AtomicLong(0L)), getSNMP);
        mockFlowFile = mockProcessSession.create();
        mockResponse = mock(SNMPSingleResponse.class);
    }

    @Test
    void testProcessResponseWithInvalidResponseThrowsException() {
        final String errorStatus = "Test error status text";
        when(mockResponse.getErrorStatusText()).thenReturn(errorStatus);


        getSNMP.handleResponse(mockProcessContext, mockProcessSession, mockFlowFile, mockResponse, GetSNMP.REL_SUCCESS, GetSNMP.REL_FAILURE, "provenanceAddress", true);

        final String actualLogMessage = getTestRunner.getLogger().getErrorMessages().getFirst().getMsg();
        final String expectedLogMessage = String.format("SNMP request failed, response error: %s", errorStatus);
        assertTrue(actualLogMessage.contains(expectedLogMessage));
    }

    @Test
    void testProcessResponseWithNoSuchObjectThrowsException() {
        when(mockResponse.isValid()).thenReturn(true);
        when(mockResponse.getVersion()).thenReturn(SnmpConstants.version2c);

        List<SNMPValue> vbs = Collections.singletonList(new SNMPValue(TEST_OID, "noSuchObject"));
        when(mockResponse.getVariableBindings()).thenReturn(vbs);


        getSNMP.handleResponse(mockProcessContext, mockProcessSession, mockFlowFile, mockResponse, GetSNMP.REL_SUCCESS, GetSNMP.REL_FAILURE, "provenanceAddress", true);


        final String actualLogMessage = getTestRunner.getLogger().getErrorMessages().getFirst().getMsg();
        final String expectedLogMessage = "SNMP request failed, response error: OID not found.";
        assertTrue(actualLogMessage.contains(expectedLogMessage));
    }

    @Test
    void testValidProcessResponseWithoutVariableBindingThrowsException() {
        when(mockResponse.isValid()).thenReturn(true);
        when(mockResponse.getVersion()).thenReturn(SnmpConstants.version2c);

        when(mockResponse.getVariableBindings()).thenReturn(Collections.emptyList());

        getSNMP.handleResponse(mockProcessContext, mockProcessSession, mockFlowFile, mockResponse, GetSNMP.REL_SUCCESS, GetSNMP.REL_FAILURE, "provenanceAddress", true);


        final String actualLogMessage = getTestRunner.getLogger().getErrorMessages().getFirst().getMsg();
        final String expectedLogMessage = "Empty SNMP response: no variable binding found.";
        assertTrue(actualLogMessage.contains(expectedLogMessage));
    }

    @Test
    void testValidProcessResponse() {
        when(mockResponse.isValid()).thenReturn(true);
        when(mockResponse.getVersion()).thenReturn(SnmpConstants.version2c);

        final List<SNMPValue> vbs = Collections.singletonList(new SNMPValue(TEST_OID, "testOIDValue"));
        when(mockResponse.getVariableBindings()).thenReturn(vbs);

        final Map<String, String> attributes = Collections.singletonMap(TEST_OID, "testOIDValue");
        when(mockResponse.getAttributes()).thenReturn(attributes);

        getSNMP.handleResponse(mockProcessContext, mockProcessSession, mockFlowFile, mockResponse, GetSNMP.REL_SUCCESS, GetSNMP.REL_FAILURE, "provenanceAddress", true);
        final List<MockFlowFile> flowFilesForRelationship = mockProcessSession.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS);

        assertEquals("testOIDValue", flowFilesForRelationship.getFirst().getAttribute(TEST_OID));
    }

    @Test
    void testProcessResponseWithReportPduWithoutErrorMessage() {
        when(mockResponse.isValid()).thenReturn(true);
        when(mockResponse.isReportPdu()).thenReturn(true);

        final List<SNMPValue> vbs = Collections.singletonList(new SNMPValue(TEST_OID, "testOIDValue"));
        when(mockResponse.getVariableBindings()).thenReturn(vbs);


        getSNMP.handleResponse(mockProcessContext, mockProcessSession, mockFlowFile, mockResponse, GetSNMP.REL_SUCCESS, GetSNMP.REL_FAILURE, "provenanceAddress", true);


        final String actualLogMessage = getTestRunner.getLogger().getErrorMessages().getFirst().getMsg();
        final String expectedLogMessage = String.format("SNMP request failed, response error: Report-PDU returned, but no error message found. " +
                "Please, check the OID %s in an online OID repository.", TEST_OID);

        assertTrue(actualLogMessage.contains(expectedLogMessage));
    }

    @Test
    void testProcessResponseWithReportPdu() {
        when(mockResponse.isValid()).thenReturn(true);
        when(mockResponse.isReportPdu()).thenReturn(true);

        final List<SNMPValue> vbs = Collections.singletonList(new SNMPValue(UNSUPPORTED_SECURITY_LEVEL, "testOIDValue"));
        when(mockResponse.getVariableBindings()).thenReturn(vbs);


        getSNMP.handleResponse(mockProcessContext, mockProcessSession, mockFlowFile, mockResponse, GetSNMP.REL_SUCCESS, GetSNMP.REL_FAILURE, "provenanceAddress", true);


        final String actualLogMessage = getTestRunner.getLogger().getErrorMessages().getFirst().getMsg();
        final String expectedLogMessage = String.format("SNMP request failed, response error: Report-PDU returned. %s: usmStatsUnsupportedSecLevels", UNSUPPORTED_SECURITY_LEVEL);

        assertTrue(actualLogMessage.contains(expectedLogMessage));
    }
}
