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
import org.apache.nifi.snmp.helper.SNMPTestUtils;
import org.apache.nifi.snmp.testagents.TestAgent;
import org.apache.nifi.snmp.testagents.TestSNMPV3Agent;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Test;
import org.snmp4j.MessageException;
import org.snmp4j.SNMP4JSettings;
import org.snmp4j.Snmp;
import org.snmp4j.UserTarget;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.SecurityLevel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SNMPV3RequestTest extends SNMPRequestTest {

    @Override
    protected TestAgent getAgentInstance() {
        return new TestSNMPV3Agent(LOCALHOST);
    }

    @Test
    public void testSuccessfulSnmpGet() throws IOException {
        final StandardSNMPRequestHandler standardSnmpRequestHandler = getSnmpV3Getter(LOCALHOST, SnmpConstants.version3,
                "SHA", "SHAAuthPassword");
        final SNMPSingleResponse response = standardSnmpRequestHandler.get(READ_ONLY_OID_1);
        assertEquals(READ_ONLY_OID_VALUE_1, response.getVariableBindings().get(0).getVariable());
        assertEquals(SUCCESS, response.getErrorStatusText());
    }

    @Test
    public void testSuccessfulSnmpWalk() throws IOException {
        final StandardSNMPRequestHandler standardSnmpRequestHandler = getSnmpV3Getter(LOCALHOST, SnmpConstants.version3,
                "SHA", "SHAAuthPassword");
        final SNMPTreeResponse response = standardSnmpRequestHandler.walk("1.3.6.1.4.1.32437");

        assertSubTreeContainsOids(response);
    }

    @Test(expected = RequestTimeoutException.class)
    public void testSnmpGetTimeoutReturnsNull() throws IOException {
        final StandardSNMPRequestHandler standardSnmpRequestHandler = getSnmpV3Getter(INVALID_HOST, SnmpConstants.version3,
                "SHA", "SHAAuthPassword");
        standardSnmpRequestHandler.get(READ_ONLY_OID_1);
    }

    @Test(expected = MessageException.class)
    public void testSnmpGetWithInvalidTargetThrowsException() throws IOException {
        final StandardSNMPRequestHandler standardSnmpRequestHandler = getSnmpV3Getter(LOCALHOST, -1, "SHA", "SHAAuthPassword");
        standardSnmpRequestHandler.get(READ_ONLY_OID_1);
    }

    @Test
    public void testSuccessfulSnmpSet() throws IOException {
        final StandardSNMPRequestHandler standardSnmpRequestHandler = getSnmpV3Getter(LOCALHOST, SnmpConstants.version3,
                "SHA", "SHAAuthPassword");

        final MockFlowFile flowFile = new MockFlowFile(1L);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(SNMP_PROP_PREFIX + WRITE_ONLY_OID, EXPECTED_OID_VALUE);
        flowFile.putAttributes(attributes);

        final SNMPSingleResponse response = standardSnmpRequestHandler.set(flowFile);

        assertEquals(EXPECTED_OID_VALUE, response.getVariableBindings().get(0).getVariable());

    }

    @Test
    public void testCannotSetReadOnlyObject() throws IOException {
        final StandardSNMPRequestHandler standardSnmpRequestHandler = getSnmpV3Getter(LOCALHOST, SnmpConstants.version3,
                "SHA", "SHAAuthPassword");

        final MockFlowFile flowFile = new MockFlowFile(1L);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(SNMP_PROP_PREFIX + READ_ONLY_OID_1, EXPECTED_OID_VALUE);
        flowFile.putAttributes(attributes);

        final SNMPSingleResponse response = standardSnmpRequestHandler.set(flowFile);

        assertEquals(NOT_WRITABLE, response.getErrorStatusText());
    }

    @Test
    public void testCannotGetWriteOnlyObject() throws IOException {
        final StandardSNMPRequestHandler standardSnmpRequestHandler = getSnmpV3Getter(LOCALHOST, SnmpConstants.version3, "SHA", "SHAAuthPassword");

        final SNMPSingleResponse response = standardSnmpRequestHandler.get(WRITE_ONLY_OID);

        assertEquals(NO_ACCESS, response.getErrorStatusText());
    }

    @Test
    public void testUnauthorizedUserSnmpGetReturnsNull() throws IOException {
        final StandardSNMPRequestHandler standardSnmpRequestHandler = getSnmpV3Getter(LOCALHOST, SnmpConstants.version3,
                "FakeUserName", "FakeAuthPassword");
        final SNMPSingleResponse response = standardSnmpRequestHandler.get(READ_ONLY_OID_1);
        assertEquals("Null", response.getVariableBindings().get(0).getVariable());
    }

    private StandardSNMPRequestHandler getSnmpV3Getter(final String host, final int version, final String sha, final String shaAuthPassword) throws IOException {
        SNMP4JSettings.setForwardRuntimeExceptions(true);
        final Snmp snmp = SNMPTestUtils.createSnmpClient();
        final UserTarget userTarget = SNMPTestUtils.prepareUser(snmp, version, host + "/" + agent.getPort(), SecurityLevel.AUTH_NOPRIV,
                sha, AuthSHA.ID, null, shaAuthPassword, null);
        return new StandardSNMPRequestHandler(snmp, userTarget);
    }
}
