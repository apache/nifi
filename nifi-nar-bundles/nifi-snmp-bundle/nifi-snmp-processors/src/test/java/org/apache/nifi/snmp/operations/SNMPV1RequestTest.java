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
import org.apache.nifi.snmp.testagents.TestAgent;
import org.apache.nifi.snmp.testagents.TestSNMPV1Agent;
import org.junit.Test;
import org.snmp4j.MessageException;
import org.snmp4j.mp.SnmpConstants;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SNMPV1RequestTest extends SNMPRequestTest {

    private static final String NO_SUCH_NAME = "No such name";

    @Override
    protected TestAgent getAgentInstance() {
        return new TestSNMPV1Agent(LOCALHOST);
    }

    @Test
    public void testSuccessfulSnmpGet() throws IOException {
        final SNMPSingleResponse response = getResponseEvent(LOCALHOST, agent.getPort(), SnmpConstants.version1, READ_ONLY_OID_1);
        assertEquals(READ_ONLY_OID_VALUE_1, response.getVariableBindings().get(0).getVariable());
        assertEquals(SUCCESS, response.getErrorStatusText());
    }

    @Test
    public void testSuccessfulSnmpWalk() throws IOException {
        final SNMPTreeResponse response = getTreeEvents(agent.getPort(), SnmpConstants.version1);
        assertSubTreeContainsOids(response);
    }

    @Test(expected = RequestTimeoutException.class)
    public void testSnmpGetTimeoutReturnsNull() throws IOException {
        getResponseEvent(INVALID_HOST, agent.getPort(), SnmpConstants.version1, READ_ONLY_OID_1);
    }

    @Test(expected = MessageException.class)
    public void testSnmpGetWithInvalidTargetThrowsException() throws IOException {
        getResponseEvent(LOCALHOST, agent.getPort(), -1, READ_ONLY_OID_1);
    }

    @Test
    public void testSuccessfulSnmpSet() throws IOException {
        final SNMPSingleResponse response = getSetResponse(agent.getPort(), SnmpConstants.version1, WRITE_ONLY_OID, EXPECTED_OID_VALUE);

        assertEquals(EXPECTED_OID_VALUE, response.getVariableBindings().get(0).getVariable());
        assertEquals(SUCCESS, response.getErrorStatusText());
    }

    @Test
    public void testCannotSetReadOnlyObject() throws IOException {
        final SNMPSingleResponse response = getSetResponse(agent.getPort(), SnmpConstants.version1, READ_ONLY_OID_1, EXPECTED_OID_VALUE);

        assertEquals(NO_SUCH_NAME, response.getErrorStatusText());
    }

    @Test
    public void testCannotGetWriteOnlyObject() throws IOException {
        final SNMPSingleResponse response = getResponseEvent(LOCALHOST, agent.getPort(), SnmpConstants.version1, WRITE_ONLY_OID);

        assertEquals(NO_SUCH_NAME, response.getErrorStatusText());
    }
}
