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
package org.apache.nifi.snmp.configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class V1TrapConfigurationTest {

    private static final String AGENT_ADDRESS = "127.0.0.1";
    private static final String ENTERPRISE_OID = "1.3.6.1.4.1.8072.2.3.0.1";
    private static final int GENERIC_TRAP_TYPE = 6;
    private static final Integer SPECIFIC_TRAP_TYPE = 2;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testMembersAreSetCorrectly() {
        final V1TrapConfiguration v1TrapConfiguration = V1TrapConfiguration.builder()
                .enterpriseOid(ENTERPRISE_OID)
                .agentAddress(AGENT_ADDRESS)
                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                .specificTrapType(String.valueOf(SPECIFIC_TRAP_TYPE))
                .build();

        assertEquals(ENTERPRISE_OID, v1TrapConfiguration.getEnterpriseOid());
        assertEquals(AGENT_ADDRESS, v1TrapConfiguration.getAgentAddress());
        assertEquals(GENERIC_TRAP_TYPE, v1TrapConfiguration.getGenericTrapType());
        assertEquals(SPECIFIC_TRAP_TYPE, v1TrapConfiguration.getSpecificTrapType());
    }

    @Test
    public void testRequireNonNullEnterpriseOid() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Enterprise OID must be specified.");
        V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                .specificTrapType(String.valueOf(SPECIFIC_TRAP_TYPE))
                .build();
    }

    @Test
    public void testRequireNonNullAgentAddress() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Agent address must be specified.");
        V1TrapConfiguration.builder()
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                .specificTrapType(String.valueOf(SPECIFIC_TRAP_TYPE))
                .build();
    }

    @Test
    public void testGenericTypeIsNegative() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType("-1")
                .build()
        );
        assertEquals("Generic Trap Type must be between 0 and 6.", exception.getMessage());
    }

    @Test
    public void testGenericTypeIsGreaterThan6() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType("7")
                .build()
        );
        assertEquals("Generic Trap Type must be between 0 and 6.", exception.getMessage());
    }

    @Test
    public void testGenericTypeIsNotANumber() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType("invalid")
                .build()
        );
        assertEquals("Generic Trap Type is not a number.", exception.getMessage());
    }

    @Test
    public void testSpecificTrapTypeIsNegative() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                .specificTrapType("-1")
                .build()
        );
        assertEquals("Specific Trap Type must be between 0 and 2147483647.", exception.getMessage());
    }

    @Test
    public void testGenericTrapTypeIsEnterpriseSpecificButSpecificTrapTypeIsNotSet() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                .build()
        );
        assertEquals("Generic Trap Type is [6 - Enterprise Specific] but Specific Trap Type is not provided or not a number.", exception.getMessage());
    }

    @Test
    public void testGenericTrapTypeIsNotEnterpriseSpecificButSpecificTrapTypeIsSet() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType("5")
                .specificTrapType("123")
                .build()
        );
        assertEquals("Invalid argument: Generic Trap Type is not [6 - Enterprise Specific] but Specific Trap Type is provided.", exception.getMessage());
    }

}
