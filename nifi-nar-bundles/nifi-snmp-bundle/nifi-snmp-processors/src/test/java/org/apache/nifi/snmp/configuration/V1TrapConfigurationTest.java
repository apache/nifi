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

import org.junit.jupiter.api.Test;

import static org.apache.nifi.snmp.configuration.V1TrapConfiguration.GENERIC_TRAP_TYPE_IS_6_ENTERPRISE_SPECIFIC_BUT_SPECIFIC_TRAP_TYPE_IS_NOT_PROVIDED;
import static org.apache.nifi.snmp.configuration.V1TrapConfiguration.GENERIC_TRAP_TYPE_MUST_BE_BETWEEN_0_AND_6;
import static org.apache.nifi.snmp.configuration.V1TrapConfiguration.SPECIFIC_TRAP_TYPE_MUST_BE_BETWEEN_0_AND_2147483647;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class V1TrapConfigurationTest {

    private static final String AGENT_ADDRESS = "127.0.0.1";
    private static final String ENTERPRISE_OID = "1.3.6.1.4.1.8072.2.3.0.1";
    private static final int GENERIC_TRAP_TYPE = 6;
    private static final Integer SPECIFIC_TRAP_TYPE = 2;

    @Test
    void testMembersAreSetCorrectly() {
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
    void testRequireNonNullEnterpriseOid() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                V1TrapConfiguration.builder()
                        .agentAddress(AGENT_ADDRESS)
                        .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                        .specificTrapType(String.valueOf(SPECIFIC_TRAP_TYPE))
                        .build()
        );
        assertEquals("Enterprise OID must be specified.", exception.getMessage());
    }

    @Test
    void testRequireNonNullAgentAddress() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                        V1TrapConfiguration.builder()
                                .enterpriseOid(ENTERPRISE_OID)
                                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                                .specificTrapType(String.valueOf(SPECIFIC_TRAP_TYPE))
                                .build()
        );

        assertEquals("Agent address must be specified.", exception.getMessage());
    }

    @Test
    void testGenericTypeIsNegative() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType("-1")
                .build()
        );
        assertEquals(GENERIC_TRAP_TYPE_MUST_BE_BETWEEN_0_AND_6, exception.getMessage());
    }

    @Test
    void testGenericTypeIsGreaterThan6() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType("7")
                .build()
        );
        assertEquals(GENERIC_TRAP_TYPE_MUST_BE_BETWEEN_0_AND_6, exception.getMessage());
    }

    @Test
    void testGenericTypeIsNotANumber() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType("invalid")
                .build()
        );
        assertEquals("Generic Trap Type is not a number.", exception.getMessage());
    }

    @Test
    void testSpecificTrapTypeIsNegative() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                .specificTrapType("-1")
                .build()
        );
        assertEquals(SPECIFIC_TRAP_TYPE_MUST_BE_BETWEEN_0_AND_2147483647, exception.getMessage());
    }

    @Test
    void testGenericTrapTypeIsEnterpriseSpecificButSpecificTrapTypeIsNotSet() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> V1TrapConfiguration.builder()
                .agentAddress(AGENT_ADDRESS)
                .enterpriseOid(ENTERPRISE_OID)
                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                .build()
        );
        assertEquals(GENERIC_TRAP_TYPE_IS_6_ENTERPRISE_SPECIFIC_BUT_SPECIFIC_TRAP_TYPE_IS_NOT_PROVIDED, exception.getMessage());
    }
}
