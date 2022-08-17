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
package org.apache.nifi.snmp.factory.trap;

import org.apache.nifi.snmp.configuration.V1TrapConfiguration;
import org.junit.jupiter.api.Test;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.Target;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class V1TrapPDUFactoryTest {

    private static final String AGENT_ADDRESS = "127.0.0.1";
    private static final String ENTERPRISE_OID = "1.3.6.1.4.1.8072.2.3.0.1";
    private static final int GENERIC_TRAP_TYPE = 6;
    private static final int SPECIFIC_TRAP_TYPE = 2;

    @Test
    void testCreateV1TrapPdu() {
        final Target mockTarget = mock(Target.class);
        final V1TrapConfiguration v1TrapConfiguration = V1TrapConfiguration.builder()
                .enterpriseOid(ENTERPRISE_OID)
                .agentAddress(AGENT_ADDRESS)
                .genericTrapType(String.valueOf(GENERIC_TRAP_TYPE))
                .specificTrapType(String.valueOf(SPECIFIC_TRAP_TYPE))
                .build();

        final V1TrapPDUFactory v1TrapPduFactory = new V1TrapPDUFactory(mockTarget, Instant.now());
        final PDU pdu = v1TrapPduFactory.get(v1TrapConfiguration);

        assertEquals(PDU.V1TRAP, pdu.getType());

        final PDUv1 pduV1 = (PDUv1) pdu;

        assertEquals(ENTERPRISE_OID, pduV1.getEnterprise().toString());
        assertEquals(AGENT_ADDRESS, pduV1.getAgentAddress().toString());
        assertEquals(GENERIC_TRAP_TYPE, pduV1.getGenericTrap());
        assertEquals(SPECIFIC_TRAP_TYPE, pduV1.getSpecificTrap());
    }
}
