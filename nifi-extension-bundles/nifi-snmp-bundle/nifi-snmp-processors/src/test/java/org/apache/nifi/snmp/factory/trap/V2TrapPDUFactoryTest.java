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

import org.apache.nifi.snmp.configuration.V2TrapConfiguration;
import org.junit.jupiter.api.Test;
import org.snmp4j.PDU;
import org.snmp4j.Target;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.VariableBinding;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class V2TrapPDUFactoryTest {

    private static final String TRAP_OID = "1.3.6.1.4.1.8072.2.3.0.1";

    @Test
    void testCreateV2TrapPdu() {
        final Target mockTarget = mock(Target.class);
        final V2TrapConfiguration v2TrapConfiguration = new V2TrapConfiguration(TRAP_OID);
        final V2TrapPDUFactory v2TrapPduFactory = new V2TrapPDUFactory(mockTarget, Instant.now());

        final PDU pdu = v2TrapPduFactory.get(v2TrapConfiguration);

        final List<? extends VariableBinding> variableBindings = pdu.getVariableBindings();

        Set<String> expected = new HashSet<>(Arrays.asList(SnmpConstants.snmpTrapOID.toString(), TRAP_OID));
        Set<String> actual = variableBindings.stream()
                .flatMap(c -> Stream.of(c.getOid().toString(), c.getVariable().toString()))
                .collect(Collectors.toSet());
        assertTrue(actual.containsAll(expected));
    }

}
