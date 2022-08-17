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
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.Target;
import org.snmp4j.smi.IpAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.util.DefaultPDUFactory;
import org.snmp4j.util.PDUFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Factory class to create SNMPv1-Trap-PDU for SNMPv1.
 */
public class V1TrapPDUFactory {

    private static final PDUFactory v1PduFactory = new DefaultPDUFactory(PDU.V1TRAP);

    final Target target;
    final Instant startTime;

    public V1TrapPDUFactory(final Target target, final Instant startTime) {
        this.target = target;
        this.startTime = startTime;
    }

    public PDU get(final V1TrapConfiguration v1TrapConfiguration) {
        final PDUv1 pdu = (PDUv1) v1PduFactory.createPDU(target);
        Optional.ofNullable(v1TrapConfiguration.getEnterpriseOid()).map(OID::new).ifPresent(pdu::setEnterprise);
        Optional.ofNullable(v1TrapConfiguration.getAgentAddress()).map(IpAddress::new).ifPresent(pdu::setAgentAddress);
        pdu.setGenericTrap(v1TrapConfiguration.getGenericTrapType());
        Optional.ofNullable(v1TrapConfiguration.getSpecificTrapType()).ifPresent(pdu::setSpecificTrap);
        final long elapsedMillis = Duration.between(startTime, Instant.now()).toMillis();
        pdu.setTimestamp(elapsedMillis);
        return pdu;
    }

}
