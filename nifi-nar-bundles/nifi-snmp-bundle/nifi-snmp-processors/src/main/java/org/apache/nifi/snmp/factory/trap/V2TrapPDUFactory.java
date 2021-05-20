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
import org.snmp4j.PDU;
import org.snmp4j.Target;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.DefaultPDUFactory;
import org.snmp4j.util.PDUFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * Factory class to create SNMPv2-Trap-PDU for SNMPv2c and SNMPv3.
 */
public class V2TrapPDUFactory {
    private static final PDUFactory v2PduFactory = new DefaultPDUFactory(PDU.TRAP);

    final Target target;
    final Instant startTime;

    public V2TrapPDUFactory(final Target target, final Instant startTime) {
        this.target = target;
        this.startTime = startTime;
    }

    public PDU get(final V2TrapConfiguration v2TrapConfiguration) {
        final PDU pdu = v2PduFactory.createPDU(target);
        pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, new OctetString(v2TrapConfiguration.getTrapOidValue())));
        final long elapsedMillis = Duration.between(startTime, Instant.now()).toMillis();
        pdu.add(new VariableBinding(SnmpConstants.sysUpTime, new TimeTicks(elapsedMillis)));
        return pdu;
    }

}