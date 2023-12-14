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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.snmp.configuration.V1TrapConfiguration;
import org.apache.nifi.snmp.configuration.V2TrapConfiguration;
import org.apache.nifi.snmp.factory.trap.V1TrapPDUFactory;
import org.apache.nifi.snmp.factory.trap.V2TrapPDUFactory;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public class SendTrapSNMPHandler {
    private final ComponentLog logger;
    private final Snmp snmpManager;
    private final Instant startTime;

    public SendTrapSNMPHandler(final Snmp snmpManager, final Instant startTime, final ComponentLog logger) {
        this.snmpManager = snmpManager;
        this.logger = logger;
        this.startTime = startTime;
    }

    public void sendTrap(final Map<String, String> flowFileAttributes, final V1TrapConfiguration trapConfiguration, final Target target) throws IOException {
        final PDU pdu = createV1TrapPduFactory(target, startTime).get(trapConfiguration);
        sendTrap(flowFileAttributes, pdu, target);
    }

    public void sendTrap(final Map<String, String> flowFileAttributes, final V2TrapConfiguration trapConfiguration, final Target target) throws IOException {
        final PDU pdu = createV2TrapPduFactory(target, startTime).get(trapConfiguration);
        sendTrap(flowFileAttributes, pdu, target);
    }

    private void sendTrap(final Map<String, String> flowFileAttributes, final PDU pdu, final Target target) throws IOException {
        final boolean isAnyVariableAdded = SNMPUtils.addVariables(pdu, flowFileAttributes);
        if (!isAnyVariableAdded) {
            logger.debug("No optional SNMP specific variables found in flowfile.");
        }

        snmpManager.send(pdu, target);
    }

    V1TrapPDUFactory createV1TrapPduFactory(final Target target, final Instant startTime) {
        return new V1TrapPDUFactory(target, startTime);
    }

    V2TrapPDUFactory createV2TrapPduFactory(final Target target, final Instant startTime) {
        return new V2TrapPDUFactory(target, startTime);
    }
}