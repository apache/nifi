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
    private final SNMPResourceHandler snmpResourceHandler;
    private final ComponentLog logger;
    private final V1TrapPDUFactory v1TrapPDUFactory;
    private final V2TrapPDUFactory v2TrapPDUFactory;

    public SendTrapSNMPHandler(final SNMPResourceHandler snmpResourceHandler, final Instant startTime, final ComponentLog logger) {
        this.snmpResourceHandler = snmpResourceHandler;
        this.logger = logger;
        v1TrapPDUFactory = createV1TrapPduFactory(startTime);
        v2TrapPDUFactory = createV2TrapPduFactory(startTime);
    }

    public void sendTrap(final Map<String, String> flowFileAttributes, final V1TrapConfiguration trapConfiguration) throws IOException {
        final PDU pdu = v1TrapPDUFactory.get(trapConfiguration);
        sendTrap(flowFileAttributes, pdu);
    }

    public void sendTrap(final Map<String, String> flowFileAttributes, final V2TrapConfiguration trapConfiguration) throws IOException {
        final PDU pdu = v2TrapPDUFactory.get(trapConfiguration);
        sendTrap(flowFileAttributes, pdu);
    }

    private void sendTrap(Map<String, String> flowFileAttributes, PDU pdu) throws IOException {
        final Target target = snmpResourceHandler.getTarget();
        final Snmp snmpManager = snmpResourceHandler.getSnmpManager();

        final boolean isAnyVariableAdded = SNMPUtils.addVariables(pdu, flowFileAttributes);
        if (!isAnyVariableAdded) {
            logger.debug("No optional SNMP specific variables found in flowfile.");
        }

        snmpManager.send(pdu, target);
    }

    V1TrapPDUFactory createV1TrapPduFactory(final Instant startTime) {
        return new V1TrapPDUFactory(snmpResourceHandler.getTarget(), startTime);
    }

    V2TrapPDUFactory createV2TrapPduFactory(final Instant startTime) {
        return new V2TrapPDUFactory(snmpResourceHandler.getTarget(), startTime);
    }
}