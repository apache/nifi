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
import org.apache.nifi.snmp.exception.RequestTimeoutException;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.util.DefaultPDUFactory;
import org.snmp4j.util.PDUFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.snmp.processors.AbstractSNMPProcessor.REQUEST_TIMEOUT_EXCEPTION_TEMPLATE;

public class SetSNMPHandler {
    private static PDUFactory setPduFactory = new DefaultPDUFactory(PDU.SET);

    private final Snmp snmpManager;

    public SetSNMPHandler(final Snmp snmpManager) {
        this.snmpManager = snmpManager;
    }

    public Optional<SNMPSingleResponse> set(final Map<String, String> flowFileAttributes, final Target target) throws IOException {
        final PDU pdu = setPduFactory.createPDU(target);
        final boolean isAnySnmpVariableInFlowFile = SNMPUtils.addVariables(pdu, flowFileAttributes);
        if (isAnySnmpVariableInFlowFile) {
            final ResponseEvent response = snmpManager.set(pdu, target);
            final PDU responsePdu = response.getResponse();
            if (responsePdu == null) {
                throw new RequestTimeoutException(String.format(REQUEST_TIMEOUT_EXCEPTION_TEMPLATE, "write"));
            }
            return Optional.of(new SNMPSingleResponse(target, responsePdu));
        } else {
            return Optional.empty();
        }
    }

    // visible for testing
    static void setSetPduFactory(final PDUFactory setPduFactory) {
        SetSNMPHandler.setPduFactory = setPduFactory;
    }
}
