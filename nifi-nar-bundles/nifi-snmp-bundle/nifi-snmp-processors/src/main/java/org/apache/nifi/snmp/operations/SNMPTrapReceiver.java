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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.smi.Address;

import java.util.Map;

import static org.apache.nifi.snmp.processors.ListenTrapSNMP.REL_FAILURE;
import static org.apache.nifi.snmp.processors.ListenTrapSNMP.REL_SUCCESS;

public class SNMPTrapReceiver implements CommandResponder {

    private final ProcessSessionFactory processSessionFactory;
    private final ComponentLog logger;

    public SNMPTrapReceiver(final ProcessSessionFactory processSessionFactory, final ComponentLog logger) {
        this.processSessionFactory = processSessionFactory;
        this.logger = logger;
    }

    @Override
    public void processPdu(final CommandResponderEvent event) {
        final PDU pdu = event.getPDU();
        if (isValidTrapPdu(pdu)) {
            final ProcessSession processSession = processSessionFactory.createSession();
            final FlowFile flowFile = createFlowFile(processSession,event);
            processSession.getProvenanceReporter().create(flowFile, event.getPeerAddress() + "/" + pdu.getRequestID());
            if (pdu.getErrorStatus() == PDU.noError) {
                processSession.transfer(flowFile, REL_SUCCESS);
            } else {
                processSession.transfer(flowFile, REL_FAILURE);
            }
            processSession.commitAsync();
        } else {
            logger.error("Request timed out or parameters are incorrect.");
        }
    }

    private FlowFile createFlowFile(final ProcessSession processSession, final  CommandResponderEvent event) {
        FlowFile flowFile = processSession.create();
        final Map<String, String> attributes;
        final PDU pdu = event.getPDU();
        final Address peerAddress = event.getPeerAddress();
        if (pdu instanceof PDUv1) {
            attributes = SNMPUtils.getV1TrapPduAttributeMap((PDUv1) pdu);
        } else {
            attributes = SNMPUtils.getPduAttributeMap(pdu);
        }
        if (peerAddress.isValid()) {
            processSession.putAttribute(flowFile, SNMPUtils.SNMP_PROP_PREFIX + "peerAddress", peerAddress.toString());
        }
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    private boolean isValidTrapPdu(final PDU pdu) {
        return pdu != null && (pdu.getType() == PDU.V1TRAP || pdu.getType() == PDU.TRAP);
    }
}
