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
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPTreeResponse;
import org.apache.nifi.snmp.exception.CloseSNMPClientException;
import org.apache.nifi.snmp.exception.InvalidFlowFileException;
import org.apache.nifi.snmp.exception.RequestTimeoutException;
import org.apache.nifi.snmp.exception.SNMPWalkException;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.DefaultPDUFactory;
import org.snmp4j.util.PDUFactory;
import org.snmp4j.util.TreeEvent;
import org.snmp4j.util.TreeUtils;

import java.io.IOException;
import java.util.List;

final class StandardSNMPRequestHandler implements SNMPRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(StandardSNMPRequestHandler.class);
    private static final PDUFactory getPduFactory = new DefaultPDUFactory(PDU.GET);
    private static final PDUFactory setPduFactory = new DefaultPDUFactory(PDU.SET);
    private final Snmp snmpManager;
    private final Target target;

    StandardSNMPRequestHandler(final Snmp snmpManager, final Target target) {
        this.snmpManager = snmpManager;
        this.target = target;
    }

    /**
     * Construct the PDU to perform the SNMP Get request and returns
     * the result in order to create the flow file.
     *
     * @return {@link ResponseEvent}
     */
    public SNMPSingleResponse get(final String oid) throws IOException {
        final PDU pdu = getPduFactory.createPDU(target);
        pdu.add(new VariableBinding(new OID(oid)));
        final ResponseEvent response = snmpManager.get(pdu, target);
        final PDU responsePdu = response.getResponse();
        if (responsePdu == null) {
            throw new RequestTimeoutException("Request timed out. Please check if (1). the agent host and port is correctly set, " +
                    "(2). the agent is running, (3). the agent SNMP version corresponds with the processor's one, (4) the " +
                    "community string is correct and has read access, (5) In case of SNMPv3 check if the user credentials " +
                    "are valid and the user in a group with read access.");
        }
        return new SNMPSingleResponse(target, responsePdu);
    }

    /**
     * Perform a SNMP walk and returns the list of {@link TreeEvent}
     *
     * @return the list of {@link TreeEvent}
     */
    public SNMPTreeResponse walk(final String oid) {
        final TreeUtils treeUtils = new TreeUtils(snmpManager, getPduFactory);
        final List<TreeEvent> subtree = treeUtils.getSubtree(target, new OID(oid));
        if (subtree.isEmpty()) {
            throw new SNMPWalkException(String.format("The subtree associated with the specified OID %s is empty.", oid));
        }
        if (isSnmpError(subtree)) {
            throw new SNMPWalkException("Agent is not available, OID not found or user not found. Please, check if (1) the " +
                    "agent is available, (2) the processor's SNMP version matches the agent version, (3) the OID is " +
                    "correct, (4) The user is valid.");
        }
        if (isLeafElement(subtree)) {
            throw new SNMPWalkException(String.format("OID not found or it is a single leaf element. The leaf element " +
                    "associated with this %s OID does not contain child OIDs. Please check if the OID exists in the agent " +
                    "MIB or specify a parent OID with at least one child element", oid));
        }

        return new SNMPTreeResponse(target, subtree);
    }

    private boolean isSnmpError(final List<TreeEvent> subtree) {
        return subtree.size() == 1 && subtree.get(0).getVariableBindings() == null;
    }

    private boolean isLeafElement(final List<TreeEvent> subtree) {
        return subtree.size() == 1 && subtree.get(0).getVariableBindings().length == 0;
    }

    /**
     * Executes the SNMP set request and returns the response.
     *
     * @param flowFile FlowFile which contains variables for the PDU
     * @return Response event
     * @throws IOException IO Exception
     */
    public SNMPSingleResponse set(final FlowFile flowFile) throws IOException {
        final PDU pdu = setPduFactory.createPDU(target);
        if (SNMPUtils.addVariables(pdu, flowFile.getAttributes())) {
            final ResponseEvent response = snmpManager.set(pdu, target);
            final PDU responsePdu = response.getResponse();
            if (responsePdu == null) {
                throw new RequestTimeoutException("Request timed out. Please check if (1). the agent host and port is correctly set, " +
                        "(2). the agent is running, (3). the agent SNMP version corresponds with the processor's one, (4) the " +
                        "community string is correct and has write access, (5) In case of SNMPv3 check if the user credentials " +
                        "are valid and the user in a group with write access.");
            }
            return new SNMPSingleResponse(target, responsePdu);
        }
        throw new InvalidFlowFileException("Could not read the variable bindings from the flowfile. Please, " +
                "add the OIDs to set in separate properties. E.g. Property name: snmp$1.3.6.1.2.1.1.1.0 Value: Example value. ");
    }

    public void close() {
        try {
            if (snmpManager.getUSM() != null) {
                snmpManager.getUSM().removeAllUsers();
                SecurityModels.getInstance().removeSecurityModel(new Integer32(snmpManager.getUSM().getID()));
            }
            snmpManager.close();
        } catch (IOException e) {
            final String errorMessage = "Could not close SNMP client.";
            logger.error(errorMessage, e);
            throw new CloseSNMPClientException(errorMessage);
        }
    }
}
