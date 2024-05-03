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
import org.apache.nifi.snmp.dto.SNMPTreeResponse;
import org.apache.nifi.snmp.exception.RequestTimeoutException;
import org.apache.nifi.snmp.exception.SNMPWalkException;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.DefaultPDUFactory;
import org.snmp4j.util.PDUFactory;
import org.snmp4j.util.TreeEvent;
import org.snmp4j.util.TreeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.snmp.processors.AbstractSNMPProcessor.REQUEST_TIMEOUT_EXCEPTION_TEMPLATE;

public class GetSNMPHandler {

    private static final PDUFactory getPduFactory = new DefaultPDUFactory(PDU.GET);

    protected static final String EMPTY_SUBTREE_EXCEPTION_MESSAGE = "Agent is not available, the %s OID not found or user not found. " +
            "Please, check if (1) the agent is available, (2) the processor's SNMP version matches the agent version, " +
            "(3) the OID is correct, (4) The user is valid.";

    protected static final String SNMP_ERROR_EXCEPTION_MESSAGE = "Agent is not available, OID not found or user not found. " +
            "Please, check if (1) the agent is available, (2) the processor's SNMP version matches the agent version, " +
            "(3) the OID is correct, (4) The user is valid.";

    protected static final String LEAF_ELEMENT_EXCEPTION_MESSAGE = "OID not found or it is a single leaf element. The leaf element " +
            "associated with this %s OID does not contain child OIDs. Please check if the OID exists in the agent " +
            "MIB or specify a parent OID with at least one child element";

    private Snmp snmpManager;
    private TreeUtils treeUtils;

    public GetSNMPHandler(final Snmp snmpManager) {
        this.snmpManager = snmpManager;
        this.treeUtils = new TreeUtils(snmpManager, getPduFactory);
    }

    public Optional<SNMPSingleResponse> get(final Map<String, String> flowFileAttributes, final Target target) throws IOException {
        final PDU pdu = getPduFactory.createPDU(target);
        VariableBinding[] variableBindings = SNMPUtils.addGetVariables(flowFileAttributes);
        if (variableBindings.length == 0) {
            return Optional.empty();
        }
        pdu.addAll(variableBindings);

        final PDU responsePdu = getResponsePdu(target, pdu);
        return Optional.of(new SNMPSingleResponse(target, responsePdu));
    }

    public Optional<SNMPTreeResponse> walk(final Map<String, String> flowFileAttributes, final Target target) {
        final List<TreeEvent> subtree;

        final OID[] oids = SNMPUtils.addWalkVariables(flowFileAttributes);
        if (oids.length == 0) {
            return Optional.empty();
        }
        subtree = treeUtils.walk(target, oids);

        evaluateSubtreeErrors(Arrays.toString(oids), subtree);

        return Optional.of(new SNMPTreeResponse(target, subtree));
    }

    private PDU getResponsePdu(final Target target, final PDU pdu) throws IOException {
        final ResponseEvent response = snmpManager.get(pdu, target);
        final PDU responsePdu = response.getResponse();
        if (responsePdu == null) {
            throw new RequestTimeoutException(String.format(REQUEST_TIMEOUT_EXCEPTION_TEMPLATE, "read"));
        }
        return responsePdu;
    }

    private void evaluateSubtreeErrors(String oid, List<TreeEvent> subtree) {
        if (subtree.isEmpty()) {
            throw new SNMPWalkException(String.format(EMPTY_SUBTREE_EXCEPTION_MESSAGE, oid));
        }
        if (isSnmpError(subtree)) {
            throw new SNMPWalkException(SNMP_ERROR_EXCEPTION_MESSAGE);
        }
        if (isLeafElement(subtree)) {
            throw new SNMPWalkException(String.format(LEAF_ELEMENT_EXCEPTION_MESSAGE, oid));
        }
    }

    private boolean isSnmpError(final List<TreeEvent> subtree) {
        return subtree.size() == 1 && subtree.get(0).getVariableBindings() == null;
    }

    private boolean isLeafElement(final List<TreeEvent> subtree) {
        return subtree.size() == 1 && subtree.get(0).getVariableBindings().length == 0;
    }

    // Visible for testing
    void setTreeUtils(final TreeUtils treeUtils) {
        this.treeUtils = treeUtils;
    }
}
