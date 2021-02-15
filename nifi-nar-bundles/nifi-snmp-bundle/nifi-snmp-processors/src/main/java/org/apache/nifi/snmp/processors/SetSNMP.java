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
package org.apache.nifi.snmp.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.snmp.operations.SNMPSetter;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.PDU;
import org.snmp4j.ScopedPDU;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Performs a SNMP Set operation based on attributes of incoming FlowFile.
 * Upon each invocation of {@link #onTrigger(ProcessContext, ProcessSession)}
 * method, it will inspect attributes of FlowFile and look for attributes with
 * name formatted as "snmp$OID" to set the attribute value to this OID.
 */
@Tags({"snmp", "set", "oid"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Based on incoming FlowFile attributes, the processor will execute SNMP Set requests." +
        " When founding attributes with name like snmp$<OID>, the processor will atempt to set the value of" +
        " attribute to the corresponding OID given in the attribute name")
public class SetSNMP extends AbstractSNMPProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that have been successfully used to perform SNMP Set are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that failed during the SNMP Set care routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    private SNMPSetter snmpSetter;

    @OnScheduled
    public void initSnmpClient(ProcessContext context) {
        super.initSnmpClient(context);
        snmpSetter = new SNMPSetter(snmpContext.getSnmp(), snmpContext.getTarget());
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession processSession) {
        FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            PDU pdu = createPdu();
            if (SNMPUtils.addVariables(pdu, flowFile.getAttributes(), getLogger())) {
                pdu.setType(PDU.SET);
                processPdu(context, processSession, flowFile, pdu);
            } else {
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                getLogger().warn("No attributes found in the FlowFile to perform SNMP Set");
            }
        }
    }

    private void processPdu(ProcessContext context, ProcessSession processSession, FlowFile flowFile, PDU pdu) {
        try {
            ResponseEvent response = snmpSetter.set(pdu);
            if (response.getResponse() == null) {
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                getLogger().error("Set request timed out or parameters are incorrect.");
                context.yield();
            } else if (response.getResponse().getErrorStatus() == PDU.noError) {
                flowFile = SNMPUtils.updateFlowFileAttributesWithPduProperties(pdu, flowFile, processSession);
                processSession.transfer(flowFile, REL_SUCCESS);
                processSession.getProvenanceReporter().send(flowFile, snmpContext.getTarget().getAddress().toString());
            } else {
                final String error = response.getResponse().getErrorStatusText();
                flowFile = SNMPUtils.addAttribute(SNMPUtils.SNMP_PROP_PREFIX + "error", error, flowFile, processSession);
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                getLogger().error("Failed while executing SNMP Set [{}] via {}. Error = {}", response.getRequest().getVariableBindings(), snmpSetter, error);
            }
        } catch (IOException e) {
            processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
            getLogger().error("Failed while executing SNMP Set via " + snmpSetter, e);
            context.yield();
        }
    }

    private PDU createPdu() {
        if (snmpContext.getTarget().getVersion() == SnmpConstants.version3) {
            return new ScopedPDU();
        } else {
            return new PDU();
        }
    }

    /**
     * @see org.apache.nifi.components.AbstractConfigurableComponent#getSupportedPropertyDescriptors()
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return BASIC_PROPERTIES;
    }

    /**
     * @see org.apache.nifi.processor.AbstractSessionFactoryProcessor#getRelationships()
     */
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }
}
