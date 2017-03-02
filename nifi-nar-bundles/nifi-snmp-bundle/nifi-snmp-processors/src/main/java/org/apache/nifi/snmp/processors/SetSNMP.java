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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.snmp4j.PDU;
import org.snmp4j.ScopedPDU;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.AbstractVariable;
import org.snmp4j.smi.AssignableFromInteger;
import org.snmp4j.smi.AssignableFromLong;
import org.snmp4j.smi.AssignableFromString;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;

/**
 * Performs a SNMP Set operation based on attributes of incoming FlowFile.
 * Upon each invocation of {@link #onTrigger(ProcessContext, ProcessSession)}
 * method, it will inspect attributes of FlowFile and look for attributes with
 * name formatted as "snmp$OID" to set the attribute value to this OID.
 */
@Tags({ "snmp", "set", "oid" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Based on incoming FlowFile attributes, the processor will execute SNMP Set requests." +
        " When founding attributes with name like snmp$<OID>, the processor will atempt to set the value of" +
        " attribute to the corresponding OID given in the attribute name")
public class SetSNMP extends AbstractSNMPProcessor<SNMPSetter> {

    /** relationship for success */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that have been successfully used to perform SNMP Set are routed to this relationship")
            .build();
    /** relationship for failure */
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that failed during the SNMP Set care routed to this relationship")
            .build();

    /** list of properties descriptors */
    private final static List<PropertyDescriptor> propertyDescriptors;

    /** list of relationships */
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * @see org.apache.nifi.snmp.processors.AbstractSNMPProcessor#onTriggerSnmp(org.apache.nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
     */
    @Override
    protected void onTriggerSnmp(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            // Create the PDU object
            PDU pdu = null;
            if(this.snmpTarget.getVersion() == SnmpConstants.version3) {
                pdu = new ScopedPDU();
            } else {
                pdu = new PDU();
            }
            if(this.addVariables(pdu, flowFile.getAttributes())) {
                pdu.setType(PDU.SET);
                try {
                    ResponseEvent response = this.targetResource.set(pdu);
                    if(response.getResponse() == null) {
                        processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                        this.getLogger().error("Set request timed out or parameters are incorrect.");
                        context.yield();
                    } else if(response.getResponse().getErrorStatus() == PDU.noError) {
                        flowFile = SNMPUtils.updateFlowFileAttributesWithPduProperties(pdu, flowFile, processSession);
                        processSession.transfer(flowFile, REL_SUCCESS);
                        processSession.getProvenanceReporter().send(flowFile, this.snmpTarget.getAddress().toString());
                    } else {
                        final String error = response.getResponse().getErrorStatusText();
                        flowFile = SNMPUtils.addAttribute(SNMPUtils.SNMP_PROP_PREFIX + "error", error, flowFile, processSession);
                        processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                        this.getLogger().error("Failed while executing SNMP Set [{}] via " + this.targetResource + ". Error = {}", new Object[]{response.getRequest().getVariableBindings(), error});
                    }
                } catch (IOException e) {
                    processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                    this.getLogger().error("Failed while executing SNMP Set via " + this.targetResource, e);
                    context.yield();
                }
            } else {
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                this.getLogger().warn("No attributes found in the FlowFile to perform SNMP Set");
            }
        }
    }

    /**
     * Method to construct {@link VariableBinding} based on {@link FlowFile}
     * attributes in order to update the {@link PDU} that is going to be sent to
     * the SNMP Agent.
     * @param pdu {@link PDU} to be sent
     * @param attributes {@link FlowFile} attributes
     * @return true if at least one {@link VariableBinding} has been created, false otherwise
     */
    private boolean addVariables(PDU pdu, Map<String, String> attributes) {
        boolean result = false;
        for (Entry<String, String> attributeEntry : attributes.entrySet()) {
            if (attributeEntry.getKey().startsWith(SNMPUtils.SNMP_PROP_PREFIX)) {
                String[] splits = attributeEntry.getKey().split("\\" + SNMPUtils.SNMP_PROP_DELIMITER);
                String snmpPropName = splits[1];
                String snmpPropValue = attributeEntry.getValue();
                if(SNMPUtils.OID_PATTERN.matcher(snmpPropName).matches()) {
                    Variable var = null;
                    if (splits.length == 2) { // no SMI syntax defined
                        var = new OctetString(snmpPropValue);
                    } else {
                        int smiSyntax = Integer.valueOf(splits[2]);
                        var = this.stringToVariable(snmpPropValue, smiSyntax);
                    }
                    if(var != null) {
                        VariableBinding varBind = new VariableBinding(new OID(snmpPropName), var);
                        pdu.add(varBind);
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Method to create the variable from the attribute value and the given SMI syntax value
     * @param value attribute value
     * @param smiSyntax attribute SMI Syntax
     * @return variable
     */
    private Variable stringToVariable(String value, int smiSyntax) {
        Variable var = AbstractVariable.createFromSyntax(smiSyntax);
        try {
            if (var instanceof AssignableFromString) {
                ((AssignableFromString) var).setValue(value);
            } else if (var instanceof AssignableFromInteger) {
                ((AssignableFromInteger) var).setValue(Integer.valueOf(value));
            } else if (var instanceof AssignableFromLong) {
                ((AssignableFromLong) var).setValue(Long.valueOf(value));
            } else {
                this.getLogger().error("Unsupported conversion of [" + value +"] to " + var.getSyntaxString());
                var = null;
            }
        } catch (IllegalArgumentException e) {
            this.getLogger().error("Unsupported conversion of [" + value +"] to " + var.getSyntaxString(), e);
            var = null;
        }
        return var;
    }

    /**
     * @see org.apache.nifi.components.AbstractConfigurableComponent#getSupportedPropertyDescriptors()
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     * @see org.apache.nifi.processor.AbstractSessionFactoryProcessor#getRelationships()
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * @see org.apache.nifi.snmp.processors.AbstractSNMPProcessor#finishBuildingTargetResource(org.apache.nifi.processor.ProcessContext)
     */
    @Override
    protected SNMPSetter finishBuildingTargetResource(ProcessContext context) {
        return new SNMPSetter(this.snmp, this.snmpTarget);
    }

}
