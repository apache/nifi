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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.snmp4j.PDU;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.smi.OID;
import org.snmp4j.util.TreeEvent;

/**
 * Retrieving data from configured SNMP agent which, upon each invocation of
 * {@link #onTrigger(ProcessContext, ProcessSession)} method, will construct a
 * {@link FlowFile} containing in its properties the information retrieved.
 * The output {@link FlowFile} won't have any content.
 */
@Tags({ "snmp", "get", "oid", "walk" })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Retrieves information from SNMP Agent and outputs a FlowFile with information in attributes and without any content")
public class GetSNMP extends AbstractSNMPProcessor<SNMPGetter> {

    /** OID to request (if walk, it is the root ID of the request) */
    public static final PropertyDescriptor OID = new PropertyDescriptor.Builder()
            .name("snmp-oid")
            .displayName("OID")
            .description("The OID to request")
            .required(true)
            .addValidator(SNMPUtils.SNMP_OID_VALIDATOR)
            .build();

    /** SNMP strategy for SNMP Get processor : simple get or walk */
    public static final PropertyDescriptor SNMP_STRATEGY = new PropertyDescriptor.Builder()
            .name("snmp-strategy")
            .displayName("SNMP strategy (GET/WALK)")
            .description("SNMP strategy to use (SNMP Get or SNMP Walk)")
            .required(true)
            .allowableValues("GET", "WALK")
            .defaultValue("GET")
            .build();

    /** relationship for success */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the SNMP agent are routed to this relationship")
            .build();

    /** relationship for failure */
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot received from the SNMP agent are routed to this relationship")
            .build();

    /** list of property descriptors */
    private final static List<PropertyDescriptor> propertyDescriptors;

    /** list of relationships */
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(OID);
        _propertyDescriptors.add(SNMP_STRATEGY);
        _propertyDescriptors.addAll(descriptors);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * Delegate method to supplement
     * {@link #onTrigger(ProcessContext, ProcessSession)}. It is implemented by
     * sub-classes to perform {@link Processor} specific functionality.
     *
     * @param context
     *            instance of {@link ProcessContext}
     * @param processSession
     *            instance of {@link ProcessSession}
     * @throws ProcessException Process exception
     */
    @Override
    protected void onTriggerSnmp(ProcessContext context, ProcessSession processSession) throws ProcessException {
        if("GET".equals(context.getProperty(SNMP_STRATEGY).getValue())) {
            final ResponseEvent response = this.targetResource.get();
            if (response.getResponse() != null){
                FlowFile flowFile = processSession.create();
                PDU pdu = response.getResponse();
                flowFile = SNMPUtils.updateFlowFileAttributesWithPduProperties(pdu, flowFile, processSession);
                processSession.getProvenanceReporter().receive(flowFile,
                        this.snmpTarget.getAddress().toString() + "/" + context.getProperty(OID).getValue());
                if(pdu.getErrorStatus() == PDU.noError) {
                    processSession.transfer(flowFile, REL_SUCCESS);
                } else {
                    processSession.transfer(flowFile, REL_FAILURE);
                }
            } else {
                this.getLogger().error("Get request timed out or parameters are incorrect.");
                context.yield();
            }
        } else if("WALK".equals(context.getProperty(SNMP_STRATEGY).getValue())) {
            final List<TreeEvent> events = this.targetResource.walk();
            if((events != null) && !events.isEmpty() && (events.get(0).getVariableBindings() != null)) {
                FlowFile flowFile = processSession.create();
                for (TreeEvent treeEvent : events) {
                    flowFile = SNMPUtils.updateFlowFileAttributesWithTreeEventProperties(treeEvent, flowFile, processSession);
                }
                processSession.getProvenanceReporter().receive(flowFile,
                        this.snmpTarget.getAddress().toString() + "/" + context.getProperty(OID).getValue());
                processSession.transfer(flowFile, REL_SUCCESS);
            } else {
                this.getLogger().error("Get request timed out or parameters are incorrect.");
                context.yield();
            }
        }
    }

    /**
     * Will create an instance of {@link SNMPGetter}
     */
    @Override
    protected SNMPGetter finishBuildingTargetResource(ProcessContext context) {
        String oid = context.getProperty(OID).getValue();
        return new SNMPGetter(this.snmp, this.snmpTarget, new OID(oid));
    }

    /**
     * get list of supported property descriptors
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     * get list of relationships
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
}
