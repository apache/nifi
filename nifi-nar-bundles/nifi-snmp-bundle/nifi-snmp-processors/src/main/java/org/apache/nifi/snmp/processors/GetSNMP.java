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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.operations.SNMPGetter;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.PDU;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.smi.OID;
import org.snmp4j.util.TreeEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Retrieving data from configured SNMP agent which, upon each invocation of
 * {@link #onTrigger(ProcessContext, ProcessSession)} method, will construct a
 * {@link FlowFile} containing in its properties the information retrieved.
 * The output {@link FlowFile} won't have any content.
 */
@Tags({"snmp", "get", "oid", "walk"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Retrieves information from SNMP Agent and outputs a FlowFile with information in attributes and without any content")
@WritesAttributes({
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "*", description = "Attributes retrieved from the SNMP response. It may include:"
                + " snmp$errorIndex, snmp$errorStatus, snmp$errorStatusText, snmp$nonRepeaters, snmp$requestID, snmp$type, snmp$variableBindings"),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "textualOid", description = "This attribute will exist if and only if the strategy"
                + " is GET and will be equal to the value given in Textual Oid property.")
})
public class GetSNMP extends AbstractSNMPProcessor {

    // OID to request (if walk, it is the root ID of the request).
    public static final PropertyDescriptor OID = new PropertyDescriptor.Builder()
            .name("snmp-oid")
            .displayName("OID")
            .description("The OID to request")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Textual OID to request.
    public static final PropertyDescriptor TEXTUAL_OID = new PropertyDescriptor.Builder()
            .name("snmp-textual-oid")
            .displayName("Textual OID")
            .description("The textual OID to request")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(null)
            .build();

    // SNMP strategy for SNMP Get processor: GET or WALK.
    public static final PropertyDescriptor SNMP_STRATEGY = new PropertyDescriptor.Builder()
            .name("snmp-strategy")
            .displayName("SNMP strategy (GET/WALK)")
            .description("SNMP strategy to use (SNMP Get or SNMP Walk)")
            .required(true)
            .allowableValues("GET", "WALK")
            .defaultValue("GET")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the SNMP agent are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot received from the SNMP agent are routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = createPropertyList();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    private SNMPGetter snmpGetter;

    @OnScheduled
    @Override
    public void initSnmpClient(ProcessContext context) {
        super.initSnmpClient(context);
        String oid = context.getProperty(OID).getValue();
        snmpGetter = new SNMPGetter(snmpContext.getSnmp(), snmpContext.getTarget(), new OID(oid));
    }

    /**
     * Delegate method to supplement
     * {@link #onTrigger(ProcessContext, ProcessSession)}. It is implemented by
     * sub-classes to perform {@link Processor} specific functionality.
     *
     * @param context        instance of {@link ProcessContext}
     * @param processSession instance of {@link ProcessSession}
     * @throws ProcessException Process exception
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession processSession) {
        final String targetUri = snmpContext.getTarget().getAddress().toString();
        final String snmpStrategy = context.getProperty(SNMP_STRATEGY).getValue();
        final String oid = context.getProperty(OID).getValue();

        if ("GET".equals(snmpStrategy)) {
            final ResponseEvent response = snmpGetter.get();
            if (response.getResponse() != null) {
                PDU pdu = response.getResponse();
                FlowFile flowFile = createFlowFile(context, processSession, pdu);
                processSession.getProvenanceReporter().receive(flowFile, targetUri + "/" + oid);
                if (pdu.getErrorStatus() == PDU.noError) {
                    processSession.transfer(flowFile, REL_SUCCESS);
                } else {
                    processSession.transfer(flowFile, REL_FAILURE);
                }
            } else {
                getLogger().error("Get request timed out or parameters are incorrect.");
                context.yield();
            }
        } else if ("WALK".equals(snmpStrategy)) {
            final List<TreeEvent> events = snmpGetter.walk();
            if (areValidEvents(events)) {
                FlowFile flowFile = processSession.create();
                for (TreeEvent treeEvent : events) {
                    flowFile = SNMPUtils.updateFlowFileAttributesWithTreeEventProperties(treeEvent, flowFile, processSession);
                }
                processSession.getProvenanceReporter().receive(flowFile, targetUri + "/" + oid);
                processSession.transfer(flowFile, REL_SUCCESS);
            } else {
                getLogger().error("Get request timed out or parameters are incorrect.");
                context.yield();
            }
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Creates a list of the base class' and the current properties.
     *
     * @return a list of properties
     */
    private static List<PropertyDescriptor> createPropertyList() {
        List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
        propertyDescriptors.addAll(BASIC_PROPERTIES);
        propertyDescriptors.addAll(Arrays.asList(OID, TEXTUAL_OID, SNMP_STRATEGY));
        return Collections.unmodifiableList(propertyDescriptors);
    }

    private FlowFile createFlowFile(ProcessContext context, ProcessSession processSession, PDU pdu) {
        FlowFile flowFile = processSession.create();
        flowFile = SNMPUtils.updateFlowFileAttributesWithPduProperties(pdu, flowFile, processSession);
        flowFile = SNMPUtils.addAttribute(SNMPUtils.SNMP_PROP_PREFIX + "textualOid", context.getProperty(TEXTUAL_OID).getValue(),
                flowFile, processSession);
        return flowFile;
    }

    private boolean areValidEvents(List<TreeEvent> events) {
        return (events != null) && !events.isEmpty() && (events.get(0).getVariableBindings() != null);
    }
}
