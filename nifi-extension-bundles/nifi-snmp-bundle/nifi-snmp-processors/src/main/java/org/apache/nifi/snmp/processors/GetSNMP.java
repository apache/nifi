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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPTreeResponse;
import org.apache.nifi.snmp.exception.SNMPWalkException;
import org.apache.nifi.snmp.operations.GetSNMPHandler;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.V3SecurityProperties;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.apache.nifi.snmp.validators.OIDValidator;
import org.snmp4j.Target;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Performs an SNMP Get operation based on processor or incoming FlowFile attributes.
 * Upon each invocation of {@link #onTrigger(ProcessContext, ProcessSession)}
 * method, in case of a valid incoming FlowFile, it will inspect the attributes of
 * the FlowFile and look for attributes with name formatted as "snmp$OID" to set the
 * attribute value to this OID.
 * The output {@link FlowFile} won't have any content.
 */
@Tags({"snmp", "get", "oid", "walk"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Retrieves information from SNMP Agent with SNMP Get request and outputs a FlowFile with information" +
        " in attributes and without any content")
@WritesAttributes({
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "<OID>", description = "Response variable binding: OID (e.g. 1.3.6.1.4.1.343) and its value."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "errorIndex", description = "Denotes the variable binding in which the error occured."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "errorStatus", description = "The snmp4j error status of the PDU."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "errorStatusText", description = "The description of error status."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "nonRepeaters", description = "The number of non repeater variable bindings in a GETBULK PDU (currently not supported)."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "requestID", description = "The request ID associated with the PDU."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "type", description = "The snmp4j numeric representation of the type of the PDU."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "typeString", description = "The name of the PDU type."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "textualOid", description = "This attribute will exist if and only if the strategy"
                + " is GET and will be equal to the value given in Textual Oid property.")
})
public class GetSNMP extends AbstractSNMPProcessor {

    // SNMP strategies
    public static final AllowableValue GET = new AllowableValue("GET", "GET",
            "A manager-to-agent request to retrieve the value of a variable. A response with the current value returned.");
    public static final AllowableValue WALK = new AllowableValue("WALK", "WALK",
            "A manager-to-agent request to retrieve the value of multiple variables. Snmp WALK also traverses all subnodes " +
                    "under the specified OID.");

    public static final PropertyDescriptor OID = new PropertyDescriptor.Builder()
            .name("snmp-oid")
            .displayName("OID")
            .description("Each OID (object identifier) identifies a variable that can be read or set via SNMP." +
                    " This value is not taken into account for an input flowfile and will be omitted. Can be set to empty" +
                    "string when the OIDs are provided through flowfile.")
            .addValidator(new OIDValidator())
            .build();

    public static final PropertyDescriptor SNMP_STRATEGY = new PropertyDescriptor.Builder()
            .name("snmp-strategy")
            .displayName("SNMP Strategy")
            .description("SNMP strategy to use (SNMP Get or SNMP Walk)")
            .required(true)
            .allowableValues(GET, WALK)
            .defaultValue(GET.getValue())
            .build();

    public static final PropertyDescriptor TEXTUAL_OID = new PropertyDescriptor.Builder()
            .name("snmp-textual-oid")
            .displayName("Textual OID")
            .description("The textual form of the numeric OID to request. This property is user defined, not processed and appended to " +
                    "the outgoing flowfile.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the SNMP agent are routed to this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot received from the SNMP agent are routed to this relationship.")
            .build();

    protected static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AGENT_HOST,
            AGENT_PORT,
            BasicProperties.SNMP_VERSION,
            BasicProperties.SNMP_COMMUNITY,
            V3SecurityProperties.SNMP_SECURITY_LEVEL,
            V3SecurityProperties.SNMP_SECURITY_NAME,
            V3SecurityProperties.SNMP_AUTH_PROTOCOL,
            V3SecurityProperties.SNMP_AUTH_PASSWORD,
            V3SecurityProperties.SNMP_PRIVACY_PROTOCOL,
            V3SecurityProperties.SNMP_PRIVACY_PASSWORD,
            BasicProperties.SNMP_RETRIES,
            BasicProperties.SNMP_TIMEOUT,
            OID,
            TEXTUAL_OID,
            SNMP_STRATEGY
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private volatile GetSNMPHandler snmpHandler;

    @OnScheduled
    public void init(final ProcessContext context) {
        initSnmpManager(context);
        snmpHandler = new GetSNMPHandler(snmpManager);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) {
        final SNMPStrategy snmpStrategy = SNMPStrategy.valueOf(context.getProperty(SNMP_STRATEGY).getValue());
        final String oid = context.getProperty(OID).getValue();
        final boolean isNewFlowFileCreated;
        FlowFile flowfile = processSession.get();
        if (flowfile == null) {
            isNewFlowFileCreated = true;
            flowfile = processSession.create();
        } else {
            isNewFlowFileCreated = false;
        }

        final Target target = factory.createTargetInstance(getTargetConfiguration(context, flowfile));
        if (SNMPStrategy.GET == snmpStrategy) {
            performSnmpGet(context, processSession, oid, target, flowfile, isNewFlowFileCreated);
        } else if (SNMPStrategy.WALK == snmpStrategy) {
            performSnmpWalk(context, processSession, oid, target, flowfile, isNewFlowFileCreated);
        }
    }

    void performSnmpWalk(final ProcessContext context, final ProcessSession processSession, final String oid,
                         final Target target, FlowFile flowFile, final boolean isNewFlowFileCreated) {

        if (oid != null) {
            String prefixedOid = SNMPUtils.SNMP_PROP_PREFIX + oid;
            flowFile = processSession.putAttribute(flowFile, prefixedOid, "");
        }

        try {
            final Optional<SNMPTreeResponse> optionalResponse = snmpHandler.walk(flowFile.getAttributes(), target);
            if (optionalResponse.isPresent()) {
                final SNMPTreeResponse response = optionalResponse.get();
                response.logErrors(getLogger());
                flowFile = processSession.putAllAttributes(flowFile, response.getAttributes());
                if (isNewFlowFileCreated) {
                    processSession.getProvenanceReporter().receive(flowFile, "/walk");
                } else {
                    processSession.getProvenanceReporter().fetch(flowFile, "/walk");
                }
                processSession.transfer(flowFile, response.isError() ? REL_FAILURE : REL_SUCCESS);
            } else {
                getLogger().warn("No SNMP specific attributes found in flowfile.");
                processSession.transfer(flowFile, REL_FAILURE);
            }
        } catch (SNMPWalkException e) {
            getLogger().error(e.getMessage());
            context.yield();
        }
    }

    void performSnmpGet(final ProcessContext context, final ProcessSession processSession, final String oid,
                        final Target target, FlowFile flowFile, final boolean isNewFlowFileCreated) {
        final String textualOidKey = SNMPUtils.SNMP_PROP_PREFIX + "textualOid";
        final Map<String, String> textualOidMap = Collections.singletonMap(textualOidKey, context.getProperty(TEXTUAL_OID).getValue());

        if (oid != null) {
            String prefixedOid = SNMPUtils.SNMP_PROP_PREFIX + oid;
            flowFile = processSession.putAttribute(flowFile, prefixedOid, "");
        }

        try {
            final Optional<SNMPSingleResponse> optionalResponse = snmpHandler.get(flowFile.getAttributes(), target);
            if (optionalResponse.isPresent()) {
                final SNMPSingleResponse response = optionalResponse.get();
                flowFile = processSession.putAllAttributes(flowFile, textualOidMap);
                handleResponse(context, processSession, flowFile, response, REL_SUCCESS, REL_FAILURE, "/get", isNewFlowFileCreated);
            } else {
                getLogger().warn("No SNMP specific attributes found in flowfile.");
                processSession.transfer(flowFile, REL_FAILURE);
                context.yield();
            }

        } catch (IOException e) {
            getLogger().error("Failed to send request to the agent. Check if the agent supports the used version.", e);
            context.yield();
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    protected String getTargetHost(final ProcessContext processContext, final FlowFile flowFile) {
        return processContext.getProperty(AGENT_HOST).evaluateAttributeExpressions(flowFile).getValue();
    }

    @Override
    protected String getTargetPort(final ProcessContext processContext, final FlowFile flowFile) {
        return processContext.getProperty(AGENT_PORT).evaluateAttributeExpressions(flowFile).getValue();
    }

    private enum SNMPStrategy {
        GET, WALK
    }
}
