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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.operations.SetSNMPHandler;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.V3SecurityProperties;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.Target;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Performs an SNMP Set operation based on attributes of incoming FlowFile.
 * Upon each invocation of {@link #onTrigger(ProcessContext, ProcessSession)}
 * method, it will inspect attributes of FlowFile and look for attributes with
 * name formatted as "snmp$OID" to set the attribute value to this OID.
 * The output {@link FlowFile} won't have any content.
 */
@Tags({"snmp", "set", "oid"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Based on incoming FlowFile attributes, the processor will execute SNMP Set requests." +
        " When finding attributes with the name snmp$<OID>, the processor will attempt to set the value of" +
        " the attribute to the corresponding OID given in the attribute name.")
@WritesAttributes({
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "<OID>", description = "Response variable binding: OID (e.g. 1.3.6.1.4.1.343) and its value."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "errorIndex", description = "Denotes the variable binding in which the error occured."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "errorStatus", description = "The snmp4j error status of the PDU."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "errorStatusText", description = "The description of error status."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "nonRepeaters", description = "The number of non repeater variable bindings in a GETBULK PDU (currently not supported)."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "requestID", description = "The request ID associated with the PDU."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "type", description = "The snmp4j numeric representation of the type of the PDU."),
        @WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "typeString", description = "The name of the PDU type."),
})
public class SetSNMP extends AbstractSNMPProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that have been successfully used to perform SNMP Set are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that failed during the SNMP Set care routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
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
            BasicProperties.SNMP_TIMEOUT
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private volatile SetSNMPHandler snmpHandler;

    @OnScheduled
    public void init(final ProcessContext context) {
        initSnmpManager(context);
        snmpHandler = new SetSNMPHandler(snmpManager);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) {
        final FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            try {
                final Target target = factory.createTargetInstance(getTargetConfiguration(context, flowFile));
                final Optional<SNMPSingleResponse> optionalResponse = snmpHandler.set(flowFile.getAttributes(), target);
                if (optionalResponse.isPresent()) {
                    processSession.remove(flowFile);
                    final FlowFile outgoingFlowFile = processSession.create();
                    final SNMPSingleResponse response = optionalResponse.get();
                    handleResponse(context, processSession, outgoingFlowFile, response, REL_SUCCESS, REL_FAILURE, "/set", true);
                } else {
                    getLogger().warn("No SNMP specific attributes found in flowfile.");
                    processSession.transfer(flowFile, REL_FAILURE);
                    processSession.getProvenanceReporter().receive(flowFile, "/set");
                }
            } catch (IOException e) {
                getLogger().error("Failed to send request to the agent. Check if the agent supports the used version.");
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                context.yield();
            }
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    protected String getTargetHost(final ProcessContext processContext, final FlowFile flowFile) {
        return processContext.getProperty(AGENT_HOST).evaluateAttributeExpressions(flowFile).getValue();
    }

    @Override
    protected String getTargetPort(final ProcessContext processContext, final FlowFile flowFile) {
        return processContext.getProperty(AGENT_PORT).evaluateAttributeExpressions(flowFile).getValue();
    }
}
