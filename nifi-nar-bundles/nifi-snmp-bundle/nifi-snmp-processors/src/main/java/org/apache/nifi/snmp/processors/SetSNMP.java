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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.exception.SNMPException;
import org.apache.nifi.snmp.utils.SNMPUtils;

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
        " When finding attributes with the name snmp$<OID>, the processor will attempt to set the value of" +
        " the attribute to the corresponding OID given in the attribute name")
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

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            AGENT_HOST,
            AGENT_PORT,
            SNMP_VERSION,
            SNMP_COMMUNITY,
            SNMP_SECURITY_LEVEL,
            SNMP_SECURITY_NAME,
            SNMP_AUTH_PROTOCOL,
            SNMP_AUTH_PASSWORD,
            SNMP_PRIVACY_PROTOCOL,
            SNMP_PRIVACY_PASSWORD,
            SNMP_RETRIES,
            SNMP_TIMEOUT
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) {
        final FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            try {
                final SNMPSingleResponse response = snmpRequestHandler.set(flowFile);
                processResponse(processSession, flowFile, response, response.getTargetAddress(), REL_SUCCESS);
            } catch (SNMPException e) {
                getLogger().error(e.getMessage());
                processError(context, processSession, flowFile);
            } catch (IOException e) {
                getLogger().error("Failed to send request to the agent. Check if the agent supports the used version.");
                processError(context, processSession, flowFile);
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


    private void processError(final ProcessContext context, final ProcessSession processSession, final FlowFile flowFile) {
        processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
        context.yield();
    }
}
