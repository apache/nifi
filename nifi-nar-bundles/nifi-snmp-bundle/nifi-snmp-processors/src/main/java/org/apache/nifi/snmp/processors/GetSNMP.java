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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPTreeResponse;
import org.apache.nifi.snmp.exception.SNMPException;
import org.apache.nifi.snmp.exception.SNMPWalkException;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.apache.nifi.snmp.validators.OIDValidator;

import java.io.IOException;
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

    // OID to request (if walk, it is the root ID of the request).
    public static final PropertyDescriptor OID = new PropertyDescriptor.Builder()
            .name("snmp-oid")
            .displayName("OID")
            .description("Each OID (object identifier) identifies a variable that can be read or set via SNMP.")
            .required(true)
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
            .description("The textual OID to request.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(null)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the SNMP agent are routed to this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot received from the SNMP agent are routed to this relationship.")
            .build();

    protected static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
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
            SNMP_TIMEOUT,
            OID,
            TEXTUAL_OID,
            SNMP_STRATEGY
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) {
        final SNMPStrategy snmpStrategy = SNMPStrategy.valueOf(context.getProperty(SNMP_STRATEGY).getValue());
        final String oid = context.getProperty(OID).getValue();

        if (SNMPStrategy.GET == snmpStrategy) {
            performSnmpGet(context, processSession, oid);
        } else if (SNMPStrategy.WALK == snmpStrategy) {
            performSnmpWalk(context, processSession, oid);
        }
    }

    private void performSnmpWalk(final ProcessContext context, final ProcessSession processSession, final String oid) {
        try {
            final SNMPTreeResponse response = snmpRequestHandler.walk(oid);
            response.logErrors(getLogger());
            FlowFile flowFile = createFlowFileWithTreeEventProperties(response, processSession);
            processSession.getProvenanceReporter().receive(flowFile, response.getTargetAddress() + "/" + oid);
            processSession.transfer(flowFile, REL_SUCCESS);
        } catch (SNMPWalkException e) {
            getLogger().error(e.getMessage());
            context.yield();
            processSession.rollback();
        }
    }

    private void performSnmpGet(final ProcessContext context, final ProcessSession processSession, final String oid) {
        final SNMPSingleResponse response;
        try {
            response = snmpRequestHandler.get(oid);
            final FlowFile flowFile = processSession.create();
            addAttribute(SNMPUtils.SNMP_PROP_PREFIX + "textualOid", context.getProperty(TEXTUAL_OID).getValue(), flowFile, processSession);
            final String provenanceAddress = response.getTargetAddress() + "/" + oid;
            processResponse(processSession, flowFile, response, provenanceAddress, REL_SUCCESS);
        } catch (SNMPException e) {
            getLogger().error(e.getMessage());
            context.yield();
            processSession.rollback();
        } catch (IOException e) {
            getLogger().error("Failed to send request to the agent. Check if the agent supports the used version.");
            context.yield();
            processSession.rollback();
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

    private FlowFile createFlowFileWithTreeEventProperties(final SNMPTreeResponse response, final ProcessSession processSession) {
        FlowFile flowFile = processSession.create();
        flowFile = processSession.putAllAttributes(flowFile, response.getAttributes());
        return flowFile;
    }

    private enum SNMPStrategy {
        GET, WALK
    }
}
