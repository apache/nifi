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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.configuration.V1TrapConfiguration;
import org.apache.nifi.snmp.configuration.V2TrapConfiguration;
import org.apache.nifi.snmp.operations.SendTrapSNMPHandler;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.V1TrapProperties;
import org.apache.nifi.snmp.processors.properties.V2TrapProperties;
import org.apache.nifi.snmp.processors.properties.V3SecurityProperties;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.mp.SnmpConstants;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sends data to an SNMP manager which, upon each invocation of
 * {@link #onTrigger(ProcessContext, ProcessSession)} method, will construct a
 * {@link FlowFile} containing in its properties the information retrieved.
 * The output {@link FlowFile} won't have any content.
 */
@Tags({"snmp", "send", "trap"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Sends information to SNMP Manager.")
public class SendTrapSNMP extends AbstractSNMPProcessor {

    public static final PropertyDescriptor SNMP_MANAGER_HOST = new PropertyDescriptor.Builder()
            .name("snmp-trap-manager-host")
            .displayName("SNMP Manager Host")
            .description("The host where the SNMP Manager sends the trap.")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNMP_MANAGER_PORT = new PropertyDescriptor.Builder()
            .name("snmp-trap-manager-port")
            .displayName("SNMP Manager Port")
            .description("The port where the SNMP Manager listens to the incoming traps.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that have been successfully used to perform SNMP Set are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot received from the SNMP agent are routed to this relationship")
            .build();

    protected static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            SNMP_MANAGER_HOST,
            SNMP_MANAGER_PORT,
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
            V1TrapProperties.ENTERPRISE_OID,
            V1TrapProperties.AGENT_ADDRESS,
            V1TrapProperties.GENERIC_TRAP_TYPE,
            V1TrapProperties.SPECIFIC_TRAP_TYPE,
            V2TrapProperties.TRAP_OID_VALUE
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS, REL_FAILURE
    )));

    private volatile SendTrapSNMPHandler snmpHandler;

    @OnScheduled
    public void init(ProcessContext context) {
        Instant startTime = Instant.now();
        initSnmpManager(context);
        snmpHandler = new SendTrapSNMPHandler(snmpResourceHandler, startTime, getLogger());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) {
        final FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            try {
                final int snmpVersion = SNMPUtils.getVersion(context.getProperty(BasicProperties.SNMP_VERSION).getValue());
                if (SnmpConstants.version1 == snmpVersion) {
                    V1TrapConfiguration v1TrapConfiguration = V1TrapConfiguration.builder()
                            .enterpriseOid(context.getProperty(V1TrapProperties.ENTERPRISE_OID).evaluateAttributeExpressions(flowFile).getValue())
                            .agentAddress(context.getProperty(V1TrapProperties.AGENT_ADDRESS).evaluateAttributeExpressions(flowFile).getValue())
                            .genericTrapType(context.getProperty(V1TrapProperties.GENERIC_TRAP_TYPE).evaluateAttributeExpressions(flowFile).getValue())
                            .specificTrapType(context.getProperty(V1TrapProperties.SPECIFIC_TRAP_TYPE).evaluateAttributeExpressions(flowFile).getValue())
                            .build();
                    snmpHandler.sendTrap(flowFile.getAttributes(), v1TrapConfiguration);
                } else {
                    V2TrapConfiguration v2TrapConfiguration = new V2TrapConfiguration(
                            context.getProperty(V2TrapProperties.TRAP_OID_VALUE).evaluateAttributeExpressions(flowFile).getValue()
                    );
                    snmpHandler.sendTrap(flowFile.getAttributes(), v2TrapConfiguration);
                }
                processSession.transfer(flowFile, REL_SUCCESS);
            } catch (IOException e) {
                getLogger().error("Failed to send request to the agent. Check if the agent supports the used version.", e);
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
            } catch (IllegalArgumentException e) {
                getLogger().error("Invalid trap configuration.", e);
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
            }
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

    @Override
    protected String getTargetHost(ProcessContext processContext) {
        return processContext.getProperty(SNMP_MANAGER_HOST).getValue();
    }

    @Override
    protected String getTargetPort(ProcessContext processContext) {
        return processContext.getProperty(SNMP_MANAGER_PORT).getValue();
    }
}
