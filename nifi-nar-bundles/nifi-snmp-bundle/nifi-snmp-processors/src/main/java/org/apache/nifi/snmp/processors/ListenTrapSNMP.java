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
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.operations.SNMPTrapReceiverHandler;
import org.apache.nifi.snmp.utils.UsmReader;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.V3SecurityProperties;
import org.apache.nifi.snmp.utils.SNMPUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Receiving data from a configured SNMP agent which, upon each invocation of
 * {@link #onTrigger(ProcessContext, ProcessSessionFactory)} method, will construct a
 * {@link FlowFile} containing in its properties the information retrieved.
 * The output {@link FlowFile} won't have any content.
 */
@Tags({"snmp", "listen", "trap"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Receives information from SNMP Agent and outputs a FlowFile with information in attributes and without any content")
@WritesAttribute(attribute = SNMPUtils.SNMP_PROP_PREFIX + "*", description = "Attributes retrieved from the SNMP response. It may include:"
        + " snmp$errorIndex, snmp$errorStatus, snmp$errorStatusText, snmp$nonRepeaters, snmp$requestID, snmp$type, snmp$variableBindings")
@RequiresInstanceClassLoading
public class ListenTrapSNMP extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor SNMP_MANAGER_PORT = new PropertyDescriptor.Builder()
            .name("snmp-manager-port")
            .displayName("SNMP Manager Port")
            .description("The port where the SNMP Manager listens to the incoming traps.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNMP_USM_USERS_JSON_FILE = new PropertyDescriptor.Builder()
            .name("snmp-usm-users-file-path")
            .displayName("SNMP Users JSON File")
            .description("The path of the json file containing the user credentials for SNMPv3. Check Usage for more details.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .dependsOn(BasicProperties.SNMP_VERSION, BasicProperties.SNMP_V3)
            .build();

    public static final PropertyDescriptor SNMP_USM_USERS_JSON = new PropertyDescriptor.Builder()
            .name("snmp-usm-users-json")
            .displayName("SNMP Users JSON")
            .description("The JSON containing the user credentials for SNMPv3. Check Usage for more details.")
            .required(false)
            .dependsOn(BasicProperties.SNMP_VERSION, BasicProperties.SNMP_V3)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    public static final PropertyDescriptor SNMP_USM_SECURITY_NAMES = new PropertyDescriptor.Builder()
            .name("snmp-usm-security-names")
            .displayName("SNMP Users Security Names")
            .description("Security names listed separated by commas in SNMPv3. Check Usage for more details.")
            .required(false)
            .dependsOn(BasicProperties.SNMP_VERSION, BasicProperties.SNMP_V3)
            .dependsOn(V3SecurityProperties.SNMP_SECURITY_LEVEL, V3SecurityProperties.NO_AUTH_NO_PRIV)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the SNMP agent are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot received from the SNMP agent are routed to this relationship")
            .build();

    protected static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            SNMP_MANAGER_PORT,
            BasicProperties.SNMP_VERSION,
            BasicProperties.SNMP_COMMUNITY,
            V3SecurityProperties.SNMP_SECURITY_LEVEL,
            SNMP_USM_USERS_JSON_FILE,
            SNMP_USM_USERS_JSON,
            SNMP_USM_SECURITY_NAMES
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    private volatile SNMPTrapReceiverHandler snmpTrapReceiverHandler;

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final boolean snmpUsersJsonFileIsSet = validationContext.getProperty(SNMP_USM_USERS_JSON_FILE).isSet();
        final boolean snmpUsersJsonIsSet = validationContext.getProperty(SNMP_USM_USERS_JSON).isSet();
        final boolean snmpUsersSecurityNames = validationContext.getProperty(SNMP_USM_SECURITY_NAMES).isSet();

        final boolean isOnlyOneIsSetFromThree = Stream.of(snmpUsersJsonFileIsSet, snmpUsersJsonIsSet, snmpUsersSecurityNames).filter(a -> a).count() == 1;
        final boolean isBothSetOrNotSetFromTwo = snmpUsersJsonFileIsSet && snmpUsersJsonIsSet || !snmpUsersJsonFileIsSet && !snmpUsersJsonIsSet;
        final boolean isSnmpv3 = validationContext.getProperty(BasicProperties.SNMP_VERSION).getValue().equals(BasicProperties.SNMP_V3.getValue());
        final boolean isNoAuthNoPriv = validationContext.getProperty(V3SecurityProperties.SNMP_SECURITY_LEVEL).getValue().equals(V3SecurityProperties.NO_AUTH_NO_PRIV.getValue());

        if (isSnmpv3 && isNoAuthNoPriv && isOnlyOneIsSetFromThree) {
            results.add(new ValidationResult.Builder()
                    .subject("Provide snmpv3 USM Users JSON.")
                    .valid(false)
                    .explanation("Only one of '" + SNMP_USM_USERS_JSON_FILE.getDisplayName() + "' or '" +
                            SNMP_USM_USERS_JSON.getDisplayName() + "' or '" + SNMP_USM_SECURITY_NAMES.getDisplayName() + "' should be set!")
                    .build());
        } else if (isSnmpv3 && isBothSetOrNotSetFromTwo) {
            results.add(new ValidationResult.Builder()
                    .subject("Provide snmpv3 USM Users JSON.")
                    .valid(false)
                    .explanation("Only one of '" + SNMP_USM_USERS_JSON_FILE.getDisplayName() + "' or '" +
                            SNMP_USM_USERS_JSON.getDisplayName() + "' should be set!")
                    .build());
        }

        return results;
    }

    @OnScheduled
    public void initSnmpManager(ProcessContext context) {
        final int version = SNMPUtils.getVersion(context.getProperty(BasicProperties.SNMP_VERSION).getValue());
        final int managerPort = context.getProperty(SNMP_MANAGER_PORT).asInteger();
        final String securityLevel = context.getProperty(V3SecurityProperties.SNMP_SECURITY_LEVEL).getValue();
        final String usmUsersJsonFile = context.getProperty(SNMP_USM_USERS_JSON_FILE).getValue();
        final String usmUsersJson = context.getProperty(SNMP_USM_USERS_JSON).getValue();
        final String usmSecurityNames = context.getProperty(SNMP_USM_SECURITY_NAMES).getValue();
        SNMPConfiguration configuration;

        configuration = SNMPConfiguration.builder()
                .setManagerPort(managerPort)
                .setVersion(version)
                .setSecurityLevel(securityLevel)
                .setCommunityString(context.getProperty(BasicProperties.SNMP_COMMUNITY).getValue())
                .build();

        if (version == SNMPUtils.getVersion(BasicProperties.SNMP_V3.getValue())) {
            if (securityLevel.equals(V3SecurityProperties.NO_AUTH_NO_PRIV.getValue())) {
                snmpTrapReceiverHandler = new SNMPTrapReceiverHandler(configuration, usmSecurityNames, UsmReader.securityNamesUsmReader());
            } else if (usmUsersJsonFile != null) {
                snmpTrapReceiverHandler = new SNMPTrapReceiverHandler(configuration, usmUsersJsonFile, UsmReader.jsonFileUsmReader());
            } else {
                snmpTrapReceiverHandler = new SNMPTrapReceiverHandler(configuration, usmUsersJson, UsmReader.jsonUsmReader());
            }
        } else {
            snmpTrapReceiverHandler = new SNMPTrapReceiverHandler(configuration);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory processSessionFactory) {
        if (!snmpTrapReceiverHandler.isStarted()) {
            snmpTrapReceiverHandler.createTrapReceiver(processSessionFactory, getLogger());
        }
    }

    @OnStopped
    public void close() {
        if (snmpTrapReceiverHandler != null) {
            snmpTrapReceiverHandler.close();
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
}
