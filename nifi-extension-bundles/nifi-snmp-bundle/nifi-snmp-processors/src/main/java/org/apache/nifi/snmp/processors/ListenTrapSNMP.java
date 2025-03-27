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
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.operations.SNMPTrapReceiverHandler;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.UsmUserInputMethod;
import org.apache.nifi.snmp.processors.properties.V3SecurityProperties;
import org.apache.nifi.snmp.utils.JsonFileUsmReader;
import org.apache.nifi.snmp.utils.JsonUsmReader;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.apache.nifi.snmp.utils.SecurityNamesUsmReader;
import org.apache.nifi.snmp.utils.UsmReader;
import org.snmp4j.security.UsmUser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
public class ListenTrapSNMP extends AbstractSessionFactoryProcessor implements VerifiableProcessor {

    public static final PropertyDescriptor SNMP_MANAGER_PORT = new PropertyDescriptor.Builder()
            .name("snmp-manager-port")
            .displayName("SNMP Manager Port")
            .description("The port where the SNMP Manager listens to the incoming traps.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNMP_USM_USER_INPUT_METHOD = new PropertyDescriptor.Builder()
            .name("snmp-usm-users-source")
            .displayName("USM Users Input Method")
            .description("Specifies how USM user data is provided.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(UsmUserInputMethod.class)
            .dependsOn(BasicProperties.SNMP_VERSION, BasicProperties.SNMP_V3)
            .build();

    public static final PropertyDescriptor SNMP_USM_USERS_JSON_FILE_PATH = new PropertyDescriptor.Builder()
            .name("snmp-usm-users-file-path")
            .displayName("USM Users JSON File Path")
            .description("The path of the json file containing the user credentials for SNMPv3. Check Usage for more details.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .dependsOn(BasicProperties.SNMP_VERSION, BasicProperties.SNMP_V3)
            .dependsOn(SNMP_USM_USER_INPUT_METHOD, UsmUserInputMethod.USM_JSON_FILE_PATH)
            .build();

    public static final PropertyDescriptor SNMP_USM_USERS_JSON = new PropertyDescriptor.Builder()
            .name("snmp-usm-users-json-content")
            .displayName("USM Users JSON content")
            .description("The JSON containing the user credentials for SNMPv3. Check Usage for more details.")
            .required(false)
            .sensitive(true)
            .dependsOn(BasicProperties.SNMP_VERSION, BasicProperties.SNMP_V3)
            .dependsOn(SNMP_USM_USER_INPUT_METHOD, UsmUserInputMethod.USM_JSON_CONTENT)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    public static final PropertyDescriptor SNMP_USM_SECURITY_NAMES = new PropertyDescriptor.Builder()
            .name("snmp-usm-security-names")
            .displayName("SNMP Users Security Names")
            .description("Security names listed separated by commas in SNMPv3. Check Usage for more details.")
            .required(false)
            .dependsOn(BasicProperties.SNMP_VERSION, BasicProperties.SNMP_V3)
            .dependsOn(V3SecurityProperties.SNMP_SECURITY_LEVEL, V3SecurityProperties.NO_AUTH_NO_PRIV)
            .dependsOn(SNMP_USM_USER_INPUT_METHOD, UsmUserInputMethod.USM_SECURITY_NAMES)
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

    protected static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SNMP_MANAGER_PORT,
            BasicProperties.SNMP_VERSION,
            BasicProperties.SNMP_COMMUNITY,
            V3SecurityProperties.SNMP_SECURITY_LEVEL,
            SNMP_USM_USER_INPUT_METHOD,
            SNMP_USM_USERS_JSON_FILE_PATH,
            SNMP_USM_USERS_JSON,
            SNMP_USM_SECURITY_NAMES
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private volatile SNMPTrapReceiverHandler snmpTrapReceiverHandler;
    private volatile List<UsmUser> usmUsers;


    @Override
    public List<ConfigVerificationResult> verify(ProcessContext context, ComponentLog verificationLogger, Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final String userInputMethodValue = context.getProperty(SNMP_USM_USER_INPUT_METHOD).getValue();
        if (userInputMethodValue != null) {
            final UsmUserInputMethod usmUserInputMethod = UsmUserInputMethod.fromValue(context.getProperty(SNMP_USM_USER_INPUT_METHOD).getValue());
            final UsmReader usmReader = getUsmReader(context, usmUserInputMethod);
            try {
                usmReader.readUsm();
            } catch (Exception e) {
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("USM User processing")
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(e.getMessage() + " " + e.getCause().getMessage())
                        .build());
            }
        }
        return results;
    }

    @OnScheduled
    public void initSnmpManager(ProcessContext context) {
        final int version = SNMPUtils.getVersion(context.getProperty(BasicProperties.SNMP_VERSION).getValue());
        final int managerPort = context.getProperty(SNMP_MANAGER_PORT).asInteger();
        final String securityLevel = context.getProperty(V3SecurityProperties.SNMP_SECURITY_LEVEL).getValue();

        final SNMPConfiguration configuration = SNMPConfiguration.builder()
                .setManagerPort(managerPort)
                .setVersion(version)
                .setSecurityLevel(securityLevel)
                .setCommunityString(context.getProperty(BasicProperties.SNMP_COMMUNITY).getValue())
                .build();

        final String userInputMethodValue = context.getProperty(SNMP_USM_USER_INPUT_METHOD).getValue();

        if (userInputMethodValue != null) {
            final UsmUserInputMethod usmUserInputMethod = UsmUserInputMethod.fromValue(context.getProperty(SNMP_USM_USER_INPUT_METHOD).getValue());
            final UsmReader usmReader = getUsmReader(context, usmUserInputMethod);
            usmUsers = usmReader.readUsm();
        }

        snmpTrapReceiverHandler = new SNMPTrapReceiverHandler(configuration, usmUsers);
    }

    public int getListeningPort() {
        if (snmpTrapReceiverHandler == null || !snmpTrapReceiverHandler.isStarted()) {
            return 0;
        }

        return snmpTrapReceiverHandler.getListeningPort();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory processSessionFactory) {
        if (!snmpTrapReceiverHandler.isStarted()) {
            snmpTrapReceiverHandler.createTrapReceiver(processSessionFactory, getLogger());
        }
        context.yield();
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

    private UsmReader getUsmReader(ProcessContext context, UsmUserInputMethod usmUserInputMethod) {
        final String usmUsersJsonFilePath = context.getProperty(SNMP_USM_USERS_JSON_FILE_PATH).getValue();
        final String usmUsersJson = context.getProperty(SNMP_USM_USERS_JSON).getValue();
        final String usmSecurityNames = context.getProperty(SNMP_USM_SECURITY_NAMES).getValue();

        return switch (usmUserInputMethod) {
            case USM_JSON_FILE_PATH -> new JsonFileUsmReader(usmUsersJsonFilePath);
            case USM_JSON_CONTENT -> new JsonUsmReader(usmUsersJson);
            case USM_SECURITY_NAMES -> new SecurityNamesUsmReader(usmSecurityNames);
        };
    }
}
