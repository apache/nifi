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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.operations.SNMPTrapReceiverHandler;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.V3SecurityProperties;
import org.apache.nifi.snmp.utils.SNMPUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
public class ListenTrapSNMP extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor SNMP_MANAGER_PORT = new PropertyDescriptor.Builder()
            .name("snmp-manager-port")
            .displayName("SNMP Manager Port")
            .description("The port where the SNMP Manager listens to the incoming traps.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNMP_USM_USERS_FILE_PATH = new PropertyDescriptor.Builder()
            .name("snmp-usm-users-file-path")
            .displayName("SNMP Users File Path")
            .description("The path of the json file containing the user credentials for SNMPv3. Check Usage for more details.")
            .required(true)
            .defaultValue("")
            .dependsOn(BasicProperties.SNMP_VERSION, BasicProperties.SNMP_V3)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
            SNMP_USM_USERS_FILE_PATH
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    private volatile SNMPTrapReceiverHandler snmpTrapReceiverHandler;

    @OnScheduled
    public void initSnmpManager(ProcessContext context) {
        final int version = SNMPUtils.getVersion(context.getProperty(BasicProperties.SNMP_VERSION).getValue());
        final int managerPort = context.getProperty(SNMP_MANAGER_PORT).asInteger();
        final String usmUsersFilePath = context.getProperty(SNMP_USM_USERS_FILE_PATH).getValue();
        SNMPConfiguration configuration;

        configuration = SNMPConfiguration.builder()
                .setManagerPort(managerPort)
                .setVersion(version)
                .setSecurityLevel(context.getProperty(V3SecurityProperties.SNMP_SECURITY_LEVEL).getValue())
                .setCommunityString(context.getProperty(BasicProperties.SNMP_COMMUNITY).getValue())
                .build();

        snmpTrapReceiverHandler = new SNMPTrapReceiverHandler(configuration, usmUsersFilePath);
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
