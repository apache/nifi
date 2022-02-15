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

import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.dto.ErrorStatus;
import org.apache.nifi.snmp.dto.SNMPResponseStatus;
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPValue;
import org.apache.nifi.snmp.factory.core.SNMPFactoryProvider;
import org.apache.nifi.snmp.logging.SLF4JLogFactory;
import org.apache.nifi.snmp.operations.SNMPResourceHandler;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.V3SecurityProperties;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.log.LogFactory;
import org.snmp4j.mp.SnmpConstants;

import java.util.Optional;

/**
 * Base processor that uses SNMP4J client API.
 * (http://www.snmp4j.org/)
 */
@RequiresInstanceClassLoading
abstract class AbstractSNMPProcessor extends AbstractProcessor {

    static {
        LogFactory.setLogFactory(new SLF4JLogFactory());
    }

    private static final String NO_SUCH_OBJECT = "noSuchObject";

    public static final PropertyDescriptor AGENT_HOST = new PropertyDescriptor.Builder()
            .name("snmp-hostname")
            .displayName("SNMP Agent Hostname")
            .description("Hostname or network address of the SNMP Agent.")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AGENT_PORT = new PropertyDescriptor.Builder()
            .name("snmp-port")
            .displayName("SNMP Agent Port")
            .description("Port of the SNMP Agent.")
            .required(true)
            .defaultValue("161")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    protected volatile SNMPResourceHandler snmpResourceHandler;

    @OnScheduled
    public void initSnmpManager(final ProcessContext context) {
        final int version = SNMPUtils.getVersion(context.getProperty(BasicProperties.SNMP_VERSION).getValue());
        final SNMPConfiguration configuration;
        final String targetHost = getTargetHost(context);
        final String targetPort = getTargetPort(context);

        configuration = SNMPConfiguration.builder()
                .setTargetHost(targetHost)
                .setTargetPort(targetPort)
                .setRetries(context.getProperty(BasicProperties.SNMP_RETRIES).asInteger())
                .setTimeoutInMs(context.getProperty(BasicProperties.SNMP_TIMEOUT).asInteger())
                .setVersion(version)
                .setAuthProtocol(context.getProperty(V3SecurityProperties.SNMP_AUTH_PROTOCOL).getValue())
                .setAuthPassphrase(context.getProperty(V3SecurityProperties.SNMP_AUTH_PASSWORD).getValue())
                .setPrivacyProtocol(context.getProperty(V3SecurityProperties.SNMP_PRIVACY_PROTOCOL).getValue())
                .setPrivacyPassphrase(context.getProperty(V3SecurityProperties.SNMP_PRIVACY_PASSWORD).getValue())
                .setSecurityName(context.getProperty(V3SecurityProperties.SNMP_SECURITY_NAME).getValue())
                .setSecurityLevel(context.getProperty(V3SecurityProperties.SNMP_SECURITY_LEVEL).getValue())
                .setCommunityString(context.getProperty(BasicProperties.SNMP_COMMUNITY).getValue())
                .build();

        snmpResourceHandler = SNMPFactoryProvider.getFactory(version).createSNMPResourceHandler(configuration);
    }

    /**
     * Closes the current SNMP mapping.
     */
    @OnStopped
    public void close() {
        if (snmpResourceHandler != null) {
            snmpResourceHandler.close();
            snmpResourceHandler = null;
        }
    }

    protected void handleResponse(final ProcessContext context, final ProcessSession processSession, final FlowFile flowFile, final SNMPSingleResponse response,
                                  final Relationship success, final Relationship failure, final String provenanceAddress) {
        final SNMPResponseStatus snmpResponseStatus = processResponse(response);
        processSession.putAllAttributes(flowFile, response.getAttributes());
        if (snmpResponseStatus.getErrorStatus() == ErrorStatus.FAILURE) {
            getLogger().error("SNMP request failed, response error: " + snmpResponseStatus.getErrorMessage());
            processSession.getProvenanceReporter().modifyAttributes(flowFile, response.getTargetAddress() + provenanceAddress);
            processSession.transfer(flowFile, failure);
            context.yield();
        } else {
            processSession.getProvenanceReporter().modifyAttributes(flowFile, response.getTargetAddress() + provenanceAddress);
            processSession.transfer(flowFile, success);
        }
    }

    protected SNMPResponseStatus processResponse(final SNMPSingleResponse response) {
        if (response.isValid()) {
            if (response.isReportPdu()) {
                final String oid = response.getVariableBindings().get(0).getOid();
                final Optional<String> reportPduErrorMessage = SNMPUtils.getErrorMessage(oid);
                if (!reportPduErrorMessage.isPresent()) {
                    return new SNMPResponseStatus(String.format("Report-PDU returned, but no error message found. " +
                            "Please, check the OID %s in an online OID repository.", oid), ErrorStatus.FAILURE);
                }
                return new SNMPResponseStatus("Report-PDU returned. " + reportPduErrorMessage.get(), ErrorStatus.FAILURE);
            }
            return checkV2cV3VariableBindings(response);
        } else {
            final String errorMessage = response.getErrorStatusText();
            return new SNMPResponseStatus(errorMessage, ErrorStatus.FAILURE);
        }
    }

    private SNMPResponseStatus checkV2cV3VariableBindings(SNMPSingleResponse response) {
        if (response.getVersion() == SnmpConstants.version2c || response.getVersion() == SnmpConstants.version3) {
            final Optional<SNMPValue> firstVariableBinding = response.getVariableBindings().stream().findFirst();
            if (firstVariableBinding.isPresent()) {
                final String value = firstVariableBinding.get().getVariable();
                if (NO_SUCH_OBJECT.equals(value)) {
                    return new SNMPResponseStatus("OID not found.", ErrorStatus.FAILURE);
                }
            } else {
                return new SNMPResponseStatus("Empty SNMP response: no variable binding found.", ErrorStatus.FAILURE);
            }
        }
        return new SNMPResponseStatus("Successful SNMP Response", ErrorStatus.SUCCESS);
    }

    protected abstract String getTargetHost(ProcessContext processContext);

    protected abstract String getTargetPort(ProcessContext processContext);
}
