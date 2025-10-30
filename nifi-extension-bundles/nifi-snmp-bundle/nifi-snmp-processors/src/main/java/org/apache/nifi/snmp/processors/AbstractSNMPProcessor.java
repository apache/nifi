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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.dto.ErrorStatus;
import org.apache.nifi.snmp.dto.SNMPResponseStatus;
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPValue;
import org.apache.nifi.snmp.factory.core.SNMPContext;
import org.apache.nifi.snmp.factory.core.SNMPFactoryProvider;
import org.apache.nifi.snmp.logging.SLF4JLogFactory;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.V3SecurityProperties;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.Snmp;
import org.snmp4j.log.LogFactory;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.smi.Integer32;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_V1;
import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_V2C;
import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_V3;
import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_VERSION;
import static org.apache.nifi.snmp.processors.properties.V3SecurityProperties.AUTH_NO_PRIV;
import static org.apache.nifi.snmp.processors.properties.V3SecurityProperties.AUTH_PRIV;

/**
 * Base processor that uses SNMP4J client API.
 * (http://www.snmp4j.org/)
 */
@RequiresInstanceClassLoading
public abstract class AbstractSNMPProcessor extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSNMPProcessor.class);

    static {
        LogFactory.setLogFactory(new SLF4JLogFactory());
    }

    public static final String REQUEST_TIMEOUT_EXCEPTION_TEMPLATE = "Request timed out. Please check if (1). the " +
            "agent host and port is correctly set, (2). the agent is running, (3). the agent SNMP version corresponds" +
            " with the processor's one, (4) the community string is correct and has %1$s access, (5) In case of SNMPv3" +
            " check if the user credentials are valid and the user in a group with %1$s access.";

    private static final String NO_SUCH_OBJECT = "noSuchObject";

    public static final PropertyDescriptor AGENT_HOST = new PropertyDescriptor.Builder()
            .name("SNMP Agent Hostname")
            .description("Hostname or network address of the SNMP Agent.")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor AGENT_PORT = new PropertyDescriptor.Builder()
            .name("SNMP Agent Port")
            .description("Port of the SNMP Agent.")
            .required(true)
            .defaultValue("161")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected volatile Snmp snmpManager;
    protected volatile SNMPContext factory;

    @OnScheduled
    public void initSnmpManager(final ProcessContext context) {
        final String snmpVersion = context.getProperty(SNMP_VERSION).getValue();
        final String securityLevel = getSecurityLevel(context, snmpVersion);

        final SNMPConfiguration configuration = SNMPConfiguration.builder()
                .setAuthProtocol(getAuthProtocol(context, securityLevel))
                .setAuthPassphrase(getAuthPassphrase(context, securityLevel))
                .setPrivacyProtocol(getPrivacyProtocol(context, securityLevel))
                .setPrivacyPassphrase(getPrivacyPassphrase(context, securityLevel))
                .setSecurityName(getSecurityName(context, snmpVersion))
                .build();

        factory = SNMPFactoryProvider.getFactory(SNMPUtils.getVersion(snmpVersion));
        snmpManager = factory.createSnmpManagerInstance(configuration);
    }

    protected SNMPConfiguration getTargetConfiguration(final ProcessContext context, final FlowFile flowFile) {
        final String snmpVersion = context.getProperty(SNMP_VERSION).getValue();

        return SNMPConfiguration.builder()
                .setVersion(SNMPUtils.getVersion(snmpVersion))
                .setTargetHost(getTargetHost(context, flowFile))
                .setTargetPort(getTargetPort(context, flowFile))
                .setRetries(context.getProperty(BasicProperties.SNMP_RETRIES).asInteger())
                .setTimeoutInMs(context.getProperty(BasicProperties.SNMP_TIMEOUT).asInteger())
                .setSecurityName(getSecurityName(context, snmpVersion))
                .setSecurityLevel(getSecurityLevel(context, snmpVersion))
                .setCommunityString(getCommunity(context, snmpVersion))
                .build();
    }

    /**
     * Closes the current SNMP mapping.
     */
    @OnStopped
    public void close() {
        try {
            if (snmpManager.getUSM() != null) {
                snmpManager.getUSM().removeAllUsers();
                SecurityModels.getInstance().removeSecurityModel(new Integer32(snmpManager.getUSM().getID()));
            }
            snmpManager.close();
        } catch (IOException e) {
            final String errorMessage = "Could not close SNMP manager.";
            logger.error(errorMessage, e);
            throw new ProcessException(errorMessage);
        }
    }

    protected void handleResponse(final ProcessContext context, final ProcessSession processSession, final FlowFile flowFile, final SNMPSingleResponse response,
                                  final Relationship success, final Relationship failure, final String provenanceAddress, final boolean isNewFlowFileCreated) {
        final SNMPResponseStatus snmpResponseStatus = processResponse(response);
        processSession.putAllAttributes(flowFile, response.getAttributes());
        if (snmpResponseStatus.getErrorStatus() == ErrorStatus.FAILURE) {
            getLogger().error("SNMP request failed, response error: {}", snmpResponseStatus.getErrorMessage());
            processSession.transfer(flowFile, failure);
            context.yield();
        } else {
            if (isNewFlowFileCreated) {
                processSession.getProvenanceReporter().receive(flowFile, response.getTargetAddress() + provenanceAddress);
            } else {
                processSession.getProvenanceReporter().fetch(flowFile, response.getTargetAddress() + provenanceAddress);
            }
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

    protected abstract String getTargetHost(final ProcessContext processContext, final FlowFile flowFile);

    protected abstract String getTargetPort(final ProcessContext processContext, final FlowFile flowFile);

    public static String getSecurityName(final ProcessContext context, final String snmpVersion) {
        return snmpVersion.equals(SNMP_V3.getValue())
                ? context.getProperty(V3SecurityProperties.SNMP_SECURITY_NAME).getValue() : null;
    }

    public static String getPrivacyPassphrase(final ProcessContext context, final String securityLevel) {
        return Objects.equals(securityLevel, AUTH_PRIV.getValue())
                ? context.getProperty(V3SecurityProperties.SNMP_PRIVACY_PASSWORD).getValue() : null;
    }

    public static String getPrivacyProtocol(final ProcessContext context, final String securityLevel) {
        return Objects.equals(securityLevel, AUTH_PRIV.getValue())
                ?  context.getProperty(V3SecurityProperties.SNMP_PRIVACY_PROTOCOL).getValue() : null;
    }

    public static String getAuthPassphrase(final ProcessContext context, final String securityLevel) {
        return Objects.equals(securityLevel, AUTH_NO_PRIV.getValue()) || Objects.equals(securityLevel, AUTH_PRIV.getValue())
                ? context.getProperty(V3SecurityProperties.SNMP_AUTH_PASSWORD).getValue() : null;
    }

    public static String getAuthProtocol(final ProcessContext context, final String securityLevel) {
        return Objects.equals(securityLevel, AUTH_NO_PRIV.getValue()) || Objects.equals(securityLevel, AUTH_PRIV.getValue())
                ?  context.getProperty(V3SecurityProperties.SNMP_AUTH_PROTOCOL).getValue() : null;
    }

    public static String getCommunity(final ProcessContext context, final String snmpVersion) {
        return snmpVersion.equals(SNMP_V1.getValue()) || snmpVersion.equals(SNMP_V2C.getValue())
                ? context.getProperty(BasicProperties.SNMP_COMMUNITY).getValue() : null;
    }

    public static String getSecurityLevel(final ProcessContext context, final String snmpVersion) {
        return snmpVersion.equals(SNMP_V3.getValue())
                ? context.getProperty(V3SecurityProperties.SNMP_SECURITY_LEVEL).getValue() : null;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("snmp-hostname", AGENT_HOST.getName());
        config.renameProperty("snmp-port", AGENT_PORT.getName());
    }
}
