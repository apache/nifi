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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.configuration.SNMPConfigurationBuilder;
import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPValue;
import org.apache.nifi.snmp.exception.SNMPException;
import org.apache.nifi.snmp.logging.SLF4JLogFactory;
import org.apache.nifi.snmp.operations.SNMPRequestHandler;
import org.apache.nifi.snmp.operations.SNMPRequestHandlerFactory;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.log.LogFactory;
import org.snmp4j.mp.SnmpConstants;

import java.util.HashMap;
import java.util.Map;
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

    private static final String SHA_2_ALGORITHM = "Provides authentication based on the HMAC-SHA-2 algorithm.";
    private static final String NO_SUCH_OBJECT = "noSuchObject";
    // SNMP versions
    public static final AllowableValue SNMP_V1 = new AllowableValue("SNMPv1", "v1", "SNMP version 1");
    public static final AllowableValue SNMP_V2C = new AllowableValue("SNMPv2c", "v2c", "SNMP version 2c");
    public static final AllowableValue SNMP_V3 = new AllowableValue("SNMPv3", "v3", "SNMP version 3 with improved security");

    // SNMPv3 security levels
    public static final AllowableValue NO_AUTH_NO_PRIV = new AllowableValue("noAuthNoPriv", "noAuthNoPriv",
            "No authentication or encryption.");
    public static final AllowableValue AUTH_NO_PRIV = new AllowableValue("authNoPriv", "authNoPriv",
            "Authentication without encryption.");
    public static final AllowableValue AUTH_PRIV = new AllowableValue("authPriv", "authPriv",
            "Authentication and encryption.");

    // SNMPv3 authentication protocols
    public static final AllowableValue MD5 = new AllowableValue("MD5", "MD5",
            "Provides authentication based on the HMAC-MD5 algorithm.");
    public static final AllowableValue SHA = new AllowableValue("SHA", "SHA",
            "Provides authentication based on the HMAC-SHA algorithm.");
    public static final AllowableValue HMAC128SHA224 = new AllowableValue("HMAC128SHA224", "SHA224",
            SHA_2_ALGORITHM);
    public static final AllowableValue HMAC192SHA256 = new AllowableValue("HMAC192SHA256", "SHA256",
            SHA_2_ALGORITHM);
    public static final AllowableValue HMAC256SHA384 = new AllowableValue("HMAC256SHA384", "SHA384",
            SHA_2_ALGORITHM);
    public static final AllowableValue HMAC384SHA512 = new AllowableValue("HMAC384SHA512", "SHA512",
            SHA_2_ALGORITHM);

    // SNMPv3 encryption
    public static final AllowableValue DES = new AllowableValue("DES", "DES",
            "Symmetric-key algorithm for the encryption of digital data. DES has been considered insecure" +
                    "because of the feasilibity of brute-force attacks. We recommend using the AES encryption protocol.");
    public static final AllowableValue DES3 = new AllowableValue("3DES", "3DES",
            "Symmetric-key block cipher, which applies the DES cipher algorithm three times to each data block." +
                    " 3DES has been considered insecure has been deprecated by NIST in 2017. We recommend using the AES encryption protocol.");

    private static final String AES_DESCRIPTION = "AES is a symmetric algorithm which uses the same 128, 192, or 256 bit" +
            " key for both encryption and decryption (the security of an AES system increases exponentially with key length).";

    public static final AllowableValue AES128 = new AllowableValue("AES128", "AES128", AES_DESCRIPTION);
    public static final AllowableValue AES192 = new AllowableValue("AES192", "AES192", AES_DESCRIPTION);
    public static final AllowableValue AES256 = new AllowableValue("AES256", "AES256", AES_DESCRIPTION);

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

    public static final PropertyDescriptor SNMP_VERSION = new PropertyDescriptor.Builder()
            .name("snmp-version")
            .displayName("SNMP Version")
            .description("Three significant versions of SNMP have been developed and deployed. " +
                    "SNMPv1 is the original version of the protocol. More recent versions, " +
                    "SNMPv2c and SNMPv3, feature improvements in performance, flexibility and security.")
            .required(true)
            .allowableValues(SNMP_V1, SNMP_V2C, SNMP_V3)
            .defaultValue(SNMP_V1.getValue())
            .build();

    public static final PropertyDescriptor SNMP_COMMUNITY = new PropertyDescriptor.Builder()
            .name("snmp-community")
            .displayName("SNMP Community")
            .description("SNMPv1 and SNMPv2 use communities to establish trust between managers and agents." +
                    " Most agents support three community names, one each for read-only, read-write and trap." +
                    " These three community strings control different types of activities. The read-only community" +
                    " applies to get requests. The read-write community string applies to set requests. The trap" +
                    " community string applies to receipt of traps.")
            .required(true)
            .sensitive(true)
            .defaultValue("public")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V1, SNMP_V2C)
            .build();

    public static final PropertyDescriptor SNMP_SECURITY_LEVEL = new PropertyDescriptor.Builder()
            .name("snmp-security-level")
            .displayName("SNMP Security Level")
            .description("SNMP version 3 provides extra security with User Based Security Model (USM). The three levels of security is " +
                    "1. Communication without authentication and encryption (NoAuthNoPriv). " +
                    "2. Communication with authentication and without encryption (AuthNoPriv). " +
                    "3. Communication with authentication and encryption (AuthPriv).")
            .required(true)
            .allowableValues(NO_AUTH_NO_PRIV, AUTH_NO_PRIV, AUTH_PRIV)
            .defaultValue(NO_AUTH_NO_PRIV.getValue())
            .dependsOn(SNMP_VERSION, SNMP_V3)
            .build();

    public static final PropertyDescriptor SNMP_SECURITY_NAME = new PropertyDescriptor.Builder()
            .name("snmp-security-name")
            .displayName("SNMP Security Name")
            .description("User name used for SNMP v3 Authentication.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V3)
            .build();

    public static final PropertyDescriptor SNMP_AUTH_PROTOCOL = new PropertyDescriptor.Builder()
            .name("snmp-authentication-protocol")
            .displayName("SNMP Authentication Protocol")
            .description("Hash based authentication protocol for secure authentication.")
            .required(true)
            .allowableValues(MD5, SHA, HMAC128SHA224, HMAC192SHA256, HMAC256SHA384, HMAC384SHA512)
            .dependsOn(SNMP_SECURITY_LEVEL, AUTH_NO_PRIV, AUTH_PRIV)
            .build();

    public static final PropertyDescriptor SNMP_AUTH_PASSWORD = new PropertyDescriptor.Builder()
            .name("snmp-authentication-passphrase")
            .displayName("SNMP Authentication Passphrase")
            .description("Passphrase used for SNMP authentication protocol.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .dependsOn(SNMP_SECURITY_LEVEL, AUTH_NO_PRIV, AUTH_PRIV)
            .build();

    public static final PropertyDescriptor SNMP_PRIVACY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("snmp-private-protocol")
            .displayName("SNMP Privacy Protocol")
            .description("Privacy allows for encryption of SNMP v3 messages to ensure confidentiality of data.")
            .required(true)
            .allowableValues(DES, DES3, AES128, AES192, AES256)
            .dependsOn(SNMP_SECURITY_LEVEL, AUTH_PRIV)
            .build();

    public static final PropertyDescriptor SNMP_PRIVACY_PASSWORD = new PropertyDescriptor.Builder()
            .name("snmp-private-protocol-passphrase")
            .displayName("SNMP Privacy Passphrase")
            .description("Passphrase used for SNMP privacy protocol.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .dependsOn(SNMP_SECURITY_LEVEL, AUTH_PRIV)
            .build();

    public static final PropertyDescriptor SNMP_RETRIES = new PropertyDescriptor.Builder()
            .name("snmp-retries")
            .displayName("Number of Retries")
            .description("Set the number of retries when requesting the SNMP Agent.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNMP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("snmp-timeout")
            .displayName("Timeout (ms)")
            .description("Set the timeout (in milliseconds) when requesting the SNMP Agent.")
            .required(false)
            .defaultValue("5000")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();


    protected volatile SNMPRequestHandler snmpRequestHandler;

    @OnScheduled
    public void initSnmpManager(final ProcessContext context) throws InitializationException {
        final int version = SNMPUtils.getVersion(context.getProperty(SNMP_VERSION).getValue());
        final SNMPConfiguration configuration;
        try {
            configuration = new SNMPConfigurationBuilder()
                    .setAgentHost(context.getProperty(AGENT_HOST).getValue())
                    .setAgentPort(context.getProperty(AGENT_PORT).toString())
                    .setRetries(context.getProperty(SNMP_RETRIES).asInteger())
                    .setTimeout(context.getProperty(SNMP_TIMEOUT).asInteger())
                    .setVersion(version)
                    .setAuthProtocol(context.getProperty(SNMP_AUTH_PROTOCOL).getValue())
                    .setAuthPassphrase(context.getProperty(SNMP_AUTH_PASSWORD).getValue())
                    .setPrivacyProtocol(context.getProperty(SNMP_PRIVACY_PROTOCOL).getValue())
                    .setPrivacyPassphrase(context.getProperty(SNMP_PRIVACY_PASSWORD).getValue())
                    .setSecurityName(context.getProperty(SNMP_SECURITY_NAME).getValue())
                    .setSecurityLevel(context.getProperty(SNMP_SECURITY_LEVEL).getValue())
                    .setCommunityString(context.getProperty(SNMP_COMMUNITY).getValue())
                    .build();
        } catch (IllegalStateException e) {
            throw new InitializationException(e);
        }
        snmpRequestHandler = SNMPRequestHandlerFactory.createStandardRequestHandler(configuration);
    }

    /**
     * Closes the current SNMP mapping.
     */
    @OnStopped
    public void close() {
        if (snmpRequestHandler != null) {
            snmpRequestHandler.close();
            snmpRequestHandler = null;
        }
    }

    /**
     * Method to add attribute in flow file
     *
     * @param key            attribute key
     * @param value          attribute value
     * @param flowFile       flow file to update
     * @param processSession session
     * @return updated flow file
     */
    protected FlowFile addAttribute(final String key, final String value, FlowFile flowFile, final ProcessSession processSession) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(key, value);
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    protected void processResponse(final ProcessSession processSession, FlowFile flowFile, final SNMPSingleResponse response,
                                   final String provenanceAddress, final Relationship success) {
        if (response.isValid()) {
            if (response.isReportPdu()) {
                final String oid = response.getVariableBindings().get(0).getOid();
                final Optional<String> reportPduErrorMessage = SNMPUtils.getErrorMessage(oid);
                if (!reportPduErrorMessage.isPresent()) {
                    throw new SNMPException(String.format("SNMP request failed, Report-PDU returned, but no error message found. " +
                            "Please, check the OID %s in an online OID repository.", oid));
                }
                throw new SNMPException("SNMPRequest failed, Report-PDU returned. " + reportPduErrorMessage.get());
            }
            checkV2cV3VariableBindings(response);
            flowFile = processSession.putAllAttributes(flowFile, response.getAttributes());
            processSession.transfer(flowFile, success);
            processSession.getProvenanceReporter().receive(flowFile, provenanceAddress);
        } else {
            final String error = response.getErrorStatusText();
            throw new SNMPException("SNMP request failed, response error: " + error);
        }
    }

    private void checkV2cV3VariableBindings(SNMPSingleResponse response) {
        if (response.getVersion() != SnmpConstants.version1) {
            final Optional<SNMPValue> firstVariableBinding = response.getVariableBindings().stream().findFirst();
            if (firstVariableBinding.isPresent()) {
                final String value = firstVariableBinding.get().getVariable();
                if (NO_SUCH_OBJECT.equals(value)) {
                    throw new SNMPException("SNMP Request failed, OID not found.");
                }
            } else {
                throw new SNMPException("Empty SNMP response: no variable binding found.");
            }
        }
    }
}
