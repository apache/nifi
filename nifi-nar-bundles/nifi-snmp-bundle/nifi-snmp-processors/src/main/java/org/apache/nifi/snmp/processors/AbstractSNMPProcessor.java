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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.snmp4j.AbstractTarget;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.UserTarget;
import org.snmp4j.mp.MPv3;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;

/**
 * Base processor that uses SNMP4J client API
 * (http://www.snmp4j.org/)
 *
 * @param <T> the type of {@link SNMPWorker}. Please see {@link SNMPSetter}
 *            and {@link SNMPGetter}
 */
abstract class AbstractSNMPProcessor<T extends SNMPWorker> extends AbstractProcessor {

    /** property to define host of the SNMP agent */
    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("snmp-hostname")
            .displayName("Host Name")
            .description("Network address of SNMP Agent (e.g., localhost)")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /** property to define port of the SNMP agent */
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("snmp-port")
            .displayName("Port")
            .description("Numeric value identifying Port of SNMP Agent (e.g., 161)")
            .required(true)
            .defaultValue("161")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    /** property to define SNMP version to use */
    public static final PropertyDescriptor SNMP_VERSION = new PropertyDescriptor.Builder()
            .name("snmp-version")
            .displayName("SNMP Version")
            .description("SNMP Version to use")
            .required(true)
            .allowableValues("SNMPv1", "SNMPv2c", "SNMPv3")
            .defaultValue("SNMPv1")
            .build();

    /** property to define SNMP community to use */
    public static final PropertyDescriptor SNMP_COMMUNITY = new PropertyDescriptor.Builder()
            .name("snmp-community")
            .displayName("SNMP Community (v1 & v2c)")
            .description("SNMP Community to use (e.g., public)")
            .required(false)
            .defaultValue("public")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /** property to define SNMP security level to use */
    public static final PropertyDescriptor SNMP_SECURITY_LEVEL = new PropertyDescriptor.Builder()
            .name("snmp-security-level")
            .displayName("SNMP Security Level")
            .description("SNMP Security Level to use")
            .required(true)
            .allowableValues("noAuthNoPriv", "authNoPriv", "authPriv")
            .defaultValue("authPriv")
            .build();

    /** property to define SNMP security name to use */
    public static final PropertyDescriptor SNMP_SECURITY_NAME = new PropertyDescriptor.Builder()
            .name("snmp-security-name")
            .displayName("SNMP Security name / user name")
            .description("Security name used for SNMP exchanges")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /** property to define SNMP authentication protocol to use */
    public static final PropertyDescriptor SNMP_AUTH_PROTOCOL = new PropertyDescriptor.Builder()
            .name("snmp-authentication-protocol")
            .displayName("SNMP Authentication Protocol")
            .description("SNMP Authentication Protocol to use")
            .required(true)
            .allowableValues("MD5", "SHA", "")
            .defaultValue("")
            .build();

    /** property to define SNMP authentication password to use */
    public static final PropertyDescriptor SNMP_AUTH_PASSWORD = new PropertyDescriptor.Builder()
            .name("snmp-authentication-passphrase")
            .displayName("SNMP Authentication pass phrase")
            .description("Pass phrase used for SNMP authentication protocol")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    /** property to define SNMP private protocol to use */
    public static final PropertyDescriptor SNMP_PRIV_PROTOCOL = new PropertyDescriptor.Builder()
            .name("snmp-private-protocol")
            .displayName("SNMP Private Protocol")
            .description("SNMP Private Protocol to use")
            .required(true)
            .allowableValues("DES", "3DES", "AES128", "AES192", "AES256", "")
            .defaultValue("")
            .build();

    /** property to define SNMP private password to use */
    public static final PropertyDescriptor SNMP_PRIV_PASSWORD = new PropertyDescriptor.Builder()
            .name("snmp-private-protocol-passphrase")
            .displayName("SNMP Private protocol pass phrase")
            .description("Pass phrase used for SNMP private protocol")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    /** property to define the number of SNMP retries when requesting the SNMP Agent */
    public static final PropertyDescriptor SNMP_RETRIES = new PropertyDescriptor.Builder()
            .name("snmp-retries")
            .displayName("Number of retries")
            .description("Set the number of retries when requesting the SNMP Agent")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    /** property to define the timeout when requesting the SNMP Agent */
    public static final PropertyDescriptor SNMP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("snmp-timeout")
            .displayName("Timeout (ms)")
            .description("Set the timeout (in milliseconds) when requesting the SNMP Agent")
            .required(true)
            .defaultValue("5000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    /** list of property descriptors */
    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    /*
     * Will ensure that list of PropertyDescriptors is build only once, since
     * all other life cycle methods are invoked multiple times.
     */
    static {
        descriptors.add(HOST);
        descriptors.add(PORT);
        descriptors.add(SNMP_VERSION);
        descriptors.add(SNMP_COMMUNITY);
        descriptors.add(SNMP_SECURITY_LEVEL);
        descriptors.add(SNMP_SECURITY_NAME);
        descriptors.add(SNMP_AUTH_PROTOCOL);
        descriptors.add(SNMP_AUTH_PASSWORD);
        descriptors.add(SNMP_PRIV_PROTOCOL);
        descriptors.add(SNMP_PRIV_PASSWORD);
        descriptors.add(SNMP_RETRIES);
        descriptors.add(SNMP_TIMEOUT);
    }

    /** SNMP target */
    protected volatile AbstractTarget snmpTarget;

    /** transport mapping */
    protected volatile TransportMapping transportMapping;

    /** SNMP */
    protected volatile Snmp snmp;

    /** target resource */
    protected volatile T targetResource;

    /**
     * Will builds target resource upon first invocation and will delegate to the
     * implementation of {@link #onTriggerSnmp(ProcessContext, ProcessSession)} method for
     * further processing.
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        synchronized (this) {
            this.buildTargetResource(context);
        }
        this.onTriggerSnmp(context, session);
    }

    /**
     * Will close current SNMP mapping.
     */
    @OnStopped
    public void close() {
        try {
            if (this.targetResource != null) {
                this.targetResource.close();
            }
        } catch (Exception e) {
            this.getLogger().warn("Failure while closing target resource " + this.targetResource, e);
        }
        this.targetResource = null;

        try {
            if (this.transportMapping != null) {
                this.transportMapping.close();
            }
        } catch (IOException e) {
            this.getLogger().warn("Failure while closing UDP transport mapping", e);
        }
        this.transportMapping = null;

        try {
            if (this.snmp != null) {
                this.snmp.close();
            }
        } catch (IOException e) {
            this.getLogger().warn("Failure while closing UDP transport mapping", e);
        }
        this.snmp = null;
    }

    /**
     * @see org.apache.nifi.components.AbstractConfigurableComponent#customValidate(org.apache.nifi.components.ValidationContext)
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean isVersion3 = "SNMPv3".equals(validationContext.getProperty(SNMP_VERSION).getValue());

        if(isVersion3) {
            final boolean isSecurityNameSet = validationContext.getProperty(SNMP_SECURITY_NAME).isSet();
            if(!isSecurityNameSet) {
                problems.add(new ValidationResult.Builder()
                        .input("SNMP Security Name")
                        .valid(false)
                        .explanation("SNMP Security Name must be set with SNMPv3.")
                        .build());
            }

            final boolean isAuthProtOK = !"".equals(validationContext.getProperty(SNMP_AUTH_PROTOCOL).getValue());
            final boolean isAuthPwdSet = validationContext.getProperty(SNMP_AUTH_PASSWORD).isSet();
            final boolean isPrivProtOK = !"".equals(validationContext.getProperty(SNMP_PRIV_PROTOCOL).getValue());
            final boolean isPrivPwdSet = validationContext.getProperty(SNMP_PRIV_PASSWORD).isSet();

            switch(validationContext.getProperty(SNMP_SECURITY_LEVEL).getValue()) {
            case "authNoPriv":
                if(!isAuthProtOK || !isAuthPwdSet) {
                    problems.add(new ValidationResult.Builder()
                            .input("SNMP Security Level")
                            .valid(false)
                            .explanation("Authentication protocol and password must be set when using authNoPriv security level.")
                            .build());
                }
                break;
            case "authPriv":
                if(!isAuthProtOK || !isAuthPwdSet || !isPrivProtOK || !isPrivPwdSet) {
                    problems.add(new ValidationResult.Builder()
                            .input("SNMP Security Level")
                            .valid(false)
                            .explanation("All protocols and passwords must be set when using authPriv security level.")
                            .build());
                }
                break;
            case "noAuthNoPriv":
            default:
                break;
            }
        } else {
            final boolean isCommunitySet = validationContext.getProperty(SNMP_COMMUNITY).isSet();
            if(!isCommunitySet) {
                problems.add(new ValidationResult.Builder()
                        .input("SNMP Community")
                        .valid(false)
                        .explanation("SNMP Community must be set with SNMPv1 and SNMPv2c.")
                        .build());
            }
        }

        return problems;
    }

    /**
     * Delegate method to supplement
     * {@link #onTrigger(ProcessContext, ProcessSession)}. It is implemented by
     * sub-classes to perform {@link Processor} specific functionality.
     *
     * @param context
     *            instance of {@link ProcessContext}
     * @param session
     *            instance of {@link ProcessSession}
     * @throws ProcessException Process exception
     */
    protected abstract void onTriggerSnmp(ProcessContext context, ProcessSession session) throws ProcessException;

    /**
     * Delegate method to supplement building of target {@link SNMPWorker} (see
     * {@link SNMPSetter} or {@link SNMPGetter}) and is implemented by
     * sub-classes.
     *
     * @param context
     *            instance of {@link ProcessContext}
     * @return new instance of {@link SNMPWorker}
     */
    protected abstract T finishBuildingTargetResource(ProcessContext context);

    /**
     * Builds target resource.
     * @param context Process context
     */
    private void buildTargetResource(ProcessContext context) {
        if((this.transportMapping == null) || !this.transportMapping.isListening() || (this.snmp == null)) {
            try {
                this.transportMapping = new DefaultUdpTransportMapping();
                this.snmp = new Snmp(this.transportMapping);

                if("SNMPv3".equals(context.getProperty(SNMP_VERSION).getValue())) {
                    USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(MPv3.createLocalEngineID()), 0);
                    SecurityModels.getInstance().addSecurityModel(usm);
                }

                this.transportMapping.listen();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to initialize UDP transport mapping", e);
            }
        }
        if (this.snmpTarget == null) {
            this.snmpTarget = this.createSnmpTarget(context);
        }
        if (this.targetResource == null) {
            this.targetResource = this.finishBuildingTargetResource(context);
        }
    }

    /**
     * Creates {@link AbstractTarget} to request SNMP agent.
     * @param context Process context
     * @return the SNMP target
     */
    private AbstractTarget createSnmpTarget(ProcessContext context) {
        AbstractTarget result = null;
        String snmpVersion = context.getProperty(SNMP_VERSION).getValue();
        int version = 0;
        switch (snmpVersion) {
        case "SNMPv2c":
            version = SnmpConstants.version2c;
            break;
        case "SNMPv3":
            version = SnmpConstants.version3;
            break;
        case "SNMPv1":
        default:
            version = SnmpConstants.version1;
            break;
        }

        if(version == SnmpConstants.version3) {
            final String username = context.getProperty(SNMP_SECURITY_NAME).getValue();
            final String authPassword = context.getProperty(SNMP_AUTH_PASSWORD).getValue();
            final String privPassword = context.getProperty(SNMP_PRIV_PASSWORD).getValue();
            final String authProtocol = context.getProperty(SNMP_AUTH_PROTOCOL).getValue();
            final String privProtocol = context.getProperty(SNMP_PRIV_PROTOCOL).getValue();
            OctetString aPwd = authPassword != null ? new OctetString(authPassword) : null;
            OctetString pPwd = privPassword != null ? new OctetString(privPassword) : null;

            // add user information
            this.snmp.getUSM().addUser(new OctetString(username),
                    new UsmUser(new OctetString(username), SNMPUtils.getAuth(authProtocol), aPwd, SNMPUtils.getPriv(privProtocol), pPwd));

            result = new UserTarget();

            ((UserTarget) result).setSecurityLevel(SNMPUtils.getSecLevel(context.getProperty(SNMP_SECURITY_LEVEL).getValue()));
            final String securityName = context.getProperty(SNMP_SECURITY_NAME).getValue();
            if(securityName != null) {
                ((UserTarget) result).setSecurityName(new OctetString(securityName));
            }
        } else {
            result = new CommunityTarget();
            String community = context.getProperty(SNMP_COMMUNITY).getValue();
            if(community != null) {
                ((CommunityTarget) result).setCommunity(new OctetString(community));
            }
        }

        result.setVersion(version);
        result.setAddress(new UdpAddress(context.getProperty(HOST).getValue() + "/" + context.getProperty(PORT).getValue()));
        result.setRetries(context.getProperty(SNMP_RETRIES).asInteger());
        result.setTimeout(context.getProperty(SNMP_TIMEOUT).asInteger());

        return result;
    }

}
