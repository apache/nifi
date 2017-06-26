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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.PDU;
import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.Priv3DES;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.PrivAES192;
import org.snmp4j.security.PrivAES256;
import org.snmp4j.security.PrivDES;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.TreeEvent;

/**
 * Utility helper class that simplifies interactions with target SNMP API and NIFI API.
 */
public final class SNMPUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SNMPUtils.class);

    /** OID Pattern */
    public static final Pattern OID_PATTERN = Pattern.compile("[[0-9]+\\.]*");

    /** delimiter for properties name */
    public static final String SNMP_PROP_DELIMITER = "$";

    /** prefix for SNMP properties in flow file */
    public static final String SNMP_PROP_PREFIX = "snmp" + SNMP_PROP_DELIMITER;

    /** used to validate OID syntax */
    public static final Validator SNMP_OID_VALIDATOR = (subject, input, context) -> {
        ValidationResult.Builder builder = new ValidationResult.Builder();
        builder.subject(subject).input(input);
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return builder.valid(true).explanation("Contains Expression Language").build();
        }
        try {
            if (OID_PATTERN.matcher(input).matches()) {
                builder.valid(true);
            } else {
                builder.valid(false).explanation(input + "is not a valid OID");
            }
        } catch (final IllegalArgumentException e) {
            builder.valid(false).explanation(e.getMessage());
        }
        return builder.build();
    };

    /**
     * Updates {@link FlowFile} with attributes representing PDU properties
     * @param response PDU retrieved from SNMP Agent
     * @param flowFile instance of target {@link FlowFile}
     * @param processSession instance of {@link ProcessSession}
     * @return updated {@link FlowFile}
     */
    public static FlowFile updateFlowFileAttributesWithPduProperties(PDU response, FlowFile flowFile, ProcessSession processSession) {
        if (response != null) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(SNMP_PROP_PREFIX + "errorIndex", String.valueOf(response.getErrorIndex()));
            attributes.put(SNMP_PROP_PREFIX + "errorStatus", String.valueOf(response.getErrorStatus()));
            attributes.put(SNMP_PROP_PREFIX + "errorStatusText", response.getErrorStatusText());
            attributes.put(SNMP_PROP_PREFIX + "nonRepeaters", String.valueOf(response.getNonRepeaters()));
            attributes.put(SNMP_PROP_PREFIX + "requestId", String.valueOf(response.getRequestID()));
            attributes.put(SNMP_PROP_PREFIX + "type", String.valueOf(response.getType()));

            addGetOidValues(attributes, response.getVariableBindings());

            flowFile = processSession.putAllAttributes(flowFile, attributes);
        }
        return flowFile;
    }

    /**
     * Method to add attribute in flow file
     * @param key attribute key
     * @param value attribute value
     * @param flowFile flow file to update
     * @param processSession session
     * @return updated flow file
     */
    public static FlowFile addAttribute(String key, String value, FlowFile flowFile, ProcessSession processSession) {
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(key, value);
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    /**
     * Method to construct {@link FlowFile} attributes from a {@link TreeEvent}
     * @param treeEvent a {@link TreeEvent}
     * @param flowFile instance of the {@link FlowFile} to update
     * @param processSession instance of {@link ProcessSession}
     * @return updated {@link FlowFile}
     */
    public static FlowFile updateFlowFileAttributesWithTreeEventProperties(TreeEvent treeEvent, FlowFile flowFile, ProcessSession processSession) {
        Map<String, String> attributes = new HashMap<String, String>();
        addWalkOidValues(attributes, treeEvent.getVariableBindings());
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    /**
     * Method to construct {@link FlowFile} attributes from a vector of {@link VariableBinding}
     * @param attributes attributes
     * @param vector vector of {@link VariableBinding}
     */
    private static void addWalkOidValues(Map<String, String> attributes, Object vector) {
        if (vector instanceof VariableBinding[]) {
            VariableBinding[] variables = (VariableBinding[]) vector;
            for (VariableBinding variableBinding : variables) {
                addAttributeFromVariable(variableBinding, attributes);
            }
        }
    }

    /**
     * Method to construct {@link FlowFile} attributes from a vector of {@link VariableBinding}
     * @param attributes attributes
     * @param vector vector of {@link VariableBinding}
     */
    private static void addGetOidValues(Map<String, String> attributes, Iterable<? extends VariableBinding> vector) {
        if (vector != null) {
            for (VariableBinding variableBinding : vector) {
                addAttributeFromVariable(variableBinding, attributes);
            }
        }
    }

    /**
     * Method to add {@link FlowFile} attributes from a {@link VariableBinding}
     * @param variableBinding {@link VariableBinding}
     * @param attributes {@link FlowFile} attributes to update
     */
    private static void addAttributeFromVariable(VariableBinding variableBinding, Map<String, String> attributes) {
        attributes.put(SNMP_PROP_PREFIX + variableBinding.getOid() + SNMP_PROP_DELIMITER + variableBinding.getVariable().getSyntax(), variableBinding.getVariable().toString());
    }

    /**
     * Method to return the private protocol given the property
     * @param privProtocol property
     * @return protocol
     */
    public static OID getPriv(String privProtocol) {
        switch (privProtocol) {
        case "DES":
            return PrivDES.ID;
        case "3DES":
            return Priv3DES.ID;
        case "AES128":
            return PrivAES128.ID;
        case "AES192":
            return PrivAES192.ID;
        case "AES256":
            return PrivAES256.ID;
        default:
            return null;
        }
    }

    /**
     * Method to return the authentication protocol given the property
     * @param authProtocol property
     * @return protocol
     */
    public static OID getAuth(String authProtocol) {
        switch (authProtocol) {
        case "SHA":
            return AuthSHA.ID;
        case "MD5":
            return AuthMD5.ID;
        default:
            return null;
        }
    }

    /**
     * Method to get security level from string representation of level
     * @param level level
     * @return security level as integer
     */
    public static int getSecLevel(String level) {
        switch (level) {
        case "noAuthNoPriv":
            return SecurityLevel.NOAUTH_NOPRIV;
        case "authNoPriv":
            return SecurityLevel.AUTH_NOPRIV;
        case "authPriv":
        default:
            return SecurityLevel.AUTH_PRIV;
        }
    }

    private SNMPUtils() {
    }
}
