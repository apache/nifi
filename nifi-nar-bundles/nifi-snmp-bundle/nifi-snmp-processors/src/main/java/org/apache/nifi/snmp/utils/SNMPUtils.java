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
package org.apache.nifi.snmp.utils;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.PDU;
import org.snmp4j.mp.SnmpConstants;


import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.Priv3DES;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.PrivAES192;
import org.snmp4j.security.PrivAES256;
import org.snmp4j.security.PrivDES;
import org.snmp4j.smi.AbstractVariable;
import org.snmp4j.smi.AssignableFromInteger;
import org.snmp4j.smi.AssignableFromLong;
import org.snmp4j.smi.AssignableFromString;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.TreeEvent;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Pattern;

/**
 * Utility helper class that simplifies interactions with target SNMP API and NIFI API.
 */
public class SNMPUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SNMPUtils.class);

    public static final Pattern OID_PATTERN = Pattern.compile("[[0-9]+\\.]*");

    // Delimiter for properties name.
    public static final String SNMP_PROP_DELIMITER = "$";

    // Prefix for SNMP properties in flow file.
    public static final String SNMP_PROP_PREFIX = "snmp" + SNMP_PROP_DELIMITER;

    // List of properties name when performing simple get.
    private static final List<String> PROPERTY_NAMES = Arrays.asList("snmp$errorIndex", "snmp$errorStatus", "snmp$errorStatusText",
            "snmp$nonRepeaters", "snmp$requestID", "snmp$type", "snmp$variableBindings");

    // Used to validate OID syntax.
    public static final Validator SNMP_OID_VALIDATOR = (subject, input, context) -> {
        final ValidationResult.Builder builder = new ValidationResult.Builder();
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
     *
     * @param response       PDU retried from SNMP Agent
     * @param flowFile       instance of target {@link FlowFile}
     * @param processSession instance of {@link ProcessSession}
     * @return updated {@link FlowFile}
     */
    public static FlowFile updateFlowFileAttributesWithPduProperties(PDU response, FlowFile flowFile, ProcessSession processSession) {
        if (response != null) {
            try {
                Method[] methods = PDU.class.getDeclaredMethods();
                Map<String, String> attributes = new HashMap<>();
                for (Method method : methods) {
                    if (Modifier.isPublic(method.getModifiers()) && (method.getParameterTypes().length == 0) && method.getName().startsWith("get")) {
                        String propertyName = extractPropertyNameFromMethod(method);
                        if (isValidSnmpPropertyName(propertyName)) {
                            Object amqpPropertyValue = method.invoke(response);
                            if (amqpPropertyValue != null) {
                                if (propertyName.equals(SNMP_PROP_PREFIX + "variableBindings") && (amqpPropertyValue instanceof Vector)) {
                                    addGetOidValues(attributes, amqpPropertyValue);
                                } else {
                                    attributes.put(propertyName, amqpPropertyValue.toString());
                                }
                            }
                        }
                    }
                }
                flowFile = processSession.putAllAttributes(flowFile, attributes);
            } catch (Exception e) {
                LOGGER.warn("Failed to update FlowFile with AMQP attributes", e);
            }
        }
        return flowFile;
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
    public static FlowFile addAttribute(String key, String value, FlowFile flowFile, ProcessSession processSession) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(key, value);
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    /**
     * Method to construct {@link FlowFile} attributes from a {@link TreeEvent}
     *
     * @param treeEvent      a {@link TreeEvent}
     * @param flowFile       instance of the {@link FlowFile} to update
     * @param processSession instance of {@link ProcessSession}
     * @return updated {@link FlowFile}
     */
    public static FlowFile updateFlowFileAttributesWithTreeEventProperties(TreeEvent treeEvent, FlowFile flowFile, ProcessSession processSession) {
        Map<String, String> attributes = new HashMap<>();
        addWalkOidValues(attributes, treeEvent.getVariableBindings());
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    /**
     * Method to construct {@link FlowFile} attributes from a vector of {@link VariableBinding}
     *
     * @param attributes attributes
     * @param vector     vector of {@link VariableBinding}
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
     *
     * @param attributes attributes
     * @param vector     vector of {@link VariableBinding}
     */
    private static void addGetOidValues(Map<String, String> attributes, Object vector) {
        if (vector instanceof Vector) {
            @SuppressWarnings("unchecked")
            Vector<VariableBinding> variables = (Vector<VariableBinding>) vector;
            for (VariableBinding variableBinding : variables) {
                addAttributeFromVariable(variableBinding, attributes);
            }
        }
    }

    /**
     * Method to add {@link FlowFile} attributes from a {@link VariableBinding}
     *
     * @param variableBinding {@link VariableBinding}
     * @param attributes      {@link FlowFile} attributes to update
     */
    private static void addAttributeFromVariable(VariableBinding variableBinding, Map<String, String> attributes) {
        attributes.put(SNMP_PROP_PREFIX + variableBinding.getOid() + SNMP_PROP_DELIMITER + variableBinding.getVariable().getSyntax(), variableBinding.getVariable().toString());
    }

    /**
     * Will validate if provided name corresponds to valid SNMP property.
     *
     * @param name the name of the property
     * @return 'true' if valid otherwise 'false'
     */
    public static boolean isValidSnmpPropertyName(String name) {
        return PROPERTY_NAMES.contains(name);
    }

    /**
     * Method to extract property name from given {@link Method}
     *
     * @param method method
     * @return property name
     */
    private static String extractPropertyNameFromMethod(Method method) {
        char[] c = method.getName().substring(3).toCharArray();
        c[0] = Character.toLowerCase(c[0]);
        return SNMP_PROP_PREFIX + new String(c);
    }

    /**
     * Method to return the private protocol given the property.
     *
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
     * Method to return the authentication protocol given the property.
     *
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

    public static int getSnmpVersion(String snmpVersion) {
        if ("SNMPv1".equals(snmpVersion)) {
            return SnmpConstants.version1;
        } else if ("SNMPv2c".equals(snmpVersion)) {
            return SnmpConstants.version2c;
        } else if ("SNMPv3".equals(snmpVersion)) {
            return SnmpConstants.version3;
        }
        throw new RuntimeException("SNMP version is invalid or not supported.");
    }

    /**
     * Method to create the variable from the attribute value and the given SMI syntax value
     *
     * @param value     attribute value
     * @param smiSyntax attribute SMI Syntax
     * @return variable
     */
    public static Variable stringToVariable(String value, int smiSyntax, ComponentLog logger) {
        Variable var = AbstractVariable.createFromSyntax(smiSyntax);
        try {
            if (var instanceof AssignableFromString) {
                ((AssignableFromString) var).setValue(value);
            } else if (var instanceof AssignableFromInteger) {
                ((AssignableFromInteger) var).setValue(Integer.parseInt(value));
            } else if (var instanceof AssignableFromLong) {
                ((AssignableFromLong) var).setValue(Long.parseLong(value));
            } else {
                logger.error("Unsupported conversion of [ {} ] to ", var.getSyntaxString());
                var = null;
            }
        } catch (IllegalArgumentException e) {
            logger.error("Unsupported conversion of [ {} ] to ", var.getSyntaxString(), e);
            var = null;
        }
        return var;
    }

    /**
     * Method to construct {@link VariableBinding} based on {@link FlowFile}
     * attributes in order to update the {@link PDU} that is going to be sent to
     * the SNMP Agent.
     *
     * @param pdu        {@link PDU} to be sent
     * @param attributes {@link FlowFile} attributes
     * @return true if at least one {@link VariableBinding} has been created, false otherwise
     */
    public static boolean addVariables(PDU pdu, Map<String, String> attributes, ComponentLog logger) {
        boolean result = false;
        for (Map.Entry<String, String> attributeEntry : attributes.entrySet()) {
            if (attributeEntry.getKey().startsWith(SNMPUtils.SNMP_PROP_PREFIX)) {
                String[] splits = attributeEntry.getKey().split("\\" + SNMPUtils.SNMP_PROP_DELIMITER);
                String snmpPropName = splits[1];
                String snmpPropValue = attributeEntry.getValue();
                if (SNMPUtils.OID_PATTERN.matcher(snmpPropName).matches()) {
                    Variable var;
                    if (splits.length == 2) { // no SMI syntax defined
                        var = new OctetString(snmpPropValue);
                    } else {
                        int smiSyntax = Integer.parseInt(splits[2]);
                        var = SNMPUtils.stringToVariable(snmpPropValue, smiSyntax, logger);
                    }
                    if (var != null) {
                        VariableBinding varBind = new VariableBinding(new OID(snmpPropName), var);
                        pdu.add(varBind);
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    private SNMPUtils() {
        // hide implicit constructor
    }
}
