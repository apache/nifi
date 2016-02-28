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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.nifi.components.ValidationContext;
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
abstract class SNMPUtils {

    /** logger */
    private final static Logger logger = LoggerFactory.getLogger(SNMPUtils.class);

    /** OID Pattern */
    public final static Pattern OID_PATTERN = Pattern.compile("[[0-9]+\\.]*");

    /** delimiter for properties name */
    public final static String SNMP_PROP_DELIMITER = "$";

    /** prefix for SNMP properties in flow file */
    public final static String SNMP_PROP_PREFIX = "snmp" + SNMP_PROP_DELIMITER;

    /** list of properties name when performing simple get */
    private final static List<String> propertyNames = Arrays.asList("snmp$errorIndex", "snmp$errorStatus", "snmp$errorStatusText",
            "snmp$nonRepeaters", "snmp$requestID", "snmp$type", "snmp$variableBindings");

    /** used to validate OID syntax */
    public static final Validator SNMP_OID_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
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
        }
    };

    /**
     * Updates {@link FlowFile} with attributes representing PDU properties
     * @param response PDU retried from SNMP Agent
     * @param flowFile instance of target {@link FlowFile}
     * @param processSession instance of {@link ProcessSession}
     * @return updated {@link FlowFile}
     */
    public static FlowFile updateFlowFileAttributesWithPduProperties(PDU response, FlowFile flowFile, ProcessSession processSession) {
        if (response != null) {
            try {
                Method[] methods = PDU.class.getDeclaredMethods();
                Map<String, String> attributes = new HashMap<String, String>();
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
                logger.warn("Failed to update FlowFile with AMQP attributes", e);
            }
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
     * @param variableBinding {@link VariableBinding}
     * @param attributes {@link FlowFile} attributes to update
     */
    private static void addAttributeFromVariable(VariableBinding variableBinding, Map<String, String> attributes) {
        attributes.put(SNMP_PROP_PREFIX + variableBinding.getOid() + SNMP_PROP_DELIMITER + variableBinding.getVariable().getSyntax(), variableBinding.getVariable().toString());
    }

    /**
     * Will validate if provided name corresponds to valid SNMP property.
     * @param name the name of the property
     * @return 'true' if valid otherwise 'false'
     */
    public static boolean isValidSnmpPropertyName(String name) {
        return propertyNames.contains(name);
    }

    /**
     * Method to extract property name from given {@link Method}
     * @param method method
     * @return property name
     */
    private static String extractPropertyNameFromMethod(Method method) {
        char c[] = method.getName().substring(3).toCharArray();
        c[0] = Character.toLowerCase(c[0]);
        return SNMP_PROP_PREFIX + new String(c);
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

}
