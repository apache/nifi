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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.snmp.exception.InvalidAuthProtocolException;
import org.apache.nifi.snmp.exception.InvalidPrivProtocolException;
import org.apache.nifi.snmp.exception.InvalidSnmpVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.PDU;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.AuthHMAC128SHA224;
import org.snmp4j.security.AuthHMAC192SHA256;
import org.snmp4j.security.AuthHMAC256SHA384;
import org.snmp4j.security.AuthHMAC384SHA512;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility helper class that simplifies interactions with target SNMP API and NIFI API.
 */
public final class SNMPUtils {

    public static final String SNMP_PROP_DELIMITER = "$";
    public static final String SNMP_PROP_PREFIX = "snmp" + SNMP_PROP_DELIMITER;

    private static final Logger logger = LoggerFactory.getLogger(SNMPUtils.class);
    private static final String OID_PROP_PATTERN = SNMP_PROP_PREFIX + "%s" + SNMP_PROP_DELIMITER + "%s";
    private static final Pattern OID_PATTERN = Pattern.compile("[0-9+.]*");

    private static final Map<String, OID> AUTH_MAP;
    private static final Map<String, OID> PRIV_MAP;
    private static final Map<String, Integer> VERSION_MAP;
    private static final Map<String, String> REPORT_MAP;

    private SNMPUtils() {
        // hide implicit constructor
    }

    static {
        final Map<String, String> map = new HashMap<>();
        map.put("1.3.6.1.6.3.15.1.1.1", "usmStatsUnsupportedSecLevels");
        map.put("1.3.6.1.6.3.15.1.1.2", "usmStatsNotInTimeWindows");
        map.put("1.3.6.1.6.3.15.1.1.3", "usmStatsUnknownUserNames");
        map.put("1.3.6.1.6.3.15.1.1.4", "usmStatsUnknownEngineIDs");
        map.put("1.3.6.1.6.3.15.1.1.5", "usmStatsWrongDigests");
        map.put("1.3.6.1.6.3.15.1.1.6", "usmStatsDecryptionErrors");
        REPORT_MAP = Collections.unmodifiableMap(map);
    }

    static {
        final Map<String, OID> map = new HashMap<>();
        map.put("DES", PrivDES.ID);
        map.put("3DES", Priv3DES.ID);
        map.put("AES128", PrivAES128.ID);
        map.put("AES192", PrivAES192.ID);
        map.put("AES256", PrivAES256.ID);
        PRIV_MAP = Collections.unmodifiableMap(map);
    }

    static {
        final Map<String, OID> map = new HashMap<>();
        map.put("SHA", AuthSHA.ID);
        map.put("MD5", AuthMD5.ID);
        map.put("HMAC128SHA224", AuthHMAC128SHA224.ID);
        map.put("HMAC192SHA256", AuthHMAC192SHA256.ID);
        map.put("HMAC256SHA384", AuthHMAC256SHA384.ID);
        map.put("HMAC384SHA512", AuthHMAC384SHA512.ID);
        AUTH_MAP = Collections.unmodifiableMap(map);
    }

    static {
        final Map<String, Integer> map = new HashMap<>();
        map.put("SNMPv1", SnmpConstants.version1);
        map.put("SNMPv2c", SnmpConstants.version2c);
        map.put("SNMPv3", SnmpConstants.version3);
        VERSION_MAP = Collections.unmodifiableMap(map);
    }

    public static Map<String, String> getPduAttributeMap(final PDU response) {
        final Vector<? extends VariableBinding> variableBindings = response.getVariableBindings();
        final Map<String, String> attributes = variableBindings.stream()
                .collect(Collectors.toMap(k -> String.format(OID_PROP_PATTERN, k.getOid(), k.getSyntax()),
                        v -> v.getVariable().toString()
                ));

        attributes.computeIfAbsent(SNMP_PROP_PREFIX + "errorIndex", v -> String.valueOf(response.getErrorIndex()));
        attributes.computeIfAbsent(SNMP_PROP_PREFIX + "errorStatus", v -> String.valueOf(response.getErrorStatus()));
        attributes.computeIfAbsent(SNMP_PROP_PREFIX + "errorStatusText", v -> response.getErrorStatusText());
        attributes.computeIfAbsent(SNMP_PROP_PREFIX + "nonRepeaters", v -> String.valueOf(response.getNonRepeaters()));
        attributes.computeIfAbsent(SNMP_PROP_PREFIX + "requestID", v -> String.valueOf(response.getRequestID()));
        attributes.computeIfAbsent(SNMP_PROP_PREFIX + "type", v -> String.valueOf(response.getType()));
        attributes.computeIfAbsent(SNMP_PROP_PREFIX + "typeString", v -> PDU.getTypeString(response.getType()));

        return attributes;
    }

    /**
     * Method to construct {@link FlowFile} attributes from a vector of {@link VariableBinding}
     *
     * @param variableBindings list of {@link VariableBinding}
     * @return the attributes map
     */
    public static Map<String, String> createWalkOidValuesMap(final List<VariableBinding> variableBindings) {
        final Map<String, String> attributes = new HashMap<>();
        variableBindings.forEach(vb -> addAttributeFromVariable(vb, attributes));
        return attributes;
    }

    public static OID getPriv(final String privProtocol) {
        if (PRIV_MAP.containsKey(privProtocol)) {
            return PRIV_MAP.get(privProtocol);
        }
        throw new InvalidPrivProtocolException("Invalid privacy protocol provided.");
    }


    public static OID getAuth(final String authProtocol) {
        if (AUTH_MAP.containsKey(authProtocol)) {
            return AUTH_MAP.get(authProtocol);
        }
        throw new InvalidAuthProtocolException("Invalid authentication protocol provided.");
    }

    public static int getVersion(final String snmpVersion) {
        return Optional.ofNullable(VERSION_MAP.get(snmpVersion))
                .orElseThrow(() -> new InvalidSnmpVersionException("Invalid SNMP version provided."));
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
    public static boolean addVariables(final PDU pdu, final Map<String, String> attributes) {
        boolean result = false;
        for (Map.Entry<String, String> attributeEntry : attributes.entrySet()) {
            if (attributeEntry.getKey().startsWith(SNMPUtils.SNMP_PROP_PREFIX)) {
                final String[] splits = attributeEntry.getKey().split("\\" + SNMPUtils.SNMP_PROP_DELIMITER);
                final String snmpPropName = splits[1];
                final String snmpPropValue = attributeEntry.getValue();
                if (SNMPUtils.OID_PATTERN.matcher(snmpPropName).matches()) {
                    final Optional<Variable> var;
                    if (splits.length == 2) { // no SMI syntax defined
                        var = Optional.of(new OctetString(snmpPropValue));
                    } else {
                        final int smiSyntax = Integer.parseInt(splits[2]);
                        var = SNMPUtils.stringToVariable(snmpPropValue, smiSyntax);
                    }
                    if (var.isPresent()) {
                        final VariableBinding varBind = new VariableBinding(new OID(snmpPropName), var.get());
                        pdu.add(varBind);
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    private static void addAttributeFromVariable(final VariableBinding variableBinding, final Map<String, String> attributes) {
        attributes.put(SNMP_PROP_PREFIX + variableBinding.getOid() + SNMP_PROP_DELIMITER + variableBinding.getVariable().getSyntax(), variableBinding.getVariable().toString());
    }

    private static Optional<Variable> stringToVariable(final String value, final int smiSyntax) {
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
        return Optional.ofNullable(var);
    }

    public static Optional<String> getErrorMessage(final String oid) {
        Optional<String> errorMessage = Optional.ofNullable(REPORT_MAP.get(oid));
        if (!errorMessage.isPresent() && oid.charAt(oid.length() - 1) == '0') {
            final String cutLastOidValue = oid.substring(0, oid.length() - 2);
            errorMessage = Optional.ofNullable(REPORT_MAP.get(cutLastOidValue));
        }
        return errorMessage.map(s -> oid + ": " + s);
    }
}
