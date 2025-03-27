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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.nifi.snmp.processors.properties.AuthenticationProtocol;
import org.apache.nifi.snmp.processors.properties.PrivacyProtocol;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

class UsmUserDeserializer extends StdDeserializer<UsmUser> {

    public UsmUserDeserializer() {
        super((Class<?>) null);
    }

    @Override
    public UsmUser deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        final JsonNode node = jp.getCodec().readTree(jp);

        final String securityName = node.get("securityName").asText();
        final OID authProtocol = getOid(node, "authProtocol", AuthenticationProtocol::isValid, SNMPUtils::getAuth);
        final OctetString authPassphrase = getOctetString(node, "authPassphrase");

        validatePassphrase(authProtocol, authPassphrase, "Authentication");

        final OID privProtocol = getOid(node, "privProtocol", PrivacyProtocol::isValid, SNMPUtils::getPriv);
        final OctetString privPassphrase = getOctetString(node, "privPassphrase");

        validatePassphrase(privProtocol, privPassphrase, "Privacy");

        return new UsmUser(new OctetString(securityName), authProtocol, authPassphrase, privProtocol, privPassphrase);
    }

    private OID getOid(final JsonNode node, final String key, final Predicate<String> isValid, final Function<String, OID> mapper) {
        return Optional.ofNullable(node.get(key))
                .map(JsonNode::asText)
                .filter(isValid)
                .map(mapper)
                .orElseThrow(() -> new IllegalArgumentException("Invalid protocol: " + node.get(key)));
    }

    private OctetString getOctetString(final JsonNode node, final String key) {
        return Optional.ofNullable(node.get(key))
                .map(JsonNode::asText)
                .map(OctetString::new)
                .orElse(null);
    }

    private void validatePassphrase(final OID protocol, final OctetString passphrase, final String type) {
        if (protocol != null && passphrase == null) {
            throw new IllegalArgumentException(type + " passphrase must be set and at least 8 bytes long if " + type.toLowerCase() + " protocol is specified.");
        }
    }
}