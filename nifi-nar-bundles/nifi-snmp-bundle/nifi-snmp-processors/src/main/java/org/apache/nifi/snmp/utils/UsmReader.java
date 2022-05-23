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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.nifi.processor.exception.ProcessException;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public interface UsmReader {

    List<UsmUser> readUsm(String usmUsers);

    static UsmReader jsonFileUsmReader() {
        return usmUsersFilePath -> {
            final List<UsmUser> userDetails;
            try (Scanner scanner = new Scanner(new File(usmUsersFilePath))) {
                final String content = scanner.useDelimiter("\\Z").next();
                ObjectMapper mapper = new ObjectMapper();
                SimpleModule module = new SimpleModule();
                module.addDeserializer(UsmUser.class, new UsmUserDeserializer());
                mapper.registerModule(module);
                userDetails = mapper.readValue(content, new TypeReference<List<UsmUser>>() {
                });
            } catch (FileNotFoundException e) {
                throw new ProcessException("USM user file not found, please check the file path and file permissions.", e);
            } catch (JsonProcessingException e) {
                throw new ProcessException("Could not parse USM user file, please check the processor details for examples.", e);
            }
            return userDetails;
        };
    }

    static UsmReader jsonUsmReader() {
        return usmUsersJson -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                SimpleModule module = new SimpleModule();
                module.addDeserializer(UsmUser.class, new UsmUserDeserializer());
                mapper.registerModule(module);
                return mapper.readValue(usmUsersJson, new TypeReference<List<UsmUser>>() {
                });
            } catch (JsonProcessingException e) {
                throw new ProcessException("Could not parse USM user file, please check the processor details for examples.", e);
            }
        };
    }

    static UsmReader securityNamesUsmReader() {
        return usmSecurityNames -> Arrays.stream(usmSecurityNames.trim().split(","))
                .map(securityName -> new UsmUser(
                        new OctetString(securityName), null, null, null, null)
                )
                .collect(Collectors.toList());
    }

    class UsmUserDeserializer extends StdDeserializer<UsmUser> {

        public UsmUserDeserializer() {
            this(null);
        }

        public UsmUserDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public UsmUser deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            String securityName = node.get("securityName").asText();

            OID authProtocol = null;
            final JsonNode authProtocolNode = node.get("authProtocol");
            if (authProtocolNode != null) {
                authProtocol = SNMPUtils.getAuth(authProtocolNode.asText());
            }

            OctetString authPassphrase = null;
            final JsonNode authPassphraseNode = node.get("authPassphrase");
            if (authPassphraseNode != null) {
                authPassphrase = new OctetString(authPassphraseNode.asText());
            }

            if (authProtocol != null && authPassphrase == null) {
                throw new IllegalArgumentException("Authentication passphrase must be set and at least 8 bytes long if" +
                        "authentication protocol is specified.");
            }

            OID privProtocol = null;
            final JsonNode privProtocolNode = node.get("privProtocol");
            if (privProtocolNode != null) {
                privProtocol = SNMPUtils.getPriv(privProtocolNode.asText());
            }

            OctetString privPassphrase = null;
            final JsonNode privPassphraseNode = node.get("privPassphrase");
            if (privPassphraseNode != null) {
                privPassphrase = new OctetString(privPassphraseNode.asText());
            }

            if (privProtocol != null && privPassphrase == null) {
                throw new IllegalArgumentException("Privacy passphrase must be set and at least 8 bytes long if" +
                        "authentication protocol is specified.");
            }

            return new UsmUser(
                    new OctetString(securityName),
                    authProtocol,
                    authPassphrase,
                    privProtocol,
                    privPassphrase
            );
        }
    }
}
