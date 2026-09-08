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
package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.mapping.ComponentIdLookup;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.VersionedComponentFlowMapper;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.security.encryption.PropertyEncryptionProviderInitializationContext;
import org.apache.nifi.security.encryption.SensitivePropertyContext;

import java.nio.charset.StandardCharsets;

public class FlowAnalysisUtil {
    public static final String ENCRYPTED_SENSITIVE_VALUE_SUBSTITUTE = "*****";

    public static VersionedComponentFlowMapper createMapper(ExtensionManager extensionManager) {
        final FlowMappingOptions flowMappingOptions = new FlowMappingOptions.Builder()
            .mapPropertyDescriptors(true)
            .mapControllerServiceReferencesToVersionedId(true)
            .stateLookup(VersionedComponentStateLookup.IDENTITY_LOOKUP)
            .componentIdLookup(ComponentIdLookup.USE_COMPONENT_ID)
            .mapSensitiveConfiguration(true)
            .propertyEncryptionProvider(new PlaceholderPropertyEncryptionProvider())
            .mapAssetReferences(true)
            .build();

        final VersionedComponentFlowMapper mapper = new VersionedComponentFlowMapper(extensionManager, flowMappingOptions) {
            @Override
            public String getGroupId(String groupId) {
                return groupId;
            }

            @Override
            protected String encrypt(String value, SensitivePropertyContext context) {
                return ENCRYPTED_SENSITIVE_VALUE_SUBSTITUTE;
            }
        };

        return mapper;
    }

    private static class PlaceholderPropertyEncryptionProvider implements PropertyEncryptionProvider {

        @Override
        public void initialize(PropertyEncryptionProviderInitializationContext context) {

        }

        @Override
        public byte[] encrypt(byte[] property, SensitivePropertyContext context) {
            return ENCRYPTED_SENSITIVE_VALUE_SUBSTITUTE.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] decrypt(byte[] encryptedProperty, SensitivePropertyContext context) {
            return encryptedProperty;
        }
    }
}
