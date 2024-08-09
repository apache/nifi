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
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;

public class FlowAnalysisUtil {
    public static final String ENCRYPTED_SENSITIVE_VALUE_SUBSTITUTE = "*****";

    public static NiFiRegistryFlowMapper createMapper(ExtensionManager extensionManager) {
        final FlowMappingOptions flowMappingOptions = new FlowMappingOptions.Builder()
            .mapPropertyDescriptors(true)
            .mapControllerServiceReferencesToVersionedId(true)
            .stateLookup(VersionedComponentStateLookup.IDENTITY_LOOKUP)
            .componentIdLookup(ComponentIdLookup.USE_COMPONENT_ID)
            .mapSensitiveConfiguration(true)
            .sensitiveValueEncryptor(value -> ENCRYPTED_SENSITIVE_VALUE_SUBSTITUTE)
            .mapAssetReferences(true)
            .build();

        final NiFiRegistryFlowMapper mapper = new NiFiRegistryFlowMapper(extensionManager, flowMappingOptions) {
            @Override
            public String getGroupId(String groupId) {
                return groupId;
            }

            @Override
            protected String encrypt(String value) {
                return ENCRYPTED_SENSITIVE_VALUE_SUBSTITUTE;
            }
        };

        return mapper;
    }

}
