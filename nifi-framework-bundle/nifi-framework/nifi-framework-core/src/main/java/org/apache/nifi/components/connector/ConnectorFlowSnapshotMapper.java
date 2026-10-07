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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.flow.mapping.VersionedComponentFlowMapper;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Maps the current Connector-managed flow to its authoritative persisted representation.
 */
public final class ConnectorFlowSnapshotMapper {
    private final ExtensionManager extensionManager;
    private final ControllerServiceProvider controllerServiceProvider;
    private final PropertyEncryptionProvider propertyEncryptionProvider;

    public ConnectorFlowSnapshotMapper(final ExtensionManager extensionManager, final ControllerServiceProvider controllerServiceProvider,
            final PropertyEncryptionProvider propertyEncryptionProvider) {
        this.extensionManager = Objects.requireNonNull(extensionManager, "Extension Manager required");
        this.controllerServiceProvider = Objects.requireNonNull(controllerServiceProvider, "Controller Service Provider required");
        this.propertyEncryptionProvider = Objects.requireNonNull(propertyEncryptionProvider, "Property Encryption Provider required");
    }

    ConnectorFlowSnapshot capture(final FrameworkFlowContext flowContext, final Bundle bundle, final SecretsManager secretsManager) {
        final VersionedComponentFlowMapper mapper = VersionedComponentFlowMapper.forPersistedFlow(extensionManager, propertyEncryptionProvider, VersionedComponentStateLookup.IDENTITY_LOOKUP);

        final VersionedProcessGroup managedProcessGroup = mapper.mapNonVersionedProcessGroup(flowContext.getManagedProcessGroup(), controllerServiceProvider);

        final ParameterContext parameterContext = flowContext.getManagedProcessGroup().getParameterContext();
        final VersionedParameterContext mappedParameterContext = parameterContext == null ? null : mapper.mapParameterContext(parameterContext);

        final ConnectorConfiguration activeConfiguration = flowContext.getConfigurationContext().toConnectorConfiguration();

        final Set<SecretReference> configurationSecretReferences = new HashSet<>();
        for (final NamedStepConfiguration stepConfiguration : activeConfiguration.getNamedStepConfigurations()) {
            for (final ConnectorValueReference valueReference : stepConfiguration.configuration().getPropertyValues().values()) {
                if (valueReference instanceof final SecretReference secretReference) {
                    configurationSecretReferences.add(secretReference);
                }
            }
        }

        final Map<SecretReference, Secret> secrets = configurationSecretReferences.isEmpty()
            ? Map.of()
            : secretsManager.getSecrets(configurationSecretReferences);

        final Map<String, SecretReference> parameterSecretReferences = ParameterSecretReferences.matchSensitiveParametersToSecretReferences(parameterContext, secrets);
        final VersionedParameterContext managedParameterContext = ParameterSecretReferences.stripSensitiveValues(mappedParameterContext);

        return new ConnectorFlowSnapshot(managedProcessGroup, managedParameterContext, parameterSecretReferences, bundle, activeConfiguration);
    }
}
