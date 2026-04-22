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

package org.apache.nifi.registry.flow.mapping;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.attribute.expression.language.VariableImpact;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.BackoffMechanism;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.state.StandardStateMap;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedNodeState;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestVersionedComponentFlowMapper {
    private static final SensitiveValueEncryptor ENCRYPTOR = value -> new StringBuilder(value).reverse().toString();

    @Test
    public void testMappingProcessorWithSensitiveValuesGivesNullValue() {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final FlowMappingOptions mappingOptions = FlowMappingOptions.DEFAULT_OPTIONS;
        final VersionedComponentFlowMapper mapper = new VersionedComponentFlowMapper(extensionManager, mappingOptions);

        final ControllerServiceProvider serviceProvider = mock(ControllerServiceProvider.class);
        final Map<String, String> properties = new HashMap<>();
        properties.put("Property A", "A");
        properties.put("Sensitive Property B", "B");

        final ProcessorNode procNode = createProcessorNode(properties);
        final VersionedProcessor versionedProcessor = mapper.mapProcessor(procNode, serviceProvider, Collections.emptySet(), Collections.emptyMap());
        assertNotNull(versionedProcessor);

        final Map<String, String> versionedProperties = versionedProcessor.getProperties();
        assertEquals("A", versionedProperties.get("Property A"));
        assertNull(versionedProperties.get("Sensitive Property B"));
    }

    @Test
    public void testMappingProcessorWithSensitiveValuesLeavesSensitiveParameterReference() {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final FlowMappingOptions mappingOptions = FlowMappingOptions.DEFAULT_OPTIONS;
        final VersionedComponentFlowMapper mapper = new VersionedComponentFlowMapper(extensionManager, mappingOptions);

        final ControllerServiceProvider serviceProvider = mock(ControllerServiceProvider.class);
        final Map<String, String> properties = new HashMap<>();
        properties.put("Property A", "A");
        properties.put("Sensitive Property B", "#{Param B}");

        final ProcessorNode procNode = createProcessorNode(properties);
        final VersionedProcessor versionedProcessor = mapper.mapProcessor(procNode, serviceProvider, Collections.emptySet(), Collections.emptyMap());
        assertNotNull(versionedProcessor);

        final Map<String, String> versionedProperties = versionedProcessor.getProperties();
        assertEquals("A", versionedProperties.get("Property A"));
        assertEquals("#{Param B}", versionedProperties.get("Sensitive Property B"));
    }

    @Test
    public void testMappingProcessorWithSensitiveValuesProvidesEncryptedValue() {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final FlowMappingOptions mappingOptions = new FlowMappingOptions.Builder()
            .stateLookup(VersionedComponentStateLookup.IDENTITY_LOOKUP)
            .componentIdLookup(ComponentIdLookup.USE_COMPONENT_ID)
            .mapSensitiveConfiguration(true)
            .sensitiveValueEncryptor(ENCRYPTOR)
            .build();

        final VersionedComponentFlowMapper mapper = new VersionedComponentFlowMapper(extensionManager, mappingOptions);

        final ControllerServiceProvider serviceProvider = mock(ControllerServiceProvider.class);
        final Map<String, String> properties = new HashMap<>();
        properties.put("Property A", "A");
        properties.put("Sensitive Property B", "A B C");

        final ProcessorNode procNode = createProcessorNode(properties);
        final VersionedProcessor versionedProcessor = mapper.mapProcessor(procNode, serviceProvider, Collections.emptySet(), Collections.emptyMap());
        assertNotNull(versionedProcessor);

        final Map<String, String> versionedProperties = versionedProcessor.getProperties();
        assertEquals("A", versionedProperties.get("Property A"));
        assertEquals("enc{C B A}", versionedProperties.get("Sensitive Property B"));
    }

    @Test
    public void testControllerServicesMapsToVersionedId() {
        testControllerServicesMapsToProperId(true);
    }

    @Test
    public void testControllerServicesMapsToInstanceId() {
        testControllerServicesMapsToProperId(false);
    }

    private void testControllerServicesMapsToProperId(final boolean useVersionedId) {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final FlowMappingOptions mappingOptions = new FlowMappingOptions.Builder()
            .stateLookup(VersionedComponentStateLookup.IDENTITY_LOOKUP)
            .componentIdLookup(ComponentIdLookup.VERSIONED_OR_GENERATE)
            .mapControllerServiceReferencesToVersionedId(useVersionedId)
            .build();

        final VersionedComponentFlowMapper mapper = new VersionedComponentFlowMapper(extensionManager, mappingOptions);

        final ControllerServiceNode mockServiceNode = mock(ControllerServiceNode.class);
        when(mockServiceNode.getIdentifier()).thenReturn("1234");
        when(mockServiceNode.getVersionedComponentId()).thenReturn(Optional.of("versioned"));

        final ControllerServiceProvider serviceProvider = mock(ControllerServiceProvider.class);
        when(serviceProvider.getControllerServiceNode("1234")).thenReturn(mockServiceNode);
        final Map<String, String> properties = new HashMap<>();
        properties.put("Service A", "1234");

        final ProcessorNode procNode = createProcessorNode(properties);
        final VersionedProcessor versionedProcessor = mapper.mapProcessor(procNode, serviceProvider, Collections.emptySet(), Collections.emptyMap());
        assertNotNull(versionedProcessor);

        final Map<String, String> versionedProperties = versionedProcessor.getProperties();
        final String idReferenced = versionedProperties.get("Service A");
        if (useVersionedId) {
            assertEquals("versioned", idReferenced);
        } else {
            assertEquals("1234", idReferenced);
        }
    }

    @Test
    public void testComponentStateNotMappedWhenDisabled() {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final FlowMappingOptions mappingOptions = FlowMappingOptions.DEFAULT_OPTIONS;
        final VersionedComponentFlowMapper mapper = new VersionedComponentFlowMapper(extensionManager, mappingOptions);

        final ProcessorNode procNode = createStatefulProcessorNode();

        final VersionedProcessor versionedProcessor = mapper.mapProcessor(procNode, mock(ControllerServiceProvider.class),
                Collections.emptySet(), Collections.emptyMap());
        assertNotNull(versionedProcessor);
        assertNull(versionedProcessor.getComponentState());
    }

    @Test
    public void testStandaloneExportSkipsClusterStateForProcessorWithBothScopes() throws IOException {
        final Map<String, String> localState = Map.of("wal.position", "0/42");
        final Map<String, String> clusterState = Map.of("wal.position", "0/42");

        final ProcessorNode procNode = createStatefulProcessorNode();
        final FlowMappingOptions mappingOptions = buildComponentStateMappingOptions(procNode, localState, clusterState, false, 0);

        final VersionedComponentFlowMapper mapper = new VersionedComponentFlowMapper(mock(ExtensionManager.class), mappingOptions);
        final VersionedProcessor versionedProcessor = mapper.mapProcessor(procNode, mock(ControllerServiceProvider.class),
                Collections.emptySet(), Collections.emptyMap());

        assertNotNull(versionedProcessor);
        final VersionedComponentState mappedState = versionedProcessor.getComponentState();
        assertNotNull(mappedState);
        assertNull(mappedState.getClusterState());

        final List<VersionedNodeState> localNodeStates = mappedState.getLocalNodeStates();
        assertNotNull(localNodeStates);
        assertEquals(1, localNodeStates.size());
        assertEquals(localState, localNodeStates.get(0).getState());
    }

    @Test
    public void testClusteredExportCapturesBothLocalAndClusterState() throws IOException {
        final Map<String, String> localState = Map.of("local.key", "local-value");
        final Map<String, String> clusterState = Map.of("cluster.key", "cluster-value");

        final ProcessorNode procNode = createStatefulProcessorNode();
        final FlowMappingOptions mappingOptions = buildComponentStateMappingOptions(procNode, localState, clusterState, true, 1);

        final VersionedComponentFlowMapper mapper = new VersionedComponentFlowMapper(mock(ExtensionManager.class), mappingOptions);
        final VersionedProcessor versionedProcessor = mapper.mapProcessor(procNode, mock(ControllerServiceProvider.class),
                Collections.emptySet(), Collections.emptyMap());

        assertNotNull(versionedProcessor);
        final VersionedComponentState mappedState = versionedProcessor.getComponentState();
        assertNotNull(mappedState);
        assertEquals(clusterState, mappedState.getClusterState());

        final List<VersionedNodeState> localNodeStates = mappedState.getLocalNodeStates();
        assertNotNull(localNodeStates);
        assertEquals(2, localNodeStates.size());
        assertNull(localNodeStates.get(0));
        assertEquals(localState, localNodeStates.get(1).getState());
    }

    private FlowMappingOptions buildComponentStateMappingOptions(final ProcessorNode procNode, final Map<String, String> localState,
                                                                 final Map<String, String> clusterState, final boolean clusterProviderEnabled,
                                                                 final int localNodeOrdinal) throws IOException {
        final StateManager stateManager = mock(StateManager.class);
        when(stateManager.getState(Scope.LOCAL)).thenReturn(new StandardStateMap(localState, Optional.empty()));
        when(stateManager.getState(Scope.CLUSTER)).thenReturn(new StandardStateMap(clusterState, Optional.empty()));

        final StateManagerProvider stateManagerProvider = mock(StateManagerProvider.class);
        when(stateManagerProvider.isClusterProviderEnabled()).thenReturn(clusterProviderEnabled);
        when(stateManagerProvider.getStateManager(procNode.getIdentifier())).thenReturn(stateManager);

        return new FlowMappingOptions.Builder()
                .stateLookup(VersionedComponentStateLookup.IDENTITY_LOOKUP)
                .componentIdLookup(ComponentIdLookup.USE_COMPONENT_ID)
                .mapComponentState(true)
                .stateManagerProvider(stateManagerProvider)
                .localNodeOrdinal(localNodeOrdinal)
                .build();
    }

    private ProcessorNode createStatefulProcessorNode() {
        final ProcessorNode procNode = createProcessorNode(new HashMap<>());
        when(procNode.getComponent()).thenReturn(new StatefulLocalAndClusterProcessor());
        return procNode;
    }

    @Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "Test processor with both local and cluster state scopes")
    private static class StatefulLocalAndClusterProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        }
    }

    private ProcessorNode createProcessorNode(final Map<String, String> properties) {
        final ExpressionLanguageAgnosticParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        final Map<PropertyDescriptor, PropertyConfiguration> propertyMap = new HashMap<>();
        final Map<String, PropertyDescriptor> propertyDescriptors = new HashMap<>();

        for (final Map.Entry<String, String> property : properties.entrySet()) {
            final String propName = property.getKey();
            final String value = property.getValue();

            final PropertyDescriptor descriptor = new PropertyDescriptor.Builder()
                .name(propName)
                .sensitive(propName.toLowerCase().contains("sensitive"))
                .identifiesControllerService(propName.toLowerCase().contains("service ") ? ControllerService.class : null)
                .addValidator(Validator.VALID)
                .build();

            final ParameterTokenList tokenList = parameterParser.parseTokens(value);
            final List<ParameterReference> referenceList = tokenList.toReferenceList();
            final PropertyConfiguration configuration = new PropertyConfiguration(value, tokenList, referenceList, VariableImpact.NEVER_IMPACTED);
            propertyMap.put(descriptor, configuration);
            propertyDescriptors.put(propName, descriptor);
        }

        final ProcessorNode procNode = mock(ProcessorNode.class);
        when(procNode.getProperties()).thenReturn(propertyMap);
        when(procNode.getProperty(any(PropertyDescriptor.class))).thenAnswer((Answer<PropertyConfiguration>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return propertyMap.get(descriptor);
        });

        when(procNode.getRawPropertyValue(any(PropertyDescriptor.class))).thenAnswer((Answer<String>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return properties.get(descriptor.getName());
        });

        final String id = UUID.randomUUID().toString();
        when(procNode.getIdentifier()).thenReturn(id);

        when(procNode.getBulletinLevel()).thenReturn(LogLevel.WARN);
        when(procNode.getExecutionNode()).thenReturn(ExecutionNode.ALL);
        when(procNode.getPropertyDescriptors()).thenReturn(new ArrayList<>(propertyDescriptors.values()));
        when(procNode.getBundleCoordinate()).thenReturn(new BundleCoordinate("group", "id", "version"));
        when(procNode.getPosition()).thenReturn(new Position(0D, 0D));
        when(procNode.getSchedulingStrategy()).thenReturn(SchedulingStrategy.TIMER_DRIVEN);
        when(procNode.getPhysicalScheduledState()).thenReturn(ScheduledState.STOPPED);
        when(procNode.getBackoffMechanism()).thenReturn(BackoffMechanism.YIELD_PROCESSOR);

        return procNode;
    }
}
