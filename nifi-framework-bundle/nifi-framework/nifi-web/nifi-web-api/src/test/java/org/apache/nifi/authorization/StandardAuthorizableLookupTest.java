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
package org.apache.nifi.authorization;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.authorization.resource.AccessPolicyAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.resource.DataTransferAuthorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.resource.ProvenanceDataAuthorizable;
import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorSyncMode;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.ConnectorDAO;
import org.apache.nifi.web.dao.ConnectorManagedComponentLookup;
import org.apache.nifi.web.dao.ControllerServiceDAO;
import org.apache.nifi.web.dao.FlowAnalysisRuleDAO;
import org.apache.nifi.web.dao.FlowRegistryDAO;
import org.apache.nifi.web.dao.ParameterProviderDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandardAuthorizableLookupTest {

    private static final String COMPONENT_ID = "id";
    private static final String CONNECTOR_ID = "connector-1";
    private static final String ASSET_ID = "asset-1";
    private static final String SECRET_PROVIDER_ID = "parameter-provider-1";
    private static final String SECRET_PROVIDER_NAME = "Vault Provider";
    private static final String DYNAMIC_PROPERTY_NAME = "dynamic.property";
    private static final PropertyDescriptor SUPPORTED_PROPERTY = new PropertyDescriptor.Builder().name("supported.property").build();

    @Test
    void testGetAuthorizableFromResource() {
        final StandardAuthorizableLookup lookup = getLookup();

        Authorizable authorizable = lookup.getAuthorizableFromResource("/processors/id");
        assertInstanceOf(ProcessorNode.class, authorizable);

        authorizable = lookup.getAuthorizableFromResource("/policies/processors/id");
        assertInstanceOf(AccessPolicyAuthorizable.class, authorizable);
        assertInstanceOf(ProcessorNode.class, ((AccessPolicyAuthorizable) authorizable).getBaseAuthorizable());

        authorizable = lookup.getAuthorizableFromResource("/data/processors/id");
        assertInstanceOf(DataAuthorizable.class, authorizable);
        assertInstanceOf(ProcessorNode.class, ((DataAuthorizable) authorizable).getBaseAuthorizable());

        authorizable = lookup.getAuthorizableFromResource("/data-transfer/processors/id");
        assertInstanceOf(DataTransferAuthorizable.class, authorizable);
        assertInstanceOf(ProcessorNode.class, ((DataTransferAuthorizable) authorizable).getBaseAuthorizable());

        authorizable = lookup.getAuthorizableFromResource("/provenance-data/processors/id");
        assertInstanceOf(ProvenanceDataAuthorizable.class, authorizable);
        assertInstanceOf(ProcessorNode.class, ((ProvenanceDataAuthorizable) authorizable).getBaseAuthorizable());

        authorizable = lookup.getAuthorizableFromResource("/operation/processors/id");
        assertInstanceOf(OperationAuthorizable.class, authorizable);
        assertInstanceOf(ProcessorNode.class, ((OperationAuthorizable) authorizable).getBaseAuthorizable());
    }

    @Test
    void testGetControllerServiceReferencingComponentsResolvesRecursiveReferencesForRequestedType() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ControllerServiceDAO controllerServiceDAO = mock(ControllerServiceDAO.class);
        final ControllerServiceNode controllerService = mock(ControllerServiceNode.class);
        final ControllerServiceReference references = mock(ControllerServiceReference.class);
        final ControllerServiceNode referencingService = mock(ControllerServiceNode.class);
        final ProcessorNode referencingProcessor = mock(ProcessorNode.class);

        when(controllerServiceDAO.getControllerService(eq(COMPONENT_ID))).thenReturn(controllerService);
        when(controllerService.getReferences()).thenReturn(references);
        when(references.findRecursiveReferences(eq(ControllerServiceNode.class))).thenReturn(List.of(referencingService));
        when(references.findRecursiveReferences(eq(ProcessorNode.class))).thenReturn(List.of(referencingProcessor));
        lookup.setControllerServiceDAO(controllerServiceDAO);

        assertEquals(List.of(referencingService), lookup.getControllerServiceReferencingComponents(COMPONENT_ID, ControllerServiceNode.class));
        assertEquals(List.of(referencingProcessor), lookup.getControllerServiceReferencingComponents(COMPONENT_ID, ProcessorNode.class));
    }

    @Test
    void testGetAuthorizableFromResourceController() {
        final StandardAuthorizableLookup lookup = getLookup();

        final Authorizable authorizable = lookup.getAuthorizableFromResource("/controller");
        assertInstanceOf(ControllerFacade.class, authorizable);
    }

    @Test
    void testGetAuthorizableFromResourceRegistryClient() {
        final StandardAuthorizableLookup lookup = getLookup();
        final FlowRegistryDAO flowRegistryDAO = mock(FlowRegistryDAO.class);
        final FlowRegistryClientNode flowRegistryClientNode = mock(FlowRegistryClientNode.class);
        when(flowRegistryDAO.getFlowRegistryClient(eq(COMPONENT_ID))).thenReturn(flowRegistryClientNode);
        lookup.setFlowRegistryDAO(flowRegistryDAO);

        final Authorizable authorizable = lookup.getAuthorizableFromResource("/controller/registry-clients/id");
        assertEquals(flowRegistryClientNode, authorizable);
    }

    @Test
    void testGetAuthorizableFromResourceFlowAnalysisRule() {
        final StandardAuthorizableLookup lookup = getLookup();
        final FlowAnalysisRuleDAO flowAnalysisRuleDAO = mock(FlowAnalysisRuleDAO.class);
        final FlowAnalysisRuleNode flowAnalysisRuleNode = mock(FlowAnalysisRuleNode.class);
        when(flowAnalysisRuleDAO.getFlowAnalysisRule(eq(COMPONENT_ID))).thenReturn(flowAnalysisRuleNode);
        lookup.setFlowAnalysisRuleDAO(flowAnalysisRuleDAO);

        final Authorizable authorizable = lookup.getAuthorizableFromResource("/controller/flow-analysis-rules/id");
        assertEquals(flowAnalysisRuleNode, authorizable);
    }

    @Test
    void testGetConnectionResolvesThroughConnectionDAO() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ConnectionDAO connectionDAO = mock(ConnectionDAO.class);
        final Connection connection = mock(Connection.class);
        final Connectable sourceConnectable = mock(Connectable.class);

        when(connectionDAO.getConnection(eq(COMPONENT_ID))).thenReturn(connection);
        when(connection.getSource()).thenReturn(sourceConnectable);
        when(connection.getDestination()).thenReturn(sourceConnectable);
        when(connection.getSourceAuthorizable()).thenReturn(sourceConnectable);
        lookup.setConnectionDAO(connectionDAO);

        final ConnectionAuthorizable result = lookup.getConnection(COMPONENT_ID);

        assertNotNull(result);
        verify(connectionDAO).getConnection(eq(COMPONENT_ID));
    }

    @Test
    void testGetConnectionThroughConnectorManagedFlowFacade() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ConnectorManagedComponentLookup connectorManagedComponentLookup = mock(ConnectorManagedComponentLookup.class);
        final Connection connection = mock(Connection.class);
        final Connectable sourceConnectable = mock(Connectable.class);

        when(connectorManagedComponentLookup.getConnection(eq(COMPONENT_ID))).thenReturn(connection);
        when(connection.getSource()).thenReturn(sourceConnectable);
        when(connection.getDestination()).thenReturn(sourceConnectable);
        when(connection.getSourceAuthorizable()).thenReturn(sourceConnectable);
        lookup.setConnectorManagedComponentLookup(connectorManagedComponentLookup);

        final ConnectionAuthorizable result = lookup.forConnectorManagedFlow().getConnection(COMPONENT_ID);

        assertNotNull(result);
        verify(connectorManagedComponentLookup).getConnection(eq(COMPONENT_ID));
    }

    @Test
    void testGetProcessGroupResolvesThroughProcessGroupDAO() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ProcessGroupDAO processGroupDAO = mock(ProcessGroupDAO.class);
        final ProcessGroup processGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(eq(COMPONENT_ID))).thenReturn(processGroup);
        lookup.setProcessGroupDAO(processGroupDAO);

        final ProcessGroupAuthorizable result = lookup.getProcessGroup(COMPONENT_ID);

        assertNotNull(result);
        verify(processGroupDAO).getProcessGroup(eq(COMPONENT_ID));
    }

    @Test
    void testGetProcessGroupThroughConnectorManagedFlowFacade() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ConnectorManagedComponentLookup connectorManagedComponentLookup = mock(ConnectorManagedComponentLookup.class);
        final ProcessGroup processGroup = mock(ProcessGroup.class);

        when(connectorManagedComponentLookup.getProcessGroup(eq(COMPONENT_ID))).thenReturn(processGroup);
        lookup.setConnectorManagedComponentLookup(connectorManagedComponentLookup);

        final ProcessGroupAuthorizable result = lookup.forConnectorManagedFlow().getProcessGroup(COMPONENT_ID);

        assertNotNull(result);
        verify(connectorManagedComponentLookup).getProcessGroup(eq(COMPONENT_ID));
    }

    @Test
    void testGetConnectorAssetResolvesOwnedAsset() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ConnectorDAO connectorDAO = mock(ConnectorDAO.class);
        lookup.setConnectorDAO(connectorDAO);

        final Asset asset = mock(Asset.class);
        when(asset.getOwnerIdentifier()).thenReturn(CONNECTOR_ID);
        when(connectorDAO.getAsset(eq(ASSET_ID))).thenReturn(Optional.of(asset));

        final ConnectorNode connectorNode = mock(ConnectorNode.class);
        when(connectorDAO.getConnector(eq(CONNECTOR_ID), eq(ConnectorSyncMode.LOCAL_ONLY))).thenReturn(connectorNode);

        final Authorizable result = lookup.getConnectorAsset(CONNECTOR_ID, ASSET_ID);

        assertSame(connectorNode, result);
    }

    @Test
    void testGetConnectorAssetRejectsAssetOwnedByAnotherConnector() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ConnectorDAO connectorDAO = mock(ConnectorDAO.class);
        lookup.setConnectorDAO(connectorDAO);

        final Asset asset = mock(Asset.class);
        when(asset.getOwnerIdentifier()).thenReturn("another-connector");
        when(connectorDAO.getAsset(eq(ASSET_ID))).thenReturn(Optional.of(asset));

        assertThrows(ResourceNotFoundException.class, () -> lookup.getConnectorAsset(CONNECTOR_ID, ASSET_ID));
    }

    @Test
    void testGetConnectorAssetRejectsMissingAsset() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ConnectorDAO connectorDAO = mock(ConnectorDAO.class);
        lookup.setConnectorDAO(connectorDAO);

        when(connectorDAO.getAsset(eq(ASSET_ID))).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class, () -> lookup.getConnectorAsset(CONNECTOR_ID, ASSET_ID));
    }

    @Test
    void testGetConnectorSecretProviderResolvesById() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ParameterProviderDAO parameterProviderDAO = mock(ParameterProviderDAO.class);
        lookup.setParameterProviderDAO(parameterProviderDAO);

        final ParameterProviderNode provider = mock(ParameterProviderNode.class);
        when(parameterProviderDAO.getParameterProvider(eq(SECRET_PROVIDER_ID))).thenReturn(provider);

        final Authorizable result = lookup.getConnectorSecretProvider(SECRET_PROVIDER_ID, null, null);

        assertSame(provider, result);
    }

    @Test
    void testGetConnectorSecretProviderResolvesByName() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ParameterProviderDAO parameterProviderDAO = mock(ParameterProviderDAO.class);
        lookup.setParameterProviderDAO(parameterProviderDAO);

        final ParameterProviderNode matchingProvider = mock(ParameterProviderNode.class);
        when(matchingProvider.getName()).thenReturn(SECRET_PROVIDER_NAME);
        final ParameterProviderNode otherProvider = mock(ParameterProviderNode.class);
        when(otherProvider.getName()).thenReturn("Other Provider");
        when(parameterProviderDAO.getParameterProviders()).thenReturn(Set.of(matchingProvider, otherProvider));

        final Authorizable result = lookup.getConnectorSecretProvider(null, SECRET_PROVIDER_NAME, null);

        assertSame(matchingProvider, result);
    }

    @Test
    void testGetConnectorSecretProviderResolvesByFullyQualifiedName() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ParameterProviderDAO parameterProviderDAO = mock(ParameterProviderDAO.class);
        lookup.setParameterProviderDAO(parameterProviderDAO);

        final ParameterProviderNode matchingProvider = mock(ParameterProviderNode.class);
        when(matchingProvider.getName()).thenReturn(SECRET_PROVIDER_NAME);
        when(parameterProviderDAO.getParameterProviders()).thenReturn(Set.of(matchingProvider));

        final Authorizable result = lookup.getConnectorSecretProvider(null, null, SECRET_PROVIDER_NAME + ".api-key");

        assertSame(matchingProvider, result);
    }

    @Test
    void testGetConnectorSecretProviderFailsWhenUnresolved() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ParameterProviderDAO parameterProviderDAO = mock(ParameterProviderDAO.class);
        lookup.setParameterProviderDAO(parameterProviderDAO);
        when(parameterProviderDAO.getParameterProviders()).thenReturn(Set.of());

        assertThrows(ResourceNotFoundException.class, () -> lookup.getConnectorSecretProvider(null, "Missing Provider", null));
    }

    @Test
    void testGetConnectorSecretProviderMasksMissingProviderId() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ParameterProviderDAO parameterProviderDAO = mock(ParameterProviderDAO.class);
        lookup.setParameterProviderDAO(parameterProviderDAO);
        when(parameterProviderDAO.getParameterProvider(eq(SECRET_PROVIDER_ID)))
                .thenThrow(new ResourceNotFoundException("Unable to locate parameter provider with id '%s'.".formatted(SECRET_PROVIDER_ID)));

        final ResourceNotFoundException exception = assertThrows(ResourceNotFoundException.class,
                () -> lookup.getConnectorSecretProvider(SECRET_PROVIDER_ID, null, null));

        assertEquals("The Parameter Provider for the referenced Secret could not be found", exception.getMessage());
    }

    @Test
    void testGetConnectorSecretProviderFailsWhenMultipleProvidersShareName() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ParameterProviderDAO parameterProviderDAO = mock(ParameterProviderDAO.class);
        lookup.setParameterProviderDAO(parameterProviderDAO);

        final ParameterProviderNode firstProvider = mock(ParameterProviderNode.class);
        when(firstProvider.getName()).thenReturn(SECRET_PROVIDER_NAME);
        final ParameterProviderNode secondProvider = mock(ParameterProviderNode.class);
        when(secondProvider.getName()).thenReturn(SECRET_PROVIDER_NAME);
        when(parameterProviderDAO.getParameterProviders()).thenReturn(Set.of(firstProvider, secondProvider));

        assertThrows(IllegalArgumentException.class, () -> lookup.getConnectorSecretProvider(null, SECRET_PROVIDER_NAME, null));
    }

    @Test
    void testGetConfigurableComponentPropertyDescriptorUsesComponentClassLoader() throws IOException {
        final StandardAuthorizableLookup lookup = getLookup();
        final ContextClassLoaderRecordingComponent component = new ContextClassLoaderRecordingComponent();
        final ComponentAuthorizable authorizable = lookup.getConfigurableComponent(component);

        final ClassLoader componentClassLoader = ContextClassLoaderRecordingComponent.class.getClassLoader();
        final Thread currentThread = Thread.currentThread();
        final ClassLoader originalClassLoader = currentThread.getContextClassLoader();
        try (URLClassLoader requestClassLoader = new URLClassLoader(new URL[0], null)) {
            currentThread.setContextClassLoader(requestClassLoader);

            final PropertyDescriptor descriptor = authorizable.getPropertyDescriptor(DYNAMIC_PROPERTY_NAME);

            assertEquals(DYNAMIC_PROPERTY_NAME, descriptor.getName());
            assertSame(componentClassLoader, component.getDynamicDescriptorContextClassLoader());
            assertSame(requestClassLoader, currentThread.getContextClassLoader());
        } finally {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }

    @Test
    void testGetConfigurableComponentPropertyDescriptorsUsesComponentClassLoader() throws IOException {
        final StandardAuthorizableLookup lookup = getLookup();
        final ContextClassLoaderRecordingComponent component = new ContextClassLoaderRecordingComponent();
        final ComponentAuthorizable authorizable = lookup.getConfigurableComponent(component);

        final ClassLoader componentClassLoader = ContextClassLoaderRecordingComponent.class.getClassLoader();
        final Thread currentThread = Thread.currentThread();
        final ClassLoader originalClassLoader = currentThread.getContextClassLoader();
        try (URLClassLoader requestClassLoader = new URLClassLoader(new URL[0], null)) {
            currentThread.setContextClassLoader(requestClassLoader);

            final List<PropertyDescriptor> descriptors = authorizable.getPropertyDescriptors();

            assertEquals(List.of(SUPPORTED_PROPERTY), descriptors);
            assertSame(componentClassLoader, component.getSupportedDescriptorsContextClassLoader());
            assertSame(requestClassLoader, currentThread.getContextClassLoader());
        } finally {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }

    @Test
    void testGetControllerServicePropertyDescriptorResolvedByControllerServiceNode() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ControllerServiceDAO controllerServiceDAO = mock(ControllerServiceDAO.class);
        final ControllerServiceNode controllerServiceNode = mock(ControllerServiceNode.class);
        final ControllerService controllerService = mock(ControllerService.class);
        when(controllerServiceDAO.getControllerService(eq(COMPONENT_ID))).thenReturn(controllerServiceNode);
        when(controllerServiceNode.getControllerServiceImplementation()).thenReturn(controllerService);
        lookup.setControllerServiceDAO(controllerServiceDAO);

        final PropertyDescriptor implementationDescriptor = new PropertyDescriptor.Builder().name(DYNAMIC_PROPERTY_NAME).dynamic(true).build();
        final PropertyDescriptor nodeDescriptor = new PropertyDescriptor.Builder().fromPropertyDescriptor(implementationDescriptor).sensitive(true).build();
        when(controllerService.getPropertyDescriptor(eq(DYNAMIC_PROPERTY_NAME))).thenReturn(implementationDescriptor);
        when(controllerServiceNode.getPropertyDescriptor(eq(DYNAMIC_PROPERTY_NAME))).thenReturn(nodeDescriptor);

        final PropertyDescriptor descriptor = lookup.getControllerService(COMPONENT_ID).getPropertyDescriptor(DYNAMIC_PROPERTY_NAME);

        assertTrue(descriptor.isSensitive());
    }

    @Test
    void testGetControllerServicePropertyDescriptorsResolvedByControllerServiceNode() {
        final StandardAuthorizableLookup lookup = getLookup();
        final ControllerServiceDAO controllerServiceDAO = mock(ControllerServiceDAO.class);
        final ControllerServiceNode controllerServiceNode = mock(ControllerServiceNode.class);
        final ControllerService controllerService = mock(ControllerService.class);
        when(controllerServiceDAO.getControllerService(eq(COMPONENT_ID))).thenReturn(controllerServiceNode);
        when(controllerServiceNode.getControllerServiceImplementation()).thenReturn(controllerService);
        lookup.setControllerServiceDAO(controllerServiceDAO);

        when(controllerService.getPropertyDescriptors()).thenReturn(List.of());
        when(controllerServiceNode.getPropertyDescriptors()).thenReturn(List.of(SUPPORTED_PROPERTY));

        final List<PropertyDescriptor> descriptors = lookup.getControllerService(COMPONENT_ID).getPropertyDescriptors();

        assertEquals(List.of(SUPPORTED_PROPERTY), descriptors);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getComponentNodeAuthorizableCases")
    void testGetPropertyDescriptorResolvedByComponentNode(final ComponentNodeAuthorizableCase authorizableCase) {
        final StandardAuthorizableLookup lookup = getLookup();
        final ComponentNode componentNode = mock(authorizableCase.componentNodeClass(), Answers.RETURNS_DEEP_STUBS);
        final PropertyDescriptor nodeDescriptor = new PropertyDescriptor.Builder().name(DYNAMIC_PROPERTY_NAME).dynamic(true).sensitive(true).build();
        when(componentNode.getPropertyDescriptor(eq(DYNAMIC_PROPERTY_NAME))).thenReturn(nodeDescriptor);

        final PropertyDescriptor descriptor = authorizableCase.resolver().resolve(lookup, componentNode).getPropertyDescriptor(DYNAMIC_PROPERTY_NAME);

        assertNotNull(descriptor);
        assertTrue(descriptor.isSensitive());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getComponentNodeAuthorizableCases")
    void testGetPropertyDescriptorsResolvedByComponentNode(final ComponentNodeAuthorizableCase authorizableCase) {
        final StandardAuthorizableLookup lookup = getLookup();
        final ComponentNode componentNode = mock(authorizableCase.componentNodeClass(), Answers.RETURNS_DEEP_STUBS);
        when(componentNode.getPropertyDescriptors()).thenReturn(List.of(SUPPORTED_PROPERTY));

        final List<PropertyDescriptor> descriptors = authorizableCase.resolver().resolve(lookup, componentNode).getPropertyDescriptors();

        assertEquals(List.of(SUPPORTED_PROPERTY), descriptors);
    }

    private static Stream<ComponentNodeAuthorizableCase> getComponentNodeAuthorizableCases() {
        return Stream.of(
                new ComponentNodeAuthorizableCase("Reporting Task", ReportingTaskNode.class, (lookup, componentNode) -> {
                    final ReportingTaskDAO reportingTaskDAO = mock(ReportingTaskDAO.class);
                    when(reportingTaskDAO.getReportingTask(eq(COMPONENT_ID))).thenReturn((ReportingTaskNode) componentNode);
                    lookup.setReportingTaskDAO(reportingTaskDAO);
                    return lookup.getReportingTask(COMPONENT_ID);
                }),
                new ComponentNodeAuthorizableCase("Flow Analysis Rule", FlowAnalysisRuleNode.class, (lookup, componentNode) -> {
                    final FlowAnalysisRuleDAO flowAnalysisRuleDAO = mock(FlowAnalysisRuleDAO.class);
                    when(flowAnalysisRuleDAO.getFlowAnalysisRule(eq(COMPONENT_ID))).thenReturn((FlowAnalysisRuleNode) componentNode);
                    lookup.setFlowAnalysisRuleDAO(flowAnalysisRuleDAO);
                    return lookup.getFlowAnalysisRule(COMPONENT_ID);
                }),
                new ComponentNodeAuthorizableCase("Parameter Provider", ParameterProviderNode.class, (lookup, componentNode) -> {
                    final ParameterProviderDAO parameterProviderDAO = mock(ParameterProviderDAO.class);
                    when(parameterProviderDAO.getParameterProvider(eq(COMPONENT_ID))).thenReturn((ParameterProviderNode) componentNode);
                    lookup.setParameterProviderDAO(parameterProviderDAO);
                    return lookup.getParameterProvider(COMPONENT_ID);
                }),
                new ComponentNodeAuthorizableCase("Flow Registry Client", FlowRegistryClientNode.class, (lookup, componentNode) -> {
                    final FlowRegistryDAO flowRegistryDAO = mock(FlowRegistryDAO.class);
                    when(flowRegistryDAO.getFlowRegistryClient(eq(COMPONENT_ID))).thenReturn((FlowRegistryClientNode) componentNode);
                    lookup.setFlowRegistryDAO(flowRegistryDAO);
                    return lookup.getFlowRegistryClient(COMPONENT_ID);
                })
        );
    }

    private StandardAuthorizableLookup getLookup() {
        final ExtensionManager extensionManager = mock(ExtensionDiscoveringManager.class);
        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        when(controllerFacade.getExtensionManager()).thenReturn(extensionManager);

        final ProcessorDAO processorDAO = mock(ProcessorDAO.class);
        final ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processorDAO.getProcessor(eq(COMPONENT_ID))).thenReturn(processorNode);

        final StandardAuthorizableLookup lookup = new StandardAuthorizableLookup();
        lookup.setProcessorDAO(processorDAO);
        lookup.setControllerFacade(controllerFacade);
        return lookup;
    }

    private interface ComponentAuthorizableResolver {
        ComponentAuthorizable resolve(StandardAuthorizableLookup lookup, ComponentNode componentNode);
    }

    private record ComponentNodeAuthorizableCase(String componentType, Class<? extends ComponentNode> componentNodeClass, ComponentAuthorizableResolver resolver) {
        @Override
        public String toString() {
            return componentType;
        }
    }

    private static class ContextClassLoaderRecordingComponent extends AbstractConfigurableComponent {
        private volatile ClassLoader dynamicDescriptorContextClassLoader;
        private volatile ClassLoader supportedDescriptorsContextClassLoader;

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            supportedDescriptorsContextClassLoader = Thread.currentThread().getContextClassLoader();
            return List.of(SUPPORTED_PROPERTY);
        }

        @Override
        protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
            dynamicDescriptorContextClassLoader = Thread.currentThread().getContextClassLoader();
            return new PropertyDescriptor.Builder().name(propertyDescriptorName).dynamic(true).build();
        }

        @Override
        public String getIdentifier() {
            return COMPONENT_ID;
        }

        ClassLoader getDynamicDescriptorContextClassLoader() {
            return dynamicDescriptorContextClassLoader;
        }

        ClassLoader getSupportedDescriptorsContextClassLoader() {
            return supportedDescriptorsContextClassLoader;
        }
    }
}
