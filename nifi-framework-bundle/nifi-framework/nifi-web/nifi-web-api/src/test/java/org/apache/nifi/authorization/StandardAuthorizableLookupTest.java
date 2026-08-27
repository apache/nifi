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
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorSyncMode;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.ConnectorDAO;
import org.apache.nifi.web.dao.ConnectorManagedComponentLookup;
import org.apache.nifi.web.dao.FlowAnalysisRuleDAO;
import org.apache.nifi.web.dao.FlowRegistryDAO;
import org.apache.nifi.web.dao.ParameterProviderDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
}
