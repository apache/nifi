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
package org.apache.nifi.controller;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.GhostConnector;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.controller.exception.ConnectorInstantiationException;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.migration.ConnectorPropertyConfiguration;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestStandardReloadComponent {
    private static final String CONNECTOR_ID = "connector-id";
    private static final BundleCoordinate COORDINATE = new BundleCoordinate("org.apache.nifi", "test-connector", "2.0.0");

    @Mock
    private FlowController flowController;
    @Mock
    private ExtensionManager extensionManager;
    @Mock
    private ConnectorNode connectorNode;

    private StandardReloadComponent reloadComponent;
    private InstanceClassLoader candidateClassLoader;

    @BeforeEach
    void setUp() {
        reloadComponent = new StandardReloadComponent(flowController);
        candidateClassLoader = new InstanceClassLoader(CONNECTOR_ID, TestConnector.class.getName(), Collections.emptySet(), Collections.emptySet(), getClass().getClassLoader());

        final Bundle bundle = new Bundle(new BundleDetails.Builder().coordinate(COORDINATE).workingDir(new File(".")).build(), getClass().getClassLoader());
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(extensionManager.getBundle(COORDINATE)).thenReturn(bundle);
        when(extensionManager.createInstanceClassLoader(any(), any(), any(), any(), anyBoolean(), any())).thenReturn(candidateClassLoader);
        when(connectorNode.getIdentifier()).thenReturn(CONNECTOR_ID);
    }

    @Test
    void testReloadReplacesConnectorAndClassLoader() throws Exception {
        reloadComponent.reload(connectorNode, TestConnector.class.getName(), COORDINATE);

        final ArgumentCaptor<Connector> connectorCaptor = ArgumentCaptor.forClass(Connector.class);
        verify(connectorNode).replaceConnector(connectorCaptor.capture(), eq(COORDINATE), any());
        assertInstanceOf(TestConnector.class, connectorCaptor.getValue());
        verify(extensionManager).removeInstanceClassLoader(CONNECTOR_ID);
        verify(extensionManager).registerInstanceClassLoader(CONNECTOR_ID, candidateClassLoader);
        verify(connectorNode).resetValidationState();
    }

    @Test
    void testReloadClosesCandidateClassLoaderWhenReplacementFails() throws Exception {
        doThrow(new FlowUpdateException("replacement failed")).when(connectorNode).replaceConnector(any(), any(), any());

        assertThrows(ConnectorInstantiationException.class, () -> reloadComponent.reload(connectorNode, TestConnector.class.getName(), COORDINATE));

        verify(extensionManager).closeURLClassLoader(CONNECTOR_ID, candidateClassLoader);
        verify(extensionManager, never()).removeInstanceClassLoader(any());
        verify(extensionManager, never()).registerInstanceClassLoader(any(), any());
        verify(connectorNode, never()).resetValidationState();
    }

    @Test
    void testReloadReplacesConnectorWithGhostWhenInstantiationFails() throws Exception {
        reloadComponent.reload(connectorNode, "org.apache.nifi.MissingConnector", COORDINATE);

        final ArgumentCaptor<Connector> connectorCaptor = ArgumentCaptor.forClass(Connector.class);
        verify(connectorNode).replaceConnector(connectorCaptor.capture(), eq(COORDINATE), any());
        assertInstanceOf(GhostConnector.class, connectorCaptor.getValue());
        verify(extensionManager).closeURLClassLoader(CONNECTOR_ID, candidateClassLoader);
        verify(extensionManager).removeInstanceClassLoader(CONNECTOR_ID);
        verify(extensionManager, never()).registerInstanceClassLoader(any(), any());
        verify(connectorNode).resetValidationState();
    }

    @Test
    void testReloadRestoresNullContextClassLoader() throws Exception {
        final Thread currentThread = Thread.currentThread();
        final ClassLoader originalContextClassLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(null);
            reloadComponent.reload(connectorNode, TestConnector.class.getName(), COORDINATE);
            assertNull(currentThread.getContextClassLoader());
        } finally {
            currentThread.setContextClassLoader(originalContextClassLoader);
        }
    }

    public static class TestConnector extends AbstractConnector {
        @Override
        public VersionedExternalFlow getInitialFlow() {
            return null;
        }

        @Override
        public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
            return null;
        }

        @Override
        public void migrateProperties(final ConnectorPropertyConfiguration configuration) {
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps() {
            return List.of();
        }

        @Override
        protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
        }

        @Override
        public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
            return List.of();
        }
    }
}
