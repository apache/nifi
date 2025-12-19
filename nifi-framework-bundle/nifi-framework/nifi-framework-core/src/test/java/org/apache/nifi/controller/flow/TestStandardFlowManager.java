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

package org.apache.nifi.controller.flow;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.components.connector.ConnectorInitializationContext;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.StandardConnectorInitializationContext;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.GarbageCollectionLog;
import org.apache.nifi.controller.MockStateManagerProvider;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.scheduling.LifecycleStateManager;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardFlowManager {

    private StandardFlowManager flowManager;

    @Mock
    private NiFiProperties nifiProperties;

    @Mock
    private SSLContext sslContext;

    @Mock
    private FlowController flowController;

    @Mock
    private FlowFileEventRepository flowFileEventRepository;

    @Mock
    private ParameterContextManager parameterContextManager;

    @Mock
    private ExtensionManager extensionManager;

    @Mock
    private BundleCoordinate bundleCoordinate;

    @Mock
    private Bundle bundle;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(flowController.isInitialized()).thenReturn(true);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(extensionManager.getBundle(bundleCoordinate)).thenReturn(bundle);
        when(bundle.getClassLoader()).thenReturn(NopConnector.class.getClassLoader());

        flowManager = new StandardFlowManager(nifiProperties, sslContext, flowController, flowFileEventRepository, parameterContextManager);
        when(flowController.getFlowManager()).thenReturn(flowManager);
    }


    @Test
    public void testCreateConnectorParameterValidation() {
        final String validConnectorType = NopConnector.class.getName();
        final String validConnectorId = "test-connector-123";

        final NullPointerException typeException = assertThrows(NullPointerException.class, () -> flowManager.createConnector(null, validConnectorId, bundleCoordinate, true, true));
        assertEquals("Connector Type", typeException.getMessage());

        final NullPointerException idException = assertThrows(NullPointerException.class, () -> flowManager.createConnector(validConnectorType, null, bundleCoordinate, true, true));
        assertEquals("Connector ID", idException.getMessage());

        final NullPointerException bundleException = assertThrows(NullPointerException.class, () -> flowManager.createConnector(validConnectorType, validConnectorId, null, true, true));
        assertEquals("Bundle Coordinate", bundleException.getMessage());
    }

    @Test
    public void testCreateConnectorInitializesConnector() {
        // Prepare a real discovering manager to provide Connector type
        final StandardExtensionDiscoveringManager discoveringManager = new StandardExtensionDiscoveringManager();

        // Discover from system bundle to pick up test services on classpath
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties("src/test/resources/nifi-with-remote.properties");
        final Bundle systemBundle = SystemBundle.create(properties);
        discoveringManager.discoverExtensions(systemBundle, Collections.emptySet());

        // mock FlowController methods necessary for connector creation
        // Most of the methods are used in order to create the ProcessGroup that will be managed by the Connector
        when(flowController.getExtensionManager()).thenReturn(discoveringManager);
        when(flowController.getStateManagerProvider()).thenReturn(new MockStateManagerProvider());
        final RepositoryContextFactory repositoryContextFactory = mock(RepositoryContextFactory.class);
        when(repositoryContextFactory.getFlowFileRepository()).thenReturn(mock(FlowFileRepository.class));
        when(flowController.getRepositoryContextFactory()).thenReturn(repositoryContextFactory);
        when(flowController.getGarbageCollectionLog()).thenReturn(mock(GarbageCollectionLog.class));
        when(flowController.getControllerServiceProvider()).thenReturn(mock(ControllerServiceProvider.class));
        when(flowController.getProvenanceRepository()).thenReturn(mock(ProvenanceRepository.class));
        when(flowController.getBulletinRepository()).thenReturn(mock(BulletinRepository.class));
        when(flowController.getLifecycleStateManager()).thenReturn(mock(LifecycleStateManager.class));
        when(flowController.getFlowFileEventRepository()).thenReturn(mock(FlowFileEventRepository.class));
        when(flowController.getReloadComponent()).thenReturn(mock(ReloadComponent.class));

        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);
        when(connectorRepository.createInitializationContextBuilder()).thenAnswer(
            invocation -> new StandardConnectorInitializationContext.Builder());
        when(flowController.getConnectorRepository()).thenReturn(connectorRepository);

        // Create the connector
        final String type = NopConnector.class.getName();
        final String id = "connector-init-test";
        final ConnectorNode connectorNode = flowManager.createConnector(type, id, systemBundle.getBundleDetails().getCoordinate(), true, false);

        // Verify initialized via context presence
        final Connector connector = connectorNode.getConnector();
        assertInstanceOf(NopConnector.class, connector);

        final NopConnector nopConnector = (NopConnector) connector;
        assertTrue(nopConnector.isInitialized());
        assertFalse(nopConnector.isStarted());
        assertFalse(nopConnector.isConfigured());

        final ConnectorInitializationContext initializationContext = nopConnector.getContext();
        assertNotNull(initializationContext.getLogger());
        assertEquals(id, initializationContext.getIdentifier());
        assertEquals(NopConnector.class.getSimpleName(), initializationContext.getName());
    }

}
