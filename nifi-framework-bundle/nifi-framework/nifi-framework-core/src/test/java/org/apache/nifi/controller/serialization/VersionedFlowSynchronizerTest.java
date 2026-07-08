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
package org.apache.nifi.controller.serialization;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.ConnectorSyncMode;
import org.apache.nifi.components.connector.ConnectorSyncResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.SnippetManager;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.parameter.ParameterProviderLookup;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.parameter.StandardParameterContextManager;
import org.apache.nifi.parameter.StandardParameterProviderConfiguration;
import org.apache.nifi.persistence.FlowConfigurationArchiveManager;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class VersionedFlowSynchronizerTest {
    private static final String FLOW_CONFIGURATION = "flow.json.gz";

    private static final Bundle CORE_BUNDLE = new Bundle("org.apache.nifi", "nifi-framework-core", "2.0.0");

    private static final String SERVICE_INSTANCE_ID = "service-instance-id";

    private static final String PROPERTY_NAME = "Address";

    private static final String PROPERTY_VALUE = "127.0.0.1";

    private static final String SENSITIVE_PROPERTY_NAME = "Protected";

    private static final String ENCRYPTED_PROPERTY_VALUE = "enc{encoded}";

    private static final String DECRYPTED_PROPERTY_VALUE = "decoded";

    private static final String REPORTING_TASK_INSTANCE_ID = "reporting-task-instance-id";

    private static final String REPORTING_TASK_TYPE = "org.apache.nifi.reporting.TestReportingTask";

    @Mock
    private ExtensionManager extensionManager;

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @Mock
    private DataFlow dataFlow;

    @Mock
    private ProcessGroup rootGroup;

    @Mock
    private VersionedDataflow versionedDataflow;

    @Mock
    private VersionedProcessGroup versionedRootGroup;

    @Mock
    private FlowService flowService;

    @Mock
    private SnippetManager snippetManager;

    @Mock
    private PropertyEncryptor encryptor;

    @Mock
    private VersionedComponentStateLookup stateLookup;

    @Mock
    private ControllerServiceProvider controllerServiceProvider;

    private VersionedFlowSynchronizer versionedFlowSynchronizer;

    @BeforeEach
    void setVersionedFlowSynchronizer(@TempDir final Path tempDir) {
        final File flowStorageFile = tempDir.resolve(UUID.randomUUID().toString()).toFile();
        final Path flowConfigurationFile = tempDir.resolve(FLOW_CONFIGURATION);
        final Map<String, String> additionalProperties = Map.of(
                NiFiProperties.FLOW_CONFIGURATION_FILE, flowConfigurationFile.toString()
        );
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, additionalProperties);
        final FlowConfigurationArchiveManager archiveManager = new FlowConfigurationArchiveManager(properties);

        versionedFlowSynchronizer = new VersionedFlowSynchronizer(extensionManager, flowStorageFile, archiveManager);
    }

    @Test
    void testSyncRootGroupNotEmpty() {
        setRootGroup();
        when(rootGroup.isEmpty()).thenReturn(false);

        assertThrows(UninheritableFlowException.class, () ->
            versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST)
        );
    }

    @Test
    void testSyncInheritControllerServicesMigrateConfiguration() {
        setRootGroup();
        setFlowController();

        // Create Versioned Controller Service enabled with minimum required properties
        final VersionedControllerService controllerService = getVersionedControllerService();
        final List<VersionedControllerService> controllerServices = List.of(
                controllerService
        );
        when(versionedDataflow.getControllerServices()).thenReturn(controllerServices);

        // Return null for initial Controller Service Node lookup and then return instance created
        final ControllerServiceNode controllerServiceNode = mock(ControllerServiceNode.class);
        when(flowManager.getRootControllerService(eq(SERVICE_INSTANCE_ID))).thenReturn(null).thenReturn(controllerServiceNode);

        // Mock Property Descriptor for sensitive Property with decrypted value
        final PropertyDescriptor sensitivePropertyDescriptor = mock(PropertyDescriptor.class);
        when(controllerServiceNode.getPropertyDescriptor(eq(SENSITIVE_PROPERTY_NAME))).thenReturn(sensitivePropertyDescriptor);
        when(encryptor.decrypt(any())).thenReturn(DECRYPTED_PROPERTY_VALUE);

        // Return created Controller Service Node as a result of null returned for initial lookup method
        when(flowManager.createControllerService(any(), any(), any(), any(), eq(true), eq(true), any())).thenReturn(controllerServiceNode);

        versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST);

        final ArgumentCaptor<Map<String, String>> originalPropertiesCaptor = ArgumentCaptor.captor();
        verify(controllerServiceNode).migrateConfiguration(originalPropertiesCaptor.capture(), any());

        final Map<String, String> originalProperties = originalPropertiesCaptor.getValue();
        final Map<String, String> expectedDecryptedProperties = Map.of(
                PROPERTY_NAME, PROPERTY_VALUE,
                SENSITIVE_PROPERTY_NAME, DECRYPTED_PROPERTY_VALUE
        );
        assertEquals(expectedDecryptedProperties, originalProperties);
    }

    @Test
    void testSyncInheritReportingTasksMigrateConfigurationBeforeStart() {
        setRootGroup();
        setFlowController();

        // Create Versioned Reporting Task with RUNNING state to verify migration happens before start
        final VersionedReportingTask reportingTask = getVersionedReportingTask();
        final List<VersionedReportingTask> reportingTasks = List.of(reportingTask);
        when(versionedDataflow.getReportingTasks()).thenReturn(reportingTasks);

        // Return null for initial Reporting Task Node lookup indicating it needs to be created
        final ReportingTaskNode reportingTaskNode = mock(ReportingTaskNode.class);
        when(flowController.getReportingTaskNode(eq(REPORTING_TASK_INSTANCE_ID))).thenReturn(null);

        // Mock Property Descriptor for sensitive Property with decrypted value
        final PropertyDescriptor sensitivePropertyDescriptor = mock(PropertyDescriptor.class);
        when(reportingTaskNode.getPropertyDescriptor(eq(SENSITIVE_PROPERTY_NAME))).thenReturn(sensitivePropertyDescriptor);
        when(encryptor.decrypt(any())).thenReturn(DECRYPTED_PROPERTY_VALUE);

        // Return created Reporting Task Node
        when(flowController.createReportingTask(any(), eq(REPORTING_TASK_INSTANCE_ID), any(), eq(false))).thenReturn(reportingTaskNode);

        versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST);

        // Verify migrateConfiguration is called with decrypted properties
        final ArgumentCaptor<Map<String, String>> originalPropertiesCaptor = ArgumentCaptor.captor();
        verify(reportingTaskNode).migrateConfiguration(originalPropertiesCaptor.capture(), any());

        final Map<String, String> originalProperties = originalPropertiesCaptor.getValue();
        final Map<String, String> expectedDecryptedProperties = Map.of(
                PROPERTY_NAME, PROPERTY_VALUE,
                SENSITIVE_PROPERTY_NAME, DECRYPTED_PROPERTY_VALUE
        );
        assertEquals(expectedDecryptedProperties, originalProperties);

        // Verify that migrateConfiguration is called BEFORE startReportingTask
        // This is the key assertion: migration must complete before task is started
        final InOrder inOrder = inOrder(reportingTaskNode, flowController);
        inOrder.verify(reportingTaskNode).migrateConfiguration(any(), any());
        inOrder.verify(flowController).startReportingTask(reportingTaskNode);
    }

    private VersionedReportingTask getVersionedReportingTask() {
        final VersionedReportingTask reportingTask = new VersionedReportingTask();
        reportingTask.setInstanceIdentifier(REPORTING_TASK_INSTANCE_ID);
        reportingTask.setBundle(CORE_BUNDLE);
        reportingTask.setType(REPORTING_TASK_TYPE);
        reportingTask.setName("Test Reporting Task");
        reportingTask.setSchedulingStrategy("TIMER_DRIVEN");
        reportingTask.setSchedulingPeriod("5 mins");

        final Map<String, String> versionedProperties = Map.of(
                PROPERTY_NAME, PROPERTY_VALUE,
                SENSITIVE_PROPERTY_NAME, ENCRYPTED_PROPERTY_VALUE
        );

        reportingTask.setProperties(versionedProperties);
        reportingTask.setPropertyDescriptors(Map.of());
        // Set to RUNNING to verify migration happens before start
        reportingTask.setScheduledState(ScheduledState.RUNNING);
        return reportingTask;
    }

    private VersionedControllerService getVersionedControllerService() {
        final VersionedControllerService controllerService = new VersionedControllerService();
        controllerService.setInstanceIdentifier(SERVICE_INSTANCE_ID);
        controllerService.setBundle(CORE_BUNDLE);

        final Map<String, String> versionedProperties = Map.of(
                PROPERTY_NAME, PROPERTY_VALUE,
                SENSITIVE_PROPERTY_NAME, ENCRYPTED_PROPERTY_VALUE
        );

        controllerService.setProperties(versionedProperties);
        controllerService.setPropertyDescriptors(Map.of());
        controllerService.setScheduledState(ScheduledState.ENABLED);
        return controllerService;
    }

    @Test
    void testSyncReconcilesProviderBackedContextWithNonProvidedParameter() {
        setRootGroup();
        setFlowController();

        final String providerId = "provider-1";
        final String contextName = "openflow-rds-ingest";
        final String paramName = "db.host";

        // Parameter Provider infrastructure
        final ParameterProvider parameterProvider = mock(ParameterProvider.class);
        when(parameterProvider.getIdentifier()).thenReturn(providerId);
        final ParameterProviderNode parameterProviderNode = mock(ParameterProviderNode.class);
        when(parameterProviderNode.getParameterProvider()).thenReturn(parameterProvider);
        final ParameterProviderLookup parameterProviderLookup = mock(ParameterProviderLookup.class);
        when(parameterProviderLookup.getParameterProvider(providerId)).thenReturn(parameterProviderNode);

        // The node's existing, self-consistent flow: a provider-backed context whose parameter is provider-supplied.
        final StandardParameterContext existingContext = new StandardParameterContext.Builder()
                .id("provider-backed-context")
                .name(contextName)
                .parameterReferenceManager(ParameterReferenceManager.EMPTY)
                .parameterProviderLookup(parameterProviderLookup)
                .parameterProviderConfiguration(new StandardParameterProviderConfiguration(providerId, "Group", true))
                .build();
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(paramName).build();
        existingContext.setParameters(Collections.singletonMap(paramName,
                new Parameter.Builder().descriptor(descriptor).value("localhost").provided(true).build()));

        // Stub the Parameter Provider as VALID and supplying the parameter, so reconciliation sources the value
        // from the Provider (exercising createParameterMap's provider value-sourcing branch) rather than the
        // "provided parameter not found" fallback that yields a null value.
        when(parameterProviderNode.getIdentifier()).thenReturn(providerId);
        when(parameterProviderNode.getValidationStatus()).thenReturn(ValidationStatus.VALID);
        final ParameterGroup fetchedGroup = new ParameterGroup("Group", List.of(
                new Parameter.Builder().descriptor(descriptor).value("provider-host").provided(true).build()));
        when(parameterProviderNode.findFetchedParameterGroup("Group")).thenReturn(Optional.of(fetchedGroup));

        final StandardParameterContextManager contextManager = new StandardParameterContextManager();
        contextManager.addParameterContext(existingContext);
        when(flowManager.getParameterContextManager()).thenReturn(contextManager);
        when(flowManager.getParameterProvider(providerId)).thenReturn(parameterProviderNode);
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(flowManager).withParameterContextResolution(any());

        // The proposed (elected cluster) flow: same provider-backed context, but the parameter is flagged
        // provided=false with a divergent value. This is the serialized-flow contradiction seen in production.
        final VersionedParameter versionedParameter = new VersionedParameter();
        versionedParameter.setName(paramName);
        versionedParameter.setValue("different-host");
        versionedParameter.setSensitive(false);
        versionedParameter.setProvided(false);

        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setName(contextName);
        versionedParameterContext.setParameterProvider(providerId);
        versionedParameterContext.setParameterGroupName("Group");
        versionedParameterContext.setParameters(Collections.singleton(versionedParameter));
        when(versionedDataflow.getParameterContexts()).thenReturn(List.of(versionedParameterContext));

        // With the fix, the synchronizer recognizes the context is provider-backed and reconciles it as
        // provider-managed (provided=true, value sourced from the provider) rather than attempting a manual
        // update, so synchronization succeeds instead of throwing FlowSynchronizationException.
        assertDoesNotThrow(() ->
                versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST));

        final Optional<Parameter> reconciled = existingContext.getParameter(paramName);
        assertTrue(reconciled.isPresent(), "Parameter should still exist after reconciliation");
        assertTrue(reconciled.get().isProvided(), "Parameter must be reconciled as provider-supplied (provided=true)");
        assertEquals("provider-host", reconciled.get().getValue(),
                "Parameter value must be re-sourced from the Parameter Provider, not the corrupted serialized value or null");
    }

    private void setRootGroup() {
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowManager.getRootGroup()).thenReturn(rootGroup);
    }

    private void setFlowController() {
        setFlowController(mock(ConnectorRepository.class));
    }

    private void setFlowController(final ConnectorRepository connectorRepository) {
        when(rootGroup.isEmpty()).thenReturn(false);
        when(flowController.getSnippetManager()).thenReturn(snippetManager);
        when(snippetManager.export()).thenReturn(new byte[]{});
        when(dataFlow.getVersionedDataflow()).thenReturn(versionedDataflow);
        when(dataFlow.getFlow()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(versionedDataflow.getRootGroup()).thenReturn(versionedRootGroup);
        when(flowController.getEncryptor()).thenReturn(encryptor);
        when(flowController.createVersionedComponentStateLookup(any())).thenReturn(stateLookup);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);

        when(connectorRepository.getConnectors(ConnectorSyncMode.LOCAL_ONLY)).thenReturn(Collections.emptyList());
        when(flowController.getConnectorRepository()).thenReturn(connectorRepository);
    }

    @Test
    void testSyncInheritConnectorDelegatesSyncToConnectorRepository() {
        setRootGroup();

        final String connectorId = UUID.randomUUID().toString();
        final String connectorType = "org.apache.nifi.connectors.TestConnector";

        final VersionedConnector versionedConnector = new VersionedConnector();
        versionedConnector.setInstanceIdentifier(connectorId);
        versionedConnector.setName("Test Connector");
        versionedConnector.setType(connectorType);
        versionedConnector.setBundle(CORE_BUNDLE);
        versionedConnector.setScheduledState(VersionedConnectorState.RUNNING);

        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);
        final ConnectorNode syncedNode = mock(ConnectorNode.class);
        when(connectorRepository.syncConnector(versionedConnector))
                .thenReturn(ConnectorSyncResult.synced(syncedNode, VersionedConnectorState.RUNNING));

        setFlowController(connectorRepository);
        when(versionedDataflow.getConnectors()).thenReturn(List.of(versionedConnector));

        versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST);

        verify(connectorRepository).syncConnector(versionedConnector);
        verify(flowController).startConnector(syncedNode);
    }

    @Test
    void testSyncInheritConnectorNotStartedWhenEnabled() {
        setRootGroup();

        final String connectorId = UUID.randomUUID().toString();
        final String connectorType = "org.apache.nifi.connectors.TestConnector";

        final VersionedConnector versionedConnector = new VersionedConnector();
        versionedConnector.setInstanceIdentifier(connectorId);
        versionedConnector.setName("Test Connector");
        versionedConnector.setType(connectorType);
        versionedConnector.setBundle(CORE_BUNDLE);
        versionedConnector.setScheduledState(VersionedConnectorState.ENABLED);

        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);
        final ConnectorNode syncedNode = mock(ConnectorNode.class);
        when(connectorRepository.syncConnector(versionedConnector))
                .thenReturn(ConnectorSyncResult.syncedConfigUnchanged(syncedNode, VersionedConnectorState.ENABLED));

        setFlowController(connectorRepository);
        when(versionedDataflow.getConnectors()).thenReturn(List.of(versionedConnector));

        versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST);

        verify(connectorRepository).syncConnector(versionedConnector);
        verify(flowController, never()).startConnector(any());
        verify(connectorRepository).stopConnector(syncedNode);
    }

    @Test
    void testSyncOrphanConnectorIsRemoved() {
        setRootGroup();

        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);

        // The proposed flow has one connector, but the local repo has a different one (orphan)
        final VersionedConnector proposedConnector = new VersionedConnector();
        proposedConnector.setInstanceIdentifier("proposed-connector-id");
        proposedConnector.setName("Proposed Connector");
        proposedConnector.setType("org.apache.nifi.connectors.TestConnector");
        proposedConnector.setBundle(CORE_BUNDLE);
        proposedConnector.setScheduledState(VersionedConnectorState.ENABLED);

        final ConnectorNode orphanConnector = mock(ConnectorNode.class);
        when(orphanConnector.getIdentifier()).thenReturn("orphan-connector-id");
        when(connectorRepository.stopConnector(orphanConnector))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        final ConnectorNode syncedNode = mock(ConnectorNode.class);
        when(connectorRepository.syncConnector(proposedConnector))
                .thenReturn(ConnectorSyncResult.syncedConfigUnchanged(syncedNode, VersionedConnectorState.ENABLED));
        when(connectorRepository.stopConnector(syncedNode))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        setFlowController(connectorRepository);
        when(connectorRepository.getConnectors(ConnectorSyncMode.LOCAL_ONLY)).thenReturn(List.of(orphanConnector));
        when(versionedDataflow.getConnectors()).thenReturn(List.of(proposedConnector));

        versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST);

        verify(connectorRepository).syncConnector(proposedConnector);
        verify(connectorRepository).stopConnector(orphanConnector);
        verify(connectorRepository).removeConnector("orphan-connector-id");
    }

    @Test
    void testSyncOrphanConnectorNotRemovedWhenInProposedFlow() {
        setRootGroup();

        final String connectorId = UUID.randomUUID().toString();
        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);

        final ConnectorNode existingConnector = mock(ConnectorNode.class);
        when(existingConnector.getIdentifier()).thenReturn(connectorId);

        final VersionedConnector versionedConnector = new VersionedConnector();
        versionedConnector.setInstanceIdentifier(connectorId);
        versionedConnector.setName("Test Connector");
        versionedConnector.setType("org.apache.nifi.connectors.TestConnector");
        versionedConnector.setBundle(CORE_BUNDLE);
        versionedConnector.setScheduledState(VersionedConnectorState.ENABLED);

        final ConnectorNode syncedNode = mock(ConnectorNode.class);
        when(connectorRepository.syncConnector(versionedConnector))
                .thenReturn(ConnectorSyncResult.syncedConfigUnchanged(syncedNode, VersionedConnectorState.ENABLED));
        when(connectorRepository.stopConnector(syncedNode))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        setFlowController(connectorRepository);
        when(connectorRepository.getConnectors(ConnectorSyncMode.LOCAL_ONLY)).thenReturn(List.of(existingConnector));
        when(versionedDataflow.getConnectors()).thenReturn(List.of(versionedConnector));

        versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST);

        verify(connectorRepository, never()).removeConnector(any());
        verify(connectorRepository).syncConnector(versionedConnector);
    }

    @Test
    void testSyncOrphanRemovalFailureMarksInvalid() {
        setRootGroup();

        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);

        // The proposed flow has one connector; the orphan is a different one
        final VersionedConnector proposedConnector = new VersionedConnector();
        proposedConnector.setInstanceIdentifier("proposed-connector-id");
        proposedConnector.setName("Proposed Connector");
        proposedConnector.setType("org.apache.nifi.connectors.TestConnector");
        proposedConnector.setBundle(CORE_BUNDLE);
        proposedConnector.setScheduledState(VersionedConnectorState.ENABLED);

        final ConnectorNode orphanConnector = mock(ConnectorNode.class);
        when(orphanConnector.getIdentifier()).thenReturn("orphan-connector-id");

        final java.util.concurrent.CompletableFuture<Void> failedFuture = new java.util.concurrent.CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Stop failed"));
        when(connectorRepository.stopConnector(orphanConnector)).thenReturn(failedFuture);

        final ConnectorNode syncedNode = mock(ConnectorNode.class);
        when(connectorRepository.syncConnector(proposedConnector))
                .thenReturn(ConnectorSyncResult.syncedConfigUnchanged(syncedNode, VersionedConnectorState.ENABLED));
        when(connectorRepository.stopConnector(syncedNode))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        setFlowController(connectorRepository);
        when(connectorRepository.getConnectors(ConnectorSyncMode.LOCAL_ONLY)).thenReturn(List.of(orphanConnector));
        when(versionedDataflow.getConnectors()).thenReturn(List.of(proposedConnector));

        versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST);

        verify(connectorRepository).syncConnector(proposedConnector);
        verify(orphanConnector).markInvalid(eq("Flow Synchronization Failure"), any());
    }

    @Test
    void testSyncFailsWhenLocalConnectorTroubleshootingButClusterIsNot() {
        setRootGroup();

        final String connectorId = UUID.randomUUID().toString();
        final VersionedConnector proposedConnector = createVersionedConnector(connectorId, VersionedConnectorState.RUNNING);

        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);
        final ConnectorNode localConnector = mock(ConnectorNode.class);
        when(localConnector.getCurrentState()).thenReturn(ConnectorState.TROUBLESHOOTING);
        when(connectorRepository.getConnector(connectorId, ConnectorSyncMode.LOCAL_ONLY)).thenReturn(localConnector);

        setFlowController(connectorRepository);
        when(versionedDataflow.getConnectors()).thenReturn(List.of(proposedConnector));

        final UninheritableFlowException thrown = assertThrows(UninheritableFlowException.class, () ->
                versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST));
        assertTrue(thrown.getMessage().contains("Troubleshooting"));
        verify(connectorRepository, never()).syncConnector(any());
    }

    @Test
    void testSyncFailsWhenClusterConnectorTroubleshootingButLocalIsNot() {
        setRootGroup();

        final String connectorId = UUID.randomUUID().toString();
        final VersionedConnector proposedConnector = createVersionedConnector(connectorId, VersionedConnectorState.TROUBLESHOOTING);

        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);
        final ConnectorNode localConnector = mock(ConnectorNode.class);
        when(localConnector.getCurrentState()).thenReturn(ConnectorState.RUNNING);
        when(connectorRepository.getConnector(connectorId, ConnectorSyncMode.LOCAL_ONLY)).thenReturn(localConnector);

        setFlowController(connectorRepository);
        when(versionedDataflow.getConnectors()).thenReturn(List.of(proposedConnector));

        assertThrows(UninheritableFlowException.class, () ->
                versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST));
        verify(connectorRepository, never()).syncConnector(any());
    }

    @Test
    void testSyncProceedsWhenConnectorTroubleshootingStateMatchesCluster() {
        setRootGroup();

        final String connectorId = UUID.randomUUID().toString();
        final VersionedConnector proposedConnector = createVersionedConnector(connectorId, VersionedConnectorState.TROUBLESHOOTING);

        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);
        final ConnectorNode localConnector = mock(ConnectorNode.class);
        when(localConnector.getCurrentState()).thenReturn(ConnectorState.TROUBLESHOOTING);
        when(connectorRepository.getConnector(connectorId, ConnectorSyncMode.LOCAL_ONLY)).thenReturn(localConnector);

        final ConnectorNode syncedNode = mock(ConnectorNode.class);
        when(connectorRepository.syncConnector(proposedConnector))
                .thenReturn(ConnectorSyncResult.syncedConfigUnchanged(syncedNode, VersionedConnectorState.TROUBLESHOOTING));

        setFlowController(connectorRepository);
        when(versionedDataflow.getConnectors()).thenReturn(List.of(proposedConnector));

        versionedFlowSynchronizer.sync(flowController, dataFlow, flowService, BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST);

        verify(connectorRepository).syncConnector(proposedConnector);
    }

    private VersionedConnector createVersionedConnector(final String connectorId, final VersionedConnectorState scheduledState) {
        final VersionedConnector versionedConnector = new VersionedConnector();
        versionedConnector.setInstanceIdentifier(connectorId);
        versionedConnector.setName("Test Connector");
        versionedConnector.setType("org.apache.nifi.connectors.TestConnector");
        versionedConnector.setBundle(CORE_BUNDLE);
        versionedConnector.setScheduledState(scheduledState);
        return versionedConnector;
    }
}
