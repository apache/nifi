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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.SnippetManager;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
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

    private void setRootGroup() {
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowManager.getRootGroup()).thenReturn(rootGroup);
    }

    private void setFlowController() {
        when(rootGroup.isEmpty()).thenReturn(false);
        when(flowController.getSnippetManager()).thenReturn(snippetManager);
        when(snippetManager.export()).thenReturn(new byte[]{});
        when(dataFlow.getVersionedDataflow()).thenReturn(versionedDataflow);
        when(dataFlow.getFlow()).thenReturn("{}".getBytes(StandardCharsets.UTF_8));
        when(versionedDataflow.getRootGroup()).thenReturn(versionedRootGroup);
        when(flowController.getEncryptor()).thenReturn(encryptor);
        when(flowController.createVersionedComponentStateLookup(any())).thenReturn(stateLookup);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);
    }
}
