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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.connector.processors.CreateDummyFlowFile;
import org.apache.nifi.components.connector.processors.DuplicateFlowFile;
import org.apache.nifi.components.connector.processors.LogFlowFileContents;
import org.apache.nifi.components.connector.processors.OnPropertyModifiedTracker;
import org.apache.nifi.components.connector.processors.OverwriteFlowFile;
import org.apache.nifi.components.connector.processors.TerminateFlowFile;
import org.apache.nifi.components.connector.secrets.ParameterProviderSecretsManager;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.components.connector.services.CounterService;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.StandardConnection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.GarbageCollectionLog;
import org.apache.nifi.controller.MockStateManagerProvider;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.StandardFlowManager;
import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
import org.apache.nifi.controller.queue.DropFlowFileRequest;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileQueueFactory;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.scheduling.LifecycleStateManager;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.controller.scheduling.StandardLifecycleStateManager;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.mock.MockNodeTypeProvider;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.validation.RuleViolationsManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StandardConnectorNodeIT {

    private StandardProcessScheduler processScheduler;
    private StandardFlowManager flowManager;
    private FlowEngine componentLifecycleThreadPool;
    private ConnectorRepository connectorRepository;

    @BeforeEach
    public void setup() {
        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(controllerServiceProvider.disableControllerServicesAsync(anyCollection())).thenReturn(CompletableFuture.completedFuture(null));
        connectorRepository = new StandardConnectorRepository();

        final SecretsManager secretsManager = new ParameterProviderSecretsManager();
        final ConnectorRepositoryInitializationContext repoInitContext = mock(ConnectorRepositoryInitializationContext.class);
        when(repoInitContext.getSecretsManager()).thenReturn(secretsManager);
        connectorRepository.initialize(repoInitContext);

        final ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
        final BulletinRepository bulletinRepository = mock(BulletinRepository.class);
        final StateManagerProvider stateManagerProvider = new MockStateManagerProvider();
        final LifecycleStateManager lifecycleStateManager = new StandardLifecycleStateManager();
        final ReloadComponent reloadComponent = mock(ReloadComponent.class);

        final FlowController flowController = mock(FlowController.class);
        when(flowController.isInitialized()).thenReturn(true);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(flowController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(flowController.getReloadComponent()).thenReturn(reloadComponent);

        final RepositoryContextFactory repoContextFactory = mock(RepositoryContextFactory.class);
        final FlowFileRepository flowFileRepo = mock(FlowFileRepository.class);
        final ProvenanceRepository provRepo = mock(ProvenanceRepository.class);
        final ContentRepository contentRepo = mock(ContentRepository.class);
        when(repoContextFactory.getFlowFileRepository()).thenReturn(flowFileRepo);
        when(repoContextFactory.getProvenanceRepository()).thenReturn(provRepo);
        when(repoContextFactory.getContentRepository()).thenReturn(contentRepo);

        when(flowController.getRepositoryContextFactory()).thenReturn(repoContextFactory);
        when(flowController.getGarbageCollectionLog()).thenReturn(mock(GarbageCollectionLog.class));
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);
        when(flowController.getProvenanceRepository()).thenReturn(provRepo);
        when(flowController.getBulletinRepository()).thenReturn(bulletinRepository);
        when(flowController.getLifecycleStateManager()).thenReturn(lifecycleStateManager);
        when(flowController.getFlowFileEventRepository()).thenReturn(mock(FlowFileEventRepository.class));
        when(flowController.getConnectorRepository()).thenReturn(connectorRepository);
        when(flowController.getValidationTrigger()).thenReturn(mock(ValidationTrigger.class));
        when(flowController.getConnectorValidationTrigger()).thenReturn(mock(ConnectorValidationTrigger.class));

        doAnswer(invocation -> {
            return createConnection(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(4));
        }).when(flowController).createConnection(anyString(), nullable(String.class), any(Connectable.class), any(Connectable.class), anyCollection());

        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties("src/test/resources/conf/nifi.properties");

        final FlowFileEventRepository flowFileEventRepository = mock(FlowFileEventRepository.class);
        final ParameterContextManager parameterContextManager = mock(ParameterContextManager.class);

        final NodeTypeProvider nodeTypeProvider = new MockNodeTypeProvider();
        componentLifecycleThreadPool = new FlowEngine(4, "Component Lifecycle Thread Pool", true);
        processScheduler = new StandardProcessScheduler(componentLifecycleThreadPool, extensionManager, nodeTypeProvider, () -> controllerServiceProvider,
            reloadComponent, stateManagerProvider, nifiProperties, lifecycleStateManager);
        when(flowController.getProcessScheduler()).thenReturn(processScheduler);
        processScheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, mock(SchedulingAgent.class));
        processScheduler.setSchedulingAgent(SchedulingStrategy.CRON_DRIVEN, mock(SchedulingAgent.class));

        final Bundle systemBundle = SystemBundle.create(nifiProperties);
        extensionManager.discoverExtensions(systemBundle, Set.of());

        flowManager = new StandardFlowManager(nifiProperties, null, flowController, flowFileEventRepository, parameterContextManager);
        flowManager.initialize(controllerServiceProvider, mock(PythonBridge.class), mock(FlowAnalyzer.class), mock(RuleViolationsManager.class));
        final ProcessGroup rootGroup = flowManager.createProcessGroup("root");
        rootGroup.setName("Root");
        flowManager.setRootGroup(rootGroup);

        when(flowController.getFlowManager()).thenReturn(flowManager);
    }

    @AfterEach
    public void tearDown() {
        if (componentLifecycleThreadPool != null) {
            componentLifecycleThreadPool.shutdown();
        }
    }

    @Test
    public void testConnectorDynamicallyCreatingFlow() throws FlowUpdateException {
        final ConnectorNode connectorNode = initializeDynamicFlowConnector();
        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();

        // Configure the connector to log FlowFile contents, make 5 copies, and set text to "Second Iteration."
        final ConnectorConfiguration configuration = createConnectorConfiguration("Second Iteration.", 5, true, false);
        configure(connectorNode, configuration);

        final List<ProcessorNode> processorsAfterUpdate = rootGroup.findAllProcessors();
        assertEquals(5, processorsAfterUpdate.size());

        final List<String> processorTypesAfterUpdate = processorsAfterUpdate.stream().map(ProcessorNode::getCanonicalClassName).toList();
        assertTrue(processorTypesAfterUpdate.contains(CreateDummyFlowFile.class.getName()));
        assertTrue(processorTypesAfterUpdate.contains(OverwriteFlowFile.class.getName()));
        assertTrue(processorTypesAfterUpdate.contains(TerminateFlowFile.class.getName()));
        assertTrue(processorTypesAfterUpdate.contains(DuplicateFlowFile.class.getName()));
        assertTrue(processorTypesAfterUpdate.contains(LogFlowFileContents.class.getName()));

        // Verify the OverwriteFlowFile processor has the correct content configured
        final ProcessorNode overwriteProcessor = processorsAfterUpdate.stream()
            .filter(p -> p.getCanonicalClassName().equals(OverwriteFlowFile.class.getName()))
            .findFirst()
            .orElseThrow();
        assertEquals("Second Iteration.", overwriteProcessor.getEffectivePropertyValue(OverwriteFlowFile.CONTENT));

        // Verify the DuplicateFlowFile processor is configured for 5 copies
        final ProcessorNode duplicateProcessor = processorsAfterUpdate.stream()
            .filter(p -> p.getCanonicalClassName().equals(DuplicateFlowFile.class.getName()))
            .findFirst()
            .orElseThrow();
        assertEquals("5", duplicateProcessor.getEffectivePropertyValue(DuplicateFlowFile.NUM_DUPLICATES));

        // Verify that all of the Connections were created
        assertEquals(5, duplicateProcessor.getConnections().size());
        final String outputPortId = duplicateProcessor.getProcessGroup().getOutputPorts().iterator().next().getIdentifier();
        for (final Connection connection : duplicateProcessor.getConnections()) {
            assertEquals(duplicateProcessor.getIdentifier(), connection.getSource().getIdentifier());
            assertEquals(outputPortId, connection.getDestination().getIdentifier());
        }
    }

    private void configure(final ConnectorNode connectorNode, final ConnectorConfiguration configuration) throws FlowUpdateException {
        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        for (final NamedStepConfiguration stepConfig : configuration.getNamedStepConfigurations()) {
            connectorNode.setConfiguration(stepConfig.stepName(), stepConfig.configuration());
        }
        connectorNode.applyUpdate();
    }

    private ConnectorNode initializeDynamicFlowConnector() {
        final ConnectorNode connectorNode = flowManager.createConnector(DynamicFlowConnector.class.getName(), "dynamic-flow-connector", SystemBundle.SYSTEM_BUNDLE_COORDINATE, true, true);
        assertNotNull(connectorNode);

        final Connector connector = connectorNode.getConnector();
        assertNotNull(connector);
        assertInstanceOf(DynamicFlowConnector.class, connector);

        final DynamicFlowConnector flowConnector = (DynamicFlowConnector) connector;
        assertTrue(flowConnector.isInitialized());

        assertEquals(List.of(connectorNode), connectorRepository.getConnectors());

        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();
        assertEquals(3, rootGroup.getProcessGroups().size());

        final List<ProcessorNode> initialProcessors = rootGroup.findAllProcessors();
        assertEquals(4, initialProcessors.size());
        final List<String> initialProcessorTypes = initialProcessors.stream().map(ProcessorNode::getComponentType).toList();
        assertTrue(initialProcessorTypes.contains(CreateDummyFlowFile.class.getSimpleName()));
        assertTrue(initialProcessorTypes.contains(OverwriteFlowFile.class.getSimpleName()));
        assertTrue(initialProcessorTypes.contains(TerminateFlowFile.class.getSimpleName()));
        assertTrue(initialProcessorTypes.contains(DuplicateFlowFile.class.getSimpleName()));

        return connectorNode;
    }

    private ConnectorNode initializeParameterConnector() {
        final ConnectorNode connectorNode = flowManager.createConnector(ParameterConnector.class.getName(), "parameter-connector", SystemBundle.SYSTEM_BUNDLE_COORDINATE, true, true);
        assertNotNull(connectorNode);
        assertEquals(List.of(connectorNode), connectorRepository.getConnectors());

        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();
        assertEquals(3, rootGroup.getProcessors().size());

        return connectorNode;
    }

    // Test a scenario where the connector is initialized and the Connector then changes the value of a parameter.
    // This should result in any Processor that references that parameter being stopped and restarted. It should not result
    // in non-referencing Processors being restarted.
    @Test
    public void testParameterUpdateRestartsReferencingProcessors() throws FlowUpdateException {
        final ConnectorNode connectorNode = initializeParameterConnector();
        final Connector connector = connectorNode.getConnector();
        assertNotNull(connector);
        assertInstanceOf(ParameterConnector.class, connector);

        final ParameterConnector flowConnector = (ParameterConnector) connector;
        assertTrue(flowConnector.isInitialized());

        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();
        final ParameterContext parameterContext = rootGroup.getParameterContext();
        assertNotNull(parameterContext);
        assertEquals(2, parameterContext.getParameters().size());
        final Optional<Parameter> optionalParameter = parameterContext.getParameter("Text");
        assertTrue(optionalParameter.isPresent());

        final Parameter textParameter = optionalParameter.get();
        assertEquals("Hello", textParameter.getValue());

        // Set the value of the 'Text' property to Hi. This should result in the parameter context being updated.
        final StepConfiguration sourceStepConfig = new StepConfiguration(Map.of("Text", new StringLiteralValue("Hi.")));
        final NamedStepConfiguration sourceConfigurationStepConfiguration = new NamedStepConfiguration("Text Configuration", sourceStepConfig);
        final ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration(Set.of(sourceConfigurationStepConfiguration));
        configure(connectorNode, connectorConfiguration);

        assertEquals("Hi.", parameterContext.getParameter("Text").orElseThrow().getValue());
    }

    @Test
    @Timeout(10)
    public void testOnPropertyModifiedCalledOnApplyUpdate() throws FlowUpdateException {
        final ConnectorNode connectorNode = flowManager.createConnector(OnPropertyModifiedConnector.class.getName(),
                "on-property-modified-connector", SystemBundle.SYSTEM_BUNDLE_COORDINATE, true, true);
        assertNotNull(connectorNode);

        final StepConfiguration initialConfig = new StepConfiguration(Map.of("Number Value", new StringLiteralValue("0")));
        final NamedStepConfiguration initialStepConfig = new NamedStepConfiguration("Configuration", initialConfig);
        configure(connectorNode, new ConnectorConfiguration(Set.of(initialStepConfig)));

        final ProcessGroup activeGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();
        final ProcessorNode processorNode = activeGroup.getProcessors().iterator().next();
        final OnPropertyModifiedTracker tracker = (OnPropertyModifiedTracker) processorNode.getProcessor();

        assertEquals(0, tracker.getPropertyChangeCount());

        final StepConfiguration updatedConfig = new StepConfiguration(Map.of("Number Value", new StringLiteralValue("1")));
        final NamedStepConfiguration updatedStepConfig = new NamedStepConfiguration("Configuration", updatedConfig);

        connectorNode.setConfiguration("Configuration", updatedStepConfig.configuration());

        final ProcessGroup workingGroup = connectorNode.getWorkingFlowContext().getManagedProcessGroup();
        final ProcessorNode workingProcessorNode = workingGroup.getProcessors().iterator().next();
        final OnPropertyModifiedTracker workingTracker = (OnPropertyModifiedTracker) workingProcessorNode.getProcessor();

        assertEquals(1, workingTracker.getPropertyChangeCount());
        assertEquals("0", workingTracker.getPropertyChanges().getFirst().oldValue());
        assertEquals("1", workingTracker.getPropertyChanges().getFirst().newValue());

        workingTracker.clearPropertyChanges();

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.applyUpdate();

        assertEquals(1, tracker.getPropertyChangeCount());
        assertEquals("0", tracker.getPropertyChanges().getFirst().oldValue());
        assertEquals("1", tracker.getPropertyChanges().getFirst().newValue());

        // Ensure that no parameter contexts are registered
        final Set<ParameterContext> registeredContexts = flowManager.getParameterContextManager().getParameterContexts();
        assertEquals(Set.of(), registeredContexts);
    }

    @Test
    public void testControllerServices() throws FlowUpdateException {
        final ConnectorNode connectorNode = initializeDynamicFlowConnector();

        final ConnectorConfiguration configuration = createConnectorConfiguration("Second Iteration", 5, true, true);
        configure(connectorNode, configuration);

        final ProcessGroup managedGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();
        final Set<ControllerServiceNode> serviceNodes = managedGroup.getControllerServices(true);
        assertNotNull(serviceNodes);
        assertEquals(1, serviceNodes.size());
        assertInstanceOf(CounterService.class, serviceNodes.iterator().next().getControllerServiceImplementation());
    }

    @Test
    public void testUpdateProcessorPropertyDataQueued() throws FlowUpdateException {
        final ConnectorNode connectorNode = initializeDynamicFlowConnector();
        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();
        final Connection connection = queueDataBySource(rootGroup, "Create FlowFile");

        // Update Connector Property to ensure that the change is allowed with data queued
        final ConnectorConfiguration configuration = createConnectorConfiguration("Second Iteration", 5, true, false);
        configure(connectorNode, configuration);

        assertEquals(1, connection.getFlowFileQueue().size().getObjectCount());
    }

    @Test
    public void testRemoveConnectionDataQueued() throws FlowUpdateException {
        final ConnectorNode connectorNode = initializeDynamicFlowConnector();
        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();

        // Create a configuration that will result in the LogFlowFileContents processor being removed.
        // Because the component is being removed, it should drain the queues before doing so.
        final ConnectorConfiguration addLogConfiguration = createConnectorConfiguration("Second Iteration", 5, true, false);
        configure(connectorNode, addLogConfiguration);

        // Queue data between LogFlowFileContents and TerminateFlowFile
        final Connection connection = queueDataByDestination(rootGroup, "Terminate FlowFile");

        // Create a configuration that will result in the LogFlowFileContents processor being removed.
        // Because the component is being removed and there's data queued in its incoming connection, it should fail.
        final ConnectorConfiguration removeLogConfiguration = createConnectorConfiguration("Second Iteration", 5, false, false);

        final Throwable cause = assertThrows(FlowUpdateException.class, () -> configure(connectorNode, removeLogConfiguration));
        connectorNode.abortUpdate(cause);

        rootGroup.findAllConnections().contains(connection);
        assertFalse(connection.getFlowFileQueue().isEmpty());
    }


    @Test
    public void testDynamicProperties() throws IOException, FlowUpdateException {
        final ConnectorNode connectorNode = flowManager.createConnector(DynamicAllowableValuesConnector.class.getName(), "dynamic-allowable-values-connector",
            SystemBundle.SYSTEM_BUNDLE_COORDINATE, true, true);
        assertNotNull(connectorNode);

        assertEquals(List.of("File", "Colors"), getConfigurationStepNames(connectorNode));

        final Path tempFile = Files.createTempFile("StandardConnectorNodeIT", ".txt");
        Files.writeString(tempFile, String.join("\n", "red", "blue", "yellow"));

        final ConnectorConfiguration configuration = createFileConfiguration(tempFile.toFile().getAbsolutePath());
        configure(connectorNode, configuration);

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        assertEquals(List.of("File", "Colors"), getConfigurationStepNames(connectorNode));

        final List<String> allowableValues = connectorNode.fetchAllowableValues("Colors", "First Primary Color").stream()
            .map(DescribedValue::getValue)
            .toList();
        assertEquals(List.of("red", "blue", "yellow"), allowableValues);
    }

    @Test
    public void testSimpleValidation() throws FlowUpdateException {
        final ConnectorNode connectorNode = flowManager.createConnector(DynamicAllowableValuesConnector.class.getName(), "dynamic-allowable-values-connector",
            SystemBundle.SYSTEM_BUNDLE_COORDINATE, true, true);
        assertNotNull(connectorNode);

        assertEquals(List.of("File", "Colors"), getConfigurationStepNames(connectorNode));

        final ConnectorConfiguration configuration = createFileConfiguration("/non/existent/file");
        configure(connectorNode, configuration);

        final ValidationState validationState = connectorNode.performValidation();
        assertNotNull(validationState);
        assertEquals(ValidationStatus.INVALID, validationState.getStatus());
        assertEquals(2, validationState.getValidationErrors().size());

        final boolean hasFileError = validationState.getValidationErrors().stream()
            .anyMatch(result -> result.getInput() != null && result.getInput().contains("/non/existent/file"));
        assertTrue(hasFileError);

        final boolean hasColorError = validationState.getValidationErrors().stream()
            .anyMatch(result -> result.getSubject() != null && result.getSubject().contains("First Primary Color"));
        assertTrue(hasColorError);

        final File colorsFile = new File("src/test/resources/colors.txt");
        final ConnectorConfiguration validConfig = createFileAndColorsConfiguration(colorsFile.getAbsolutePath(), "red");
        configure(connectorNode, validConfig);

        final ValidationState updatedValidationState = connectorNode.performValidation();
        assertEquals(ValidationStatus.VALID, updatedValidationState.getStatus(),
            "Expected valid state but invalid due to " + updatedValidationState.getValidationErrors());
        assertEquals(List.of(), updatedValidationState.getValidationErrors());
    }

    @Test
    public void testValidationWithParameterContext() throws FlowUpdateException {
        final ConnectorNode connectorNode = initializeParameterConnector();

        final ValidationState initialValidationState = connectorNode.performValidation();
        assertNotNull(initialValidationState);
        assertEquals(ValidationStatus.VALID, initialValidationState.getStatus());
        assertEquals(List.of(), initialValidationState.getValidationErrors());

        final StepConfiguration sourceStepConfig = new StepConfiguration(Map.of("Sleep Duration", new StringLiteralValue("Hi.")));
        final NamedStepConfiguration sourceConfigurationStepConfiguration = new NamedStepConfiguration("Text Configuration", sourceStepConfig);
        final ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration(Set.of(sourceConfigurationStepConfiguration));
        configure(connectorNode, connectorConfiguration);

        final ValidationState validationState = connectorNode.performValidation();
        assertNotNull(validationState);
        assertEquals(ValidationStatus.INVALID, validationState.getStatus());
        assertEquals(1, validationState.getValidationErrors().size());
    }

    @Test
    public void testPurgeFlowFilesEmptiesQueues() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final ConnectorNode connectorNode = initializeDynamicFlowConnector();
        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();

        final Connection connection = queueDataBySource(rootGroup, "Create FlowFile");
        assertEquals(1, connection.getFlowFileQueue().size().getObjectCount());

        final Future<Void> purgeFuture = connectorNode.purgeFlowFiles("test-user");
        purgeFuture.get(10, TimeUnit.SECONDS);

        assertTrue(connection.getFlowFileQueue().isEmpty());
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
    }

    @Test
    public void testPurgeFlowFilesMultipleQueues() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final ConnectorNode connectorNode = initializeDynamicFlowConnector();
        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();

        final Connection connection1 = queueDataBySource(rootGroup, "Create FlowFile");
        final Connection connection2 = queueDataByDestination(rootGroup, "Terminate FlowFile");
        assertEquals(1, connection1.getFlowFileQueue().size().getObjectCount());
        assertEquals(1, connection2.getFlowFileQueue().size().getObjectCount());

        final Future<Void> purgeFuture = connectorNode.purgeFlowFiles("test-user");
        purgeFuture.get(10, TimeUnit.SECONDS);

        assertTrue(connection1.getFlowFileQueue().isEmpty());
        assertTrue(connection2.getFlowFileQueue().isEmpty());
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
    }

    @Test
    public void testPurgeFlowFilesRequiresStoppedState() {
        final ConnectorNode connectorNode = initializeDynamicFlowConnector();
        final ProcessGroup rootGroup = connectorNode.getActiveFlowContext().getManagedProcessGroup();
        queueDataBySource(rootGroup, "Create FlowFile");

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.start(componentLifecycleThreadPool);
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        final IllegalStateException exception = assertThrows(IllegalStateException.class, () -> connectorNode.purgeFlowFiles("test-user"));
        assertTrue(exception.getMessage().contains("must be STOPPED"));

        connectorNode.stop(componentLifecycleThreadPool);
    }


    private List<String> getConfigurationStepNames(final ConnectorNode connectorNode) {
        return connectorNode.getConfigurationSteps().stream()
            .map(ConfigurationStep::getName)
            .toList();
    }

    private Connection queueDataBySource(final ProcessGroup group, final String sourceComponentName) {
        return queueData(group, conn -> conn.getSource().getName().equals(sourceComponentName));
    }

    private Connection queueDataByDestination(final ProcessGroup group, final String destinationComponentName) {
        return queueData(group, conn -> conn.getDestination().getName().equals(destinationComponentName));
    }

    private Connection queueData(final ProcessGroup group, final Predicate<Connection> connectionTest) {
        final Connection connection = group.findAllConnections().stream()
            .filter(connectionTest)
            .findFirst()
            .orElseThrow();

        final FlowFileRecord flowFile = mock(FlowFileRecord.class);
        connection.getFlowFileQueue().put(flowFile);
        return connection;
    }

    private Connection createConnection(final String id, final String name, final Connectable source, final Connectable destination, final Collection<String> relationshipNames) {
        final List<Relationship> relationships = relationshipNames.stream()
            .map(relName -> new Relationship.Builder().name(relName).build())
            .toList();

        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        final List<FlowFileRecord> flowFileList = new ArrayList<>();

        // Update mock to add FlowFiles to the queue
        doAnswer(invocation -> {
            flowFileList.add(invocation.getArgument(0));
            return null;
        }).when(flowFileQueue).put(any(FlowFileRecord.class));

        // Update mock to return queue size and isEmpty status
        when(flowFileQueue.size()).thenAnswer(invocation -> new QueueSize(flowFileList.size(), flowFileList.size()));
        when(flowFileQueue.isEmpty()).thenAnswer(invocation -> flowFileList.isEmpty());
        when(flowFileQueue.getLoadBalanceStrategy()).thenReturn(LoadBalanceStrategy.DO_NOT_LOAD_BALANCE);
        when(flowFileQueue.getLoadBalanceCompression()).thenReturn(LoadBalanceCompression.DO_NOT_COMPRESS);

        // Mock dropFlowFiles to clear the list and return a completed status
        when(flowFileQueue.dropFlowFiles(anyString(), anyString())).thenAnswer(invocation -> {
            final String requestId = invocation.getArgument(0);
            final int originalCount = flowFileList.size();
            flowFileList.clear();

            final DropFlowFileRequest dropRequest = new DropFlowFileRequest(requestId);
            dropRequest.setOriginalSize(new QueueSize(originalCount, originalCount));
            dropRequest.setCurrentSize(new QueueSize(0, 0));
            dropRequest.setDroppedSize(new QueueSize(originalCount, originalCount));
            dropRequest.setState(DropFlowFileState.COMPLETE);
            return dropRequest;
        });

        final FlowFileQueueFactory flowFileQueueFactory = (loadBalanceStrategy, partitioningAttribute, processGroup) -> flowFileQueue;

        final Connection connection = new StandardConnection.Builder(processScheduler)
            .id(id)
            .name(name)
            .processGroup(destination.getProcessGroup())
            .relationships(relationships)
            .source(requireNonNull(source))
            .destination(destination)
            .flowFileQueueFactory(flowFileQueueFactory)
            .build();

        return connection;
    }

    private ConnectorConfiguration createConnectorConfiguration(final String sourceText, final int numberOfCopies, final boolean logContents, final boolean countFlowFiles) {
        // Source configuration step
        final StepConfiguration sourceStepConfig = new StepConfiguration(Map.of(
            "Source Text", new StringLiteralValue(sourceText),
            "Count FlowFiles", new StringLiteralValue(Boolean.toString(countFlowFiles))));
        final NamedStepConfiguration sourceConfigurationStepConfiguration = new NamedStepConfiguration("Source", sourceStepConfig);

        // Duplication configuration step
        final StepConfiguration duplicationStepConfig = new StepConfiguration(Map.of("Number of Copies", new StringLiteralValue(Integer.toString(numberOfCopies))));
        final NamedStepConfiguration duplicationConfigurationStepConfiguration = new NamedStepConfiguration("Duplication", duplicationStepConfig);

        // Destination configuration step
        final StepConfiguration destinationStepConfig = new StepConfiguration(Map.of("Log FlowFile Contents", new StringLiteralValue(Boolean.toString(logContents))));
        final NamedStepConfiguration destinationConfigurationStepConfiguration = new NamedStepConfiguration("Destination", destinationStepConfig);

        return new ConnectorConfiguration(Set.of(sourceConfigurationStepConfiguration, duplicationConfigurationStepConfiguration, destinationConfigurationStepConfiguration));
    }

    private ConnectorConfiguration createFileConfiguration(final String filename) {
        final StepConfiguration fileStepConfig = new StepConfiguration(Map.of("File Path", new StringLiteralValue(filename)));
        final NamedStepConfiguration fileConfigurationStepConfiguration = new NamedStepConfiguration("File", fileStepConfig);

        return new ConnectorConfiguration(Set.of(fileConfigurationStepConfiguration));
    }

    private ConnectorConfiguration createFileAndColorsConfiguration(final String filename, final String color) {
        final StepConfiguration fileStepConfig = new StepConfiguration(Map.of("File Path", new StringLiteralValue(filename)));
        final NamedStepConfiguration fileConfigurationStepConfiguration = new NamedStepConfiguration("File", fileStepConfig);

        final StepConfiguration colorsStepConfig = new StepConfiguration(Map.of("First Primary Color", new StringLiteralValue(color)));
        final NamedStepConfiguration colorsConfigurationStepConfiguration = new NamedStepConfiguration("Colors", colorsStepConfig);

        return new ConnectorConfiguration(Set.of(fileConfigurationStepConfiguration, colorsConfigurationStepConfiguration));
    }
}
