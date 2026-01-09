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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardConnectorNode {

    private FlowEngine scheduler;

    @Mock
    private ExtensionManager extensionManager;
    @Mock
    private ProcessGroup managedProcessGroup;
    @Mock
    private AssetManager assetManager;
    @Mock
    private SecretsManager secretsManager;

    private FlowContextFactory flowContextFactory;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        scheduler = new FlowEngine(1, "flow-engine");

        when(managedProcessGroup.purge()).thenReturn(CompletableFuture.completedFuture(null));
        when(managedProcessGroup.getQueueSize()).thenReturn(new QueueSize(0, 0L));

        flowContextFactory = new FlowContextFactory() {
            @Override
            public FrameworkFlowContext createActiveFlowContext(final String connectorId, final ComponentLog connectorLogger, final Bundle bundle) {
                final MutableConnectorConfigurationContext activeConfigurationContext = new StandardConnectorConfigurationContext(assetManager, secretsManager);
                final ProcessGroupFacadeFactory processGroupFacadeFactory = mock(ProcessGroupFacadeFactory.class);
                final ParameterContextFacadeFactory parameterContextFacadeFactory = mock(ParameterContextFacadeFactory.class);
                return new StandardFlowContext(managedProcessGroup, activeConfigurationContext, processGroupFacadeFactory,
                    parameterContextFacadeFactory, connectorLogger, FlowContextType.ACTIVE, bundle);
            }

            @Override
            public FrameworkFlowContext createWorkingFlowContext(final String connectorId, final ComponentLog connectorLogger,
                    final MutableConnectorConfigurationContext currentConfiguration, final Bundle bundle) {

                final ProcessGroupFacadeFactory processGroupFacadeFactory = mock(ProcessGroupFacadeFactory.class);
                final ParameterContextFacadeFactory parameterContextFacadeFactory = mock(ParameterContextFacadeFactory.class);

                return new StandardFlowContext(managedProcessGroup, currentConfiguration, processGroupFacadeFactory,
                    parameterContextFacadeFactory, connectorLogger, FlowContextType.WORKING, bundle);
            }
        };
    }

    @AfterEach
    public void teardown() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    @Test
    public void testStartFromStoppedState() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        final Future<Void> startFuture = connectorNode.start(scheduler);

        startFuture.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());
        assertTrue(startFuture.isDone());
        assertFalse(startFuture.isCancelled());
    }

    @Test
    public void testStopFromRunningState() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        final Future<Void> startFuture = connectorNode.start(scheduler);
        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        stopFuture.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());
        assertTrue(stopFuture.isDone());
        assertFalse(stopFuture.isCancelled());
    }

    @Test
    public void testStartFutureCompletedOnlyWhenRunning() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        final Future<Void> startFuture = connectorNode.start(scheduler);

        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertTrue(startFuture.isDone());
    }

    @Test
    public void testStopFutureCompletedOnlyWhenStopped() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        stopFuture.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertTrue(stopFuture.isDone());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testMultipleStartCallsReturnCompletedFutures() throws Exception {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final BlockingConnector blockingConnector = new BlockingConnector(startLatch, new CountDownLatch(0), new CountDownLatch(0));
        final StandardConnectorNode connectorNode = createConnectorNode(blockingConnector);

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        final Future<Void> startFuture1 = connectorNode.start(scheduler);
        assertEquals(ConnectorState.STARTING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        final Future<Void> startFuture2 = connectorNode.start(scheduler);
        assertEquals(ConnectorState.STARTING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        // Allow the connector to start
        startLatch.countDown();
        startFuture1.get(5, TimeUnit.SECONDS);
        startFuture2.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        assertTrue(startFuture1.isDone());
        assertTrue(startFuture2.isDone());
    }

    @Test
    public void testVerifyCanDeleteWhenStopped() throws FlowUpdateException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        connectorNode.verifyCanDelete();
    }

    @Test
    public void testCannotDeleteWhenRunning() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        assertThrows(IllegalStateException.class, connectorNode::verifyCanDelete);
    }

    @Test
    public void testVerifyCanStartWhenStopped() throws FlowUpdateException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        connectorNode.verifyCanStart();
    }

    @Test
    public void testStartAlreadyRunningReturnsImmediately() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        final Future<Void> startFuture = connectorNode.start(scheduler);
        assertTrue(startFuture.isDone());

        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
    }

    @Test
    public void testStopAlreadyStoppedReturnsImmediately() throws FlowUpdateException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        assertTrue(stopFuture.isDone());

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
    }

    @Test
    public void testStartWhileStoppingQueuesStartFuture() throws Exception {
        final CountDownLatch stopLatch = new CountDownLatch(1);
        final BlockingConnector blockingConnector = new BlockingConnector(new CountDownLatch(0), stopLatch, new CountDownLatch(0));
        final StandardConnectorNode connectorNode = createConnectorNode(blockingConnector);

        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        assertEquals(ConnectorState.STOPPING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());

        final Future<Void> startFuture = connectorNode.start(scheduler);
        assertEquals(ConnectorState.STOPPING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        stopLatch.countDown();

        stopFuture.get(5, TimeUnit.SECONDS);
        startFuture.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertTrue(stopFuture.isDone());
        assertTrue(startFuture.isDone());
    }

    @Test
    public void testCannotDeleteWhenStarting() throws Exception {
        // Use a slow-starting connector to test deletion during STARTING state
        final CountDownLatch startLatch = new CountDownLatch(1);
        final BlockingConnector blockingConnector = new BlockingConnector(startLatch, new CountDownLatch(0), new CountDownLatch(0));
        final StandardConnectorNode slowNode = createConnectorNode(blockingConnector);

        // Start the connector - this will take time
        final Future<Void> startFuture = slowNode.start(scheduler);

        // While starting, verify we cannot delete
        assertEquals(ConnectorState.STARTING, slowNode.getCurrentState());

        assertThrows(IllegalStateException.class, slowNode::verifyCanDelete);

        // Wait for start to complete
        startLatch.countDown();
        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, slowNode.getCurrentState());
    }

    @Test
    public void testSetConfigurationWhenStopped() throws FlowUpdateException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());

        final ConnectorConfiguration newConfiguration = createTestConfiguration();

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("testGroup", createStepConfiguration());
        connectorNode.applyUpdate();

        assertEquals(newConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testSetConfigurationWithPropertyChanges() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("step1", createStepConfiguration(Map.of("prop1", "value1")));
        connectorNode.applyUpdate();

        final ConnectorConfiguration newConfiguration = createTestConfiguration("step1", "prop1", "value2");

        connectorNode.stop(scheduler).get(5, TimeUnit.SECONDS);
        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("step1", createStepConfiguration(Map.of("prop1", "value2")));
        connectorNode.applyUpdate();
        assertEquals(newConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testSetConfigurationWithNewConfigurationStep() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("configurationStep1", createStepConfiguration(Map.of("prop1", "value1")));
        connectorNode.applyUpdate();

        final ConnectorConfiguration newConfiguration = createTestConfigurationWithMultipleGroups();

        // Wait for Connector to fully stop
        connectorNode.stop(scheduler).get(5, TimeUnit.SECONDS);
        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("configurationStep1", createStepConfiguration(Map.of("prop1", "value1")));
        connectorNode.setConfiguration("configurationStep2", createStepConfiguration(Map.of("prop2", "value2")));
        connectorNode.applyUpdate();

        assertEquals(newConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testSetConfigurationWithRemovedConfigurationStep() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("configurationStep1", createStepConfiguration(Map.of("prop1", "value1")));
        connectorNode.setConfiguration("configurationStep2", createStepConfiguration(Map.of("prop2", "value2")));
        connectorNode.applyUpdate();

        connectorNode.stop(scheduler).get(5, TimeUnit.SECONDS);
        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("configurationStep1", createStepConfiguration(Map.of("prop1", "value1")));
        connectorNode.applyUpdate();

        final Set<NamedStepConfiguration> expectedSteps = Set.of(
            new NamedStepConfiguration("configurationStep1", new StepConfiguration(Map.of("prop1", new StringLiteralValue("value1")))),
            new NamedStepConfiguration("configurationStep2", new StepConfiguration(Map.of("prop2", new StringLiteralValue("value2"))))
        );
        final ConnectorConfiguration expectedConfiguration = new ConnectorConfiguration(expectedSteps);
        assertEquals(expectedConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testSetConfigurationCallsOnConfigured() throws FlowUpdateException {
        final TrackingConnector trackingConnector = new TrackingConnector();
        final StandardConnectorNode connectorNode = createConnectorNode(trackingConnector);
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("testGroup", createStepConfiguration());
        connectorNode.applyUpdate();
    }

    @Test
    public void testSetConfigurationCallsOnPropertyGroupConfiguredForChangedConfigurationSteps() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final TrackingConnector trackingConnector = new TrackingConnector();
        final StandardConnectorNode connectorNode = createConnectorNode(trackingConnector);
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("configurationStep1", createStepConfiguration(Map.of("prop1", "value1")));
        connectorNode.applyUpdate();
        trackingConnector.reset();

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration("configurationStep1", createStepConfiguration(Map.of("prop1", "value2")));
        connectorNode.applyUpdate();

        assertTrue(trackingConnector.wasOnPropertyGroupConfiguredCalled("configurationStep1"));
    }

    @Test
    public void testVerifyConfigurationStepSurfacesInvalidValidationResults() throws FlowUpdateException {
        final ValidationFailingConnector validationFailingConnector = new ValidationFailingConnector();
        final StandardConnectorNode connectorNode = createConnectorNode(validationFailingConnector);
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        final List<ConfigVerificationResult> results = connectorNode.verifyConfigurationStep("testStep", new StepConfiguration(Map.of("testProperty", new StringLiteralValue("invalidValue"))));

        assertFalse(results.isEmpty());
        final List<ConfigVerificationResult> failedResults = results.stream()
            .filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.FAILED)
            .toList();
        assertFalse(failedResults.isEmpty());

        final ConfigVerificationResult failedResult = failedResults.getFirst();
        assertEquals("Property Validation - Test Property", failedResult.getVerificationStepName());
        assertEquals("Test Property", failedResult.getSubject());
        assertEquals("The property value is invalid", failedResult.getExplanation());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testDrainFlowFilesTransitionsStateToDraining() throws FlowUpdateException {
        final CompletableFuture<Void> drainCompletionFuture = new CompletableFuture<>();
        final DrainBlockingConnector drainBlockingConnector = new DrainBlockingConnector(drainCompletionFuture);
        final StandardConnectorNode connectorNode = createConnectorNode(drainBlockingConnector);

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        final Future<Void> drainFuture = connectorNode.drainFlowFiles();

        assertEquals(ConnectorState.DRAINING, connectorNode.getCurrentState());
        assertFalse(drainFuture.isDone());

        drainCompletionFuture.complete(null);

        try {
            drainFuture.get(2, TimeUnit.SECONDS);
        } catch (final Exception e) {
            throw new RuntimeException("Drain future failed to complete", e);
        }

        assertTrue(drainFuture.isDone());
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testDrainFlowFilesFutureDoesNotCompleteUntilDrainFinishes() throws FlowUpdateException, InterruptedException {
        final CompletableFuture<Void> drainCompletionFuture = new CompletableFuture<>();
        final DrainBlockingConnector drainBlockingConnector = new DrainBlockingConnector(drainCompletionFuture);
        final StandardConnectorNode connectorNode = createConnectorNode(drainBlockingConnector);

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        final Future<Void> drainFuture = connectorNode.drainFlowFiles();
        assertEquals(ConnectorState.DRAINING, connectorNode.getCurrentState());

        Thread.sleep(200);
        assertFalse(drainFuture.isDone());
        assertEquals(ConnectorState.DRAINING, connectorNode.getCurrentState());

        drainCompletionFuture.complete(null);

        try {
            drainFuture.get(2, TimeUnit.SECONDS);
        } catch (final Exception e) {
            throw new RuntimeException("Drain future failed to complete", e);
        }

        assertTrue(drainFuture.isDone());
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testDrainFlowFilesStateTransitionsBackToStoppedOnCompletion() throws FlowUpdateException {
        final CompletableFuture<Void> drainCompletionFuture = new CompletableFuture<>();
        final DrainBlockingConnector drainBlockingConnector = new DrainBlockingConnector(drainCompletionFuture);
        final StandardConnectorNode connectorNode = createConnectorNode(drainBlockingConnector);

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());

        final Future<Void> drainFuture = connectorNode.drainFlowFiles();
        assertEquals(ConnectorState.DRAINING, connectorNode.getCurrentState());

        drainCompletionFuture.complete(null);

        try {
            drainFuture.get(2, TimeUnit.SECONDS);
        } catch (final Exception e) {
            throw new RuntimeException("Drain future failed to complete", e);
        }

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());
    }

    private StandardConnectorNode createConnectorNode() throws FlowUpdateException {
        final SleepingConnector sleepingConnector = new SleepingConnector(Duration.ofMillis(1));
        return createConnectorNode(sleepingConnector);
    }

    private StandardConnectorNode createConnectorNode(final Connector connector) throws FlowUpdateException {
        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("TestConnectorNode");
        final ConnectorValidationTrigger validationTrigger = new SynchronousConnectorValidationTrigger();
        final StandardConnectorNode node = new StandardConnectorNode(
            "test-connector-id",
            mock(FlowManager.class),
            extensionManager,
            null,
            createConnectorDetails(connector),
            "TestConnector",
            connector.getClass().getCanonicalName(),
            new StandardConnectorConfigurationContext(assetManager, secretsManager),
            stateTransition,
            flowContextFactory,
            validationTrigger,
            false);

        // mock secrets manager
        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getAllSecrets()).thenReturn(List.of());
        when(secretsManager.getSecrets(anySet())).thenReturn(Collections.emptyMap());

        final FrameworkConnectorInitializationContext initializationContext = mock(FrameworkConnectorInitializationContext.class);
        when(initializationContext.getSecretsManager()).thenReturn(secretsManager);

        node.initializeConnector(initializationContext);
        node.loadInitialFlow();
        return node;
    }

    private static class SynchronousConnectorValidationTrigger implements ConnectorValidationTrigger {
        @Override
        public void triggerAsync(final ConnectorNode connector) {
            trigger(connector);
        }

        @Override
        public void trigger(final ConnectorNode connector) {
            connector.performValidation();
        }
    }

    private ConnectorDetails createConnectorDetails(final Connector connector) {
        final ComponentLog componentLog = new MockComponentLog("TestConnector", connector);
        final BundleCoordinate bundleCoordinate = new BundleCoordinate("org.apache.nifi", "test-standard-connector-node", "1.0.0");
        return new ConnectorDetails(connector, bundleCoordinate, componentLog);
    }

    private StepConfiguration createStepConfiguration() {
        return createStepConfiguration(Map.of("testProperty", "testValue"));
    }

    private StepConfiguration createStepConfiguration(final Map<String, String> properties) {
        final Map<String, ConnectorValueReference> valueReferences = new java.util.HashMap<>();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            valueReferences.put(entry.getKey(), new StringLiteralValue(entry.getValue()));
        }
        return new StepConfiguration(valueReferences);
    }

    private ConnectorConfiguration createTestConfiguration() {
        return createTestConfiguration("testGroup", "testProperty", "testValue");
    }

    private ConnectorConfiguration createTestConfiguration(final String configurationStepName, final String propertyName, final String propertyValue) {
        final StepConfiguration stepConfig = new StepConfiguration(Map.of(propertyName, new StringLiteralValue(propertyValue)));
        final NamedStepConfiguration configurationStepConfiguration = new NamedStepConfiguration(configurationStepName, stepConfig);
        return new ConnectorConfiguration(Set.of(configurationStepConfiguration));
    }

    private ConnectorConfiguration createTestConfigurationWithMultipleGroups() {
        final StepConfiguration firstStepConfig = new StepConfiguration(Map.of("prop1", new StringLiteralValue("value1")));
        final NamedStepConfiguration firstConfigurationStepConfiguration = new NamedStepConfiguration("configurationStep1", firstStepConfig);

        final StepConfiguration secondStepConfig = new StepConfiguration(Map.of("prop2", new StringLiteralValue("value2")));
        final NamedStepConfiguration secondConfigurationStepConfiguration = new NamedStepConfiguration("configurationStep2", secondStepConfig);

        return new ConnectorConfiguration(Set.of(firstConfigurationStepConfiguration, secondConfigurationStepConfiguration));
    }

    /**
     * Test connector that tracks method calls for verification
     */
    private static class TrackingConnector extends AbstractConnector {
        private final Set<String> onConfigurationStepConfiguredCalls = new HashSet<>();

        @Override
        public VersionedExternalFlow getInitialFlow() {
            return null;
        }

        @Override
        public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps() {
            return List.of();
        }

        @Override
        public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
            onConfigurationStepConfiguredCalls.add(stepName);
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
            return List.of();
        }

        public boolean wasOnPropertyGroupConfiguredCalled(final String stepName) {
            return onConfigurationStepConfiguredCalls.contains(stepName);
        }

        public void reset() {
            onConfigurationStepConfiguredCalls.clear();
        }
    }

    /**
     * Test connector that returns invalid validation results from validateConfigurationStep
     */
    private static class ValidationFailingConnector extends AbstractConnector {
        @Override
        public VersionedExternalFlow getInitialFlow() {
            return null;
        }

        @Override
        public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public List<ValidationResult> validateConfigurationStep(final ConfigurationStep configurationStep, final ConnectorConfigurationContext connectorConfigurationContext,
                final ConnectorValidationContext connectorValidationContext) {

            final ValidationResult invalidResult = new ValidationResult.Builder()
                .subject("Test Property")
                .valid(false)
                .explanation("The property value is invalid")
                .build();
            return List.of(invalidResult);
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps() {
            final ConfigurationStep testStep = new ConfigurationStep.Builder()
                .name("testStep")
                .build();
            return List.of(testStep);
        }

        @Override
        public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
            return List.of();
        }
    }

    /**
     * Test connector that allows control over when drainFlowFiles completes via a CompletableFuture
     */
    private static class DrainBlockingConnector extends AbstractConnector {
        private final CompletableFuture<Void> drainCompletionFuture;

        public DrainBlockingConnector(final CompletableFuture<Void> drainCompletionFuture) {
            this.drainCompletionFuture = drainCompletionFuture;
        }

        @Override
        public VersionedExternalFlow getInitialFlow() {
            return null;
        }

        @Override
        public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps() {
            return List.of();
        }

        @Override
        public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
            return List.of();
        }

        @Override
        public CompletableFuture<Void> drainFlowFiles(final FlowContext flowContext) {
            return drainCompletionFuture;
        }
    }

}
