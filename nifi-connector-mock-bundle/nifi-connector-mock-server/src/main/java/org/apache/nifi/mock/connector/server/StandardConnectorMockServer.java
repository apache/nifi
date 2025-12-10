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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.cluster.ClusterDetailsFactory;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.StandaloneConnectorRequestReplicator;
import org.apache.nifi.components.connector.StepConfiguration;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.connectable.FlowFileTransferCounts;
import org.apache.nifi.controller.DecommissionTask;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.status.history.StatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.controller.status.history.VolatileComponentStatusRepository;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.validation.RuleViolationsManager;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StandardConnectorMockServer implements ConnectorMockServer {
    private static final String CONNECTOR_ID = "test-connector";

    private Bundle systemBundle;
    private Set<Bundle> bundles;
    private NiFiProperties nifiProperties;
    private FlowController flowController;
    private MockExtensionDiscoveringManager extensionManager;
    private ConnectorNode connectorNode;
    private FlowEngine flowEngine;
    private MockExtensionMapper mockExtensionMapper;
    private FlowFileTransferCounts initialFlowFileTransferCounts = new FlowFileTransferCounts(0L, 0L, 0L, 0L);

    @Override
    public void start() {
        extensionManager = new MockExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, bundles);
        extensionManager.logClassLoaderMapping();

        final FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
        final Authorizer authorizer = new PermitAllAuthorizer();
        final AuditService auditService = new MockAuditService();
        final PropertyEncryptor propertyEncryptor = new NopPropertyEncryptor();
        final BulletinRepository bulletinRepository = new VolatileBulletinRepository();
        final StatusHistoryRepository statusHistoryRepository = new VolatileComponentStatusRepository(nifiProperties);
        final RuleViolationsManager ruleViolationManager = new MockRuleViolationsManager();
        final StateManagerProvider stateManagerProvider = new MockStateManagerProvider();

        flowController = FlowController.createStandaloneInstance(
            flowFileEventRepository,
            null,
            nifiProperties,
            authorizer,
            auditService,
            propertyEncryptor,
            bulletinRepository,
            extensionManager,
            statusHistoryRepository,
            ruleViolationManager,
            stateManagerProvider,
            new StandaloneConnectorRequestReplicator());

        try {
            flowController.getRepositoryContextFactory().getFlowFileRepository().loadFlowFiles(Collections::emptyList);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to initialize FlowFile Repository", e);
        }

        final ConnectorRepository connectorRepository = flowController.getConnectorRepository();
        if (!(connectorRepository instanceof MockConnectorRepository)) {
            throw new IllegalStateException("Connector Repository is not an instance of MockConnectorRepository");
        }

        mockExtensionMapper = new MockExtensionMapper();
        ((MockConnectorRepository) connectorRepository).setMockExtensionMapper(mockExtensionMapper);

        flowEngine = new FlowEngine(4, "Connector Threads");
    }

    @Override
    public void initialize(final NiFiProperties properties, final Bundle systemBundle, final Set<Bundle> bundles, final ExtensionMapping extensionMapping) {
        this.systemBundle = systemBundle;
        this.bundles = bundles;
        this.nifiProperties = properties;
    }

    @Override
    public void stop() {
        if (flowEngine != null) {
            flowEngine.shutdown();
        }
        if (flowController != null) {
            flowController.shutdown(false);
        }
    }

    @Override
    public DiagnosticsFactory getDiagnosticsFactory() {
        return null;
    }

    @Override
    public DiagnosticsFactory getThreadDumpFactory() {
        return null;
    }

    @Override
    public DecommissionTask getDecommissionTask() {
        return null;
    }

    @Override
    public ClusterDetailsFactory getClusterDetailsFactory() {
        return null;
    }

    @Override
    public StatusHistoryDumpFactory getStatusHistoryDumpFactory() {
        return null;
    }

    @Override
    public void instantiateConnector(final String connectorClassName) {
        final List<Bundle> bundles = extensionManager.getBundles(connectorClassName);
        if (bundles.isEmpty()) {
            throw new IllegalStateException("No bundles found for connector class: " + connectorClassName + " - ensure that you have included all relevant NARs in the configured lib directory");
        }
        if (bundles.size() > 1) {
            throw new IllegalStateException("Multiple bundles found for connector class: " + connectorClassName + " - unable to determine which bundle to use. Ensure that only a single version of " +
                                            "the Connector is included in the configured lib directory. Available bundles: " + bundles);
        }

        final BundleCoordinate bundleCoordinate = bundles.getFirst().getBundleDetails().getCoordinate();
        connectorNode = flowController.getFlowManager().createConnector(connectorClassName, CONNECTOR_ID, bundleCoordinate, true, true);
    }

    @Override
    public void registerMockBundle(final ClassLoader classLoader, final File workingDirectory) {
        new BundleDetails.Builder()
            .workingDir(workingDirectory)
            .coordinate(new BundleCoordinate("org.apache.nifi.mock", "nifi-connector-mock-bundle", "1.0.0"))
            .build();
    }

    @Override
    public void applyUpdate() throws FlowUpdateException {
        final ConnectorState initialDesiredState = connectorNode.getDesiredState();

        connectorNode.prepareForUpdate();
        connectorNode.applyUpdate();

        if (initialDesiredState == ConnectorState.RUNNING) {
            connectorNode.start(flowEngine);
        } else {
            connectorNode.stop(flowEngine);
        }
    }

    @Override
    public void configure(final String stepName, final StepConfiguration configuration) throws FlowUpdateException {
        connectorNode.setConfiguration(stepName, configuration);
    }

    @Override
    public void configure(final String stepName, final Map<String, String> propertyValues) throws FlowUpdateException {
        final StepConfiguration configuration = toStringLiteralConfiguration(propertyValues);
        configure(stepName, configuration);
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final Map<String, String> propertyValueOverrides) {
        final StepConfiguration configuration = toStringLiteralConfiguration(propertyValueOverrides);
        return verifyConfiguration(stepName, configuration);
    }

    private StepConfiguration toStringLiteralConfiguration(final Map<String, String> propertyValues) {
        final Map<String, ConnectorValueReference> references = new HashMap<>(propertyValues.size());
        propertyValues.forEach((key, value) -> references.put(key, new StringLiteralValue(value)));
        return new StepConfiguration(references);
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final StepConfiguration configurationOverrides) {
        final List<ConfigVerificationResult> results = connectorNode.verifyConfigurationStep(stepName, configurationOverrides);
        return new MockServerConfigVerificationResult(results);
    }

    @Override
    public void startConnector() {
        initialFlowFileTransferCounts = connectorNode.getFlowFileTransferCounts();

        connectorNode.start(flowEngine);
    }

    @Override
    public void stopConnector() {
        try {
            connectorNode.stop(flowEngine).get(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for connector to stop", e);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to stop Connector", e);
        }
    }

    @Override
    public void waitForDataIngested(final Duration maxWaitTime) {
        final long startTime = System.currentTimeMillis();
        final long expirationTime = startTime + maxWaitTime.toMillis();

        while (getTransferCountsSinceStart().getReceivedCount() == 0L) {
            if (System.currentTimeMillis() > expirationTime) {
                throw new RuntimeException("Timed out waiting for data to be ingested by the Connector");
            }

            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for data to be ingested by the Connector", e);
            }
        }
    }

    private FlowFileTransferCounts getTransferCountsSinceStart() {
        final FlowFileTransferCounts currentCounts = connectorNode.getFlowFileTransferCounts();
        return currentCounts.minus(initialFlowFileTransferCounts);
    }

    @Override
    public void waitForIdle(final Duration maxWaitTime) {
        waitForIdle(Duration.ofMillis(0L), maxWaitTime);
    }

    @Override
    public void waitForIdle(final Duration minimumIdleTime, final Duration maxWaitTime) {
        Optional<Duration> idleTime = connectorNode.getIdleDuration();

        // Wait until idleTime is not empty and is at least equal to minimumIdleTime
        final long startTime = System.currentTimeMillis();
        final long expirationTime = startTime + maxWaitTime.toMillis();

        while (idleTime.isEmpty() || idleTime.get().compareTo(minimumIdleTime) <= 0) {
            if (System.currentTimeMillis() > expirationTime) {
                throw new RuntimeException("Timed out waiting for Connector to be idle");
            }

            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Connector to be idle", e);
            }

            idleTime = connectorNode.getIdleDuration();
        }
    }

    @Override
    public List<ValidationResult> validate() {
        final ValidationState validationState = connectorNode.performValidation();
        return validationState.getValidationErrors().stream()
            .filter(result -> !result.isValid())
            .filter(result -> !DisabledServiceValidationResult.isMatch(result))
            .toList();
    }

    @Override
    public void mockProcessor(final String processorType, final Class<? extends Processor> mockProcessorClass) {
        mockExtensionMapper.mockProcessor(processorType, mockProcessorClass.getName());
        extensionManager.addProcessor(mockProcessorClass);
    }

    @Override
    public void close() {
        stop();
    }
}
