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

import jakarta.servlet.ServletContext;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.cluster.ClusterDetailsFactory;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.FrameworkFlowContext;
import org.apache.nifi.components.connector.GhostConnector;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StandaloneConnectorRequestReplicator;
import org.apache.nifi.components.connector.StepConfiguration;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.connectable.FlowFileTransferCounts;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.DecommissionTask;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.metrics.DefaultComponentMetricReporter;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.status.history.StatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.controller.status.history.VolatileComponentStatusRepository;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.mock.connector.server.secrets.ConnectorTestRunnerSecretsManager;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.mapping.ComponentIdLookup;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.VersionedComponentFlowMapper;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.validation.RuleViolationsManager;
import org.apache.nifi.web.NiFiConnectorWebContext;
import org.eclipse.jetty.ee.webapp.WebAppClassLoader;
import org.eclipse.jetty.ee11.webapp.WebAppContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.stream.Stream;

public class StandardConnectorMockServer implements ConnectorMockServer {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorMockServer.class);

    private static final String CONNECTOR_ID = "test-connector";
    private static final String NAR_DEPENDENCIES_PATH = "NAR-INF/bundled-dependencies";
    private static final String CONNECTOR_WAR_MANIFEST_PATH = "META-INF/nifi-connector";
    private static final String WAR_EXTENSION = ".war";

    private Bundle systemBundle;
    private Set<Bundle> bundles;
    private NiFiProperties nifiProperties;
    private FlowController flowController;
    private MockExtensionDiscoveringManager extensionManager;
    private ConnectorNode connectorNode;
    private ConnectorRepository connectorRepository;
    private FlowEngine flowEngine;
    private MockExtensionMapper mockExtensionMapper;
    private FlowFileTransferCounts initialFlowFileTransferCounts = new FlowFileTransferCounts(0L, 0L, 0L, 0L);
    private Server jettyServer;

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
            new DefaultComponentMetricReporter(),
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

        connectorRepository = flowController.getConnectorRepository();
        if (!(connectorRepository instanceof MockConnectorRepository)) {
            throw new IllegalStateException("Connector Repository is not an instance of MockConnectorRepository");
        }

        mockExtensionMapper = new MockExtensionMapper();
        ((MockConnectorRepository) connectorRepository).setMockExtensionMapper(mockExtensionMapper);

        flowEngine = new FlowEngine(4, "Connector Threads");

        startJettyServer();
    }

    @Override
    public void initialize(final NiFiProperties properties, final Bundle systemBundle, final Set<Bundle> bundles, final ExtensionMapping extensionMapping) {
        this.systemBundle = systemBundle;
        this.bundles = bundles;
        this.nifiProperties = properties;
    }

    @Override
    public void stop() {
        if (jettyServer != null) {
            try {
                jettyServer.stop();
                logger.info("Jetty server stopped");
            } catch (final Exception e) {
                logger.warn("Failed to stop Jetty server", e);
            }
        }
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

        if (connectorNode.isExtensionMissing()) {
            final Connector connector = connectorNode.getConnector();
            if (connector instanceof final GhostConnector ghostConnector) {
                throw new IllegalStateException("Failed to create Connector of type " + connectorClassName, ghostConnector.getCauseOfGhost());
            } else {
                throw new IllegalStateException("Failed to create Connector of type " + connectorClassName);
            }
        }
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

        connectorNode.transitionStateForUpdating();
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
        configure(stepName, propertyValues, Collections.emptyMap());
    }

    @Override
    public void configure(final String stepName, final Map<String, String> propertyValues, final Map<String, ConnectorValueReference> propertyReferences) throws FlowUpdateException {
        final StepConfiguration stepConfiguration = createStepConfiguration(propertyValues, propertyReferences);
        configure(stepName, stepConfiguration);
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final Map<String, String> propertyValueOverrides) {
        return verifyConfiguration(stepName, propertyValueOverrides, Collections.emptyMap());
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final Map<String, String> propertyValueOverrides,
            final Map<String, ConnectorValueReference> referenceOverrides) {

        final StepConfiguration configuration = createStepConfiguration(propertyValueOverrides, referenceOverrides);
        return verifyConfiguration(stepName, configuration);
    }

    private StepConfiguration createStepConfiguration(final Map<String, String> propertyValues, final Map<String, ConnectorValueReference> propertyReferences) {
        final Map<String, ConnectorValueReference> references = new HashMap<>();
        propertyValues.forEach((key, value) -> references.put(key, value == null ? null : new StringLiteralValue(value)));
        references.putAll(propertyReferences);
        return new StepConfiguration(references);
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final StepConfiguration configurationOverrides) {
        final List<ConfigVerificationResult> results = connectorNode.verifyConfigurationStep(stepName, configurationOverrides);
        return new MockServerConfigVerificationResult(results);
    }

    @Override
    public void addSecret(final String name, final String value) {
        final SecretsManager secretsManager = connectorRepository.getSecretsManager();
        if (!(secretsManager instanceof final ConnectorTestRunnerSecretsManager testRunnerSecretsManager)) {
            throw new IllegalStateException("Secrets Manager is not an instance of ConnectorTestRunnerSecretsManager");
        }

        testRunnerSecretsManager.addSecret(name, value);
    }

    @Override
    public SecretReference createSecretReference(final String secretName) {
        return new SecretReference(ConnectorTestRunner.SECRET_PROVIDER_ID, ConnectorTestRunner.SECRET_PROVIDER_NAME, secretName, secretName);
    }

    @Override
    public AssetReference addAsset(final File file) {
        final AssetManager assetManager = flowController.getConnectorAssetManager();

        try (final InputStream inputStream = new FileInputStream(file)) {
            final Asset asset = assetManager.createAsset(CONNECTOR_ID, file.getName(), inputStream);
            return new AssetReference(Set.of(asset.getIdentifier()));
        } catch (final IOException e) {
            throw new RuntimeException("Failed to add asset from file: " + file.getAbsolutePath(), e);
        }
    }

    @Override
    public AssetReference addAsset(final String assetName, final InputStream contents) {
        final AssetManager assetManager = flowController.getConnectorAssetManager();

        try {
            final Asset asset = assetManager.createAsset(CONNECTOR_ID, assetName, contents);
            return new AssetReference(Set.of(asset.getIdentifier()));
        } catch (final IOException e) {
            throw new RuntimeException("Failed to add asset: " + assetName, e);
        }
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
    public List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName) {
        return connectorNode.fetchAllowableValues(stepName, propertyName);
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
    public VersionedExternalFlow getActiveFlowSnapshot() {
        final FrameworkFlowContext activeFlowContext = connectorNode.getActiveFlowContext();
        if (activeFlowContext == null) {
            throw new IllegalStateException("Active Flow Context is not available. The Connector may not have been initialized or may not have an initial flow.");
        }
        return createFlowSnapshot(activeFlowContext);
    }

    @Override
    public VersionedExternalFlow getWorkingFlowSnapshot() {
        final FrameworkFlowContext workingFlowContext = connectorNode.getWorkingFlowContext();
        if (workingFlowContext == null) {
            throw new IllegalStateException("Working Flow Context is not available. No configuration changes may have been made since the last update was applied.");
        }
        return createFlowSnapshot(workingFlowContext);
    }

    private VersionedExternalFlow createFlowSnapshot(final FrameworkFlowContext flowContext) {
        final ProcessGroup processGroup = flowContext.getManagedProcessGroup();

        final FlowMappingOptions flowMappingOptions = new FlowMappingOptions.Builder()
            .mapSensitiveConfiguration(false)
            .mapPropertyDescriptors(true)
            .stateLookup(VersionedComponentStateLookup.ENABLED_OR_DISABLED)
            .sensitiveValueEncryptor(value -> value)
            .componentIdLookup(ComponentIdLookup.VERSIONED_OR_GENERATE)
            .mapInstanceIdentifiers(true)
            .mapControllerServiceReferencesToVersionedId(true)
            .mapFlowRegistryClientId(true)
            .mapAssetReferences(true)
            .build();

        final VersionedComponentFlowMapper flowMapper = new VersionedComponentFlowMapper(extensionManager, flowMappingOptions);
        final VersionedProcessGroup versionedGroup = flowMapper.mapProcessGroup(
            processGroup, flowController.getControllerServiceProvider(), flowController.getFlowManager(), true);

        final VersionedExternalFlow externalFlow = new VersionedExternalFlow();
        externalFlow.setFlowContents(versionedGroup);

        final ParameterContext parameterContext = processGroup.getParameterContext();
        if (parameterContext != null) {
            final Map<String, VersionedParameterContext> parameterContexts = new HashMap<>();
            final VersionedParameterContext versionedParameterContext = createVersionedParameterContext(parameterContext);
            parameterContexts.put(versionedParameterContext.getName(), versionedParameterContext);
            externalFlow.setParameterContexts(parameterContexts);
        }

        return externalFlow;
    }

    private VersionedParameterContext createVersionedParameterContext(final ParameterContext parameterContext) {
        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setName(parameterContext.getName());
        versionedParameterContext.setDescription(parameterContext.getDescription());
        versionedParameterContext.setIdentifier(parameterContext.getIdentifier());

        final Set<VersionedParameter> versionedParameters = new LinkedHashSet<>();
        for (final Parameter parameter : parameterContext.getParameters().values()) {
            final VersionedParameter versionedParameter = new VersionedParameter();
            versionedParameter.setName(parameter.getDescriptor().getName());
            versionedParameter.setDescription(parameter.getDescriptor().getDescription());
            versionedParameter.setSensitive(parameter.getDescriptor().isSensitive());
            versionedParameter.setProvided(parameter.getDescriptor().isSensitive());
            if (!parameter.getDescriptor().isSensitive()) {
                versionedParameter.setValue(parameter.getValue());
            }
            versionedParameters.add(versionedParameter);
        }
        versionedParameterContext.setParameters(versionedParameters);

        return versionedParameterContext;
    }

    @Override
    public void mockProcessor(final String processorType, final Class<? extends Processor> mockProcessorClass) {
        mockExtensionMapper.mockProcessor(processorType, mockProcessorClass.getName());
        extensionManager.addProcessor(mockProcessorClass);
    }

    @Override
    public void mockControllerService(final String controllerServiceType, final Class<? extends ControllerService> mockControllerServiceClass) {
        mockExtensionMapper.mockControllerService(controllerServiceType, mockControllerServiceClass.getName());
        extensionManager.addControllerService(mockControllerServiceClass);
    }

    @Override
    public int getHttpPort() {
        if (jettyServer == null) {
            return -1;
        }

        final ServerConnector connector = (ServerConnector) jettyServer.getConnectors()[0];
        return connector.getLocalPort();
    }

    @Override
    public void close() {
        stop();
    }

    private void startJettyServer() {
        final String httpPortValue = nifiProperties.getProperty(NiFiProperties.WEB_HTTP_PORT);
        if (httpPortValue == null || httpPortValue.isBlank()) {
            logger.debug("No HTTP port configured; skipping Jetty server startup");
            return;
        }

        final int httpPort = Integer.parseInt(httpPortValue);
        final Map<File, Bundle> wars = findWars(bundles);
        if (wars.isEmpty()) {
            logger.debug("No WAR files found in NAR bundles; skipping Jetty server startup");
            return;
        }

        jettyServer = new Server();

        final ServerConnector serverConnector = new ServerConnector(jettyServer);
        serverConnector.setPort(httpPort);
        jettyServer.addConnector(serverConnector);

        final List<WebAppContext> webAppContexts = new ArrayList<>();
        final ContextHandlerCollection handlers = new ContextHandlerCollection();
        for (final Map.Entry<File, Bundle> entry : wars.entrySet()) {
            final File warFile = entry.getKey();
            final Bundle bundle = entry.getValue();

            final String warName = warFile.getName();
            final String contextPath = "/" + warName.substring(0, warName.length() - WAR_EXTENSION.length());

            final WebAppContext webAppContext = new WebAppContext(warFile.getPath(), contextPath);
            webAppContext.setClassLoader(new WebAppClassLoader(bundle.getClassLoader(), webAppContext));

            handlers.addHandler(webAppContext);
            webAppContexts.add(webAppContext);
            logger.info("Deploying WAR [{}] at context path [{}]", warFile.getAbsolutePath(), contextPath);
        }

        jettyServer.setHandler(handlers);

        try {
            jettyServer.start();
            logger.info("Jetty server started on port [{}]", getHttpPort());
        } catch (final Exception e) {
            throw new RuntimeException("Failed to start Jetty server", e);
        }

        performInjectionForConnectorUis(webAppContexts);
    }

    private void performInjectionForConnectorUis(final List<WebAppContext> webAppContexts) {
        final NiFiConnectorWebContext connectorWebContext = new MockNiFiConnectorWebContext(connectorRepository);
        for (final WebAppContext webAppContext : webAppContexts) {
            final ServletContext servletContext = webAppContext.getServletHandler().getServletContext();
            servletContext.setAttribute("nifi-connector-web-context", connectorWebContext);
            logger.info("Injected NiFiConnectorWebContext into WAR context [{}]", webAppContext.getContextPath());
        }
    }

    public Map<File, Bundle> findWars(final Set<Bundle> bundles) {
        final Map<File, Bundle> wars = new HashMap<>();

        bundles.forEach(bundle -> {
            final BundleDetails details = bundle.getBundleDetails();
            final Path bundledDependencies = new File(details.getWorkingDirectory(), NAR_DEPENDENCIES_PATH).toPath();
            if (Files.isDirectory(bundledDependencies)) {
                try (final Stream<Path> dependencies = Files.list(bundledDependencies)) {
                    dependencies.filter(dependency -> dependency.getFileName().toString().endsWith(WAR_EXTENSION))
                            .map(Path::toFile)
                            .filter(this::isConnectorWar)
                            .forEach(dependency -> wars.put(dependency, bundle));
                } catch (final IOException e) {
                    logger.warn("Failed to find WAR files in bundled-dependencies [{}]", bundledDependencies, e);
                }
            }
        });

        return wars;
    }

    private boolean isConnectorWar(final File warFile) {
        try (final JarFile jarFile = new JarFile(warFile)) {
            return jarFile.getJarEntry(CONNECTOR_WAR_MANIFEST_PATH) != null;
        } catch (final IOException e) {
            logger.warn("Unable to inspect WAR file [{}] for connector manifest", warFile, e);
            return false;
        }
    }
}
