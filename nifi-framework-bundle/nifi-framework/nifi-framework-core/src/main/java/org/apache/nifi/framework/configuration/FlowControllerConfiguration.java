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
package org.apache.nifi.framework.configuration;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.asset.AssetComponentManager;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.asset.AssetSynchronizer;
import org.apache.nifi.asset.StandardAssetComponentManager;
import org.apache.nifi.asset.StandardAssetSynchronizer;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.impl.NodeProtocolSenderListener;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.StandardFlowService;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.status.history.JsonNodeStatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.controller.status.history.VolatileComponentStatusRepository;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.diagnostics.bootstrap.BootstrapDiagnosticsFactory;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorFactory;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.apache.nifi.extension.manifest.parser.jaxb.JAXBExtensionManifestParser;
import org.apache.nifi.manifest.RuntimeManifestService;
import org.apache.nifi.manifest.StandardRuntimeManifestService;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.NarComponentManager;
import org.apache.nifi.nar.NarLoader;
import org.apache.nifi.nar.NarLoaderHolder;
import org.apache.nifi.nar.NarManager;
import org.apache.nifi.nar.NarPersistenceProvider;
import org.apache.nifi.nar.NarPersistenceProviderFactoryBean;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.nar.StandardNarComponentManager;
import org.apache.nifi.nar.StandardNarManager;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.validation.RuleViolationsManager;
import org.apache.nifi.validation.StandardRuleViolationsManager;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.redirect.RedirectHandling;
import org.apache.nifi.web.client.ssl.TlsContext;
import org.apache.nifi.web.revision.RevisionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Framework Flow Controller Configuration class for Spring Application
 */
@Configuration
public class FlowControllerConfiguration {

    private NiFiProperties properties;

    private ExtensionDiscoveringManager extensionManager;

    private AuditService auditService;

    private Authorizer authorizer;

    private RevisionManager revisionManager;

    private LeaderElectionManager leaderElectionManager;

    private SSLContext sslContext;

    private X509KeyManager keyManager;

    private X509TrustManager trustManager;

    private StateManagerProvider stateManagerProvider;

    private BulletinRepository bulletinRepository;

    private NodeProtocolSender nodeProtocolSender;

    private NodeProtocolSenderListener nodeProtocolSenderListener;

    private HeartbeatMonitor heartbeatMonitor;

    private ClusterCoordinator clusterCoordinator;

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void setExtensionManager(final ExtensionDiscoveringManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Autowired
    public void setAuditService(final AuditService auditService) {
        this.auditService = auditService;
    }

    @Autowired
    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Autowired
    public void setRevisionManager(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    @Autowired
    public void setLeaderElectionManager(final LeaderElectionManager leaderElectionManager) {
        this.leaderElectionManager = leaderElectionManager;
    }

    @Autowired
    public void setStateManagerProvider(final StateManagerProvider stateManagerProvider) {
        this.stateManagerProvider = stateManagerProvider;
    }

    @Autowired
    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }

    @Autowired(required = false)
    public void setSslContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Autowired(required = false)
    public void setKeyManager(final X509KeyManager keyManager) {
        this.keyManager = keyManager;
    }

    @Autowired(required = false)
    public void setTrustManager(final X509TrustManager trustManager) {
        this.trustManager = trustManager;
    }

    @Qualifier("nodeProtocolSender")
    @Autowired(required = false)
    public void setNodeProtocolSender(final NodeProtocolSender nodeProtocolSender) {
        this.nodeProtocolSender = nodeProtocolSender;
    }

    @Autowired(required = false)
    public void setNodeProtocolSenderListener(final NodeProtocolSenderListener nodeProtocolSenderListener) {
        this.nodeProtocolSenderListener = nodeProtocolSenderListener;
    }

    @Autowired(required = false)
    public void setHeartbeatMonitor(final HeartbeatMonitor heartbeatMonitor) {
        this.heartbeatMonitor = heartbeatMonitor;
    }

    @Autowired(required = false)
    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    /**
     * Flow Controller implementation depends on cluster configuration
     *
     * @return Flow Controller
     * @throws Exception Thrown on failures to create Flow Controller
     */
    @Bean
    public FlowController flowController() throws Exception {
        final FlowController flowController;

        if (clusterCoordinator == null) {
            flowController = FlowController.createStandaloneInstance(
                    flowFileEventRepository(),
                    sslContext,
                    properties,
                    authorizer,
                    auditService,
                    propertyEncryptor(),
                    bulletinRepository,
                    extensionManager,
                    statusHistoryRepository(),
                    ruleViolationsManager(),
                    stateManagerProvider
            );
        } else {
            flowController = FlowController.createClusteredInstance(
                    flowFileEventRepository(),
                    sslContext,
                    properties,
                    authorizer,
                    auditService,
                    propertyEncryptor(),
                    nodeProtocolSender,
                    bulletinRepository,
                    clusterCoordinator,
                    heartbeatMonitor,
                    leaderElectionManager,
                    extensionManager,
                    revisionManager,
                    statusHistoryRepository(),
                    ruleViolationsManager(),
                    stateManagerProvider
            );
        }

        return flowController;
    }

    /**
     * Flow Service implementation depends on cluster configuration
     *
     * @return Flow Service
     * @throws Exception Thrown on failures to create Flow Service
     */
    @Bean
    public FlowService flowService(@Autowired final NarManager narManager) throws Exception {
        final FlowService flowService;

        if (clusterCoordinator == null) {
            flowService = StandardFlowService.createStandaloneInstance(
                    flowController(),
                    properties,
                    revisionManager,
                    narManager,
                    assetSynchronizer(),
                    authorizer
            );
        } else {
            flowService = StandardFlowService.createClusteredInstance(
                    flowController(),
                    properties,
                    nodeProtocolSenderListener,
                    clusterCoordinator,
                    revisionManager,
                    narManager,
                    assetSynchronizer(),
                    authorizer
            );
        }

        return flowService;
    }

    /**
     * FlowFile Event Repository using Ring Buffer
     *
     * @return Ring Buffer Event Repository
     */
    @Bean
    public RingBufferEventRepository flowFileEventRepository() {
        return new RingBufferEventRepository(5);
    }

    /**
     * Rule Violations Manager for Flow Analysis
     *
     * @return Rule Violations Manager
     */
    @Bean
    public RuleViolationsManager ruleViolationsManager() {
        return new StandardRuleViolationsManager();
    }

    /**
     * Property Encryptor configured using Application Properties
     *
     * @return Property Encryptor
     */
    @Bean
    public PropertyEncryptor propertyEncryptor() {
        return PropertyEncryptorFactory.getPropertyEncryptor(properties);
    }

    /**
     * Status History Repository configured from NiFi Application Properties
     *
     * @return Status History Repository
     * @throws Exception Thrown on failures to create Status History Repository
     */
    @Bean
    public StatusHistoryRepository statusHistoryRepository() throws Exception {
        final String configuredClassName = properties.getProperty(NiFiProperties.COMPONENT_STATUS_REPOSITORY_IMPLEMENTATION);
        final String className = configuredClassName == null ? VolatileComponentStatusRepository.class.getName() : configuredClassName;

        final StatusHistoryRepository statusHistoryRepository = NarThreadContextClassLoader.createInstance(extensionManager, className, StatusHistoryRepository.class, properties);
        statusHistoryRepository.start();
        return statusHistoryRepository;
    }

    /**
     * Status History Dump Factory using JSON implementation
     *
     * @return Status History Dump Factory
     * @throws Exception Thrown on failure to get Status History Repository
     */
    @Bean
    public StatusHistoryDumpFactory statusHistoryDumpFactory() throws Exception {
        final JsonNodeStatusHistoryDumpFactory statusHistoryDumpFactory = new JsonNodeStatusHistoryDumpFactory();
        statusHistoryDumpFactory.setStatusHistoryRepository(statusHistoryRepository());
        return statusHistoryDumpFactory;
    }

    /**
     * Diagnostics Factory with Bootstrap-based implementation
     *
     * @return Diagnostics Factory
     * @throws Exception Thrown on failures to Diagnostics Factory
     */
    @Bean
    public DiagnosticsFactory diagnosticsFactory() throws Exception {
        final BootstrapDiagnosticsFactory diagnosticsFactory = new BootstrapDiagnosticsFactory();
        diagnosticsFactory.setNifiProperties(properties);
        diagnosticsFactory.setFlowController(flowController());
        return diagnosticsFactory;
    }

    /**
     * Extension Manifest Parser using JAXB implementation
     *
     * @return Extension Manifest Parser
     */
    @Bean
    public ExtensionManifestParser extensionManifestParser() {
        return new JAXBExtensionManifestParser();
    }

    /**
     * Runtime Manifest Service uses Extension Manager and Manifest Parser
     *
     * @return Runtime Manifest Service
     */
    @Bean
    public RuntimeManifestService runtimeManifestService() {
        return new StandardRuntimeManifestService(extensionManager, extensionManifestParser());
    }

    @Bean
    public WebClientService webClientService() {
        final long readTimeoutMillis = FormatUtils.getTimeDuration(properties.getClusterNodeReadTimeout(), TimeUnit.MILLISECONDS);
        final Duration timeout = Duration.ofMillis(readTimeoutMillis);

        final StandardWebClientService webClientService = new StandardWebClientService();
        webClientService.setConnectTimeout(timeout);
        webClientService.setReadTimeout(timeout);
        webClientService.setRedirectHandling(RedirectHandling.FOLLOWED);

        if (sslContext != null) {
            webClientService.setTlsContext(new TlsContext() {
                @Override
                public String getProtocol() {
                    return sslContext.getProtocol();
                }

                @Override
                public X509TrustManager getTrustManager() {
                    return trustManager;
                }

                @Override
                public Optional<X509KeyManager> getKeyManager() {
                    return Optional.of(keyManager);
                }
            });
        }

        return webClientService;
    }

    @Bean
    public NarPersistenceProviderFactoryBean narPersistenceProvider() {
        return new NarPersistenceProviderFactoryBean(properties, extensionManager);
    }

    /**
     * NAR Loader from the holder that was set by Jetty.
     *
     * @return NAR Loader
     */
    @Bean
    public NarLoader narLoader() {
        return NarLoaderHolder.getNarLoader();
    }

    /**
     * NAR Component Manager using Flow Controller.
     *
     * @return Nar Component Manager
     * @throws Exception Thrown on failures to create NAR Component Manager
     */
    @Bean
    public NarComponentManager narComponentManager() throws Exception {
        return new StandardNarComponentManager(flowController());
    }

    /**
     * NAR Manager depends on NAR Persistence Provider, Flow Controller, and optional Cluster Coordinator.
     *
     * @return NAR Manager
     * @throws Exception Thrown on failures to create NAR Manager
     */
    @Bean
    public NarManager narManager(@Autowired final NarPersistenceProvider narPersistenceProvider) throws Exception {
        return new StandardNarManager(
                flowController(),
                clusterCoordinator,
                narPersistenceProvider,
                narComponentManager(),
                narLoader(),
                webClientService(),
                properties
        );
    }

    /**
     * Asset Manager from Flow Controller
     *
     * @return Asset Manager
     */
    @Bean
    public AssetManager assetManager() throws Exception {
        return flowController().getAssetManager();
    }

    /**
     * Asset Synchronizer depends on ClusterCoordinator, WebClientService, and NiFiProperties
     *
     * @return Asset Synchronizer
     */
    @Bean
    public AssetSynchronizer assetSynchronizer() throws Exception {
        return new StandardAssetSynchronizer(flowController(), clusterCoordinator, webClientService(), properties);
    }

    /**
     * Affected Component Manager depends on FlowController
     *
     * @return Affected Component Manager
     */
    @Bean
    public AssetComponentManager affectedComponentManager() throws Exception {
        return new StandardAssetComponentManager(flowController());
    }
}
