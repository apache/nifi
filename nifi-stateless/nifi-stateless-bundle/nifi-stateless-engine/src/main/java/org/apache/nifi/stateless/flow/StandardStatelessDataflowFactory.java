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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.asset.AssetManagerInitializationContext;
import org.apache.nifi.asset.AssetReferenceLookup;
import org.apache.nifi.asset.StandardAssetManager;
import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.ContentRepositoryContext;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.StandardCounterRepository;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.scheduling.StatelessProcessScheduler;
import org.apache.nifi.controller.scheduling.StatelessProcessSchedulerInitializationContext;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.encrypt.PropertyEncryptionMethod;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.extensions.ExtensionClient;
import org.apache.nifi.extensions.ExtensionRepository;
import org.apache.nifi.extensions.FileSystemExtensionRepository;
import org.apache.nifi.extensions.NexusExtensionClient;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.StandardParameterContextManager;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.python.DisabledPythonBridge;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.registry.flow.InMemoryFlowRegistry;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.stateless.bootstrap.ExtensionDiscovery;
import org.apache.nifi.stateless.config.ExtensionClientDefinition;
import org.apache.nifi.stateless.config.SslConfigurationUtil;
import org.apache.nifi.stateless.config.SslContextDefinition;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.engine.CachingProcessContextFactory;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.engine.StandardStatelessEngine;
import org.apache.nifi.stateless.engine.StatelessAuthorizer;
import org.apache.nifi.stateless.engine.StatelessEngine;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.engine.StatelessEngineInitializationContext;
import org.apache.nifi.stateless.engine.StatelessFlowManager;
import org.apache.nifi.stateless.engine.StatelessNodeTypeProvider;
import org.apache.nifi.stateless.engine.StatelessProcessContextFactory;
import org.apache.nifi.stateless.engine.StatelessProvenanceAuthorizableFactory;
import org.apache.nifi.stateless.repository.ByteArrayContentRepository;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.repository.StatelessFileSystemContentRepository;
import org.apache.nifi.stateless.repository.StatelessFlowFileRepository;
import org.apache.nifi.stateless.repository.StatelessProvenanceRepository;
import org.apache.nifi.stateless.repository.StatelessRepositoryContextFactory;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;

public class StandardStatelessDataflowFactory implements StatelessDataflowFactory {
    private static final Logger logger = LoggerFactory.getLogger(StandardStatelessDataflowFactory.class);


    @Override
    public StatelessDataflow createDataflow(final StatelessEngineConfiguration engineConfiguration, final DataflowDefinition dataflowDefinition,
                                            final ClassLoader extensionRootClassLoader)
                    throws IOException, StatelessConfigurationException {
        final long start = System.currentTimeMillis();

        ProvenanceRepository provenanceRepo = null;
        ContentRepository contentRepo = null;
        StatelessProcessScheduler processScheduler = null;
        FlowFileRepository flowFileRepo = null;
        FlowFileEventRepository flowFileEventRepo = null;

        try {
            final BulletinRepository bulletinRepository = new VolatileBulletinRepository();
            final File workingDir = engineConfiguration.getWorkingDirectory();
            final File narExpansionDirectory = new File(workingDir, "nar");
            if (!narExpansionDirectory.exists() && !narExpansionDirectory.mkdirs()) {
                throw new IOException("Working Directory " + narExpansionDirectory + " does not exist and could not be created");
            }

            final NarClassLoaders narClassLoaders = new NarClassLoaders();
            final File extensionsWorkingDir = new File(narExpansionDirectory, "extensions");
            final ExtensionDiscoveringManager extensionManager = ExtensionDiscovery.discover(
                    extensionsWorkingDir,
                    extensionRootClassLoader,
                    narClassLoaders,
                    engineConfiguration.isLogExtensionDiscovery()
            );

            final AssetManager assetManager = new StandardAssetManager();
            final File assetDir = new File(workingDir, "assets");
            final AssetManagerInitializationContext assetManagerInitializationContext = new AssetManagerInitializationContext() {
                @Override
                public AssetReferenceLookup getAssetReferenceLookup() {
                    return Set::of;
                }

                @Override
                public Map<String, String> getProperties() {
                    return Map.of(StandardAssetManager.ASSET_STORAGE_LOCATION_PROPERTY, assetDir.getAbsolutePath());
                }

                @Override
                public NodeTypeProvider getNodeTypeProvider() {
                    return new StatelessNodeTypeProvider();
                }
            };
            assetManager.initialize(assetManagerInitializationContext);

            flowFileEventRepo = new RingBufferEventRepository(5);

            final StatelessStateManagerProvider stateManagerProvider = new StatelessStateManagerProvider();

            final ParameterContextManager parameterContextManager = new StandardParameterContextManager();
            final Duration processorStartTimeoutDuration = Duration.ofSeconds((long) FormatUtils.getPreciseTimeDuration(engineConfiguration.getProcessorStartTimeout(), TimeUnit.SECONDS));
            processScheduler = new StatelessProcessScheduler(extensionManager, processorStartTimeoutDuration);
            provenanceRepo = new StatelessProvenanceRepository(1_000);
            provenanceRepo.initialize(EventReporter.NO_OP, new StatelessAuthorizer(), new StatelessProvenanceAuthorizableFactory(), IdentifierLookup.EMPTY);

            final SSLContext sslContext;
            try {
                sslContext = SslConfigurationUtil.createSslContext(engineConfiguration.getSslContext());
            } catch (StatelessConfigurationException e) {
                throw new StatelessConfigurationException("Could not create SSLContext", e);
            }

            // Build Extension Repository
            final List<ExtensionClient> extensionClients = new ArrayList<>();
            for (final ExtensionClientDefinition extensionClientDefinition : engineConfiguration.getExtensionClients()) {
                final ExtensionClient extensionClient = createExtensionClient(extensionClientDefinition, engineConfiguration.getSslContext());
                extensionClients.add(extensionClient);
            }

            final ExtensionRepository extensionRepository = new FileSystemExtensionRepository(extensionManager, engineConfiguration, narClassLoaders, extensionClients);
            extensionRepository.initialize();

            final PropertyEncryptor lazyInitializedEncryptor = new PropertyEncryptor() {
                private PropertyEncryptor created = null;

                @Override
                public String encrypt(final String property) {
                    return getEncryptor().encrypt(property);
                }

                @Override
                public String decrypt(final String encryptedProperty) {
                    return getEncryptor().decrypt(encryptedProperty);
                }

                private synchronized PropertyEncryptor getEncryptor() {
                    if (created != null) {
                        return created;
                    }

                    created = new PropertyEncryptorBuilder(engineConfiguration.getSensitivePropsKey())
                            .setAlgorithm(PropertyEncryptionMethod.NIFI_PBKDF2_AES_GCM_256.toString())
                            .build();
                    return created;
                }
            };

            final CounterRepository counterRepo = new StandardCounterRepository();

            final File krb5File = engineConfiguration.getKrb5File();
            final KerberosConfig kerberosConfig = new KerberosConfig(null, null, krb5File);
            if (krb5File != null) {
                logger.info("Setting java.security.krb5.conf to {}", krb5File.getAbsolutePath());
                System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
            }

            final StatelessEngine statelessEngine = new StandardStatelessEngine.Builder()
                    .bulletinRepository(bulletinRepository)
                    .encryptor(lazyInitializedEncryptor)
                    .extensionManager(extensionManager)
                    .assetManager(assetManager)
                    .stateManagerProvider(stateManagerProvider)
                    .processScheduler(processScheduler)
                    .kerberosConfiguration(kerberosConfig)
                    .flowFileEventRepository(flowFileEventRepo)
                    .provenanceRepository(provenanceRepo)
                    .extensionRepository(extensionRepository)
                    .counterRepository(counterRepo)
                    .statusTaskInterval(engineConfiguration.getStatusTaskInterval())
                    .componentEnableTimeout(engineConfiguration.getComponentEnableTimeout())
                    .build();

            final StatelessFlowManager flowManager = new StatelessFlowManager(flowFileEventRepo, parameterContextManager, statelessEngine, () -> true, sslContext, bulletinRepository);
            flowManager.createFlowRegistryClient(InMemoryFlowRegistry.class.getTypeName(), "in-memory-flow-registry", null, Collections.emptySet(), true, true, null);
            ((InMemoryFlowRegistry) flowManager.getFlowRegistryClient("in-memory-flow-registry").getComponent()).addFlowSnapshot(dataflowDefinition.getVersionedExternalFlow());

            final ControllerServiceProvider controllerServiceProvider = new StandardControllerServiceProvider(processScheduler, bulletinRepository, flowManager, extensionManager);

            final ProcessContextFactory rawProcessContextFactory = new StatelessProcessContextFactory(controllerServiceProvider, stateManagerProvider);
            final ProcessContextFactory processContextFactory = new CachingProcessContextFactory(rawProcessContextFactory);
            contentRepo = createContentRepository(engineConfiguration);
            flowFileRepo = new StatelessFlowFileRepository();

            final RepositoryContextFactory repositoryContextFactory = new StatelessRepositoryContextFactory(contentRepo, flowFileRepo, flowFileEventRepo,
                counterRepo, provenanceRepo, stateManagerProvider);
            final StatelessEngineInitializationContext statelessEngineInitializationContext = new StatelessEngineInitializationContext(controllerServiceProvider, flowManager, processContextFactory,
                repositoryContextFactory);

            final String flowName = dataflowDefinition.getFlowName();
            final String threadNameSuffix = flowName == null ? "" : " for dataflow " + flowName;
            final StatelessProcessSchedulerInitializationContext schedulerInitializationContext = new StatelessProcessSchedulerInitializationContext.Builder()
                .componentLifeCycleThreadPool(new FlowEngine(8, "Component Lifecycle" + threadNameSuffix, true))
                .componentMonitoringThreadPool(new FlowEngine(2, "Monitor Processor Lifecycle" + threadNameSuffix, true))
                .frameworkTaskThreadPool(new FlowEngine(2, "Framework Task" + threadNameSuffix, true))
                .processContextFactory(processContextFactory)
                .manageThreadPools(true)
                .build();
            processScheduler.initialize(schedulerInitializationContext);
            statelessEngine.initialize(statelessEngineInitializationContext);

            // Initialize components. This is generally needed because of the interdependencies between the components.
            // There are some circular dependencies that are resolved by passing objects via initialization rather than by providing to the constructors.
            final ResourceClaimManager resourceClaimManager = new StandardResourceClaimManager();
            final EventReporter eventReporter = (severity, category, message) -> {
                final Bulletin bulletin = BulletinFactory.createBulletin(category, severity.name(), message);
                bulletinRepository.addBulletin(bulletin);
            };
            contentRepo.initialize(new ContentRepositoryContext() {
                @Override
                public ResourceClaimManager getResourceClaimManager() {
                    return resourceClaimManager;
                }

                @Override
                public EventReporter getEventReporter() {
                    return eventReporter;
                }
            });
            flowFileRepo.initialize(resourceClaimManager);

            final PythonBridge pythonBridge = new DisabledPythonBridge();
            flowManager.initialize(
                    controllerServiceProvider,
                    pythonBridge,
                    null,
                    null
            );

            // Create flow
            final ProcessGroup rootGroup = flowManager.createProcessGroup("root");
            rootGroup.setName("root");
            flowManager.setRootGroup(rootGroup);

            final StatelessDataflow dataflow = statelessEngine.createFlow(dataflowDefinition);
            final long millis = System.currentTimeMillis() - start;
            logger.info("NiFi Stateless Engine and Dataflow created and initialized in {} millis", millis);

            return dataflow;
        } catch (final Exception e) {
            try {
                if (provenanceRepo != null) {
                    provenanceRepo.close();
                }
            } catch (final IOException ioe) {
                e.addSuppressed(ioe);
            }

            if (contentRepo != null) {
                contentRepo.shutdown();
            }

            if (processScheduler != null) {
                processScheduler.shutdown();
            }

            if (flowFileRepo != null) {
                try {
                    flowFileRepo.close();
                } catch (final IOException ioe) {
                    e.addSuppressed(ioe);
                }
            }

            if (flowFileEventRepo != null) {
                try {
                    flowFileEventRepo.close();
                } catch (final IOException ioe) {
                    e.addSuppressed(ioe);
                }
            }

            throw e;
        }
    }

    private ContentRepository createContentRepository(final StatelessEngineConfiguration engineConfiguration) {
        final Optional<File> contentRepoStorageDirectory = engineConfiguration.getContentRepositoryDirectory();
        if (contentRepoStorageDirectory.isPresent()) {
            return new StatelessFileSystemContentRepository(contentRepoStorageDirectory.get());
        } else {
            return new ByteArrayContentRepository();
        }
    }

    private ExtensionClient createExtensionClient(final ExtensionClientDefinition definition, final SslContextDefinition sslContextDefinition) {
        final String type = definition.getExtensionClientType();
        if (!isValidExtensionClientType(type)) {
            throw new IllegalArgumentException("Invalid Extension Client type: <" + definition.getExtensionClientType() + ">. Currently, the only supported type is <nexus>");
        }

        final SslContextDefinition sslContext = (definition.isUseSslContext() && sslContextDefinition != null) ? sslContextDefinition : null;
        return new NexusExtensionClient(definition.getBaseUrl(), sslContext, definition.getCommsTimeout());
    }

    private boolean isValidExtensionClientType(final String type) {
        return "nexus".equalsIgnoreCase(type.trim());
    }
}
