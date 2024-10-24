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
import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.NonPurgeableContentRepository;
import org.apache.nifi.controller.repository.StatelessBridgeFlowFileRepository;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.scheduling.StatelessProcessScheduler;
import org.apache.nifi.controller.scheduling.StatelessProcessSchedulerInitializationContext;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.extensions.BundleAvailability;
import org.apache.nifi.extensions.ExtensionRepository;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedExternalFlowMetadata;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.groups.ComponentIdGenerator;
import org.apache.nifi.groups.ComponentScheduler;
import org.apache.nifi.groups.FlowSynchronizationOptions;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.StandardStatelessGroupNode;
import org.apache.nifi.groups.StatelessGroupFactory;
import org.apache.nifi.groups.StatelessGroupNode;
import org.apache.nifi.groups.StatelessGroupNodeFactory;
import org.apache.nifi.groups.StatelessGroupNodeInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.ConnectableLogObserver;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.LoggingContext;
import org.apache.nifi.logging.StandardLoggingContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.registry.flow.mapping.ComponentIdLookup;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.engine.StandardStatelessEngine;
import org.apache.nifi.stateless.engine.StatelessEngine;
import org.apache.nifi.stateless.engine.StatelessEngineInitializationContext;
import org.apache.nifi.stateless.engine.StatelessFlowManager;
import org.apache.nifi.stateless.engine.StatelessProcessContextFactory;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.repository.StatelessFlowFileRepository;
import org.apache.nifi.stateless.repository.StatelessProvenanceRepository;
import org.apache.nifi.stateless.repository.StatelessRepositoryContextFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;

public class StandardStatelessGroupNodeFactory implements StatelessGroupNodeFactory {
    private final FlowController flowController;
    private final SSLContext sslContext;
    private final KerberosConfig kerberosConfig;

    private final AtomicReference<FlowEngine> statelessComponentLifecycleThreadPool = new AtomicReference<>();
    private final AtomicReference<FlowEngine> statelessComponentMonitoringThreadPool = new AtomicReference<>();
    private final AtomicReference<FlowEngine> statelessFrameworkTaskThreadPool = new AtomicReference<>();


    public StandardStatelessGroupNodeFactory(final FlowController flowController, final SSLContext sslContext, final KerberosConfig kerberosConfig) {
        this.flowController = flowController;
        this.sslContext = sslContext;
        this.kerberosConfig = kerberosConfig;
    }

    @Override
    public StatelessGroupNode createStatelessGroupNode(final ProcessGroup group) {
        // Use a StatelessFlowFile Repository because we want all transient data lost upon restart. We only want to keep the data that's
        // completed the flow. However, it's important that when the getNextFlowFileSequence() method is called that we call the method
        // on the underlying FlowFile Repository in order to ensure that the FlowFile Sequence is incremented. It's also important that if
        // any Content Claims have their claimant count decremented to 0 in the flow and the resource claim is no longer in use that we do
        // allow NiFi's FlowFile Repository to handle orchestrating the destruction of the Content Claim. It cannot be done in the Stateless
        // flow because execution may be rolled back.
        final ResourceClaimManager resourceClaimManager = flowController.getResourceClaimManager();
        final BulletinRepository bulletinRepository = flowController.getBulletinRepository();

        final FlowFileRepository underlyingFlowFileRepository = flowController.getRepositoryContextFactory().getFlowFileRepository();
        final StatelessFlowFileRepository flowFileRepository = new StatelessBridgeFlowFileRepository(underlyingFlowFileRepository, resourceClaimManager);

        final StatelessProvenanceRepository statelessProvenanceRepository = new StatelessProvenanceRepository(1_000);
        flowFileRepository.initialize(resourceClaimManager);

        final ContentRepository contentRepository = new NonPurgeableContentRepository(flowController.getRepositoryContextFactory().getContentRepository());
        final RepositoryContextFactory statelessRepoContextFactory = new StatelessRepositoryContextFactory(
            contentRepository,
            flowFileRepository,
            flowController.getFlowFileEventRepository(),
            flowController.getCounterRepository(),
            statelessProvenanceRepository,
            flowController.getStateManagerProvider());

        final FlowMappingOptions flowMappingOptions = new FlowMappingOptions.Builder()
            .componentIdLookup(ComponentIdLookup.USE_COMPONENT_ID)
            .mapControllerServiceReferencesToVersionedId(false)
            .mapInstanceIdentifiers(true)
            .mapPropertyDescriptors(false)
            .mapSensitiveConfiguration(true)
            .sensitiveValueEncryptor(value -> value)    // No need to encrypt, since we won't be persisting the flow
            .stateLookup(VersionedComponentStateLookup.IDENTITY_LOOKUP)
            .mapAssetReferences(true)
            .build();

        final StatelessGroupFactory statelessGroupFactory = new StatelessGroupFactory() {
            @Override
            public VersionedExternalFlow createVersionedExternalFlow(final ProcessGroup group) {
                return StandardStatelessGroupNodeFactory.this.createVersionedExternalFlow(group, flowMappingOptions);
            }

            @Override
            public ProcessGroup createStatelessProcessGroup(final ProcessGroup group, final VersionedExternalFlow versionedExternalFlow) {
                return StandardStatelessGroupNodeFactory.this.createStatelessProcessGroup(group, versionedExternalFlow, statelessRepoContextFactory, flowMappingOptions);
            }
        };

        final LogRepository logRepository = LogRepositoryFactory.getRepository(group.getIdentifier());
        final StatelessGroupNode statelessGroupNode = new StandardStatelessGroupNode.Builder()
            .rootGroup(group)
            .controllerServiceProvider(flowController.getControllerServiceProvider())
            .extensionManager(flowController.getExtensionManager())
            .statelessRepositoryContextFactory(statelessRepoContextFactory)
            .nifiFlowFileRepository(underlyingFlowFileRepository)
            .nifiContentRepository(contentRepository)
            .nifiProvenanceRepository(flowController.getProvenanceRepository())
            .flowFileEventRepository(flowController.getFlowFileEventRepository())
            .stateManagerProvider(flowController.getStateManagerProvider())
            .bulletinRepository(flowController.getBulletinRepository())
            .statelessGroupFactory(statelessGroupFactory)
            .lifecycleStateManager(flowController.getLifecycleStateManager())
            .boredYieldDuration(flowController.getBoredYieldDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
            .build();

        logRepository.removeAllObservers();
        logRepository.addObserver(LogLevel.WARN, new ConnectableLogObserver(bulletinRepository, statelessGroupNode));
        final LoggingContext loggingContext = new StandardLoggingContext(statelessGroupNode);
        final ComponentLog componentLog = new SimpleProcessLogger(statelessGroupNode, logRepository, loggingContext);

        final StatelessGroupNodeInitializationContext initContext = () -> componentLog;
        statelessGroupNode.initialize(initContext);

        return statelessGroupNode;
    }

    private VersionedExternalFlow createVersionedExternalFlow(final ProcessGroup group, final FlowMappingOptions flowMappingOptions) {
        final NiFiRegistryFlowMapper flowMapper = new NiFiRegistryFlowMapper(flowController.getExtensionManager(), flowMappingOptions);
        final InstantiatedVersionedProcessGroup versionedGroup = flowMapper.mapNonVersionedProcessGroup(group, flowController.getControllerServiceProvider());
        final Map<String, VersionedParameterContext> parameterContexts = flowMapper.mapParameterContexts(group, true, new HashMap<>());
        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences =
            Optional.ofNullable(versionedGroup.getExternalControllerServiceReferences()).orElse(Collections.emptyMap());

        final VersionedExternalFlow versionedExternalFlow = new VersionedExternalFlow();
        versionedExternalFlow.setFlowContents(versionedGroup);
        versionedExternalFlow.setExternalControllerServices(externalControllerServiceReferences);
        versionedExternalFlow.setParameterContexts(parameterContexts);
        versionedExternalFlow.setParameterProviders(Collections.emptyMap());

        final VersionedExternalFlowMetadata metadata = new VersionedExternalFlowMetadata();
        metadata.setFlowName(group.getName());
        versionedExternalFlow.setMetadata(metadata);

        return versionedExternalFlow;
    }


    private ProcessGroup createStatelessProcessGroup(final ProcessGroup group, final VersionedExternalFlow versionedExternalFlow, final RepositoryContextFactory statelessRepositoryContextFactory,
                                                     final FlowMappingOptions flowMappingOptions) {

        final FlowFileEventRepository flowFileEventRepository = flowController.getFlowFileEventRepository();
        final BooleanSupplier flowInitializedCheck = flowController::isInitialized;
        final ParameterContextManager parameterContextManager = flowController.getFlowManager().getParameterContextManager();

        final ExtensionRepository extensionRepository = new ExtensionRepository() {
            @Override
            public void initialize() {
            }

            @Override
            public BundleAvailability getBundleAvailability(final BundleCoordinate bundleCoordinate) {
                return BundleAvailability.BUNDLE_AVAILABLE;
            }

            @Override
            public Future<Set<Bundle>> fetch(final Set<BundleCoordinate> bundleCoordinates, final ExecutorService executorService, final int concurrentDownloads) {
                return CompletableFuture.completedFuture(Collections.emptySet());
            }
        };

        final StatelessProcessScheduler statelessScheduler = new StatelessProcessScheduler(flowController.getExtensionManager(), Duration.of(10, ChronoUnit.SECONDS));

        final StatelessStateManagerProvider stateManagerProvider = new StatelessStateManagerProvider();
        final StatelessEngine statelessEngine = new StandardStatelessEngine.Builder()
            .bulletinRepository(flowController.getBulletinRepository())
            .counterRepository(flowController.getCounterRepository())
            .encryptor(flowController.getEncryptor())
            .extensionManager(flowController.getExtensionManager())
            .assetManager(flowController.getAssetManager())
            .extensionRepository(extensionRepository)
            .flowFileEventRepository(flowFileEventRepository)
            .processScheduler(statelessScheduler)
            .provenanceRepository(flowController.getProvenanceRepository())
            .stateManagerProvider(stateManagerProvider)
            .kerberosConfiguration(kerberosConfig)
            .statusTaskInterval(null)
            .build();

        final BulletinRepository bulletinRepository = flowController.getBulletinRepository();

        final StatelessFlowManager statelessFlowManager = new StatelessFlowManager(flowFileEventRepository, parameterContextManager, statelessEngine, flowInitializedCheck,
            sslContext, bulletinRepository);

        final ProcessContextFactory processContextFactory = new StatelessProcessContextFactory(flowController.getControllerServiceProvider(), stateManagerProvider);
        final StatelessEngineInitializationContext engineInitContext = new StatelessEngineInitializationContext(flowController.getControllerServiceProvider(), statelessFlowManager,
            processContextFactory, statelessRepositoryContextFactory);
        statelessEngine.initialize(engineInitContext);

        final FlowEngine componentLifecycleThreadPool = lazyInitializeThreadPool(statelessComponentLifecycleThreadPool,
            () -> new FlowEngine(8, "Stateless Component Lifecycle", true));
        final FlowEngine componentMonitoringThreadPool = lazyInitializeThreadPool(statelessComponentMonitoringThreadPool,
            () -> new FlowEngine(2, "Stateless Component Monitoring", true));
        final FlowEngine frameworkTaskThreadPool = lazyInitializeThreadPool(statelessFrameworkTaskThreadPool,
            () -> new FlowEngine(2, "Stateless Framework Tasks", true));

        final StatelessProcessSchedulerInitializationContext schedulerInitializationContext = new StatelessProcessSchedulerInitializationContext.Builder()
            .processContextFactory(processContextFactory)
            .componentLifeCycleThreadPool(componentLifecycleThreadPool)
            .componentMonitoringThreadPool(componentMonitoringThreadPool)
            .frameworkTaskThreadPool(frameworkTaskThreadPool)
            .manageThreadPools(false)
            .build();
        statelessScheduler.initialize(schedulerInitializationContext);

        final ProcessGroup tempRootGroup = statelessFlowManager.createProcessGroup("root");
        tempRootGroup.setName("root");
        statelessFlowManager.setRootGroup(tempRootGroup);

        final ProcessGroup child = statelessFlowManager.createProcessGroup(group.getIdentifier());
        child.setName(group.getName());
        child.setParent(tempRootGroup);

        final ComponentIdGenerator idGenerator = (proposedId, instanceId, destinationGroupId) -> instanceId;
        final FlowSynchronizationOptions synchronizationOptions = new FlowSynchronizationOptions.Builder()
            .componentComparisonIdLookup(VersionedComponent::getInstanceIdentifier)
            .componentIdGenerator(idGenerator)
            .componentScheduler(ComponentScheduler.NOP_SCHEDULER)
            .componentStopTimeout(Duration.ofSeconds(60))
            .propertyDecryptor(value -> value)
            .topLevelGroupId(group.getIdentifier())
            .updateDescendantVersionedFlows(true)
            .updateGroupSettings(true)
            .updateGroupVersionControlSnapshot(false)
            .updateRpgUrls(true)
            .ignoreLocalModifications(true)
            .build();

        child.synchronizeFlow(versionedExternalFlow, synchronizationOptions, flowMappingOptions);
        child.setParent(group);

        return child;
    }


    private FlowEngine lazyInitializeThreadPool(final AtomicReference<FlowEngine> reference, final Supplier<FlowEngine> factory) {
        FlowEngine threadPool = reference.get();
        if (threadPool == null) {
            threadPool = factory.get();
            final boolean updated = reference.compareAndSet(null, threadPool);
            if (!updated) {
                threadPool.shutdown();
                threadPool = reference.get();
            }
        }

        return threadPool;
    }

}
