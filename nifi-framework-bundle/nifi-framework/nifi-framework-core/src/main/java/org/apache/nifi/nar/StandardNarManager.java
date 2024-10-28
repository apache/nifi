/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.client.NiFiRestApiRetryableException;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StandardNarManager implements NarManager, InitializingBean, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(StandardNarManager.class);

    private static final Duration MAX_WAIT_TIME_FOR_CLUSTER_COORDINATOR = Duration.ofSeconds(60);
    private static final Duration MAX_WAIT_TIME_FOR_NARS = Duration.ofMinutes(5);

    private final ClusterCoordinator clusterCoordinator;
    private final ExtensionManager extensionManager;
    private final ControllerServiceProvider controllerServiceProvider;
    private final NarPersistenceProvider persistenceProvider;
    private final NarComponentManager narComponentManager;
    private final NarLoader narLoader;
    private final WebClientService webClientService;
    private final NiFiProperties properties;

    private final Map<String, NarNode> narNodesById = new ConcurrentHashMap<>();
    private final Map<String, Future<?>> installFuturesById = new ConcurrentHashMap<>();
    private final ExecutorService installExecutorService;
    private final ExecutorService deleteExecutorService;

    public StandardNarManager(final FlowController flowController,
                              final ClusterCoordinator clusterCoordinator,
                              final NarPersistenceProvider narPersistenceProvider,
                              final NarComponentManager narComponentManager,
                              final NarLoader narLoader,
                              final WebClientService webClientService,
                              final NiFiProperties properties) {
        this.clusterCoordinator = clusterCoordinator;
        this.extensionManager = flowController.getExtensionManager();
        this.controllerServiceProvider = flowController.getControllerServiceProvider();
        this.persistenceProvider = narPersistenceProvider;
        this.narComponentManager = narComponentManager;
        this.narLoader = narLoader;
        this.webClientService = webClientService;
        this.properties = properties;
        this.installExecutorService = Executors.newSingleThreadExecutor();
        this.deleteExecutorService = Executors.newSingleThreadExecutor();
    }

    /* This serves two purposes...
     * 1. Any previously stored NARs need to have their extensions loaded and made available for use during start up since they won't be in any of the standard NAR directories
     * 2. NarLoader keeps track of NARs that were missing dependencies to consider them on future loads, so this restores state that may have been lost on a restart
     */
    @Override
    public void afterPropertiesSet() throws IOException {
        final Collection<NarPersistenceInfo> narInfos = getExistingNarInfos();
        restoreNarNodes(narInfos);
    }

    @Override
    public void close() {
        shutdownExecutor(installExecutorService, "Forcing shutdown of NAR Manager Upload Executor Service");
        shutdownExecutor(deleteExecutorService, "Forcing shutdown of NAR Manager Delete Executor Service");
    }

    private void shutdownExecutor(final ExecutorService executorService, final String interruptedMessage) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5000, MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ignore) {
            logger.info(interruptedMessage);
            executorService.shutdownNow();
        }
    }

    @Override
    public NarNode installNar(final NarInstallRequest installRequest) throws IOException {
        return installNar(installRequest, true);
    }

    @Override
    public void completeInstall(final String identifier) {
        logger.info("Completed install for NAR [{}]", identifier);
        installFuturesById.remove(identifier);
    }

    @Override
    public synchronized void updateState(final BundleCoordinate coordinate, final NarState narState) {
        final NarNode narNode = narNodesById.values().stream()
                .filter(n -> n.getManifest().getCoordinate().equals(coordinate))
                .findFirst()
                .orElseThrow(() -> new NarNotFoundException(coordinate));
        narNode.setState(narState);
    }

    @Override
    public void updateFailed(final BundleCoordinate coordinate, final Throwable failure) {
        final NarNode narNode = narNodesById.values().stream()
                .filter(n -> n.getManifest().getCoordinate().equals(coordinate))
                .findFirst()
                .orElseThrow(() -> new NarNotFoundException(coordinate));
        narNode.setFailure(failure);
    }

    @Override
    public Collection<NarNode> getNars() {
        return new ArrayList<>(narNodesById.values());
    }

    @Override
    public Optional<NarNode> getNar(final String identifier) {
        final NarNode narNode = narNodesById.get(identifier);
        return Optional.ofNullable(narNode);
    }

    @Override
    public synchronized void verifyDeleteNar(final String identifier, final boolean forceDelete) {
        final NarNode narNode = getNarNodeOrThrowNotFound(identifier);

        // Always allow deletion of a NAR that is not fully installed
        if (narNode.getState() != NarState.INSTALLED) {
            return;
        }

        final BundleCoordinate coordinate = narNode.getManifest().getCoordinate();
        final Set<Bundle> bundlesWithMatchingDependency = extensionManager.getDependentBundles(coordinate);
        if (!bundlesWithMatchingDependency.isEmpty()) {
            throw new IllegalStateException("Unable to delete NAR [%s] because it is a dependency of other NARs".formatted(coordinate));
        }

        final Set<ExtensionDefinition> extensionDefinitions = new HashSet<>(extensionManager.getTypes(coordinate));
        extensionDefinitions.addAll(extensionManager.getPythonExtensions(coordinate));

        if (!forceDelete && narComponentManager.componentsExist(coordinate, extensionDefinitions)) {
            throw new IllegalStateException("Unable to delete NAR [%s] because components are instantiated from this NAR".formatted(coordinate));
        }
    }

    @Override
    public synchronized NarNode deleteNar(final String identifier) throws IOException {
        final NarNode narNode = getNarNodeOrThrowNotFound(identifier);
        final BundleCoordinate coordinate = narNode.getManifest().getCoordinate();
        logger.info("Deleting NAR with id [{}] and coordinate [{}]", identifier, coordinate);

        final Future<?> installTask = installFuturesById.remove(identifier);
        if (installTask != null) {
            installTask.cancel(true);
        }

        final Set<ExtensionDefinition> extensionDefinitions = new HashSet<>(extensionManager.getTypes(coordinate));
        extensionDefinitions.addAll(extensionManager.getPythonExtensions(coordinate));

        final Bundle existingBundle = extensionManager.getBundle(coordinate);
        if (existingBundle != null) {
            narLoader.unload(existingBundle);
        }

        deleteExecutorService.submit(() -> {
            final StandardStoppedComponents stoppedComponents = new StandardStoppedComponents(controllerServiceProvider);
            narComponentManager.unloadComponents(coordinate, extensionDefinitions, stoppedComponents);
            logger.info("Completed unloading components for deleting NAR with id [{}] and coordinate [{}]", identifier, coordinate);
        });

        persistenceProvider.deleteNar(coordinate);
        narNodesById.remove(identifier);
        return narNode;
    }

    @Override
    public synchronized InputStream readNar(final String identifier) {
        final NarNode narNode = getNarNodeOrThrowNotFound(identifier);
        final BundleCoordinate coordinate = narNode.getManifest().getCoordinate();
        try {
            return persistenceProvider.readNar(coordinate);
        } catch (final FileNotFoundException e) {
            throw new NarNotFoundException(coordinate);
        }
    }

    @Override
    public synchronized void syncWithClusterCoordinator() {
        if (clusterCoordinator == null) {
            logger.info("Clustering is not configured: NAR synchronization disabled");
            return;
        }

        // This sync method is called from the method that loads the flow from a connection response, which means there must already be a cluster coordinator to
        // have gotten a response from, but during testing there were cases where calling clusterCoordinator.getElectedActiveCoordinatorNode() was still null, so
        // the helper method here will keep checking for the identifier up to a certain threshold to avoid slight timing issues
        final NodeIdentifier coordinatorNodeId = getElectedActiveCoordinatorNode();
        if (coordinatorNodeId == null) {
            logger.warn("Unable to obtain the node identifier for the cluster coordinator: NAR synchronization disabled");
            return;
        }

        logger.debug("NAR synchronization determined cluster coordinator [{}]", coordinatorNodeId);
        if (clusterCoordinator.isActiveClusterCoordinator()) {
            logger.info("Current node is the cluster coordinator: NAR synchronization disabled");
            return;
        }

        logger.info("Synchronizing NARs with cluster coordinator");
        final String coordinatorAddress = coordinatorNodeId.getApiAddress();
        final int coordinatorPort = coordinatorNodeId.getApiPort();
        final NarRestApiClient narRestApiClient = new NarRestApiClient(webClientService, coordinatorAddress, coordinatorPort, properties.isHTTPSConfigured());

        final int localNarCountBeforeSync = narNodesById.size();
        try {
            // This node may try to retrieve the summaries from the coordinator while the coordinator still hasn't finished initializing its flow controller and
            // the response will be a 409, so the helper method here will catch any retryable exceptions and retry the request up to a configured threshold
            final NarSummariesEntity narSummaries = getNarSummariesFromCoordinator(narRestApiClient);
            if (narSummaries == null) {
                logger.error("Timeout listing NARs from cluster coordinator [{}]", coordinatorAddress);
                return;
            }

            logger.info("Cluster coordinator returned {} NAR summaries", narSummaries.getNarSummaries().size());

            for (final NarSummaryEntity narSummaryEntity : narSummaries.getNarSummaries()) {
                final NarSummaryDTO narSummaryDTO = narSummaryEntity.getNarSummary();
                final String coordinatorNarId = narSummaryDTO.getIdentifier();
                final String coordinatorNarDigest = narSummaryDTO.getDigest();
                final NarNode matchingNar = narNodesById.get(coordinatorNarId);
                if (matchingNar == null) {
                    logger.info("Downloading NAR [{}] from cluster coordinator [{}]", coordinatorNarId, coordinatorAddress);
                    downloadNar(narRestApiClient, narSummaryDTO);
                } else if (!coordinatorNarDigest.equals(matchingNar.getNarFileDigest())) {
                    logger.info("Downloading NAR [{}] from cluster coordinator [{}] with updated digest [{}]", coordinatorNarId, coordinatorAddress, matchingNar.getNarFileDigest());
                    downloadNar(narRestApiClient, narSummaryDTO);
                } else {
                    logger.info("Coordinator NAR [{}] found: download not required", coordinatorNarId);
                }
            }
        } catch (final Exception e) {
            // if the current node has existing NARs and the sync fails then we throw an exception to fail start up, otherwise we don't know if the digests of the local NARs
            // match with the coordinator which could result in joining the cluster and running slightly different code on one node
            // if the current node has no existing NARs then we can let the node proceed and attempt to join the cluster because maybe the cluster coordinator had no NARs anyway,
            // and if it did then flow synchronization will fail after this because the local flow will have ghosted components that are not ghosted in the cluster
            if (localNarCountBeforeSync > 0) {
                throw new RuntimeException("Failed to synchronize NARs from cluster coordinator [%s]".formatted(coordinatorAddress), e);
            } else {
                logger.error("Failed to synchronize NARs from cluster coordinator [{}] no local NARs found", coordinatorAddress, e);
            }
        }
    }

    private void downloadNar(final NarRestApiClient narRestApiClient, final NarSummaryDTO narSummary) throws IOException {
        try (final InputStream coordinatorNarInputStream = narRestApiClient.downloadNar(narSummary.getIdentifier())) {
            final NarInstallRequest installRequest = NarInstallRequest.builder()
                    .source(NarSource.valueOf(narSummary.getSourceType()))
                    .sourceIdentifier(narSummary.getSourceIdentifier())
                    .inputStream(coordinatorNarInputStream)
                    .build();
            installNar(installRequest, false);
        }
    }

    private NarSummariesEntity getNarSummariesFromCoordinator(final NarRestApiClient narRestApiClient) {
        final Instant waitUntilInstant = Instant.ofEpochMilli(System.currentTimeMillis() + MAX_WAIT_TIME_FOR_NARS.toMillis());
        while (System.currentTimeMillis() < waitUntilInstant.toEpochMilli()) {
            final NarSummariesEntity narSummaries = listNarSummaries(narRestApiClient);
            if (narSummaries != null) {
                return narSummaries;
            }
            logger.info("Unable to retrieve NAR summaries from cluster coordinator: retrying until [{}]", waitUntilInstant);
            sleep(Duration.ofSeconds(5));
        }
        return null;
    }

    private NarSummariesEntity listNarSummaries(final NarRestApiClient narRestApiClient) {
        try {
            return narRestApiClient.listNarSummaries();
        } catch (final NiFiRestApiRetryableException e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            logger.warn("{}, root cause [{}]: retrying", e.getMessage(), rootCause.getMessage());
            return null;
        }
    }

    private NodeIdentifier getElectedActiveCoordinatorNode() {
        final Instant waitUntilInstant = Instant.ofEpochMilli(System.currentTimeMillis() + MAX_WAIT_TIME_FOR_CLUSTER_COORDINATOR.toMillis());
        while (System.currentTimeMillis() < waitUntilInstant.toEpochMilli()) {
            final NodeIdentifier coordinatorNodeId = clusterCoordinator.getElectedActiveCoordinatorNode();
            if (coordinatorNodeId != null) {
                return coordinatorNodeId;
            }
            logger.info("Node identifier for the active cluster coordinator is not known yet: retrying until [{}]", waitUntilInstant);
            sleep(Duration.ofSeconds(2));
        }
        return null;
    }

    private void sleep(final Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Collection<NarPersistenceInfo> getExistingNarInfos() throws IOException {
        final Collection<NarPersistenceInfo> narInfos = persistenceProvider.getAllNarInfo();
        logger.info("Loading stored NAR files [{}]", narInfos.size());
        if (narInfos.isEmpty()) {
            return narInfos;
        }

        logger.debug("NAR Infos before reordering: {}", narInfos);

        // If any NARs being loaded are parents of other NARs being loaded, we need to ensure parents load before children, otherwise a child NAR may
        // select a compatible parent NAR from the other NARs provided by NiFi, rather than selecting the parent NAR from within the NAR Manager
        final int numNarInfos = narInfos.size();
        final Stack<NarPersistenceInfo> narHierarchy = new Stack<>();
        createNarHierarchy(narInfos, narHierarchy);

        // Create a new list and add the layers of the Stack from top to bottom which gets parents ahead of children
        final List<NarPersistenceInfo> orderedNarInfos = new ArrayList<>(numNarInfos);
        while (!narHierarchy.isEmpty()) {
            final NarPersistenceInfo narInfoToAdd = narHierarchy.pop();
            orderedNarInfos.add(narInfoToAdd);
        }

        logger.debug("NAR Infos after reordering: {}", orderedNarInfos);
        return orderedNarInfos;
    }

    private void createNarHierarchy(final Collection<NarPersistenceInfo> narInfos, final Stack<NarPersistenceInfo> narHierarchy) {
        if (narInfos == null || narInfos.isEmpty()) {
            return;
        }

        // Create lookup from coordinate to NAR info
        final Map<BundleCoordinate, NarPersistenceInfo> narInfosByBundleCoordinate = new HashMap<>();
        narInfos.forEach(narInfo -> narInfosByBundleCoordinate.put(narInfo.getNarProperties().getCoordinate(), narInfo));

        // Determine all the NARs that are parents of other NARs
        final Set<NarPersistenceInfo> parentNarInfos = new HashSet<>();
        for (final NarPersistenceInfo narInfo : narInfos) {
            narInfo.getNarProperties().getDependencyCoordinate().ifPresent(parentCoordinate -> {
                final NarPersistenceInfo parentNarInfo = narInfosByBundleCoordinate.get(parentCoordinate);
                if (parentNarInfo != null) {
                    parentNarInfos.add(parentNarInfo);
                }
            });
        }

        // Remove the parent NARs from the current collection, and recurse on the parents
        narInfos.removeAll(parentNarInfos);

        // Add the remaining non-parent NARs to the hierarchy
        narInfos.forEach(narHierarchy::push);

        // Recurse on the parents NARs to further re-order them
        createNarHierarchy(parentNarInfos, narHierarchy);
    }

    private void restoreNarNodes(final Collection<NarPersistenceInfo> narInfos) {
        for (final NarPersistenceInfo narInfo : narInfos) {
            try {
                final File narFile = narInfo.getNarFile();
                final NarManifest manifest = NarManifest.fromNarFile(narFile);
                final NarNode narNode = installNar(narInfo, manifest, false);
                logger.debug("Restored NAR [{}] with state [{}] and identifier [{}]", narNode.getManifest().getCoordinate(), narNode.getState(), narNode.getIdentifier());
            } catch (final Exception e) {
                logger.warn("Failed to restore NAR for [{}]", narInfo.getNarFile().getAbsolutePath(), e);
            }
        }
    }

    private NarNode installNar(final NarInstallRequest installRequest, final boolean async) throws IOException {
        final InputStream inputStream = installRequest.getInputStream();
        final File tempNarFile = persistenceProvider.createTempFile(inputStream);
        try {
            return installNar(installRequest, tempNarFile, async);
        } finally {
            if (tempNarFile.exists() && !tempNarFile.delete()) {
                logger.warn("Failed to delete temp NAR file at [{}], file must be cleaned up manually", tempNarFile.getAbsolutePath());
            }
        }
    }

    // The outer install method is not synchronized since copying the stream to the temp file make take a long time, so we
    // synchronize here after already having the temp file to ensure only one request is checked and submitted for installing
    private synchronized NarNode installNar(final NarInstallRequest installRequest, final File tempNarFile, final boolean async) throws IOException {
        final NarManifest manifest = getNarManifest(tempNarFile);
        final BundleCoordinate coordinate = manifest.getCoordinate();

        final Bundle existingBundle = extensionManager.getBundle(coordinate);
        if (existingBundle != null && !persistenceProvider.exists(coordinate)) {
            throw new IllegalStateException("A NAR is already registered with the same coordinate [%s], and can not be replaced because it is not part of the NAR Manager".formatted(coordinate));
        }

        final NarPersistenceContext persistenceContext = NarPersistenceContext.builder()
                .manifest(manifest)
                .source(installRequest.getSource())
                .sourceIdentifier(installRequest.getSourceIdentifier())
                .clusterCoordinator(clusterCoordinator != null && clusterCoordinator.isActiveClusterCoordinator())
                .build();

        final NarPersistenceInfo narPersistenceInfo = persistenceProvider.saveNar(persistenceContext, tempNarFile);
        return installNar(narPersistenceInfo, manifest, async);
    }

    private NarNode installNar(final NarPersistenceInfo narPersistenceInfo, final NarManifest manifest, final boolean async) throws IOException {
        final File narFile = narPersistenceInfo.getNarFile();
        final BundleCoordinate coordinate = narPersistenceInfo.getNarProperties().getCoordinate();
        final String identifier = createIdentifier(coordinate);
        final String narDigest = computeNarDigest(narFile);

        final NarNode narNode = NarNode.builder()
                .identifier(identifier)
                .narFile(narFile)
                .narFileDigest(narDigest)
                .manifest(manifest)
                .source(NarSource.valueOf(narPersistenceInfo.getNarProperties().getSourceType()))
                .sourceIdentifier(narPersistenceInfo.getNarProperties().getSourceId())
                .state(NarState.WAITING_TO_INSTALL)
                .build();
        narNodesById.put(identifier, narNode);

        final NarInstallTask installTask = createInstallTask(narNode);
        if (async) {
            logger.debug("Submitting install task for NAR with id [{}] and coordinate [{}]", identifier, coordinate);
            final Future<?> installTaskFuture = installExecutorService.submit(installTask);
            installFuturesById.put(identifier, installTaskFuture);
        } else {
            logger.debug("Synchronously installing NAR with id [{}] and coordinate [{}]", identifier, coordinate);
            installTask.run();
        }
        return narNode;
    }

    private NarNode getNarNodeOrThrowNotFound(final String identifier) {
        final NarNode narNode = narNodesById.get(identifier);
        if (narNode == null) {
            throw new ResourceNotFoundException("A NAR does not exist with the identifier %s".formatted(identifier));
        }
        return narNode;
    }

    private void deleteFileQuietly(final File tempNarFile) {
        if (!tempNarFile.delete()) {
            logger.warn("Failed to delete temp NAR file [{}], this file should be cleaned up manually", tempNarFile.getAbsolutePath());
        }
    }

    private NarManifest getNarManifest(final File tempNarFile) throws IOException {
        try {
            return NarManifest.fromNarFile(tempNarFile);
        } catch (final RuntimeException | IOException e) {
            deleteFileQuietly(tempNarFile);
            throw e;
        }
    }

    private String createIdentifier(final BundleCoordinate coordinate) {
        return UUID.nameUUIDFromBytes(coordinate.getCoordinate().getBytes(StandardCharsets.UTF_8)).toString();
    }

    private NarInstallTask createInstallTask(final NarNode narNode) {
        return NarInstallTask.builder()
                .narNode(narNode)
                .narManager(this)
                .narComponentManager(narComponentManager)
                .narLoader(narLoader)
                .extensionManager(extensionManager)
                .controllerServiceProvider(controllerServiceProvider)
                .build();
    }

    private String computeNarDigest(final File narFile) throws IOException {
        return HexFormat.of().formatHex(FileDigestUtils.getDigest(narFile));
    }

}
