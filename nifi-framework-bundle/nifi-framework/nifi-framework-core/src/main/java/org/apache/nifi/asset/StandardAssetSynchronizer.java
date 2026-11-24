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

package org.apache.nifi.asset;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.client.NiFiRestApiResponseException;
import org.apache.nifi.client.NiFiRestApiRetryableException;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.AssetDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.api.WebClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Synchronizes assets from the cluster coordinator using the NiFi REST API. When the local node is the cluster coordinator but did not provide the
 * elected flow, missing assets are synchronized from the nodes that did.
 */
public class StandardAssetSynchronizer implements AssetSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(StandardAssetSynchronizer.class);

    private static final Duration CLUSTER_COORDINATOR_RETRY_DURATION = Duration.ofSeconds(60);
    private static final Duration LIST_ASSETS_RETRY_DURATION = Duration.ofMinutes(5);
    private static final Duration SYNC_ASSET_RETRY_DURATION = Duration.ofSeconds(30);

    private final FlowController flowController;
    private final AssetManager assetManager;
    private final AssetComponentManager assetComponentManager;
    private final FlowManager flowManager;
    private final ClusterCoordinator clusterCoordinator;
    private final WebClientService webClientService;
    private final NiFiProperties properties;

    public StandardAssetSynchronizer(final FlowController flowController,
                                     final ClusterCoordinator clusterCoordinator,
                                     final WebClientService webClientService,
                                     final NiFiProperties properties,
                                     final AssetComponentManager assetComponentManager) {
        this.flowController = flowController;
        this.assetManager = flowController.getAssetManager();
        this.assetComponentManager = assetComponentManager;
        this.flowManager = flowController.getFlowManager();
        this.clusterCoordinator = clusterCoordinator;
        this.webClientService = webClientService;
        this.properties = properties;
    }

    @Override
    public void synchronize() {
        if (clusterCoordinator == null) {
            logger.info("Clustering is not configured: Asset synchronization disabled");
            return;
        }

        // This sync method is called from the method that loads the flow from a connection response, which means there must already be a cluster coordinator to
        // have gotten a response from, but during testing there were cases where calling clusterCoordinator.getElectedActiveCoordinatorNode() was still null, so
        // the helper method here will keep checking for the identifier up to a certain threshold to avoid slight timing issues
        final NodeIdentifier coordinatorNodeId = getElectedActiveCoordinatorNode();
        if (coordinatorNodeId == null) {
            logger.warn("Unable to obtain the node identifier for the cluster coordinator: Asset synchronization disabled");
            return;
        }

        if (clusterCoordinator.isActiveClusterCoordinator()) {
            final List<NodeIdentifier> sourceNodeIds = AssetSynchronizationSources.getCoordinatorSourceNodes(clusterCoordinator, flowController.getNodeId());
            if (sourceNodeIds.isEmpty()) {
                logger.info("Current node is the cluster coordinator: Asset synchronization disabled");
                return;
            }

            logger.info("Current node is the cluster coordinator but did not provide the elected flow: synchronizing missing assets from nodes {}", sourceNodeIds);
            final List<Asset> syncedAssets = synchronizeFromElectedFlowNodes(sourceNodeIds);
            restartReferencingComponents(syncedAssets);
            return;
        }

        final String coordinatorAddress = coordinatorNodeId.getApiAddress();
        final int coordinatorPort = coordinatorNodeId.getApiPort();
        final AssetsRestApiClient assetsRestApiClient = new AssetsRestApiClient(webClientService, coordinatorAddress, coordinatorPort, properties.isHTTPSConfigured());
        logger.info("Synchronizing assets with cluster coordinator at {}:{}", coordinatorAddress, coordinatorPort);

        final ParameterContextManager parameterContextManager = flowManager.getParameterContextManager();
        final Set<ParameterContext> parameterContexts = parameterContextManager.getParameterContexts();
        logger.info("Found {} parameter contexts for synchronizing assets", parameterContexts.size());

        final List<Asset> allSyncedAssets = new ArrayList<>();
        for (final ParameterContext parameterContext : parameterContexts) {
            try {
                final List<Asset> syncedAssets = synchronize(assetsRestApiClient, parameterContext);
                allSyncedAssets.addAll(syncedAssets);
            } catch (final Exception e) {
                logger.error("Failed to synchronize assets for parameter context [{}]", parameterContext.getIdentifier(), e);
            }
        }

        restartReferencingComponents(allSyncedAssets);
    }

    private List<Asset> synchronizeFromElectedFlowNodes(final List<NodeIdentifier> sourceNodeIds) {
        final List<Asset> syncedAssets = new ArrayList<>();
        final Set<ParameterContext> parameterContexts = flowManager.getParameterContextManager().getParameterContexts();

        for (final NodeIdentifier sourceNodeId : sourceNodeIds) {
            final AssetsRestApiClient assetsRestApiClient = new AssetsRestApiClient(webClientService, sourceNodeId.getApiAddress(), sourceNodeId.getApiPort(), properties.isHTTPSConfigured());

            for (final ParameterContext parameterContext : parameterContexts) {
                if (getAssetsMissingContent(parameterContext).isEmpty()) {
                    continue;
                }

                final AssetsEntity sourceAssetsEntity;
                try {
                    sourceAssetsEntity = listAssetsWithRetry(assetsRestApiClient, parameterContext.getIdentifier(), true);
                } catch (final IllegalStateException e) {
                    logger.error("Failed to list assets from node [{}] for parameter context [{}]", sourceNodeId, parameterContext.getIdentifier(), e);
                    continue;
                }

                if (sourceAssetsEntity == null) {
                    logger.error("Timeout listing assets from node [{}] for parameter context [{}]: skipping node", sourceNodeId, parameterContext.getIdentifier());
                    break;
                }

                try {
                    syncedAssets.addAll(synchronizeMissingAssets(assetsRestApiClient, sourceNodeId, parameterContext, sourceAssetsEntity));
                } catch (final Exception e) {
                    logger.error("Failed to synchronize missing assets for parameter context [{}] from node [{}]", parameterContext.getIdentifier(), sourceNodeId, e);
                }
            }
        }

        for (final ParameterContext parameterContext : parameterContexts) {
            final List<Asset> assetsMissingContent = getAssetsMissingContent(parameterContext);
            if (assetsMissingContent.isEmpty()) {
                continue;
            }

            final List<String> missingAssetNames = new ArrayList<>();
            for (final Asset asset : assetsMissingContent) {
                missingAssetNames.add(asset.getName());
            }

            logger.warn("Parameter context [{}] still has {} assets without content after synchronizing from nodes {}: {}",
                    parameterContext.getIdentifier(), assetsMissingContent.size(), sourceNodeIds, missingAssetNames);
        }

        return syncedAssets;
    }

    private void restartReferencingComponents(final List<Asset> syncedAssets) {
        if (!syncedAssets.isEmpty()) {
            try {
                logger.info("Restarting components referencing {} synchronized assets", syncedAssets.size());
                assetComponentManager.restartReferencingComponents(syncedAssets);
            } catch (final Exception e) {
                logger.error("Failed to restart components referencing synchronized assets", e);
            }
        }
    }

    private List<Asset> getAssetsMissingContent(final ParameterContext parameterContext) {
        final Map<String, Asset> assetsMissingContent = new LinkedHashMap<>();
        for (final Parameter parameter : parameterContext.getParameters().values()) {
            for (final Asset asset : parameter.getReferencedAssets()) {
                if (!asset.getFile().exists()) {
                    assetsMissingContent.putIfAbsent(asset.getIdentifier(), asset);
                }
            }
        }

        return new ArrayList<>(assetsMissingContent.values());
    }

    private List<Asset> synchronizeMissingAssets(final AssetsRestApiClient assetsRestApiClient, final NodeIdentifier sourceNodeId, final ParameterContext parameterContext,
                                                 final AssetsEntity sourceAssetsEntity) {
        final List<Asset> syncedAssets = new ArrayList<>();
        final Collection<AssetEntity> sourceAssets = sourceAssetsEntity.getAssets();
        if (sourceAssets == null) {
            return syncedAssets;
        }

        final String paramContextId = parameterContext.getIdentifier();
        final Map<String, Asset> assetsMissingContent = new LinkedHashMap<>();
        for (final Asset asset : getAssetsMissingContent(parameterContext)) {
            assetsMissingContent.put(asset.getIdentifier(), asset);
        }

        for (final AssetEntity sourceAssetEntity : sourceAssets) {
            final AssetDTO sourceAsset = sourceAssetEntity.getAsset();
            if (!assetsMissingContent.containsKey(sourceAsset.getId())) {
                continue;
            }

            if (Boolean.TRUE.equals(sourceAsset.getMissingContent())) {
                logger.info("Node [{}] is also missing content for asset [id={},name={}] in parameter context [{}]", sourceNodeId, sourceAsset.getId(), sourceAsset.getName(), paramContextId);
                continue;
            }

            try {
                final Asset syncedAsset = synchronizeAssetWithRetry(assetsRestApiClient, paramContextId, sourceAsset);
                if (syncedAsset != null) {
                    logger.info("Successfully synchronized asset [id={},name={}] for parameter context [{}]", syncedAsset.getIdentifier(), syncedAsset.getName(), paramContextId);
                    syncedAssets.add(syncedAsset);
                }
            } catch (final Exception e) {
                logger.error("Failed to synchronize asset [id={},name={}] for parameter context [{}] from node [{}]", sourceAsset.getId(), sourceAsset.getName(), paramContextId, sourceNodeId, e);
            }
        }

        return syncedAssets;
    }

    private List<Asset> synchronize(final AssetsRestApiClient assetsRestApiClient, final ParameterContext parameterContext) {
        final List<Asset> syncedAssets = new ArrayList<>();

        final Map<String, Asset> existingAssets = parameterContext.getParameters().values().stream()
                .map(Parameter::getReferencedAssets)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Asset::getIdentifier, Function.identity(), (existing, replacement) -> existing));

        if (existingAssets.isEmpty()) {
            logger.info("Parameter context [{}] does not contain any assets to synchronize", parameterContext.getIdentifier());
            return syncedAssets;
        }

        logger.info("Parameter context [{}] has {} assets on the current node", parameterContext.getIdentifier(), existingAssets.size());

        final AssetsEntity coordinatorAssetsEntity = listAssetsWithRetry(assetsRestApiClient, parameterContext.getIdentifier(), false);
        if (coordinatorAssetsEntity == null) {
            logger.error("Timeout listing assets from cluster coordinator for parameter context [{}]", parameterContext.getIdentifier());
            return syncedAssets;
        }

        final Collection<AssetEntity> coordinatorAssets = coordinatorAssetsEntity.getAssets();
        if (coordinatorAssets == null || coordinatorAssets.isEmpty()) {
            logger.info("Parameter context [{}] did not return any assets from the cluster coordinator", parameterContext.getIdentifier());
            return syncedAssets;
        }

        logger.info("Parameter context [{}] returned {} assets from the cluster coordinator", parameterContext.getIdentifier(), coordinatorAssets.size());

        for (final AssetEntity coordinatorAssetEntity : coordinatorAssets) {
            final AssetDTO coordinatorAsset = coordinatorAssetEntity.getAsset();
            final Asset matchingAsset = existingAssets.get(coordinatorAsset.getId());
            try {
                final Asset syncedAsset = synchronize(assetsRestApiClient, parameterContext, coordinatorAsset, matchingAsset);
                if (syncedAsset != null) {
                    logger.info("Successfully synchronized asset [id={},name={}] for parameter context [{}]",
                            syncedAsset.getIdentifier(), syncedAsset.getName(), parameterContext.getIdentifier());
                    syncedAssets.add(syncedAsset);
                }
            } catch (final Exception e) {
                logger.error("Failed to synchronize asset [id={},name={}] for parameter context [{}]",
                        coordinatorAsset.getId(), coordinatorAsset.getName(), parameterContext.getIdentifier(), e);
            }
        }

        return syncedAssets;
    }

    private Asset synchronize(final AssetsRestApiClient assetsRestApiClient, final ParameterContext parameterContext, final AssetDTO coordinatorAsset, final Asset matchingAsset) {
        final String paramContextId = parameterContext.getIdentifier();
        final String assetId = coordinatorAsset.getId();
        final String assetName = coordinatorAsset.getName();
        if (matchingAsset == null || !matchingAsset.getFile().exists()) {
            logger.info("Synchronizing missing asset [id={},name={}] for parameter context [{}]", assetId, assetName, paramContextId);
            return synchronizeAssetWithRetry(assetsRestApiClient, paramContextId, coordinatorAsset);
        } else {
            if (Boolean.TRUE.equals(coordinatorAsset.getMissingContent())) {
                logger.warn("Cluster coordinator is missing content for asset [id={},name={}] in parameter context [{}]: keeping local asset file", assetId, assetName, paramContextId);
                return null;
            }

            final String coordinatorAssetDigest = coordinatorAsset.getDigest();
            final String matchingAssetDigest = matchingAsset.getDigest().orElse(null);
            if (!coordinatorAssetDigest.equals(matchingAssetDigest)) {
                logger.info("Synchronizing asset [id={},name={}] with updated digest [{}] for parameter context [{}]",
                        assetId, assetName, coordinatorAssetDigest, paramContextId);
                return synchronizeAssetWithRetry(assetsRestApiClient, paramContextId, coordinatorAsset);
            } else {
                logger.info("Coordinator asset [id={},name={}] found for parameter context [{}]: retrieval not required",  assetId, assetName, paramContextId);
                return null;
            }
        }
    }

    private AssetsEntity listAssetsWithRetry(final AssetsRestApiClient assetsRestApiClient, final String parameterContextId, final boolean retryNotFound) {
        final Instant expirationTime = Instant.ofEpochMilli(System.currentTimeMillis() + LIST_ASSETS_RETRY_DURATION.toMillis());
        while (System.currentTimeMillis() < expirationTime.toEpochMilli()) {
            final AssetsEntity assetsEntity = listAssets(assetsRestApiClient, parameterContextId, retryNotFound);
            if (assetsEntity != null) {
                return assetsEntity;
            }
            logger.info("Unable to list assets for parameter context [{}]: retrying until [{}]", parameterContextId, expirationTime);
            sleep(Duration.ofSeconds(5));
        }
        return null;
    }

    private AssetsEntity listAssets(final AssetsRestApiClient assetsRestApiClient, final String parameterContextId, final boolean retryNotFound) {
        try {
            return assetsRestApiClient.getParameterContextAssets(parameterContextId);
        } catch (final NiFiRestApiRetryableException e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            logger.warn("{}, root cause [{}]: retrying", e.getMessage(), rootCause.getMessage());
            return null;
        } catch (final NiFiRestApiResponseException e) {
            if (!retryNotFound || e.getStatusCode() != HttpResponseStatus.NOT_FOUND.getCode()) {
                throw e;
            }

            logger.warn("{}: retrying", e.getMessage());
            return null;
        }
    }

    private Asset synchronizeAssetWithRetry(final AssetsRestApiClient assetsRestApiClient, final String parameterContextId, final AssetDTO coordinatorAsset) {
        final Instant expirationTime = Instant.ofEpochMilli(System.currentTimeMillis() + SYNC_ASSET_RETRY_DURATION.toMillis());
        while (System.currentTimeMillis() < expirationTime.toEpochMilli()) {
            final Asset syncedAsset = synchronizeAsset(assetsRestApiClient, parameterContextId, coordinatorAsset);
            if (syncedAsset != null) {
                logger.info("Synchronizing asset complete [id={},name={},file={},digest={}]",
                        syncedAsset.getIdentifier(), syncedAsset.getName(), syncedAsset.getFile().getAbsolutePath(), syncedAsset.getDigest().orElse("[Missing]"));
                return syncedAsset;
            }
            logger.info("Unable to synchronize asset [id={},name={}] for parameter context [{}]: retrying until [{}]",
                    coordinatorAsset.getId(), coordinatorAsset.getName(), parameterContextId, expirationTime);
            sleep(Duration.ofSeconds(5));
        }
        return null;
    }

    private Asset synchronizeAsset(final AssetsRestApiClient assetsRestApiClient, final String parameterContextId, final AssetDTO coordinatorAsset) {
        final String assetId = coordinatorAsset.getId();
        final String assetName = coordinatorAsset.getName();
        if (Boolean.TRUE == coordinatorAsset.getMissingContent()) {
            return assetManager.createMissingAsset(parameterContextId, assetName);
        } else {
            try (final InputStream assetInputStream = assetsRestApiClient.getParameterContextAssetContent(parameterContextId, assetId)) {
                return assetManager.createAsset(parameterContextId, assetName, assetInputStream);
            } catch (final NiFiRestApiRetryableException e) {
                final Throwable rootCause = ExceptionUtils.getRootCause(e);
                logger.warn("{}, root cause [{}]: retrying", e.getMessage(), rootCause.getMessage());
                return null;
            } catch (final IOException e) {
                final Throwable rootCause = ExceptionUtils.getRootCause(e);
                logger.warn("Asset Manager failed to create asset [id={},name={}], root cause [{}]: retrying", assetId, assetId, rootCause.getMessage());
                return null;
            }
        }
    }

    private NodeIdentifier getElectedActiveCoordinatorNode() {
        final Instant expirationTime = Instant.ofEpochMilli(System.currentTimeMillis() + CLUSTER_COORDINATOR_RETRY_DURATION.toMillis());
        while (System.currentTimeMillis() < expirationTime.toEpochMilli()) {
            final NodeIdentifier coordinatorNodeId = clusterCoordinator.getElectedActiveCoordinatorNode();
            if (coordinatorNodeId != null) {
                return coordinatorNodeId;
            }
            logger.info("Node identifier for the active cluster coordinator is not known yet: retrying until [{}]", expirationTime);
            sleep(Duration.ofSeconds(2));
        }
        return null;
    }

    private void sleep(final Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
