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
import org.apache.nifi.web.client.api.WebClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Synchronizes assets from the cluster coordinator using the NiFi REST API.
 */
public class StandardAssetSynchronizer implements AssetSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(StandardAssetSynchronizer.class);

    private static final Duration CLUSTER_COORDINATOR_RETRY_DURATION = Duration.ofSeconds(60);
    private static final Duration LIST_ASSETS_RETRY_DURATION = Duration.ofMinutes(5);
    private static final Duration SYNC_ASSET_RETRY_DURATION = Duration.ofSeconds(30);

    private final AssetManager assetManager;
    private final FlowManager flowManager;
    private final ClusterCoordinator clusterCoordinator;
    private final WebClientService webClientService;
    private final NiFiProperties properties;

    public StandardAssetSynchronizer(final FlowController flowController,
                                     final ClusterCoordinator clusterCoordinator,
                                     final WebClientService webClientService,
                                     final NiFiProperties properties) {
        this.assetManager = flowController.getAssetManager();
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
            logger.info("Current node is the cluster coordinator: Asset synchronization disabled");
            return;
        }

        final String coordinatorAddress = coordinatorNodeId.getApiAddress();
        final int coordinatorPort = coordinatorNodeId.getApiPort();
        final AssetsRestApiClient assetsRestApiClient = new AssetsRestApiClient(webClientService, coordinatorAddress, coordinatorPort, properties.isHTTPSConfigured());
        logger.info("Synchronizing assets with cluster coordinator at {}:{}", coordinatorAddress, coordinatorPort);

        final ParameterContextManager parameterContextManager = flowManager.getParameterContextManager();
        final Set<ParameterContext> parameterContexts = parameterContextManager.getParameterContexts();
        logger.info("Found {} parameter contexts for synchronizing assets", parameterContexts.size());

        for (final ParameterContext parameterContext : parameterContexts) {
            try {
                synchronize(assetsRestApiClient, parameterContext);
            } catch (final Exception e) {
                logger.error("Failed to synchronize assets for parameter context [{}]", parameterContext.getIdentifier(), e);
            }
        }
    }

    private void synchronize(final AssetsRestApiClient assetsRestApiClient, final ParameterContext parameterContext) {
        final Map<String, Asset> existingAssets = parameterContext.getParameters().values().stream()
                .map(Parameter::getReferencedAssets)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Asset::getIdentifier, Function.identity()));

        if (existingAssets.isEmpty()) {
            logger.info("Parameter context [{}] does not contain any assets to synchronize", parameterContext.getIdentifier());
            return;
        }

        logger.info("Parameter context [{}] has {} assets on the current node", parameterContext.getIdentifier(), existingAssets.size());

        final AssetsEntity coordinatorAssetsEntity = listAssetsWithRetry(assetsRestApiClient, parameterContext.getIdentifier());
        if (coordinatorAssetsEntity == null) {
            logger.error("Timeout listing assets from cluster coordinator for parameter context [{}]", parameterContext.getIdentifier());
            return;
        }

        final Collection<AssetEntity> coordinatorAssets = coordinatorAssetsEntity.getAssets();
        if (coordinatorAssets == null || coordinatorAssets.isEmpty()) {
            logger.info("Parameter context [{}] did not return any assets from the cluster coordinator", parameterContext.getIdentifier());
            return;
        }

        logger.info("Parameter context [{}] returned {} assets from the cluster coordinator", parameterContext.getIdentifier(), coordinatorAssets.size());

        for (final AssetEntity coordinatorAssetEntity : coordinatorAssets) {
            final AssetDTO coordinatorAsset = coordinatorAssetEntity.getAsset();
            final Asset matchingAsset = existingAssets.get(coordinatorAsset.getId());
            try {
                synchronize(assetsRestApiClient, parameterContext, coordinatorAsset, matchingAsset);
            } catch (final Exception e) {
                logger.error("Failed to synchronize asset [id={},name={}] for parameter context [{}]",
                        coordinatorAsset.getId(), coordinatorAsset.getName(), parameterContext.getIdentifier(), e);
            }
        }
    }

    private void synchronize(final AssetsRestApiClient assetsRestApiClient, final ParameterContext parameterContext, final AssetDTO coordinatorAsset, final Asset matchingAsset) {
        final String paramContextId = parameterContext.getIdentifier();
        final String assetId = coordinatorAsset.getId();
        final String assetName = coordinatorAsset.getName();
        if (matchingAsset == null || !matchingAsset.getFile().exists()) {
            logger.info("Synchronizing missing asset [id={},name={}] for parameter context [{}]", assetId, assetName, paramContextId);
            synchronizeAssetWithRetry(assetsRestApiClient, paramContextId, coordinatorAsset);
        } else {
            final String coordinatorAssetDigest = coordinatorAsset.getDigest();
            final String matchingAssetDigest = matchingAsset.getDigest().orElse(null);
            if (!coordinatorAssetDigest.equals(matchingAssetDigest)) {
                logger.info("Synchronizing asset [id={},name={}] with updated digest [{}] for parameter context [{}]",
                        assetId, assetName, coordinatorAssetDigest, paramContextId);
                synchronizeAssetWithRetry(assetsRestApiClient, paramContextId, coordinatorAsset);
            } else {
                logger.info("Coordinator asset [id={},name={}] found for parameter context [{}]: retrieval not required",  assetId, assetName, paramContextId);
            }
        }
    }

    private AssetsEntity listAssetsWithRetry(final AssetsRestApiClient assetsRestApiClient, final String parameterContextId) {
        final Instant expirationTime = Instant.ofEpochMilli(System.currentTimeMillis() + LIST_ASSETS_RETRY_DURATION.toMillis());
        while (System.currentTimeMillis() < expirationTime.toEpochMilli()) {
            final AssetsEntity assetsEntity = listAssets(assetsRestApiClient, parameterContextId);
            if (assetsEntity != null) {
                return assetsEntity;
            }
            logger.info("Unable to list assets from cluster coordinator for parameter context [{}]: retrying until [{}]", parameterContextId, expirationTime);
            sleep(Duration.ofSeconds(5));
        }
        return null;
    }

    private AssetsEntity listAssets(final AssetsRestApiClient assetsRestApiClient, final String parameterContextId) {
        try {
            return assetsRestApiClient.getAssets(parameterContextId);
        } catch (final NiFiRestApiRetryableException e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            logger.warn("{}, root cause [{}]: retrying", e.getMessage(), rootCause.getMessage());
            return null;
        }
    }

    private void synchronizeAssetWithRetry(final AssetsRestApiClient assetsRestApiClient, final String parameterContextId, final AssetDTO coordinatorAsset) {
        final Instant expirationTime = Instant.ofEpochMilli(System.currentTimeMillis() + SYNC_ASSET_RETRY_DURATION.toMillis());
        while (System.currentTimeMillis() < expirationTime.toEpochMilli()) {
            final Asset syncedAsset = synchronizeAsset(assetsRestApiClient, parameterContextId, coordinatorAsset);
            if (syncedAsset != null) {
                return;
            }
            logger.info("Unable to synchronize asset [id={},name={}] for parameter context [{}]: retrying until [{}]",
                    coordinatorAsset.getId(), coordinatorAsset.getName(), parameterContextId, expirationTime);
            sleep(Duration.ofSeconds(5));
        }
    }

    private Asset synchronizeAsset(final AssetsRestApiClient assetsRestApiClient, final String parameterContextId, final AssetDTO coordinatorAsset) {
        final String assetId = coordinatorAsset.getId();
        final String assetName = coordinatorAsset.getName();
        if (Boolean.TRUE == coordinatorAsset.getMissingContent()) {
            return assetManager.createMissingAsset(parameterContextId, assetName);
        } else {
            try (final InputStream assetInputStream = assetsRestApiClient.getAssetContent(parameterContextId, assetId)) {
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
