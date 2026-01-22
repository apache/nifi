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
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Synchronizes connector assets from the cluster coordinator using the NiFi REST API.
 */
public class StandardConnectorAssetSynchronizer implements AssetSynchronizer {

    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorAssetSynchronizer.class);

    private static final Duration CLUSTER_COORDINATOR_RETRY_DURATION = Duration.ofSeconds(60);
    private static final Duration LIST_ASSETS_RETRY_DURATION = Duration.ofMinutes(5);
    private static final Duration SYNC_ASSET_RETRY_DURATION = Duration.ofSeconds(30);

    private final AssetManager assetManager;
    private final FlowManager flowManager;
    private final ConnectorRepository connectorRepository;
    private final ClusterCoordinator clusterCoordinator;
    private final WebClientService webClientService;
    private final NiFiProperties properties;

    public StandardConnectorAssetSynchronizer(final FlowController flowController,
                                              final ClusterCoordinator clusterCoordinator,
                                              final WebClientService webClientService,
                                              final NiFiProperties properties) {
        this.assetManager = flowController.getConnectorAssetManager();
        this.flowManager = flowController.getFlowManager();
        this.connectorRepository = flowController.getConnectorRepository();
        this.clusterCoordinator = clusterCoordinator;
        this.webClientService = webClientService;
        this.properties = properties;
    }

    @Override
    public void synchronize() {
        if (clusterCoordinator == null) {
            logger.info("Clustering is not configured: Connector asset synchronization disabled");
            return;
        }

        final NodeIdentifier coordinatorNodeId = getElectedActiveCoordinatorNode();
        if (coordinatorNodeId == null) {
            logger.warn("Unable to obtain the node identifier for the cluster coordinator: Connector asset synchronization disabled");
            return;
        }

        if (clusterCoordinator.isActiveClusterCoordinator()) {
            logger.info("Current node is the cluster coordinator: Connector asset synchronization disabled");
            return;
        }

        final String coordinatorAddress = coordinatorNodeId.getApiAddress();
        final int coordinatorPort = coordinatorNodeId.getApiPort();
        final AssetsRestApiClient assetsRestApiClient = new AssetsRestApiClient(webClientService, coordinatorAddress, coordinatorPort, properties.isHTTPSConfigured());
        logger.info("Synchronizing connector assets with cluster coordinator at {}:{}", coordinatorAddress, coordinatorPort);

        final List<ConnectorNode> connectors = flowManager.getAllConnectors();
        logger.info("Found {} connectors for synchronizing assets", connectors.size());

        final Set<ConnectorNode> connectorsWithSynchronizedAssets = new HashSet<>();
        for (final ConnectorNode connector : connectors) {
            try {
                final boolean assetSynchronized = synchronize(assetsRestApiClient, connector);
                if (assetSynchronized) {
                    connectorsWithSynchronizedAssets.add(connector);
                }
            } catch (final Exception e) {
                logger.error("Failed to synchronize assets for connector [{}]", connector.getIdentifier(), e);
            }
        }

        restartConnectorsWithSynchronizedAssets(connectorsWithSynchronizedAssets);
    }

    private void restartConnectorsWithSynchronizedAssets(final Set<ConnectorNode> connectorsWithSynchronizedAssets) {
        for (final ConnectorNode connector : connectorsWithSynchronizedAssets) {
            final ConnectorState currentState = connector.getDesiredState();
            if (currentState == ConnectorState.RUNNING) {
                logger.info("Restarting connector [{}] after asset synchronization", connector.getIdentifier());
                try {
                    final Future<Void> restartFuture = connectorRepository.restartConnector(connector);
                    restartFuture.get();
                    logger.info("Successfully restarted connector [{}] after asset synchronization", connector.getIdentifier());
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted while restarting connector [{}] after asset synchronization", connector.getIdentifier(), e);
                } catch (final ExecutionException e) {
                    logger.error("Failed to restart connector [{}] after asset synchronization", connector.getIdentifier(), e.getCause());
                }
            } else {
                logger.info("Connector [{}] is not running (state={}): skipping restart after asset synchronization", connector.getIdentifier(), currentState);
            }
        }
    }

    private boolean synchronize(final AssetsRestApiClient assetsRestApiClient, final ConnectorNode connector) {
        final String connectorId = connector.getIdentifier();
        final Map<String, Asset> existingAssets = assetManager.getAssets(connectorId).stream()
                .collect(Collectors.toMap(Asset::getIdentifier, Function.identity()));

        final AssetsEntity coordinatorAssetsEntity = listConnectorAssetsWithRetry(assetsRestApiClient, connectorId);
        if (coordinatorAssetsEntity == null) {
            logger.error("Timeout listing assets from cluster coordinator for connector [{}]", connectorId);
            return false;
        }

        final Collection<AssetEntity> coordinatorAssets = coordinatorAssetsEntity.getAssets();
        if (coordinatorAssets == null || coordinatorAssets.isEmpty()) {
            logger.info("Connector [{}] did not return any assets from the cluster coordinator", connectorId);
            return false;
        }

        logger.info("Connector [{}] returned {} assets from the cluster coordinator", connectorId, coordinatorAssets.size());

        boolean assetSynchronized = false;
        for (final AssetEntity coordinatorAssetEntity : coordinatorAssets) {
            final AssetDTO coordinatorAsset = coordinatorAssetEntity.getAsset();
            final Asset matchingAsset = existingAssets.get(coordinatorAsset.getId());
            try {
                final boolean assetWasSynchronized = synchronize(assetsRestApiClient, connectorId, coordinatorAsset, matchingAsset);
                if (assetWasSynchronized) {
                    assetSynchronized = true;
                }
            } catch (final Exception e) {
                logger.error("Failed to synchronize asset [id={},name={}] for connector [{}]",
                        coordinatorAsset.getId(), coordinatorAsset.getName(), connectorId, e);
            }
        }
        return assetSynchronized;
    }

    private boolean synchronize(final AssetsRestApiClient assetsRestApiClient, final String connectorId, final AssetDTO coordinatorAsset, final Asset matchingAsset) {
        final String assetId = coordinatorAsset.getId();
        final String assetName = coordinatorAsset.getName();
        if (matchingAsset == null || !matchingAsset.getFile().exists()) {
            logger.info("Synchronizing missing asset [id={},name={}] for connector [{}]", assetId, assetName, connectorId);
            synchronizeConnectorAssetWithRetry(assetsRestApiClient, connectorId, coordinatorAsset);
            return true;
        } else {
            final String coordinatorAssetDigest = coordinatorAsset.getDigest();
            final String matchingAssetDigest = matchingAsset.getDigest().orElse(null);
            if (!coordinatorAssetDigest.equals(matchingAssetDigest)) {
                logger.info("Synchronizing asset [id={},name={}] with updated digest [{}] for connector [{}]",
                        assetId, assetName, coordinatorAssetDigest, connectorId);
                synchronizeConnectorAssetWithRetry(assetsRestApiClient, connectorId, coordinatorAsset);
                return true;
            } else {
                logger.info("Coordinator asset [id={},name={}] found for connector [{}]: retrieval not required", assetId, assetName, connectorId);
                return false;
            }
        }
    }

    private AssetsEntity listConnectorAssetsWithRetry(final AssetsRestApiClient assetsRestApiClient, final String connectorId) {
        final Instant expirationTime = Instant.ofEpochMilli(System.currentTimeMillis() + LIST_ASSETS_RETRY_DURATION.toMillis());
        while (System.currentTimeMillis() < expirationTime.toEpochMilli()) {
            final AssetsEntity assetsEntity = listConnectorAssets(assetsRestApiClient, connectorId);
            if (assetsEntity != null) {
                return assetsEntity;
            }
            logger.info("Unable to list assets from cluster coordinator for connector [{}]: retrying until [{}]", connectorId, expirationTime);
            sleep(Duration.ofSeconds(5));
        }
        return null;
    }

    private AssetsEntity listConnectorAssets(final AssetsRestApiClient assetsRestApiClient, final String connectorId) {
        try {
            return assetsRestApiClient.getConnectorAssets(connectorId);
        } catch (final NiFiRestApiRetryableException e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            logger.warn("{}, root cause [{}]: retrying", e.getMessage(), rootCause.getMessage());
            return null;
        }
    }

    private void synchronizeConnectorAssetWithRetry(final AssetsRestApiClient assetsRestApiClient, final String connectorId, final AssetDTO coordinatorAsset) {
        final Instant expirationTime = Instant.ofEpochMilli(System.currentTimeMillis() + SYNC_ASSET_RETRY_DURATION.toMillis());
        while (System.currentTimeMillis() < expirationTime.toEpochMilli()) {
            final Asset syncedAsset = synchronizeConnectorAsset(assetsRestApiClient, connectorId, coordinatorAsset);
            if (syncedAsset != null) {
                return;
            }
            logger.info("Unable to synchronize asset [id={},name={}] for connector [{}]: retrying until [{}]",
                    coordinatorAsset.getId(), coordinatorAsset.getName(), connectorId, expirationTime);
            sleep(Duration.ofSeconds(5));
        }
    }

    private Asset synchronizeConnectorAsset(final AssetsRestApiClient assetsRestApiClient, final String connectorId, final AssetDTO coordinatorAsset) {
        final String assetId = coordinatorAsset.getId();
        final String assetName = coordinatorAsset.getName();
        if (Boolean.TRUE == coordinatorAsset.getMissingContent()) {
            return assetManager.createMissingAsset(connectorId, assetName);
        } else {
            try (final InputStream assetInputStream = assetsRestApiClient.getConnectorAssetContent(connectorId, assetId)) {
                return assetManager.saveAsset(connectorId, assetId, assetName, assetInputStream);
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


