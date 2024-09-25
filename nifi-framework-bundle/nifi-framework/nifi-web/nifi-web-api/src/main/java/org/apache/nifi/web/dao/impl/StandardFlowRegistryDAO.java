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

package org.apache.nifi.web.dao.impl;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.registry.flow.BucketLocation;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.FlowRegistryBranch;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.FlowRegistryClientUserContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.dao.FlowRegistryDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

@Repository
public class StandardFlowRegistryDAO extends ComponentDAO implements FlowRegistryDAO {
    private FlowController flowController;

    @Override
    public FlowRegistryClientNode createFlowRegistryClient(final FlowRegistryClientDTO flowRegistryClientDto) {
        if (flowRegistryClientDto.getType() == null) {
            throw new IllegalArgumentException("The flow registry client type must be specified.");
        }

        verifyCreate(flowController.getExtensionManager(),  flowRegistryClientDto.getType(), flowRegistryClientDto.getBundle());

        final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(flowController.getExtensionManager(), flowRegistryClientDto.getType(), flowRegistryClientDto.getBundle());
        final FlowRegistryClientNode flowRegistryClient = flowController.getFlowManager().createFlowRegistryClient(
                flowRegistryClientDto.getType(), flowRegistryClientDto.getId(), bundleCoordinate, Collections.emptySet(), true, true, null);

        configureFlowRegistry(flowRegistryClient, flowRegistryClientDto);

        return flowRegistryClient;
    }

    @Override
    public FlowRegistryClientNode updateFlowRegistryClient(final FlowRegistryClientDTO flowRegistryClientDto) {
        final FlowRegistryClientNode client = getFlowRegistryClient(flowRegistryClientDto.getId());

        // ensure we can perform the update
        verifyUpdate(client, flowRegistryClientDto);

        // perform the update
        configureFlowRegistry(client, flowRegistryClientDto);

        return client;
    }

    private void verifyUpdate(final FlowRegistryClientNode client, final FlowRegistryClientDTO flowRegistryClientDto) {
        final boolean duplicateName = getFlowRegistryClients().stream()
                .anyMatch(reg -> reg.getName().equals(flowRegistryClientDto.getName()) && !reg.getIdentifier().equals(flowRegistryClientDto.getId()));

        if (duplicateName) {
            throw new IllegalStateException("Cannot update Flow Registry because a Flow Registry already exists with the name " + flowRegistryClientDto.getName());
        }
    }


    @Override
    public FlowRegistryClientNode getFlowRegistryClient(final String registryId) {
        final FlowRegistryClientNode registry = flowController.getFlowManager().getFlowRegistryClient(registryId);

        if (registry == null) {
            throw new ResourceNotFoundException("Unable to find Flow Registry with id '" + registryId + "'");
        }

        return registry;
    }

    @Override
    public Set<FlowRegistryClientNode> getFlowRegistryClients() {
        return flowController.getFlowManager().getAllFlowRegistryClients();
    }

    @Override
    public Set<FlowRegistryClientNode> getFlowRegistryClientsForUser(final FlowRegistryClientUserContext context) {
        return getFlowRegistryClients();
    }

    @Override
    public Set<FlowRegistryBranch> getBranchesForUser(final FlowRegistryClientUserContext context, final String registryId) {
        try {
            final FlowRegistryClientNode flowRegistry = flowController.getFlowManager().getFlowRegistryClient(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }
            final FlowRegistryBranch defaultBranch = flowRegistry.getDefaultBranch(context);
            final Set<FlowRegistryBranch> branches = flowRegistry.getBranches(context);
            // Sort the default branch first, this allows main to always be the first
            final Set<FlowRegistryBranch> sortedBranches = new TreeSet<>((branch1, branch2) -> {
                if (branch1.getName().equals(defaultBranch.getName())) {
                    return -1;
                } else if (branch2.getName().equals(defaultBranch.getName())) {
                    return 1;
                }
                return branch1.getName().compareTo(branch2.getName());
            });
            sortedBranches.addAll(branches);
            return sortedBranches;
        } catch (final IOException | FlowRegistryException ioe) {
            throw new NiFiCoreException("Unable to get branches for registry with ID " + registryId + ": " + ioe.getMessage(), ioe);
        }
    }

    @Override
    public FlowRegistryBranch getDefaultBranchForUser(final FlowRegistryClientUserContext context, final String registryId) {
        try {
            final FlowRegistryClientNode flowRegistry = flowController.getFlowManager().getFlowRegistryClient(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }
            return flowRegistry.getDefaultBranch(context);
        } catch (final IOException | FlowRegistryException ioe) {
            throw new NiFiCoreException("Unable to get default branch for registry with ID " + registryId + ": " + ioe.getMessage(), ioe);
        }
    }

    @Override
    public Set<FlowRegistryBucket> getBucketsForUser(final FlowRegistryClientUserContext context, final String registryId, final String branch) {
        try {
            final FlowRegistryClientNode flowRegistry = flowController.getFlowManager().getFlowRegistryClient(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }

            final Set<FlowRegistryBucket> buckets = flowRegistry.getBuckets(context, branch);
            final Set<FlowRegistryBucket> sortedBuckets = new TreeSet<>(Comparator.comparing(FlowRegistryBucket::getName));
            sortedBuckets.addAll(buckets);
            return sortedBuckets;
        } catch (final FlowRegistryException e) {
            throw new IllegalStateException(e.getMessage(), e);
        } catch (final IOException ioe) {
            throw new NiFiCoreException("Unable to obtain listing of buckets: " + ioe.getMessage(), ioe);
        }
    }

    @Override
    public Set<RegisteredFlow> getFlowsForUser(final FlowRegistryClientUserContext context, final String registryId, final String branch, final String bucketId) {
        try {
            final FlowRegistryClientNode flowRegistry = flowController.getFlowManager().getFlowRegistryClient(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }

            final BucketLocation bucketLocation = new BucketLocation(branch, bucketId);
            final Set<RegisteredFlow> flows = flowRegistry.getFlows(context, bucketLocation);
            final Set<RegisteredFlow> sortedFlows = new TreeSet<>(Comparator.comparing(RegisteredFlow::getName));
            sortedFlows.addAll(flows);
            return sortedFlows;
        } catch (final IOException | FlowRegistryException ioe) {
            throw new NiFiCoreException("Unable to obtain listing of flows for bucket with ID " + bucketId + ": " + ioe.getMessage(), ioe);
        }
    }

    @Override
    public RegisteredFlow getFlowForUser(final FlowRegistryClientUserContext context, final String registryId, final String branch, final String bucketId, final String flowId) {
        try {
            final FlowRegistryClientNode flowRegistry = flowController.getFlowManager().getFlowRegistryClient(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }

            final FlowLocation flowLocation = new FlowLocation(branch, bucketId, flowId);
            return flowRegistry.getFlow(context, flowLocation);
        } catch (final IOException | FlowRegistryException ioe) {
            throw new NiFiCoreException("Unable to obtain listing of flows for bucket with ID " + bucketId + ": " + ioe.getMessage(), ioe);
        }
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersionsForUser(final FlowRegistryClientUserContext context, final String registryId, final String branch,
                                                                      final String bucketId, final String flowId) {
        try {
            final FlowRegistryClientNode flowRegistry = flowController.getFlowManager().getFlowRegistryClient(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }

            final FlowLocation flowLocation = new FlowLocation(branch, bucketId, flowId);
            final Set<RegisteredFlowSnapshotMetadata> flowVersions = flowRegistry.getFlowVersions(context, flowLocation);

            // if somehow the timestamp of two versions is exactly the same, then we use version as a secondary comparison,
            // otherwise one of the objects won't be added to the set since compareTo returns 0 indicating it already exists
            final Set<RegisteredFlowSnapshotMetadata> sortedFlowVersions = new TreeSet<>(
                    Comparator.comparingLong(RegisteredFlowSnapshotMetadata::getTimestamp)
                            .thenComparing(RegisteredFlowSnapshotMetadata::getVersion));
            sortedFlowVersions.addAll(flowVersions);
            return sortedFlowVersions;
        } catch (final IOException | FlowRegistryException ioe) {
            throw new NiFiCoreException("Unable to obtain listing of versions for bucket with ID " + bucketId + " and flow with ID " + flowId + ": " + ioe.getMessage(), ioe);
        }
    }

    @Override
    public FlowRegistryClientNode removeFlowRegistry(final String registryId) {
        final FlowRegistryClientNode flowRegistry = flowController.getFlowManager().getFlowRegistryClient(registryId);
        if (flowRegistry == null) {
            throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
        }

        flowController.getFlowManager().removeFlowRegistryClient(flowRegistry);

        return flowRegistry;
    }

    @Autowired
    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    private void configureFlowRegistry(final FlowRegistryClientNode node, final FlowRegistryClientDTO dto) {
        final String name = dto.getName();
        final String description = dto.getDescription();
        final Map<String, String> properties = dto.getProperties();

        node.pauseValidationTrigger();

        try {
            if (isNotNull(name)) {
                node.setName(name);
            }

            if (isNotNull(description)) {
                node.setDescription(description);
            }

            if (isNotNull(properties)) {
                final Set<String> sensitiveDynamicPropertyNames = Optional.ofNullable(dto.getSensitiveDynamicPropertyNames()).orElse(Collections.emptySet());
                node.setProperties(properties, false, sensitiveDynamicPropertyNames);
            }
        } finally {
            node.resumeValidationTrigger();
        }
    }
}
