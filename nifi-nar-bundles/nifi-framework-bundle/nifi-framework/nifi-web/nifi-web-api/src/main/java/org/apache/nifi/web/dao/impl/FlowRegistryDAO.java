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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.dao.RegistryDAO;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class FlowRegistryDAO implements RegistryDAO {
    private FlowRegistryClient flowRegistryClient;

    @Override
    public FlowRegistry createFlowRegistry(final RegistryDTO registryDto) {
        return flowRegistryClient.addFlowRegistry(registryDto.getId(), registryDto.getName(), registryDto.getUri(), registryDto.getDescription());
    }

    @Override
    public FlowRegistry getFlowRegistry(final String registryId) {
        final FlowRegistry registry = flowRegistryClient.getFlowRegistry(registryId);
        if (registry == null) {
            throw new ResourceNotFoundException("Unable to find Flow Registry with id '" + registryId + "'");
        }

        return registry;
    }

    @Override
    public Set<FlowRegistry> getFlowRegistries() {
        return flowRegistryClient.getRegistryIdentifiers().stream()
            .map(flowRegistryClient::getFlowRegistry)
            .collect(Collectors.toSet());
    }

    @Override
    public Set<FlowRegistry> getFlowRegistriesForUser(final NiFiUser user) {
        return getFlowRegistries();
    }

    @Override
    public Set<Bucket> getBucketsForUser(final String registryId, final NiFiUser user) {
        try {
            final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }

            final Set<Bucket> buckets = flowRegistry.getBuckets(user);
            final Set<Bucket> sortedBuckets = new TreeSet<>((b1, b2) -> b1.getName().compareTo(b2.getName()));
            sortedBuckets.addAll(buckets);
            return sortedBuckets;
        } catch (final IOException | NiFiRegistryException ioe) {
            throw new NiFiCoreException("Unable to obtain listing of buckets: " + ioe, ioe);
        }
    }


    @Override
    public Set<VersionedFlow> getFlowsForUser(String registryId, String bucketId, NiFiUser user) {
        try {
            final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }

            final Set<VersionedFlow> flows = flowRegistry.getFlows(bucketId, user);
            final Set<VersionedFlow> sortedFlows = new TreeSet<>((f1, f2) -> f1.getName().compareTo(f2.getName()));
            sortedFlows.addAll(flows);
            return sortedFlows;
        } catch (final IOException | NiFiRegistryException ioe) {
            throw new NiFiCoreException("Unable to obtain listing of flows for bucket with ID " + bucketId + ": " + ioe, ioe);
        }
    }

    @Override
    public Set<VersionedFlowSnapshotMetadata> getFlowVersionsForUser(String registryId, String bucketId, String flowId, NiFiUser user) {
        try {
            final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
            if (flowRegistry == null) {
                throw new IllegalArgumentException("The specified registry id is unknown to this NiFi.");
            }

            final Set<VersionedFlowSnapshotMetadata> flowVersions = flowRegistry.getFlowVersions(bucketId, flowId, user);
            final Set<VersionedFlowSnapshotMetadata> sortedFlowVersions = new TreeSet<>((f1, f2) -> Integer.compare(f1.getVersion(), f2.getVersion()));
            sortedFlowVersions.addAll(flowVersions);
            return sortedFlowVersions;
        } catch (final IOException | NiFiRegistryException ioe) {
            throw new NiFiCoreException("Unable to obtain listing of versions for bucket with ID " + bucketId + " and flow with ID " + flowId + ": " + ioe, ioe);
        }
    }

    @Override
    public FlowRegistry removeFlowRegistry(final String registryId) {
        final FlowRegistry registry = flowRegistryClient.removeFlowRegistry(registryId);
        if (registry == null) {
            throw new ResourceNotFoundException("Unable to find Flow Registry with id '" + registryId + "'");
        }
        return registry;
    }

    public void setFlowRegistryClient(FlowRegistryClient client) {
        this.flowRegistryClient = client;
    }
}
