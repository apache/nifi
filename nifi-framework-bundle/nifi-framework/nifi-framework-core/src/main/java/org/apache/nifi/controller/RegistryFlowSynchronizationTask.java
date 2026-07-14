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
package org.apache.nifi.controller;

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Periodic task that synchronizes version-controlled Process Groups with their external Flow Registries. The task is
 * scheduled on a fixed tick, but each Process Group is only synchronized at the interval configured on its Flow
 * Registry Client via {@link AbstractFlowRegistryClient#SYNCHRONIZATION_INTERVAL}. When a Flow Registry Client does not
 * specify an interval, the configurable default (the {@code nifi.flowcontroller.registry.sync.interval} property) is
 * used. Process Groups are grouped by their Flow Registry Client so that all groups belonging to a given client are
 * synchronized together on that client's cadence.
 */
final class RegistryFlowSynchronizationTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(RegistryFlowSynchronizationTask.class);

    private final FlowManager flowManager;
    private final long defaultIntervalSeconds;
    private final Map<String, Long> lastSynchronizationTimestamps = new ConcurrentHashMap<>();

    RegistryFlowSynchronizationTask(final FlowManager flowManager, final long defaultIntervalSeconds) {
        this.flowManager = flowManager;
        this.defaultIntervalSeconds = defaultIntervalSeconds;
    }

    @Override
    public void run() {
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        final List<ProcessGroup> allGroups = rootGroup.findAllProcessGroups();
        allGroups.add(rootGroup);

        final Map<String, List<ProcessGroup>> groupsByRegistryClientId = new HashMap<>();
        for (final ProcessGroup group : allGroups) {
            final VersionControlInformation versionControlInformation = group.getVersionControlInformation();
            if (versionControlInformation == null) {
                continue;
            }

            groupsByRegistryClientId.computeIfAbsent(versionControlInformation.getRegistryIdentifier(), key -> new ArrayList<>()).add(group);
        }

        final long now = System.currentTimeMillis();
        for (final Map.Entry<String, List<ProcessGroup>> entry : groupsByRegistryClientId.entrySet()) {
            final String registryClientId = entry.getKey();
            final long intervalSeconds = getEffectiveIntervalSeconds(registryClientId);
            final Long lastSynchronization = lastSynchronizationTimestamps.get(registryClientId);

            if (lastSynchronization != null && (now - lastSynchronization) < TimeUnit.SECONDS.toMillis(intervalSeconds)) {
                continue;
            }

            for (final ProcessGroup group : entry.getValue()) {
                try {
                    group.synchronizeWithFlowRegistry(flowManager);
                } catch (final Exception e) {
                    logger.error("Failed to synchronize {} with Flow Registry", group, e);
                }
            }

            lastSynchronizationTimestamps.put(registryClientId, now);
        }

        // Stop tracking Flow Registry Clients that no longer have any version-controlled Process Groups so that a client
        // is synchronized immediately if it is removed and later re-added.
        lastSynchronizationTimestamps.keySet().retainAll(groupsByRegistryClientId.keySet());
    }

    long getEffectiveIntervalSeconds(final String registryClientId) {
        final FlowRegistryClientNode clientNode = flowManager.getFlowRegistryClient(registryClientId);
        if (clientNode == null) {
            return defaultIntervalSeconds;
        }

        final String configuredInterval = clientNode.getEffectivePropertyValue(AbstractFlowRegistryClient.SYNCHRONIZATION_INTERVAL);
        return parseIntervalSeconds(configuredInterval, defaultIntervalSeconds);
    }

    static long parseIntervalSeconds(final String configuredInterval, final long defaultIntervalSeconds) {
        if (configuredInterval == null || configuredInterval.isBlank()) {
            return defaultIntervalSeconds;
        }

        try {
            return FormatUtils.getTimeDuration(configuredInterval.trim(), TimeUnit.SECONDS);
        } catch (final IllegalArgumentException e) {
            logger.warn("Configured Flow Registry Synchronization Interval [{}] is not valid; using default of {} seconds", configuredInterval, defaultIntervalSeconds);
            return defaultIntervalSeconds;
        }
    }
}
