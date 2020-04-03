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
package org.apache.nifi.hdfs.repository;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors and sets disk full status and performs failure status reset.
 * Also coordinates fallback when one of the fallback operating modes is configured.
 */
public class ContainerHealthMonitor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerHealthMonitor.class);
    private final HdfsContentRepository repository;
    private final ContainerGroup primary;
    private final ContainerGroup secondary;
    private final boolean capacityFallback;
    private final boolean failureFallback;
    private final long failureTimeoutMs;
    private volatile boolean secondaryActive = false;
    private volatile boolean reportedFullDisk = false;
    private volatile boolean firstRun = true;

    public ContainerHealthMonitor(HdfsContentRepository repository, ContainerGroup primary, ContainerGroup secondary, RepositoryConfig repoConfig) {
        this.repository = repository;
        this.primary = primary;
        this.secondary = secondary;
        this.capacityFallback = repoConfig.hasMode(OperatingMode.CapacityFallback);
        this.failureFallback = repoConfig.hasMode(OperatingMode.FailureFallback);
        this.failureTimeoutMs = repoConfig.getFailureTimeoutMs();
    }

    @Override
    public void run() {
        try {
            boolean primaryDiskOkay = doesGroupHaveFreeSpace(primary);
            doesGroupHaveFreeSpace(secondary);

            if (capacityFallback) {
                if (primaryDiskOkay && secondaryActive) {
                    switchActive("primary group is no longer full");
                } else if (!primaryDiskOkay && !secondaryActive) {
                    switchActive("primary group is full");
                }
            } else if (!primaryDiskOkay && !secondaryActive) {
                if (!reportedFullDisk) {
                    LOG.warn("The primary group is full and is active - all writes will be held");
                    reportedFullDisk = true;
                }
            } else if (reportedFullDisk) {
                LOG.warn("The primary group is no longer full");
                reportedFullDisk = false;
            }

            if (failureTimeoutMs <= 0) {
                return;
            }

            boolean primaryHealthOkay = isGroupHealthy(primary);
            isGroupHealthy(secondary);

            if (failureFallback) {
                if (primaryHealthOkay && secondaryActive) {
                    switchActive("primary group is no longer completely failed");
                } else if (!primaryHealthOkay && !secondaryActive) {
                    switchActive("primary group is completely failed");
                }
            }
        } catch (Throwable ex) {
            LOG.error("Failed to run content repository fallback monitor", ex);
        }
        firstRun = false;
    }

    protected boolean isGroupHealthy(ContainerGroup group) {
        if (group == null) {
            return true;
        }

        boolean hasNonFailedContainer = false;

        long resetBefore = System.currentTimeMillis() - failureTimeoutMs;

        for (Container container : group) {
            if (!container.isFailedRecently()) {
                hasNonFailedContainer = true;
            } else {
                long lastFailure = container.getLastFailure();
                if (lastFailure < resetBefore && container.clearFailure(lastFailure)) {
                    hasNonFailedContainer = true;
                }
            }
        }

        return hasNonFailedContainer;
    }

    protected boolean doesGroupHaveFreeSpace(ContainerGroup group) {
        if (group == null) {
            return true;
        }

        boolean haveFreeSpace = false;

        for (Container container : group) {
            try {
                FileSystem fs = container.getFileSystem();
                FsStatus status = fs.getStatus(container.getPath());
                int replication = container.getConfig().getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
                long used = status.getUsed() / replication;
                if (firstRun) {
                    LOG.debug("FS used is: " + used + " for " + container.getName() + " at " + container.getPath());
                }
                boolean full = used > container.getFullThreshold();
                if (!full) {
                    haveFreeSpace = true;
                }
                if (container.isFull() != full) {
                    LOG.debug("Container disk full status changed: " + container.isFull() + " -> " + full + " with used space: " + used);
                    container.setFull(full);
                }
            } catch (IOException ex) {
                if (!container.isFull()) {
                    haveFreeSpace = true;
                }
                LOG.error("Failed to check filesystem status for container: " + container.getName(), ex);
            }
        }

        return haveFreeSpace;
    }

    protected void switchActive(String reason) {
        if (secondaryActive) {
            LOG.info("Switching active container group back to primary: " + reason);
            repository.setActiveGroup(primary);
        } else {
            LOG.info("Switching active container group to secondary: " + reason);
            repository.setActiveGroup(secondary);
        }
        secondaryActive = !secondaryActive;
    }


}
