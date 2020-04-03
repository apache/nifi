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

import static org.apache.nifi.hdfs.repository.HdfsContentRepository.FAILURE_TIMEOUT_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.FULL_PERCENTAGE_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.HEALTH_CHECK_RUN_INTERVAL_SECONDS;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.OPERATING_MODE_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.SECTIONS_PER_CONTAINER_PROPERTY;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_ENABLED;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_ALWAYS_SYNC;
import static org.apache.nifi.util.NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_MAX_TIME;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryConfig {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoryConfig.class);
    private final Set<OperatingMode> modes;
    private final double maxArchiveRatio;
    private final long failureTimeoutMs;
    private final double fullPercentage;
    private final boolean alwaysSync;
    private final int maxFlowFilesPerClaim;
    private final boolean archiveData;
    private final long maxAppendableClaimLength;
    private final long maxArchiveMillis;
    private final long waitForContainerTimeoutMs;
    private final int sectionsPerContainer;

    public RepositoryConfig(NiFiProperties properties) {
        Set<OperatingMode> modes = new HashSet<>();
        modes.add(OperatingMode.Normal);
        for (String modeStr : properties.getProperty(OPERATING_MODE_PROPERTY, "").split(",")) {
            if (modeStr.isEmpty()) {
                continue;
            }
            OperatingMode mode = OperatingMode.valueOf(modeStr);
            modes.add(mode);
        }
        this.modes = Collections.unmodifiableSet(modes);

        if (hasMode(OperatingMode.CapacityFallback) && hasMode(OperatingMode.FailureFallback)) {
            throw new RuntimeException("Both CapacityFallback and FailureFallback modes were specified in '" +
                OPERATING_MODE_PROPERTY + "' - however they are mutually exclusive.");
        }

        this.sectionsPerContainer = properties.getIntegerProperty(SECTIONS_PER_CONTAINER_PROPERTY, 1024);
        this.maxFlowFilesPerClaim = properties.getMaxFlowFilesPerClaim();
        this.alwaysSync = properties.getProperty(FLOWFILE_REPOSITORY_ALWAYS_SYNC, "false").equalsIgnoreCase("true");
        this.archiveData = properties.getProperty(CONTENT_ARCHIVE_ENABLED, "false").equalsIgnoreCase("true");

        // ignore 'max size' - this is HDFS
        this.maxAppendableClaimLength = DataUnit.parseDataSize(properties.getMaxAppendableClaimSize(), DataUnit.B).longValue();

        this.maxArchiveMillis = parseTime(properties, FLOW_CONFIGURATION_ARCHIVE_MAX_TIME, Long.MAX_VALUE);

        String maxArchiveSize = properties.getProperty(CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE);
        if (maxArchiveSize != null) {
            this.maxArchiveRatio = parsePercentage(maxArchiveSize, CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE);
        } else {
            this.maxArchiveRatio = 0;
        }

        String fullPercentageStr = properties.getProperty(FULL_PERCENTAGE_PROPERTY, "95%");
        this.fullPercentage = parsePercentage(fullPercentageStr, FULL_PERCENTAGE_PROPERTY);

        this.failureTimeoutMs = parseTime(properties, FAILURE_TIMEOUT_PROPERTY, 0);
        if (this.failureTimeoutMs > 0 && this.failureTimeoutMs < (HEALTH_CHECK_RUN_INTERVAL_SECONDS * 1000)) {
            // the failure reset mechanism could probably be moved out of
            // the health monitor in order to accomodate a smaller failure timeout
            LOG.warn("Failure timeout is less than the minimum (seconds): " +
                (this.failureTimeoutMs / 1000) + " < " + HEALTH_CHECK_RUN_INTERVAL_SECONDS + " - effectively using minimum");
        }

        this.waitForContainerTimeoutMs = parseTime(properties, HdfsContentRepository.WAIT_FOR_CONTAINERS_TIMEOUT, Long.MAX_VALUE);

        if (this.failureTimeoutMs <= 0 && hasMode(OperatingMode.FailureFallback)) {
            throw new RuntimeException("FailureFallback operating mode is active, and failure timeout is not specified.");
        }
    }

    private long parseTime(NiFiProperties properties, String property, long defaultValue) {
        String valueStr = properties.getProperty(property, "");
        if (valueStr.isEmpty()) {
            return defaultValue;
        } else {
            return (long)FormatUtils.getPreciseTimeDuration(valueStr, TimeUnit.MILLISECONDS);
        }
    }

    private double parsePercentage(String percentageStr, String property) {
        if (!HdfsContentRepository.MAX_ARCHIVE_SIZE_PATTERN.matcher(percentageStr.trim()).matches()) {
            throw new RuntimeException("Invalid value specified for the '" + property + "' property. Value must be in format: <XX>%");
        }
        final String trimmed = percentageStr.trim();
        final String percentage = trimmed.substring(0, trimmed.length() - 1);
        return Integer.parseInt(percentage) / 100D;
    }

    public int getSectionsPerContainer() {
        return sectionsPerContainer;
    }
    public boolean hasMode(OperatingMode mode) {
        return modes.contains(mode);
    }
    public Set<OperatingMode> getModes() {
        return modes;
    }

    public double getMaxArchiveRatio() {
        return maxArchiveRatio;
    }

    public long getFailureTimeoutMs() {
        return failureTimeoutMs;
    }
    public long getWaitForContainerTimeoutMs() {
        return waitForContainerTimeoutMs;
    }

    public double getFullPercentage() {
        return fullPercentage;
    }

    public boolean isAlwaysSync() {
        return alwaysSync;
    }

    public int getMaxFlowFilesPerClaim() {
        return maxFlowFilesPerClaim;
    }

    public boolean isArchiveData() {
        return archiveData;
    }

    public long getMaxAppendableClaimLength() {
        return maxAppendableClaimLength;
    }

    public long getMaxArchiveMillis() {
        return maxArchiveMillis;
    }

}
