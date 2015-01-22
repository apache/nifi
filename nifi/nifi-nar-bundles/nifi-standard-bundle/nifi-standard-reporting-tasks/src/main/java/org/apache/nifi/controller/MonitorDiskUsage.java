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

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"disk", "storage", "warning", "monitoring", "repo"})
@CapabilityDescription("Checks the amount of storage space available for the Content Repository and FlowFile Repository"
        + " and warns (via a log message and a System-Level Bulletin) if the partition on which either repository exceeds"
        + " some configurable threshold of storage space")
public class MonitorDiskUsage extends AbstractReportingTask {

    private static final Logger logger = LoggerFactory.getLogger(MonitorDiskUsage.class);

    private static final Pattern PERCENT_PATTERN = Pattern.compile("(\\d+{1,2})%");

    public static final PropertyDescriptor CONTENT_REPO_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Content Repository Threshold")
            .description("The threshold at which a bulletin will be generated to indicate that the disk usage of the Content Repository is of concern")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(PERCENT_PATTERN))
            .defaultValue("80%")
            .build();
    public static final PropertyDescriptor FLOWFILE_REPO_THRESHOLD = new PropertyDescriptor.Builder()
            .name("FlowFile Repository Threshold")
            .description("The threshold at which a bulletin will be generated to indicate that the disk usage of the FlowFile Repository is of concern")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(PERCENT_PATTERN))
            .defaultValue("80%")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(2);
        descriptors.add(CONTENT_REPO_THRESHOLD);
        descriptors.add(FLOWFILE_REPO_THRESHOLD);
        return descriptors;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final String contentRepoTresholdValue = context.getProperty(CONTENT_REPO_THRESHOLD).getValue();
        final Matcher contentRepoMatcher = PERCENT_PATTERN.matcher(contentRepoTresholdValue.trim());
        contentRepoMatcher.find();
        final String contentRepoPercentageVal = contentRepoMatcher.group(1);
        final int contentRepoThreshold = Integer.parseInt(contentRepoPercentageVal);

        final String flowfileRepoTresholdValue = context.getProperty(FLOWFILE_REPO_THRESHOLD).getValue();
        final Matcher flowFileRepoMatcher = PERCENT_PATTERN.matcher(flowfileRepoTresholdValue.trim());
        flowFileRepoMatcher.find();
        final String flowFileRepoPercentageVal = flowFileRepoMatcher.group(1);
        final int flowFileRepoThreshold = Integer.parseInt(flowFileRepoPercentageVal);

        final NiFiProperties properties = NiFiProperties.getInstance();

        for (final Map.Entry<String, Path> entry : properties.getContentRepositoryPaths().entrySet()) {
            checkThreshold("Content Repository (" + entry.getKey() + ")", entry.getValue(), contentRepoThreshold, context);
        }

        checkThreshold("FlowFile Repository", properties.getFlowFileRepositoryPath(), flowFileRepoThreshold, context);
    }

    static void checkThreshold(final String pathName, final Path path, final int threshold, final ReportingContext context) {
        final File file = path.toFile();
        final long totalBytes = file.getTotalSpace();
        final long freeBytes = file.getFreeSpace();
        final long usedBytes = totalBytes - freeBytes;

        final double usedPercent = (double) usedBytes / (double) totalBytes * 100D;

        if (usedPercent >= threshold) {
            final String usedSpace = FormatUtils.formatDataSize(usedBytes);
            final String totalSpace = FormatUtils.formatDataSize(totalBytes);
            final String freeSpace = FormatUtils.formatDataSize(freeBytes);

            final double freePercent = (double) freeBytes / (double) totalBytes * 100D;

            final String message = String.format("%1$s exceeds configured threshold of %2$s%%, having %3$s / %4$s (%5$.2f%%) used and %6$s (%7$.2f%%) free",
                    pathName, threshold, usedSpace, totalSpace, usedPercent, freeSpace, freePercent);
            final Bulletin bulletin = context.createBulletin("Disk Usage", Severity.WARNING, message);
            context.getBulletinRepository().addBulletin(bulletin);
            logger.warn(message);
        }
    }
}
