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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.SystemResourceConsiderations;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.scheduling.SchedulingStrategy;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DiagnosticAnalysisTask implements DiagnosticTask {
    private static final int THREAD_TO_AVAILABLE_PROCS_RATIO = 6;
    private static final int MAX_CONCURRENT_TASKS = 15;

    private final FlowController flowController;

    public DiagnosticAnalysisTask(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        final List<ProcessorNode> allProcessors = flowController.getFlowManager().getRootGroup().findAllProcessors();

        analyzeCpuUsage(details);
        analyzeHighTimerDrivenThreadCount(details);
        analyzeProcessors(allProcessors, details);
        analyzeOpenFileHandles(details);
        analyzeTimerDrivenThreadExhaustion(details);
        analyzeColocatedRepos(details);
        analyzeLeadershipChanges(details);

        if (details.isEmpty()) {
            details.add("Analysis found no concerns");
        }

        return new StandardDiagnosticsDumpElement("Analysis", details);
    }

    private void analyzeCpuUsage(final List<String> details) {
        final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

        final double loadAverage = os.getSystemLoadAverage();
        final int availableProcs = os.getAvailableProcessors();

        if (loadAverage > availableProcs) {
            details.add(String.format("1-minute CPU Load Average is %1$.2f, which exceeds the %2$d available cores. CPU is over-utilized.", loadAverage, availableProcs));
        } else if (loadAverage > 0.9 * availableProcs) {
            details.add(String.format("1-minute CPU Load Average is %1$.2f, which exceeds 90%% of the %2$d available cores. CPU may struggle to keep up.", loadAverage, availableProcs));
        }
    }

    private void analyzeHighTimerDrivenThreadCount(final List<String> details) {
        final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        final int availableProcs = os.getAvailableProcessors();

        if (flowController.getMaxTimerDrivenThreadCount() > THREAD_TO_AVAILABLE_PROCS_RATIO * availableProcs) {
            details.add("Number of Timer-Driven Threads is " + flowController.getMaxTimerDrivenThreadCount() + " with " + availableProcs
                + " available cores. Number of threads exceeds " + THREAD_TO_AVAILABLE_PROCS_RATIO + "x the number of cores available.");
        }
    }

    private void analyzeProcessors(final Collection<ProcessorNode> processors, final List<String> details) {
        final Map<String, Integer> highMemTypesToCounts = new HashMap<>();

        for (final ProcessorNode procNode : processors) {
            if (procNode.getMaxConcurrentTasks() > MAX_CONCURRENT_TASKS) {
                details.add(procNode + " is configured with a Max Concurrent Tasks of " + procNode.getMaxConcurrentTasks()
                    + ", which is very high. Under most circumstances, this value should not be set above 12-15. This processor is currently " + procNode.getScheduledState().name());
            }

            if (procNode.getSchedulingStrategy() == SchedulingStrategy.EVENT_DRIVEN) {
                details.add(procNode + " is configured with a Scheduling Strategy of Event-Driven. The Event-Driven Scheduling Strategy is experimental and may trigger unexpected behavior, such as " +
                    "a Processor \"hanging\" or becoming unresponsive.");
            }

            if (isHighMemoryUtilizer(procNode)) {
                final String processorType = procNode.getComponentType();
                final int currentCount = highMemTypesToCounts.computeIfAbsent(processorType, k -> 0);
                highMemTypesToCounts.put(processorType, currentCount + 1);
            }
        }

        for (final Map.Entry<String, Integer> entry : highMemTypesToCounts.entrySet()) {
            final String processorType = entry.getKey();
            final int count = entry.getValue();

            details.add(count + " instances of " + processorType + " are on the canvas, and this Processor is denoted as using large amounts of heap");
        }
    }

    private boolean isHighMemoryUtilizer(final ProcessorNode procNode) {
        final Processor processor = procNode.getProcessor();
        final SystemResourceConsideration consideration = processor.getClass().getAnnotation(SystemResourceConsideration.class);
        if (consideration != null) {
            if (SystemResource.MEMORY == consideration.resource()) {
                return true;
            }
        }

        final SystemResourceConsiderations considerations = processor.getClass().getAnnotation(SystemResourceConsiderations.class);
        if (considerations != null) {
            for (final SystemResourceConsideration systemResourceConsideration : considerations.value()) {
                if (SystemResource.MEMORY == systemResourceConsideration.resource()) {
                    return true;
                }
            }
        }

        return false;
    }

    private void analyzeOpenFileHandles(final List<String> details) {
        final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        final ObjectName osObjectName = os.getObjectName();

        try {
            final Object openFileCount = ManagementFactory.getPlatformMBeanServer().getAttribute(osObjectName, "OpenFileDescriptorCount");
            final Object maxOpenFileCount = ManagementFactory.getPlatformMBeanServer().getAttribute(osObjectName, "MaxFileDescriptorCount");

            if (openFileCount != null && maxOpenFileCount != null) {
                final int openCount = ((Number) openFileCount).intValue();
                final int maxCount = ((Number) maxOpenFileCount).intValue();
                if (openCount >= 0.8 * maxCount) {
                    details.add("Open File Count for NiFi is " + openCount + ", which exceeds 80% of the Max Open File Count of " + maxCount
                        + ". It may be necessary to increase the maximum number of file handles that are available to a process in the Operating System.");
                }
            }
        } catch (final Exception e) {
            details.add("Failed to determine whether or not Open File Handle Count is of concern due to " + e);
        }
    }

    private void analyzeTimerDrivenThreadExhaustion(final List<String> details) {
        final int activethreadCount = flowController.getActiveThreadCount();
        final int maxThreadCount = flowController.getMaxTimerDrivenThreadCount();

        if (activethreadCount >= 0.95 * maxThreadCount) {
            details.add("Active Thread Count is " + activethreadCount + ", with Max Active Thread Count of " + maxThreadCount + ". The Timer-Driven Thread Pool may be exhausted.");
        }
    }

    private void analyzeColocatedRepos(final List<String> details) {
        final Map<String, List<String>> fileStoreToRepos = new HashMap<>();

        final RepositoryContextFactory contextFactory = flowController.getRepositoryContextFactory();

        final String flowFileRepoStoreName = contextFactory.getFlowFileRepository().getFileStoreName();
        List<String> repos = fileStoreToRepos.computeIfAbsent(flowFileRepoStoreName, k -> new ArrayList<>());
        repos.add("FlowFile Repository");

        for (final String containerName : contextFactory.getContentRepository().getContainerNames()) {
            repos = fileStoreToRepos.computeIfAbsent(flowFileRepoStoreName, k -> new ArrayList<>());
            repos.add("Content Repository <" + containerName + ">");
        }

        for (final String containerName : contextFactory.getProvenanceRepository().getContainerNames()) {
            repos = fileStoreToRepos.computeIfAbsent(flowFileRepoStoreName, k -> new ArrayList<>());
            repos.add("Provenance Repository <" + containerName + ">");
        }

        for (final List<String> repoNamesOnSameFileStore : fileStoreToRepos.values()) {
            if (repoNamesOnSameFileStore.size() > 1) {
                details.add("The following Repositories share the same File Store: " + repoNamesOnSameFileStore);
            }
        }
    }

    private void analyzeLeadershipChanges(final List<String> details) {
        final LeaderElectionManager leaderElectionManager = flowController.getLeaderElectionManager();
        if (leaderElectionManager == null) {
            return;
        }

        final Map<String, Integer> changeCounts = leaderElectionManager.getLeadershipChangeCount(24, TimeUnit.HOURS);

        changeCounts.entrySet().stream()
            .filter(entry -> entry.getValue() > 4)
            .forEach(entry -> details.add("Leadership for role <" + entry.getKey() + "> has changed " + entry.getValue() + " times in the last 24 hours"));
    }
}
