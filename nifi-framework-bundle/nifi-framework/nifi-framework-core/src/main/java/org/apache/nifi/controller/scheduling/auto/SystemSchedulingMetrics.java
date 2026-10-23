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
package org.apache.nifi.controller.scheduling.auto;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.function.LongSupplier;

public class SystemSchedulingMetrics {
    private static final double CURRENT_CPU_LOAD_WEIGHT = 0.25D;
    private static final double CPU_MAX_THRESHOLD = 0.90D;
    private static final double CPU_INCREASE_RESUME_THRESHOLD = 0.80D;
    private static final double GARBAGE_COLLECTION_MAX_THRESHOLD = 0.10D;
    private static final int CPU_LOAD_READINGS_REQUIRED_TO_RESUME_INCREASES = 2;
    private static final int GARBAGE_COLLECTION_READINGS_REQUIRED_TO_STOP_INCREASES = 2;

    private final OperatingSystemMXBean operatingSystemMXBean;
    private final List<GarbageCollectorMXBean> garbageCollectorMXBeans;
    private final LongSupplier nanoTimeSupplier;

    private double averageCpuLoad = -1D;
    private long previousSampleNanos;
    private long previousGarbageCollectionMillis;
    private long previousProcessCpuNanos;
    private int highGarbageCollectionReadings;
    private int cpuReadingsBelowResumeThreshold;
    private boolean cpuLoadAboveLimit;
    private boolean garbageCollectionTimeAboveLimit;

    public SystemSchedulingMetrics() {
        this(ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class), ManagementFactory.getGarbageCollectorMXBeans(), System::nanoTime);
    }

    SystemSchedulingMetrics(final OperatingSystemMXBean operatingSystemMXBean,
                            final List<GarbageCollectorMXBean> garbageCollectorMXBeans, final LongSupplier nanoTimeSupplier) {
        this.operatingSystemMXBean = operatingSystemMXBean;
        // Collectors such as ZGC and Shenandoah report collection cycles that run alongside the application in beans named
        // with a "Cycles" suffix, and report the time that the application is paused in separate beans. Only pauses are counted.
        this.garbageCollectorMXBeans = garbageCollectorMXBeans.stream()
                .filter(garbageCollectorMXBean -> !garbageCollectorMXBean.getName().endsWith(" Cycles"))
                .toList();
        this.nanoTimeSupplier = nanoTimeSupplier;
        previousSampleNanos = nanoTimeSupplier.getAsLong();
        previousGarbageCollectionMillis = getGarbageCollectionMillis();
        previousProcessCpuNanos = getProcessCpuTime();
    }

    public SystemSchedulingSnapshot captureSnapshot(final int maxGlobalConcurrentTasks,
                                                    final double fractionOfSamplesWithAllGlobalTaskSlotsUsed,
                                                    final double fractionOfGlobalTaskTimeSpentWaiting) {
        final long nowNanos = nanoTimeSupplier.getAsLong();
        final long garbageCollectionMillis = getGarbageCollectionMillis();
        final long elapsedNanos = Math.max(1L, nowNanos - previousSampleNanos);
        final long garbageCollectionDeltaMillis = Math.max(0L, garbageCollectionMillis - previousGarbageCollectionMillis);
        final double fractionOfTimeSpentOnGarbageCollection = garbageCollectionDeltaMillis / (elapsedNanos / 1_000_000D);

        previousSampleNanos = nowNanos;
        previousGarbageCollectionMillis = garbageCollectionMillis;

        updateGarbageCollectionState(fractionOfTimeSpentOnGarbageCollection);

        final double cpuLoad = getCpuLoad(elapsedNanos);
        final boolean cpuLoadAvailable = cpuLoad >= 0D;
        updateCpuLoadState(cpuLoad);

        return SystemSchedulingSnapshot.createBuilder()
                .setCpuLoadAvailable(cpuLoadAvailable)
                .setCpuLoad(cpuLoad)
                .setAverageCpuLoad(averageCpuLoad)
                .setFractionOfTimeSpentOnGarbageCollection(fractionOfTimeSpentOnGarbageCollection)
                .setMaxGlobalConcurrentTasks(maxGlobalConcurrentTasks)
                .setFractionOfSamplesWithAllGlobalTaskSlotsUsed(fractionOfSamplesWithAllGlobalTaskSlotsUsed)
                .setFractionOfGlobalTaskTimeSpentWaiting(fractionOfGlobalTaskTimeSpentWaiting)
                .setCpuLoadAboveLimit(cpuLoadAboveLimit)
                .setGarbageCollectionTimeAboveLimit(garbageCollectionTimeAboveLimit)
                .build();
    }

    private void updateGarbageCollectionState(final double fractionOfTimeSpentOnGarbageCollection) {
        if (fractionOfTimeSpentOnGarbageCollection >= GARBAGE_COLLECTION_MAX_THRESHOLD) {
            highGarbageCollectionReadings++;
        } else {
            highGarbageCollectionReadings = 0;
            garbageCollectionTimeAboveLimit = false;
        }

        if (highGarbageCollectionReadings >= GARBAGE_COLLECTION_READINGS_REQUIRED_TO_STOP_INCREASES) {
            garbageCollectionTimeAboveLimit = true;
        }
    }

    private void updateCpuLoadState(final double cpuLoad) {
        if (cpuLoad < 0D) {
            averageCpuLoad = -1D;
            cpuLoadAboveLimit = false;
            cpuReadingsBelowResumeThreshold = 0;
            return;
        }

        // Retain 75% of the prior average and use 25% of the current reading so that one short spike does not stop concurrency increases.
        averageCpuLoad = averageCpuLoad < 0D ? cpuLoad : CURRENT_CPU_LOAD_WEIGHT * cpuLoad + (1D - CURRENT_CPU_LOAD_WEIGHT) * averageCpuLoad;
        if (averageCpuLoad >= CPU_MAX_THRESHOLD) {
            cpuLoadAboveLimit = true;
            cpuReadingsBelowResumeThreshold = 0;
        } else if (averageCpuLoad < CPU_INCREASE_RESUME_THRESHOLD) {
            cpuReadingsBelowResumeThreshold++;
            if (cpuReadingsBelowResumeThreshold >= CPU_LOAD_READINGS_REQUIRED_TO_RESUME_INCREASES) {
                cpuLoadAboveLimit = false;
            }
        } else {
            cpuReadingsBelowResumeThreshold = 0;
        }
    }

    private double getCpuLoad(final long elapsedNanos) {
        if (operatingSystemMXBean == null) {
            return -1D;
        }

        final double cpuLoad = Math.max(operatingSystemMXBean.getCpuLoad(), operatingSystemMXBean.getProcessCpuLoad());
        if (cpuLoad >= 0D) {
            previousProcessCpuNanos = operatingSystemMXBean.getProcessCpuTime();
            return cpuLoad;
        }

        final long processCpuNanos = operatingSystemMXBean.getProcessCpuTime();
        final long processCpuDeltaNanos = previousProcessCpuNanos < 0L ? -1L : processCpuNanos - previousProcessCpuNanos;
        previousProcessCpuNanos = processCpuNanos;
        if (processCpuDeltaNanos >= 0L) {
            return Math.min(1D, processCpuDeltaNanos / (double) (elapsedNanos * operatingSystemMXBean.getAvailableProcessors()));
        }

        return -1D;
    }

    private long getProcessCpuTime() {
        return operatingSystemMXBean == null ? -1L : operatingSystemMXBean.getProcessCpuTime();
    }

    private long getGarbageCollectionMillis() {
        long totalMillis = 0L;
        for (final GarbageCollectorMXBean garbageCollectorMXBean : garbageCollectorMXBeans) {
            final long collectionTime = garbageCollectorMXBean.getCollectionTime();
            if (collectionTime > 0L) {
                totalMillis += collectionTime;
            }
        }

        return totalMillis;
    }
}
