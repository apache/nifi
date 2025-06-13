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

package org.apache.nifi.controller.repository.metrics.tracking;

import org.apache.nifi.controller.repository.metrics.NanoTimePerformanceTracker;
import org.apache.nifi.controller.repository.metrics.NopPerformanceTracker;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

public class StandardStatsTracker implements StatsTracker {
    private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    private final LongSupplier gcMillisTracker;
    private final LongSupplier cpuTimeMillisTracker;
    private final int iterationsBetweenCpuTracking;

    private final AtomicLong iterations = new AtomicLong(0L);
    private final AtomicReference<SampledCpuMetrics> sampledCpuMetrics = new AtomicReference<>(new SampledCpuMetrics(0L, 0L));

    public StandardStatsTracker(final LongSupplier gcMillisTracker,
                                final int expensiveMetricsTrackingPercentage) {

        if (expensiveMetricsTrackingPercentage < 0) {
            throw new IllegalArgumentException("CPU tracking percentage must be non-negative");
        } else if (expensiveMetricsTrackingPercentage > 100) {
            throw new IllegalArgumentException("CPU tracking percentage must be less than or equal to 100");
        }

        if (expensiveMetricsTrackingPercentage == 0) {
            this.iterationsBetweenCpuTracking = Integer.MAX_VALUE; // never track CPU time
        } else {
            this.iterationsBetweenCpuTracking = 100 / expensiveMetricsTrackingPercentage; // calculate iterations between CPU tracking
        }

        this.gcMillisTracker = gcMillisTracker;
        this.cpuTimeMillisTracker = threadMXBean.isCurrentThreadCpuTimeSupported() ? threadMXBean::getCurrentThreadCpuTime : () -> 0L;
    }

    @Override
    public TrackedStats startTracking() {
        final long startTimeNanos = System.nanoTime();
        final long startGcMillis = gcMillisTracker.getAsLong();
        final boolean trackExpensiveMetrics = iterations.getAndIncrement() % iterationsBetweenCpuTracking == 0;
        final long startCpuTimeMillis = trackExpensiveMetrics ? cpuTimeMillisTracker.getAsLong() : 0;
        final PerformanceTracker performanceTracker = trackExpensiveMetrics ? new NanoTimePerformanceTracker() : new NopPerformanceTracker();

        return new TrackedStats() {
            @Override
            public StandardFlowFileEvent end() {
                final long endCpuTimeMillis = trackExpensiveMetrics ? cpuTimeMillisTracker.getAsLong() : 0;

                final StandardFlowFileEvent event = new StandardFlowFileEvent();
                event.setProcessingNanos(System.nanoTime() - startTimeNanos);
                event.setGarbageCollectionMillis(gcMillisTracker.getAsLong() - startGcMillis);
                event.setCpuNanoseconds(endCpuTimeMillis - startCpuTimeMillis);
                event.setContentReadNanoseconds(performanceTracker.getContentReadNanos());
                event.setContentWriteNanoseconds(performanceTracker.getContentWriteNanos());
                event.setSessionCommitNanos(performanceTracker.getSessionCommitNanos());

                if (trackExpensiveMetrics) {
                    addSampledCpuTime(event.getProcessingNanoseconds(), event.getCpuNanoseconds());
                } else {
                    event.setCpuNanoseconds(estimateCpuTime(event.getProcessingNanoseconds()));
                }

                return event;
            }

            @Override
            public PerformanceTracker getPerformanceTracker() {
                return performanceTracker;
            }
        };
    }

    private void addSampledCpuTime(final long processingNanoseconds, final long cpuNanoseconds) {
        boolean updated = false;
        while (!updated) {
            final SampledCpuMetrics existingMetrics = sampledCpuMetrics.get();
            final SampledCpuMetrics newMetrics = existingMetrics.add(processingNanoseconds, cpuNanoseconds);
            updated = sampledCpuMetrics.compareAndSet(existingMetrics, newMetrics);
        }
    }

    private long estimateCpuTime(final long processingNanoseconds) {
        final SampledCpuMetrics metrics = sampledCpuMetrics.get();
        if (metrics.processingNanos() == 0) {
            return 0; // Avoid division by zero
        }

        // Estimate CPU time based on the ratio of sampled CPU time to processing time
        return (processingNanoseconds * metrics.cpuNanos()) / metrics.processingNanos();
    }

    private record SampledCpuMetrics(long processingNanos, long cpuNanos) {

        SampledCpuMetrics add(long processingNanos, long cpuNanos) {
            return new SampledCpuMetrics(
                this.processingNanos + processingNanos,
                this.cpuNanos + cpuNanos
            );
        }
    }
}
