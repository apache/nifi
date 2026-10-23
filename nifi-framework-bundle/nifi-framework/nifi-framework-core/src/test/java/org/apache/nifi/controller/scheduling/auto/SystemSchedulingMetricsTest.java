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
import org.junit.jupiter.api.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SystemSchedulingMetricsTest {
    private static final int MAX_GLOBAL_CONCURRENT_TASKS = 32;

    @Test
    void testCpuLoadLimitUsesAverageAndResumeReadings() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(0.995D, 0.50D, 0.50D);
        final AtomicLong nowNanos = new AtomicLong();
        final SystemSchedulingMetrics metrics = new SystemSchedulingMetrics(operatingSystemMXBean, List.of(), nowNanos::get);

        assertTrue(captureSnapshotAfterOneSecond(metrics, nowNanos).cpuLoadAboveLimit());
        assertTrue(captureSnapshotAfterOneSecond(metrics, nowNanos).cpuLoadAboveLimit());
        assertTrue(captureSnapshotAfterOneSecond(metrics, nowNanos).cpuLoadAboveLimit());
        assertFalse(captureSnapshotAfterOneSecond(metrics, nowNanos).cpuLoadAboveLimit());
    }

    @Test
    void testGarbageCollectionLimitRequiresTwoHighReadings() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(0.25D);
        final GarbageCollectorMXBean garbageCollectorMXBean = mock(GarbageCollectorMXBean.class);
        when(garbageCollectorMXBean.getName()).thenReturn("G1 Young Generation");
        when(garbageCollectorMXBean.getCollectionTime()).thenReturn(0L, 100L, 200L);
        final AtomicLong nowNanos = new AtomicLong();
        final SystemSchedulingMetrics metrics = new SystemSchedulingMetrics(
                operatingSystemMXBean, List.of(garbageCollectorMXBean), nowNanos::get);

        assertFalse(captureSnapshotAfterOneSecond(metrics, nowNanos).garbageCollectionTimeAboveLimit());
        assertTrue(captureSnapshotAfterOneSecond(metrics, nowNanos).garbageCollectionTimeAboveLimit());

        // Collectors such as ZGC report the duration of collection cycles that run alongside the application; only pauses count.
        final GarbageCollectorMXBean cycleCollectorMXBean = mock(GarbageCollectorMXBean.class);
        when(cycleCollectorMXBean.getName()).thenReturn("ZGC Major Cycles");
        when(cycleCollectorMXBean.getCollectionTime()).thenReturn(0L, 900L, 1800L);
        final SystemSchedulingMetrics cycleMetrics = new SystemSchedulingMetrics(
                operatingSystemMXBean, List.of(cycleCollectorMXBean), nowNanos::get);

        assertFalse(captureSnapshotAfterOneSecond(cycleMetrics, nowNanos).garbageCollectionTimeAboveLimit());
        assertFalse(captureSnapshotAfterOneSecond(cycleMetrics, nowNanos).garbageCollectionTimeAboveLimit());
    }

    @Test
    void testSystemCpuPressureAlsoLimitsGrowth() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getCpuLoad()).thenReturn(1D);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(0.25D);
        final AtomicLong nowNanos = new AtomicLong();
        final SystemSchedulingMetrics metrics = new SystemSchedulingMetrics(operatingSystemMXBean, List.of(), nowNanos::get);

        final SystemSchedulingSnapshot snapshot = captureSnapshotAfterOneSecond(metrics, nowNanos);

        assertEquals(1D, snapshot.cpuLoad());
        assertTrue(snapshot.cpuLoadAboveLimit());
    }

    @Test
    void testUnavailableMeasurementDoesNotRetainCpuLoadLimit() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getCpuLoad()).thenReturn(1D, -1D);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(-1D);
        when(operatingSystemMXBean.getProcessCpuTime()).thenReturn(-1L);
        final AtomicLong nowNanos = new AtomicLong();
        final SystemSchedulingMetrics metrics = new SystemSchedulingMetrics(operatingSystemMXBean, List.of(), nowNanos::get);

        assertTrue(captureSnapshotAfterOneSecond(metrics, nowNanos).cpuLoadAboveLimit());
        final SystemSchedulingSnapshot unavailable = captureSnapshotAfterOneSecond(metrics, nowNanos);
        assertFalse(unavailable.cpuLoadAvailable());
        assertFalse(unavailable.cpuLoadAboveLimit());
    }

    @Test
    void testProcessCpuTimeFallback() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getCpuLoad()).thenReturn(-1D);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(-1D);
        when(operatingSystemMXBean.getProcessCpuTime()).thenReturn(0L, TimeUnit.SECONDS.toNanos(2L));
        when(operatingSystemMXBean.getAvailableProcessors()).thenReturn(4);
        final AtomicLong nowNanos = new AtomicLong();
        final SystemSchedulingMetrics metrics = new SystemSchedulingMetrics(operatingSystemMXBean, List.of(), nowNanos::get);

        final SystemSchedulingSnapshot snapshot = captureSnapshotAfterOneSecond(metrics, nowNanos);

        assertTrue(snapshot.cpuLoadAvailable());
        assertEquals(0.50D, snapshot.cpuLoad());
    }

    private SystemSchedulingSnapshot captureSnapshotAfterOneSecond(final SystemSchedulingMetrics metrics, final AtomicLong nowNanos) {
        nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        return metrics.captureSnapshot(MAX_GLOBAL_CONCURRENT_TASKS, 0.25D, 0D);
    }
}
