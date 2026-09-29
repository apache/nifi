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

import org.apache.nifi.controller.scheduling.CommittedSchedulingWork;
import org.apache.nifi.controller.scheduling.SchedulingSettings;
import org.apache.nifi.controller.tasks.InvocationObserver;
import org.apache.nifi.controller.tasks.InvocationOutcome;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessorSchedulingMeasurementsTest {
    @Test
    void testRetainedObserverReportsAllCommitsAcrossConcurrencyChanges() {
        final ProcessorSchedulingMeasurements measurements = new ProcessorSchedulingMeasurements();
        final InvocationObserver retainedObserver = measurements.createInvocationObserver();
        retainedObserver.onInvocationCompleted(InvocationResult.completed(InvocationOutcome.INVOKED_WITHOUT_ACTIVITY));
        for (int second = 1; second <= 10; second++) {
            retainedObserver.onCommit(new CommittedSchedulingWork(0, 100));
            final ProcessorSchedulingSnapshot snapshot = captureSnapshot(measurements, second);
            assertEquals(100, snapshot.committedFlowFiles());
        }
    }

    @Test
    void testOnlyCommittedWorkContributesToThroughput() {
        final ProcessorSchedulingMeasurements measurements = new ProcessorSchedulingMeasurements();
        final InvocationObserver observer = measurements.createInvocationObserver();
        observer.onActivity();
        observer.onCommitFailure();
        observer.onInvocationCompleted(InvocationResult.completed(InvocationOutcome.FAILED));
        final ProcessorSchedulingSnapshot failed = captureSnapshot(measurements, 1);
        assertEquals(0, failed.committedFlowFiles());
        assertEquals(1, failed.completedInvocations());
        assertEquals(1, failed.failedInvocations());
        observer.onCommit(new CommittedSchedulingWork(2, 10));
        final ProcessorSchedulingSnapshot committed = captureSnapshot(measurements, 2);
        assertEquals(2, committed.committedFlowFiles());
        // Until ten seconds are measured, the recent rates are the average of every one-second capture so far.
        assertEquals(new ProcessorSchedulingSnapshot.RecentDemand(1D, 0.5D, 0.5D, TimeUnit.SECONDS.toNanos(2)), committed.recentDemand());
        assertEquals(0, captureSnapshot(measurements, 3).committedFlowFiles());

        ProcessorSchedulingSnapshot later = null;
        for (int second = 0; second < 7; second++) {
            later = captureSnapshot(measurements, 3);
        }

        assertEquals(0.2D, later.recentDemand().committedInputFlowFilesPerSecond(), 0.000001D);

        // After ten seconds, each capture is weighted toward the most recent measurements, so older commits fade away.
        for (int second = 0; second < 10; second++) {
            later = captureSnapshot(measurements, 3);
        }

        assertTrue(later.recentDemand().committedInputFlowFilesPerSecond() < 0.1D);
    }

    @Test
    void testLongInvocationsContributeUtilizationBeforeCompleting() {
        final AtomicLong nowNanos = new AtomicLong();
        final ProcessorSchedulingMeasurements measurements = new ProcessorSchedulingMeasurements(nowNanos::get);
        final SchedulingSettings settings = new SchedulingSettings(2, 0L);
        measurements.recordTaskBusy();

        // The scheduling agent reads its timestamp at 1000 milliseconds, then a second task becomes busy at 1200 milliseconds before the snapshot is captured.
        final long agentTimestampNanos = TimeUnit.MILLISECONDS.toNanos(1000);
        nowNanos.set(TimeUnit.MILLISECONDS.toNanos(1200));
        measurements.recordTaskBusy();
        final ProcessorSchedulingSnapshot first = measurements.captureSnapshot(agentTimestampNanos, agentTimestampNanos, settings,
                true, true, 0L, 0D, false, false, false);
        // 1200 milliseconds of busy time out of 1200 milliseconds available to each of 2 tasks
        assertEquals(0.5D, first.concurrentTaskUtilization(), 0.000001D);
        assertEquals(0, first.completedInvocations());

        nowNanos.set(TimeUnit.MILLISECONDS.toNanos(2200));
        measurements.recordTaskIdle();
        nowNanos.set(TimeUnit.MILLISECONDS.toNanos(3200));
        final ProcessorSchedulingSnapshot second = measurements.captureSnapshot(nowNanos.get(), nowNanos.get() - agentTimestampNanos, settings,
                true, true, 0L, 0D, false, false, false);
        // 2000 milliseconds from the first task plus 1000 milliseconds from the second, out of 2000 milliseconds available to each of 2 tasks
        assertEquals(0.75D, second.concurrentTaskUtilization(), 0.000001D);
    }

    @Test
    void testBatchActivityDistinguishesEachTrigger() {
        final InvocationObserver observer = new ProcessorSchedulingMeasurements().createInvocationObserver();
        observer.onActivity();
        assertTrue(observer.isTriggerActivityObserved());
        observer.resetTriggerActivity();
        assertFalse(observer.isTriggerActivityObserved());
        assertTrue(observer.isActivityObserved());
    }

    private ProcessorSchedulingSnapshot captureSnapshot(final ProcessorSchedulingMeasurements measurements, final int concurrency) {
        return measurements.captureSnapshot(System.nanoTime(), TimeUnit.SECONDS.toNanos(1),
                new SchedulingSettings(concurrency, 0L), true, true, 0L, 0D, false, false, false);
    }
}
