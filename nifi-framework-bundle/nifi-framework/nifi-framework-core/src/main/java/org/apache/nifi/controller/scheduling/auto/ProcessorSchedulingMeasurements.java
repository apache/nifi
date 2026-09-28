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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

public class ProcessorSchedulingMeasurements {
    private final LongAdder committedFlowFiles = new LongAdder();
    private final LongAdder completedInvocations = new LongAdder();
    private final LongAdder failedInvocations = new LongAdder();
    private final LongAdder processorInvocationNanos = new LongAdder();
    private final LongSupplier nanoTimeSupplier;
    private final Object activeInvocationLock = new Object();
    private int activeInvocations;
    private long activeInvocationChangeNanos;
    private long activeInvocationWindowStartNanos;
    private long activeInvocationNanos;
    private long previousCommittedFlowFiles;
    private long previousCompletedInvocations;
    private long previousFailedInvocations;
    private long previousProcessorInvocationNanos;
    private long backpressureNanos;
    private long processorYieldNanos;

    public ProcessorSchedulingMeasurements() {
        this(System::nanoTime);
    }

    ProcessorSchedulingMeasurements(final LongSupplier nanoTimeSupplier) {
        this.nanoTimeSupplier = nanoTimeSupplier;
        activeInvocationChangeNanos = nanoTimeSupplier.getAsLong();
        activeInvocationWindowStartNanos = activeInvocationChangeNanos;
    }

    public InvocationObserver createInvocationObserver() {
        return new InvocationMeasurementObserver();
    }

    public void recordInvocationDuration(final long nanos) {
        processorInvocationNanos.add(Math.max(0L, nanos));
    }

    public void recordInvocationStarted() {
        synchronized (activeInvocationLock) {
            accumulateActiveInvocationNanos();
            activeInvocations++;
        }
    }

    public void recordInvocationFinished() {
        synchronized (activeInvocationLock) {
            accumulateActiveInvocationNanos();
            activeInvocations--;
        }
    }

    public void recordReadiness(final InvocationOutcome readinessOutcome, final long measurementNanos) {
        if (readinessOutcome == InvocationOutcome.BACKPRESSURED) {
            backpressureNanos += measurementNanos;
        } else if (readinessOutcome == InvocationOutcome.YIELDED) {
            processorYieldNanos += measurementNanos;
        }
    }

    /**
     * Captures the measurements recorded since the previous snapshot. Concurrent task utilization is the total time spent in
     * invocations since the previous snapshot, including invocations that are still running, divided by the time available
     * to the allowed concurrent tasks over that same period. Utilization periods begin and end at times read while holding the
     * lock that also guards invocation start and finish, so each moment of invocation time belongs to exactly one snapshot even
     * when the caller's timestamp was read before an invocation started.
     */
    public ProcessorSchedulingSnapshot captureSnapshot(final long timestampNanos, final long measurementWindowNanos, final SchedulingSettings settings,
                                                       final boolean processorReady, final boolean inputQueueHasFlowFiles, final double inputQueueGrowth,
                                                       final boolean sourceProcessorRecentlyReportedActivity, final boolean primaryNodeChanged) {
        final long committedFlowFileCount = committedFlowFiles.sum();
        final long completedInvocationCount = completedInvocations.sum();
        final long failedInvocationCount = failedInvocations.sum();
        final long processorInvocationDurationNanos = processorInvocationNanos.sum();
        final long windowActiveInvocationNanos;
        final long utilizationWindowNanos;
        synchronized (activeInvocationLock) {
            accumulateActiveInvocationNanos();
            windowActiveInvocationNanos = activeInvocationNanos;
            utilizationWindowNanos = activeInvocationChangeNanos - activeInvocationWindowStartNanos;
            activeInvocationNanos = 0L;
            activeInvocationWindowStartNanos = activeInvocationChangeNanos;
        }

        final double concurrentTaskUtilization = Math.min(1D,
                windowActiveInvocationNanos / ((double) Math.max(1L, utilizationWindowNanos) * settings.concurrentTasks()));
        final ProcessorSchedulingSnapshot snapshot = ProcessorSchedulingSnapshot.createBuilder()
                .setTimestampNanos(timestampNanos)
                .setMeasurementWindowNanos(measurementWindowNanos)
                .setAppliedSettings(settings)
                .setCommittedFlowFiles(committedFlowFileCount - previousCommittedFlowFiles)
                .setCompletedInvocations(completedInvocationCount - previousCompletedInvocations)
                .setFailedInvocations(failedInvocationCount - previousFailedInvocations)
                .setProcessorInvocationNanos(processorInvocationDurationNanos - previousProcessorInvocationNanos)
                .setBackpressureNanos(backpressureNanos)
                .setProcessorYieldNanos(processorYieldNanos)
                .setConcurrentTaskUtilization(concurrentTaskUtilization)
                .setProcessorReady(processorReady)
                .setInputQueueHasFlowFiles(inputQueueHasFlowFiles)
                .setInputQueueGrowth(inputQueueGrowth)
                .setSourceProcessorRecentlyReportedActivity(sourceProcessorRecentlyReportedActivity)
                .setPrimaryNodeChanged(primaryNodeChanged)
                .build();
        previousCommittedFlowFiles = committedFlowFileCount;
        previousCompletedInvocations = completedInvocationCount;
        previousFailedInvocations = failedInvocationCount;
        previousProcessorInvocationNanos = processorInvocationDurationNanos;
        backpressureNanos = 0L;
        processorYieldNanos = 0L;
        return snapshot;
    }

    /**
     * Adds the invocation time since the last change. Must be called while holding the active invocation lock; reading the
     * clock under the lock keeps the change timestamp from moving backward.
     */
    private void accumulateActiveInvocationNanos() {
        final long nowNanos = nanoTimeSupplier.getAsLong();
        activeInvocationNanos += activeInvocations * (nowNanos - activeInvocationChangeNanos);
        activeInvocationChangeNanos = nowNanos;
    }

    private void recordCompletedInvocation(final InvocationOutcome outcome) {
        if (outcome == InvocationOutcome.INVOKED_WITH_ACTIVITY || outcome == InvocationOutcome.INVOKED_WITHOUT_ACTIVITY
                || outcome == InvocationOutcome.FAILED) {
            completedInvocations.increment();
        }
    }

    private long recordCommittedFlowFiles(final CommittedSchedulingWork committedWork) {
        // Count committed input FlowFiles when present, otherwise count committed produced FlowFiles for source processors.
        final long count = committedWork.inputFlowFiles() > 0L ? committedWork.inputFlowFiles() : committedWork.producedFlowFiles();
        committedFlowFiles.add(count);
        return count;
    }

    /**
     * Observes a single invocation. The invocation is counted as failed at most once, whether the failure is reported by a
     * failed session commit, by the invocation result, or by both, so that failed invocations and completed invocations count
     * the same thing.
     */
    private class InvocationMeasurementObserver implements InvocationObserver {
        private final AtomicBoolean failureRecorded = new AtomicBoolean();
        private volatile boolean activityObserved;
        private volatile boolean triggerActivityObserved;

        @Override
        public boolean isActivityObserved() {
            return activityObserved;
        }

        @Override
        public void resetTriggerActivity() {
            triggerActivityObserved = false;
        }

        @Override
        public boolean isTriggerActivityObserved() {
            return triggerActivityObserved;
        }

        @Override
        public void onInvocationCompleted(final InvocationResult result) {
            recordCompletedInvocation(result.getOutcome());
            if (result.getOutcome() == InvocationOutcome.FAILED) {
                recordFailure();
            }
        }

        @Override
        public void onActivity() {
            activityObserved = true;
            triggerActivityObserved = true;
        }

        @Override
        public void onCommit(final CommittedSchedulingWork committedWork) {
            final long count = recordCommittedFlowFiles(committedWork);
            if (count > 0L) {
                onActivity();
            }
        }

        @Override
        public void onCommitFailure() {
            recordFailure();
        }

        private void recordFailure() {
            if (failureRecorded.compareAndSet(false, true)) {
                failedInvocations.increment();
            }
        }
    }
}
