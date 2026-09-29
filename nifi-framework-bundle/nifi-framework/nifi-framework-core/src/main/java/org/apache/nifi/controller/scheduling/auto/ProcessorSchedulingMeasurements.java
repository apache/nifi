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
    private final LongAdder committedInputFlowFiles = new LongAdder();
    private final LongAdder completedInvocations = new LongAdder();
    private final LongAdder failedInvocations = new LongAdder();
    private final LongAdder processorInvocationNanos = new LongAdder();
    private final LongSupplier nanoTimeSupplier;
    private final Object busyTaskLock = new Object();
    private int busyTasks;
    private long busyTaskChangeNanos;
    private long busyTaskWindowStartNanos;
    private long busyTaskNanos;
    private long previousCommittedFlowFiles;
    private long previousCommittedInputFlowFiles;
    private long previousCompletedInvocations;
    private long previousFailedInvocations;
    private long previousProcessorInvocationNanos;
    private ProcessorSchedulingSnapshot.RecentDemand recentDemand = ProcessorSchedulingSnapshot.RecentDemand.NONE;
    private long backpressureNanos;
    private long processorYieldNanos;

    public ProcessorSchedulingMeasurements() {
        this(System::nanoTime);
    }

    ProcessorSchedulingMeasurements(final LongSupplier nanoTimeSupplier) {
        this.nanoTimeSupplier = nanoTimeSupplier;
        busyTaskChangeNanos = nanoTimeSupplier.getAsLong();
        busyTaskWindowStartNanos = busyTaskChangeNanos;
    }

    public InvocationObserver createInvocationObserver() {
        return new InvocationMeasurementObserver();
    }

    public void recordInvocationDuration(final long nanos) {
        processorInvocationNanos.add(Math.max(0L, nanos));
    }

    /**
     * Records that a concurrent task has become busy. A task is busy from the time it starts waiting for a global permit
     * until its invocation finishes, because a task waiting for a permit is ready to work.
     */
    public void recordTaskBusy() {
        synchronized (busyTaskLock) {
            accumulateBusyTaskNanos();
            busyTasks++;
        }
    }

    public void recordTaskIdle() {
        synchronized (busyTaskLock) {
            accumulateBusyTaskNanos();
            busyTasks--;
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
     * Captures the measurements recorded since the previous snapshot. Concurrent task utilization is the total time that concurrent
     * tasks were busy since the previous snapshot, including tasks that are still busy, divided by the time available to the allowed
     * concurrent tasks over that same period. Utilization periods begin and end at times read while holding the lock that also guards
     * changes in the number of busy tasks, so each moment of busy time belongs to exactly one snapshot even when the caller's timestamp
     * was read before a task became busy.
     */
    public ProcessorSchedulingSnapshot captureSnapshot(final long timestampNanos, final long measurementWindowNanos, final SchedulingSettings settings,
                                                       final boolean processorReady, final boolean inputQueueHasFlowFiles, final long localInputQueueCount,
                                                       final double inputQueueGrowth, final boolean sourceProcessorRecentlyReportedActivity,
                                                       final boolean primaryNodeChanged, final boolean globalCapacityTestAllowed) {
        final long committedFlowFileCount = committedFlowFiles.sum();
        final long committedInputFlowFileCount = committedInputFlowFiles.sum();
        final long completedInvocationCount = completedInvocations.sum();
        final long failedInvocationCount = failedInvocations.sum();
        final long processorInvocationDurationNanos = processorInvocationNanos.sum();
        final long windowBusyTaskNanos;
        final long utilizationWindowNanos;
        synchronized (busyTaskLock) {
            accumulateBusyTaskNanos();
            windowBusyTaskNanos = busyTaskNanos;
            utilizationWindowNanos = busyTaskChangeNanos - busyTaskWindowStartNanos;
            busyTaskNanos = 0L;
            busyTaskWindowStartNanos = busyTaskChangeNanos;
        }

        final long completedInvocationDelta = completedInvocationCount - previousCompletedInvocations;
        final long failedInvocationDelta = failedInvocationCount - previousFailedInvocations;
        recentDemand = recentDemand.add(measurementWindowNanos, committedInputFlowFileCount - previousCommittedInputFlowFiles, completedInvocationDelta,
                failedInvocationDelta);

        final double concurrentTaskUtilization = Math.min(1D,
                windowBusyTaskNanos / ((double) Math.max(1L, utilizationWindowNanos) * settings.concurrentTasks()));
        final ProcessorSchedulingSnapshot snapshot = ProcessorSchedulingSnapshot.createBuilder()
                .setTimestampNanos(timestampNanos)
                .setMeasurementWindowNanos(measurementWindowNanos)
                .setAppliedSettings(settings)
                .setCommittedFlowFiles(committedFlowFileCount - previousCommittedFlowFiles)
                .setCompletedInvocations(completedInvocationDelta)
                .setFailedInvocations(failedInvocationDelta)
                .setProcessorInvocationNanos(processorInvocationDurationNanos - previousProcessorInvocationNanos)
                .setBackpressureNanos(backpressureNanos)
                .setProcessorYieldNanos(processorYieldNanos)
                .setConcurrentTaskUtilization(concurrentTaskUtilization)
                .setProcessorReady(processorReady)
                .setInputQueueHasFlowFiles(inputQueueHasFlowFiles)
                .setInputQueueGrowth(inputQueueGrowth)
                .setSourceProcessorRecentlyReportedActivity(sourceProcessorRecentlyReportedActivity)
                .setPrimaryNodeChanged(primaryNodeChanged)
                .setLocalInputQueueCount(localInputQueueCount)
                .setRecentDemand(recentDemand)
                .setGlobalCapacityTestAllowed(globalCapacityTestAllowed)
                .build();
        previousCommittedFlowFiles = committedFlowFileCount;
        previousCommittedInputFlowFiles = committedInputFlowFileCount;
        previousCompletedInvocations = completedInvocationCount;
        previousFailedInvocations = failedInvocationCount;
        previousProcessorInvocationNanos = processorInvocationDurationNanos;
        backpressureNanos = 0L;
        processorYieldNanos = 0L;
        return snapshot;
    }

    /**
     * Adds the busy time since the last change. Must be called while holding the busy task lock; reading the
     * clock under the lock keeps the change timestamp from moving backward.
     */
    private void accumulateBusyTaskNanos() {
        final long nowNanos = nanoTimeSupplier.getAsLong();
        busyTaskNanos += busyTasks * (nowNanos - busyTaskChangeNanos);
        busyTaskChangeNanos = nowNanos;
    }

    private void recordCompletedInvocation(final InvocationOutcome outcome) {
        if (outcome == InvocationOutcome.INVOKED_WITH_ACTIVITY || outcome == InvocationOutcome.INVOKED_WITHOUT_ACTIVITY
                || outcome == InvocationOutcome.FAILED) {
            completedInvocations.increment();
        }
    }

    private long recordCommittedFlowFiles(final CommittedSchedulingWork committedWork) {
        committedInputFlowFiles.add(committedWork.inputFlowFiles());
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
