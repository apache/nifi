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

import org.apache.nifi.controller.scheduling.SchedulingSettings;

public final class ProcessorSchedulingSnapshot {
    private final long timestampNanos;
    private final long measurementWindowNanos;
    private final SchedulingSettings appliedSettings;
    private final long committedFlowFiles;
    private final long completedInvocations;
    private final long failedInvocations;
    private final long processorInvocationNanos;
    private final long backpressureNanos;
    private final long processorYieldNanos;
    private final double concurrentTaskUtilization;
    private final boolean processorReady;
    private final boolean inputQueueHasFlowFiles;
    private final double inputQueueGrowth;
    private final boolean sourceProcessorRecentlyReportedActivity;
    private final boolean primaryNodeChanged;

    private ProcessorSchedulingSnapshot(final Builder builder) {
        timestampNanos = builder.timestampNanos;
        measurementWindowNanos = builder.measurementWindowNanos;
        appliedSettings = builder.appliedSettings;
        committedFlowFiles = builder.committedFlowFiles;
        completedInvocations = builder.completedInvocations;
        failedInvocations = builder.failedInvocations;
        processorInvocationNanos = builder.processorInvocationNanos;
        backpressureNanos = builder.backpressureNanos;
        processorYieldNanos = builder.processorYieldNanos;
        concurrentTaskUtilization = builder.concurrentTaskUtilization;
        processorReady = builder.processorReady;
        inputQueueHasFlowFiles = builder.inputQueueHasFlowFiles;
        inputQueueGrowth = builder.inputQueueGrowth;
        sourceProcessorRecentlyReportedActivity = builder.sourceProcessorRecentlyReportedActivity;
        primaryNodeChanged = builder.primaryNodeChanged;
    }

    public static Builder createBuilder() {
        return new Builder();
    }

    public long timestampNanos() {
        return timestampNanos;
    }

    public long measurementWindowNanos() {
        return measurementWindowNanos;
    }

    public SchedulingSettings appliedSettings() {
        return appliedSettings;
    }

    public long committedFlowFiles() {
        return committedFlowFiles;
    }

    public long completedInvocations() {
        return completedInvocations;
    }

    public long failedInvocations() {
        return failedInvocations;
    }

    public long processorInvocationNanos() {
        return processorInvocationNanos;
    }

    public long backpressureNanos() {
        return backpressureNanos;
    }

    public long processorYieldNanos() {
        return processorYieldNanos;
    }

    public double concurrentTaskUtilization() {
        return concurrentTaskUtilization;
    }

    public boolean processorReady() {
        return processorReady;
    }

    public boolean inputQueueHasFlowFiles() {
        return inputQueueHasFlowFiles;
    }

    public double inputQueueGrowth() {
        return inputQueueGrowth;
    }

    public boolean sourceProcessorRecentlyReportedActivity() {
        return sourceProcessorRecentlyReportedActivity;
    }

    public boolean primaryNodeChanged() {
        return primaryNodeChanged;
    }

    public static final class Builder {
        private long timestampNanos;
        private long measurementWindowNanos;
        private SchedulingSettings appliedSettings;
        private long committedFlowFiles;
        private long completedInvocations;
        private long failedInvocations;
        private long processorInvocationNanos;
        private long backpressureNanos;
        private long processorYieldNanos;
        private double concurrentTaskUtilization;
        private boolean processorReady;
        private boolean inputQueueHasFlowFiles;
        private double inputQueueGrowth;
        private boolean sourceProcessorRecentlyReportedActivity;
        private boolean primaryNodeChanged;

        private Builder() {
        }

        public Builder setTimestampNanos(final long timestampNanos) {
            this.timestampNanos = timestampNanos;
            return this;
        }

        public Builder setMeasurementWindowNanos(final long measurementWindowNanos) {
            this.measurementWindowNanos = measurementWindowNanos;
            return this;
        }

        public Builder setAppliedSettings(final SchedulingSettings appliedSettings) {
            this.appliedSettings = appliedSettings;
            return this;
        }

        public Builder setCommittedFlowFiles(final long committedFlowFiles) {
            this.committedFlowFiles = committedFlowFiles;
            return this;
        }

        public Builder setCompletedInvocations(final long completedInvocations) {
            this.completedInvocations = completedInvocations;
            return this;
        }

        public Builder setFailedInvocations(final long failedInvocations) {
            this.failedInvocations = failedInvocations;
            return this;
        }

        public Builder setProcessorInvocationNanos(final long processorInvocationNanos) {
            this.processorInvocationNanos = processorInvocationNanos;
            return this;
        }

        public Builder setBackpressureNanos(final long backpressureNanos) {
            this.backpressureNanos = backpressureNanos;
            return this;
        }

        public Builder setProcessorYieldNanos(final long processorYieldNanos) {
            this.processorYieldNanos = processorYieldNanos;
            return this;
        }

        public Builder setConcurrentTaskUtilization(final double concurrentTaskUtilization) {
            this.concurrentTaskUtilization = concurrentTaskUtilization;
            return this;
        }

        public Builder setProcessorReady(final boolean processorReady) {
            this.processorReady = processorReady;
            return this;
        }

        public Builder setInputQueueHasFlowFiles(final boolean inputQueueHasFlowFiles) {
            this.inputQueueHasFlowFiles = inputQueueHasFlowFiles;
            return this;
        }

        public Builder setInputQueueGrowth(final double inputQueueGrowth) {
            this.inputQueueGrowth = inputQueueGrowth;
            return this;
        }

        public Builder setSourceProcessorRecentlyReportedActivity(final boolean sourceProcessorRecentlyReportedActivity) {
            this.sourceProcessorRecentlyReportedActivity = sourceProcessorRecentlyReportedActivity;
            return this;
        }

        public Builder setPrimaryNodeChanged(final boolean primaryNodeChanged) {
            this.primaryNodeChanged = primaryNodeChanged;
            return this;
        }

        public ProcessorSchedulingSnapshot build() {
            return new ProcessorSchedulingSnapshot(this);
        }
    }
}
