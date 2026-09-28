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

public final class AutoSchedulingDiagnostics {
    private final String executionMode;
    private final int maxConcurrentTasks;
    private final int currentConcurrentTasks;
    private final int activeProcessorInvocations;
    private final long currentRunDurationMillis;
    private final String concurrencyEvaluationState;
    private final String concurrencyUpdateReason;
    private final double flowFilesPerSecond;
    private final long measurementWindowMillis;
    private final String lastConcurrencyUpdateReason;
    private final boolean collectingMeasurements;
    private final boolean flowFileMeasurementsAvailable;
    private final String concurrencyIncreaseExplanation;

    private AutoSchedulingDiagnostics(final Builder builder) {
        executionMode = builder.executionMode;
        maxConcurrentTasks = builder.maxConcurrentTasks;
        currentConcurrentTasks = builder.currentConcurrentTasks;
        activeProcessorInvocations = builder.activeProcessorInvocations;
        currentRunDurationMillis = builder.currentRunDurationMillis;
        concurrencyEvaluationState = builder.concurrencyEvaluationState;
        concurrencyUpdateReason = builder.concurrencyUpdateReason;
        flowFilesPerSecond = builder.flowFilesPerSecond;
        measurementWindowMillis = builder.measurementWindowMillis;
        lastConcurrencyUpdateReason = builder.lastConcurrencyUpdateReason;
        collectingMeasurements = builder.collectingMeasurements;
        flowFileMeasurementsAvailable = builder.flowFileMeasurementsAvailable;
        concurrencyIncreaseExplanation = builder.concurrencyIncreaseExplanation;
    }

    public static Builder createBuilder() {
        return new Builder();
    }

    public String executionMode() {
        return executionMode;
    }

    public int maxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    public int currentConcurrentTasks() {
        return currentConcurrentTasks;
    }

    public int activeProcessorInvocations() {
        return activeProcessorInvocations;
    }

    public long currentRunDurationMillis() {
        return currentRunDurationMillis;
    }

    public String concurrencyEvaluationState() {
        return concurrencyEvaluationState;
    }

    public String concurrencyUpdateReason() {
        return concurrencyUpdateReason;
    }

    public double flowFilesPerSecond() {
        return flowFilesPerSecond;
    }

    public long measurementWindowMillis() {
        return measurementWindowMillis;
    }

    public String lastConcurrencyUpdateReason() {
        return lastConcurrencyUpdateReason;
    }

    public boolean collectingMeasurements() {
        return collectingMeasurements;
    }

    public boolean flowFileMeasurementsAvailable() {
        return flowFileMeasurementsAvailable;
    }

    public String concurrencyIncreaseExplanation() {
        return concurrencyIncreaseExplanation;
    }

    public static final class Builder {
        private String executionMode;
        private int maxConcurrentTasks;
        private int currentConcurrentTasks;
        private int activeProcessorInvocations;
        private long currentRunDurationMillis;
        private String concurrencyEvaluationState;
        private String concurrencyUpdateReason;
        private double flowFilesPerSecond;
        private long measurementWindowMillis;
        private String lastConcurrencyUpdateReason;
        private boolean collectingMeasurements;
        private boolean flowFileMeasurementsAvailable;
        private String concurrencyIncreaseExplanation;

        private Builder() {
        }

        public Builder setExecutionMode(final String executionMode) {
            this.executionMode = executionMode;
            return this;
        }

        public Builder setMaxConcurrentTasks(final int maxConcurrentTasks) {
            this.maxConcurrentTasks = maxConcurrentTasks;
            return this;
        }

        public Builder setCurrentConcurrentTasks(final int currentConcurrentTasks) {
            this.currentConcurrentTasks = currentConcurrentTasks;
            return this;
        }

        public Builder setActiveProcessorInvocations(final int activeProcessorInvocations) {
            this.activeProcessorInvocations = activeProcessorInvocations;
            return this;
        }

        public Builder setCurrentRunDurationMillis(final long currentRunDurationMillis) {
            this.currentRunDurationMillis = currentRunDurationMillis;
            return this;
        }

        public Builder setConcurrencyEvaluationState(final String concurrencyEvaluationState) {
            this.concurrencyEvaluationState = concurrencyEvaluationState;
            return this;
        }

        public Builder setConcurrencyUpdateReason(final String concurrencyUpdateReason) {
            this.concurrencyUpdateReason = concurrencyUpdateReason;
            return this;
        }

        public Builder setFlowFilesPerSecond(final double flowFilesPerSecond) {
            this.flowFilesPerSecond = flowFilesPerSecond;
            return this;
        }

        public Builder setMeasurementWindowMillis(final long measurementWindowMillis) {
            this.measurementWindowMillis = measurementWindowMillis;
            return this;
        }

        public Builder setLastConcurrencyUpdateReason(final String lastConcurrencyUpdateReason) {
            this.lastConcurrencyUpdateReason = lastConcurrencyUpdateReason;
            return this;
        }

        public Builder setCollectingMeasurements(final boolean collectingMeasurements) {
            this.collectingMeasurements = collectingMeasurements;
            return this;
        }

        public Builder setFlowFileMeasurementsAvailable(final boolean flowFileMeasurementsAvailable) {
            this.flowFileMeasurementsAvailable = flowFileMeasurementsAvailable;
            return this;
        }

        public Builder setConcurrencyIncreaseExplanation(final String concurrencyIncreaseExplanation) {
            this.concurrencyIncreaseExplanation = concurrencyIncreaseExplanation;
            return this;
        }

        public AutoSchedulingDiagnostics build() {
            return new AutoSchedulingDiagnostics(this);
        }
    }
}
