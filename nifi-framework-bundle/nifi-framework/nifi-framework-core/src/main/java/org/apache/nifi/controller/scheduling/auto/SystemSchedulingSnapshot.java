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

public final class SystemSchedulingSnapshot {
    private final boolean cpuLoadAvailable;
    private final double cpuLoad;
    private final double averageCpuLoad;
    private final double fractionOfTimeSpentOnGarbageCollection;
    private final int maxGlobalConcurrentTasks;
    private final double fractionOfSamplesWithAllGlobalTaskSlotsUsed;
    private final double fractionOfGlobalTaskTimeSpentWaiting;
    private final boolean cpuLoadAboveLimit;
    private final boolean garbageCollectionTimeAboveLimit;

    private SystemSchedulingSnapshot(final Builder builder) {
        cpuLoadAvailable = builder.cpuLoadAvailable;
        cpuLoad = builder.cpuLoad;
        averageCpuLoad = builder.averageCpuLoad;
        fractionOfTimeSpentOnGarbageCollection = builder.fractionOfTimeSpentOnGarbageCollection;
        maxGlobalConcurrentTasks = builder.maxGlobalConcurrentTasks;
        fractionOfSamplesWithAllGlobalTaskSlotsUsed = builder.fractionOfSamplesWithAllGlobalTaskSlotsUsed;
        fractionOfGlobalTaskTimeSpentWaiting = builder.fractionOfGlobalTaskTimeSpentWaiting;
        cpuLoadAboveLimit = builder.cpuLoadAboveLimit;
        garbageCollectionTimeAboveLimit = builder.garbageCollectionTimeAboveLimit;
    }

    public static Builder createBuilder() {
        return new Builder();
    }

    public boolean cpuLoadAvailable() {
        return cpuLoadAvailable;
    }

    public double cpuLoad() {
        return cpuLoad;
    }

    public double averageCpuLoad() {
        return averageCpuLoad;
    }

    public double fractionOfTimeSpentOnGarbageCollection() {
        return fractionOfTimeSpentOnGarbageCollection;
    }

    public int maxGlobalConcurrentTasks() {
        return maxGlobalConcurrentTasks;
    }

    public double fractionOfSamplesWithAllGlobalTaskSlotsUsed() {
        return fractionOfSamplesWithAllGlobalTaskSlotsUsed;
    }

    public double fractionOfGlobalTaskTimeSpentWaiting() {
        return fractionOfGlobalTaskTimeSpentWaiting;
    }

    public boolean cpuLoadAboveLimit() {
        return cpuLoadAboveLimit;
    }

    public boolean garbageCollectionTimeAboveLimit() {
        return garbageCollectionTimeAboveLimit;
    }

    public static final class Builder {
        private boolean cpuLoadAvailable;
        private double cpuLoad;
        private double averageCpuLoad;
        private double fractionOfTimeSpentOnGarbageCollection;
        private int maxGlobalConcurrentTasks;
        private double fractionOfSamplesWithAllGlobalTaskSlotsUsed;
        private double fractionOfGlobalTaskTimeSpentWaiting;
        private boolean cpuLoadAboveLimit;
        private boolean garbageCollectionTimeAboveLimit;

        private Builder() {
        }

        public Builder setCpuLoadAvailable(final boolean cpuLoadAvailable) {
            this.cpuLoadAvailable = cpuLoadAvailable;
            return this;
        }

        public Builder setCpuLoad(final double cpuLoad) {
            this.cpuLoad = cpuLoad;
            return this;
        }

        public Builder setAverageCpuLoad(final double averageCpuLoad) {
            this.averageCpuLoad = averageCpuLoad;
            return this;
        }

        public Builder setFractionOfTimeSpentOnGarbageCollection(final double fractionOfTimeSpentOnGarbageCollection) {
            this.fractionOfTimeSpentOnGarbageCollection = fractionOfTimeSpentOnGarbageCollection;
            return this;
        }

        public Builder setMaxGlobalConcurrentTasks(final int maxGlobalConcurrentTasks) {
            this.maxGlobalConcurrentTasks = maxGlobalConcurrentTasks;
            return this;
        }

        public Builder setFractionOfSamplesWithAllGlobalTaskSlotsUsed(final double fractionOfSamplesWithAllGlobalTaskSlotsUsed) {
            this.fractionOfSamplesWithAllGlobalTaskSlotsUsed = fractionOfSamplesWithAllGlobalTaskSlotsUsed;
            return this;
        }

        public Builder setFractionOfGlobalTaskTimeSpentWaiting(final double fractionOfGlobalTaskTimeSpentWaiting) {
            this.fractionOfGlobalTaskTimeSpentWaiting = fractionOfGlobalTaskTimeSpentWaiting;
            return this;
        }

        public Builder setCpuLoadAboveLimit(final boolean cpuLoadAboveLimit) {
            this.cpuLoadAboveLimit = cpuLoadAboveLimit;
            return this;
        }

        public Builder setGarbageCollectionTimeAboveLimit(final boolean garbageCollectionTimeAboveLimit) {
            this.garbageCollectionTimeAboveLimit = garbageCollectionTimeAboveLimit;
            return this;
        }

        public SystemSchedulingSnapshot build() {
            return new SystemSchedulingSnapshot(this);
        }
    }
}
