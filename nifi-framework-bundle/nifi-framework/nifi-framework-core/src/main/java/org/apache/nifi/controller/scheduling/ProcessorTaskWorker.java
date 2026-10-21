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
package org.apache.nifi.controller.scheduling;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A virtual thread that repeatedly invokes a Processor that uses automatic scheduling. A worker is retired when the Processor's
 * concurrent tasks are lowered, and it stops once it notices that it has been retired.
 */
final class ProcessorTaskWorker {
    private final long identifier;
    private final ProcessorAutoSchedulingState state;
    private final AtomicBoolean retired = new AtomicBoolean();

    ProcessorTaskWorker(final long identifier, final ProcessorAutoSchedulingState state) {
        this.identifier = identifier;
        this.state = state;
    }

    ProcessorAutoSchedulingState getState() {
        return state;
    }

    long getIdentifier() {
        return identifier;
    }

    boolean isRetired() {
        return retired.get();
    }

    void retire() {
        retired.set(true);
    }

    /**
     * @return <code>true</code> if the Processor is still scheduled to run and this worker has not been retired
     */
    boolean isActive() {
        return state.isScheduledToRun() && !isRetired();
    }

    /**
     * @return <code>true</code> if this worker is still active and the Processor's automatic scheduling settings are still the given settings
     */
    boolean canKeepRunning(final SchedulingSettings schedulingSettings) {
        return isActive() && state.areSchedulingSettingsCurrent(schedulingSettings);
    }
}
