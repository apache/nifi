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
package org.apache.nifi.controller;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface SchedulingAgentCallback {
    void onTaskComplete();

    Future<?> scheduleTask(Callable<?> task);

    void trigger();

    /**
     * Cancels the start that this callback represents by completing its start future exceptionally. This allows a caller
     * that is waiting on the start future to be released when a Processor is stopped while it is still starting, even if
     * its {@code @OnScheduled} method never returns.
     */
    default void cancelStart() {
    }

    /**
     * @return {@code true} if the LifecycleState captured for this start has been terminated. A start whose LifecycleState
     * has been terminated must not transition the Processor to RUNNING, invoke {@link #trigger()}, schedule another start
     * attempt, or complete a later stop, because the Processor instance and context it holds have been abandoned.
     */
    default boolean isStartTerminated() {
        return false;
    }
}
