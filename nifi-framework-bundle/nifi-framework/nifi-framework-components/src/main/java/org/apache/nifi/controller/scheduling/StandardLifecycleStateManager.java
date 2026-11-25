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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StandardLifecycleStateManager implements LifecycleStateManager {
    private final ConcurrentMap<String, LifecycleState> lifecycleStates = new ConcurrentHashMap<>();

    @Override
    public LifecycleState getOrRegisterLifecycleState(final String componentId, final boolean replaceTerminatedState, final boolean replaceUnscheduledState) {
        Objects.requireNonNull(componentId);

        LifecycleState lifecycleState;
        while (true) {
            lifecycleState = this.lifecycleStates.get(componentId);

            if (lifecycleState == null) {
                lifecycleState = new LifecycleState(componentId);
                final LifecycleState existing = this.lifecycleStates.putIfAbsent(componentId, lifecycleState);

                if (existing == null) {
                    break;
                } else {
                    continue;
                }
            } else if (isReplace(lifecycleState, replaceTerminatedState, replaceUnscheduledState)) {
                final LifecycleState newLifecycleState = new LifecycleState(componentId);
                final boolean replaced = this.lifecycleStates.replace(componentId, lifecycleState, newLifecycleState);

                if (replaced) {
                    lifecycleState = newLifecycleState;
                    break;
                } else {
                    continue;
                }
            } else {
                break;
            }
        }

        return lifecycleState;
    }

    private boolean isReplace(final LifecycleState lifecycleState, final boolean replaceTerminated, final boolean replaceUnscheduled) {
        if (replaceTerminated && lifecycleState.isTerminated()) {
            return true;
        }
        if (replaceUnscheduled && !lifecycleState.isScheduled()) {
            return true;
        }
        return false;
    }

    @Override
    public Optional<LifecycleState> getLifecycleState(final String componentId) {
        if (componentId == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(lifecycleStates.get(componentId));
    }

    @Override
    public Optional<LifecycleState> removeLifecycleState(final String componentId) {
        if (componentId == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(lifecycleStates.remove(componentId));
    }
}
