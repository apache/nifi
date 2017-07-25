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

package org.apache.nifi.controller.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ServiceStateTransition {
    private ControllerServiceState state = ControllerServiceState.DISABLED;
    private final List<CompletableFuture<?>> enabledFutures = new ArrayList<>();
    private final List<CompletableFuture<?>> disabledFutures = new ArrayList<>();


    public synchronized boolean transitionToEnabling(final ControllerServiceState expectedState, final CompletableFuture<?> enabledFuture) {
        if (expectedState != state) {
            return false;
        }

        state = ControllerServiceState.ENABLING;
        enabledFutures.add(enabledFuture);
        return true;
    }

    public synchronized boolean enable() {
        if (state != ControllerServiceState.ENABLING) {
            return false;
        }

        state = ControllerServiceState.ENABLED;
        enabledFutures.stream().forEach(future -> future.complete(null));
        return true;
    }

    public synchronized boolean transitionToDisabling(final ControllerServiceState expectedState, final CompletableFuture<?> disabledFuture) {
        if (expectedState != state) {
            return false;
        }

        state = ControllerServiceState.DISABLING;
        disabledFutures.add(disabledFuture);
        return true;
    }

    public synchronized void disable() {
        state = ControllerServiceState.DISABLED;
        disabledFutures.stream().forEach(future -> future.complete(null));
    }

    public synchronized ControllerServiceState getState() {
        return state;
    }
}
