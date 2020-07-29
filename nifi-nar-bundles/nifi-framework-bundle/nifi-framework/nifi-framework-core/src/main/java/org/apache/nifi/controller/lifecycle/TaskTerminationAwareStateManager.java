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

package org.apache.nifi.controller.lifecycle;

import java.io.IOException;
import java.util.Map;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.exception.TerminatedTaskException;

public class TaskTerminationAwareStateManager implements StateManager {
    private final StateManager stateManager;
    private final TaskTermination taskTermination;

    public TaskTerminationAwareStateManager(final StateManager stateManager, final TaskTermination taskTermination) {
        this.stateManager = stateManager;
        this.taskTermination = taskTermination;
    }

    private void verifyNotTerminated() {
        if (taskTermination.isTerminated()) {
            throw new TerminatedTaskException();
        }
    }

    @Override
    public void clear(final Scope scope) throws IOException {
        verifyNotTerminated();
        stateManager.clear(scope);
    }

    @Override
    public StateMap getState(final Scope scope) throws IOException {
        verifyNotTerminated();
        return stateManager.getState(scope);
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
        verifyNotTerminated();
        return stateManager.replace(oldValue, newValue, scope);
    }

    @Override
    public void setState(final Map<String, String> state, final Scope scope) throws IOException {
        verifyNotTerminated();
        stateManager.setState(state, scope);
    }
}
