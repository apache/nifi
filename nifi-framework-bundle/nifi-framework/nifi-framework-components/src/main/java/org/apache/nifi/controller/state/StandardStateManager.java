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

package org.apache.nifi.controller.state;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.StandardLoggingContext;
import org.apache.nifi.processor.SimpleProcessLogger;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

public class StandardStateManager implements StateManager {
    private final StateProvider localProvider;
    private final StateProvider clusterProvider;
    private final String componentId;
    private final Supplier<Boolean> dropStateKeySupportedSupplier;

    public StandardStateManager(final StateProvider localProvider, final StateProvider clusterProvider, final String componentId) {
        this(localProvider, clusterProvider, componentId, () -> false);
    }

    public StandardStateManager(final StateProvider localProvider, final StateProvider clusterProvider,
                                final String componentId, final Supplier<Boolean> dropStateKeySupportedSupplier) {
        this.localProvider = localProvider;
        this.clusterProvider = clusterProvider;
        this.componentId = componentId;
        this.dropStateKeySupportedSupplier = dropStateKeySupportedSupplier;
    }

    private StateProvider getProvider(final Scope scope) {
        if (scope == Scope.LOCAL || clusterProvider == null || !clusterProvider.isEnabled()) {
            return localProvider;
        }

        return clusterProvider;
    }

    private ComponentLog getLogger(final String componentId) {
        final LogRepository repo = LogRepositoryFactory.getRepository(componentId);
        final ComponentLog logger = (repo == null) ? null : repo.getLogger();
        if (repo == null || logger == null) {
            return new SimpleProcessLogger(componentId, this, new StandardLoggingContext(null));
        }

        return logger;
    }

    @Override
    public StateMap getState(final Scope scope) throws IOException {
        final StateMap stateMap = getProvider(scope).getState(componentId);
        getLogger(componentId).debug("Returning {} State: {}", scope, stateMap);
        return stateMap;
    }


    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
        final boolean replaced = getProvider(scope).replace(oldValue, newValue, componentId);
        getLogger(componentId).debug("{} State from old value {} to new value {} was {}", scope, oldValue, newValue, replaced);
        return replaced;
    }

    @Override
    public void setState(final Map<String, String> state, final Scope scope) throws IOException {
        getLogger(componentId).debug("Setting {} State to {}", scope, state);
        getProvider(scope).setState(state, componentId);
    }

    @Override
    public void clear(final Scope scope) throws IOException {
        getLogger(componentId).debug("Clearing {} State", scope);
        getProvider(scope).clear(componentId);
    }

    @Override
    public boolean isStateKeyDropSupported() {
        return dropStateKeySupportedSupplier.get();
    }

    @Override
    public String toString() {
        return "StandardStateManager[componentId=" + componentId + "]";
    }
}
