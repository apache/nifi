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

package org.apache.nifi.cs.tests.system;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Store whose contents live in the component's local state, so that they are subject to the framework's own state
 * lifecycle. The store records when it was first created and how many rows it holds.
 *
 * Removing a Controller Service clears its state, so a service that was torn down and recreated comes back with a
 * later creation timestamp and no rows. Comparing the creation timestamp across an operation therefore distinguishes
 * a preserved service from one that was replaced, even when the replacement carries the same identifier.
 */
@Stateful(scopes = Scope.LOCAL, description = "Holds the creation timestamp of the store and the number of rows written to it.")
public class StateBackedStoreService extends AbstractControllerService implements StoreService {

    static final PropertyDescriptor STORE_NAME = new PropertyDescriptor.Builder()
            .name("Store Name")
            .required(true)
            .defaultValue("store")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final String CREATED_KEY = "created";
    static final String ROW_COUNT_KEY = "rowCount";
    static final String LAST_ROW_KEY = "lastRow";

    private static final List<PropertyDescriptor> PROPERTIES = List.of(STORE_NAME);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    /**
     * Establishes the store on first enable and leaves it alone afterwards, so that the creation timestamp survives
     * the disable and re-enable cycle that a version change performs.
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws IOException {
        final StateManager stateManager = getStateManager();
        final StateMap stateMap = stateManager.getState(Scope.LOCAL);
        if (stateMap.get(CREATED_KEY) != null) {
            return;
        }

        final Map<String, String> state = new HashMap<>();
        state.put(CREATED_KEY, String.valueOf(System.currentTimeMillis()));
        state.put(ROW_COUNT_KEY, "0");
        stateManager.setState(state, Scope.LOCAL);
    }

    @Override
    public synchronized void append(final String row) {
        try {
            final StateManager stateManager = getStateManager();
            final Map<String, String> state = new HashMap<>(stateManager.getState(Scope.LOCAL).toMap());
            final long rowCount = Long.parseLong(state.getOrDefault(ROW_COUNT_KEY, "0"));
            state.put(ROW_COUNT_KEY, String.valueOf(rowCount + 1));
            state.put(LAST_ROW_KEY, row);
            stateManager.setState(state, Scope.LOCAL);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
