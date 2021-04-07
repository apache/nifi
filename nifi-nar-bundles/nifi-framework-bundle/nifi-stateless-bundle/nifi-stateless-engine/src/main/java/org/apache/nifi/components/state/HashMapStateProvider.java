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

package org.apache.nifi.components.state;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.state.StandardStateMap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashMapStateProvider implements StateProvider {
    private static final int UNKNOWN_STATE_VERSION = -1;
    private final Map<String, StateMap> committedStates = new HashMap<>();
    private final Map<String, StateMap> activeStates = new HashMap<>();

    @Override
    public void initialize(final StateProviderInitializationContext context) {
    }

    @Override
    public void shutdown() {
    }

    public synchronized void rollback() {
        activeStates.clear();
    }

    public synchronized void commit() {
        committedStates.putAll(activeStates);
    }

    public synchronized Map<String, StateMap> getAllComponentsState() {
        final Map<String, StateMap> allComponents = new HashMap<>(committedStates);
        allComponents.putAll(activeStates);
        return allComponents;
    }

    public synchronized void updateAllComponentsStates(final Map<String, StateMap> componentStates) {
        if (componentStates == null) {
            return;
        }

        this.activeStates.putAll(componentStates);
    }

    @Override
    public synchronized void setState(final Map<String, String> state, final String componentId) {
        final StateMap existing = getState(componentId);
        final long version = existing == null ? UNKNOWN_STATE_VERSION : existing.getVersion();
        final StateMap updated = new StandardStateMap(state, version + 1);
        activeStates.put(componentId, updated);
    }

    @Override
    public synchronized StateMap getState(final String componentId) {
        StateMap existing = activeStates.get(componentId);
        if (existing == null) {
            existing = committedStates.get(componentId);
        }

        return existing == null ? new StandardStateMap(Collections.emptyMap(), -1) : existing;
    }

    @Override
    public synchronized boolean replace(final StateMap oldValue, final Map<String, String> newValue, final String componentId) {
        final StateMap existing = getState(componentId);
        if (oldValue.getVersion() == existing.getVersion() && oldValue.toMap().equals(existing.toMap())) {
            setState(newValue, componentId);
            return true;
        }

        return false;
    }

    @Override
    public synchronized void clear(final String componentId) {
        activeStates.remove(componentId);
        committedStates.remove(componentId);
    }

    @Override
    public void onComponentRemoved(final String componentId) {
        clear(componentId);
    }

    @Override
    public void enable() {
    }

    @Override
    public void disable() {
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public Scope[] getSupportedScopes() {
        return new Scope[] { Scope.CLUSTER, Scope.LOCAL };
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext context) {
        return Collections.emptyList();
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(final String name) {
        return new PropertyDescriptor.Builder()
            .name(name)
            .addValidator(Validator.VALID)
            .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public String getIdentifier() {
        return "stateless-state-provider";
    }
}
