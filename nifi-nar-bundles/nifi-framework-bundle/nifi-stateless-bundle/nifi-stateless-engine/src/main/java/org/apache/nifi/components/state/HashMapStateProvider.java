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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HashMapStateProvider implements StateProvider {
    private static final int UNKNOWN_STATE_VERSION = -1;
    private final ConcurrentMap<String, StateMap> states = new ConcurrentHashMap<>();

    @Override
    public void initialize(final StateProviderInitializationContext context) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void setState(final Map<String, String> state, final String componentId) {
        final StateMap existing = states.get(componentId);
        final long version = existing == null ? UNKNOWN_STATE_VERSION : existing.getVersion();
        final StateMap updated = new StandardStateMap(state, version + 1);
        states.put(componentId, updated);
    }

    @Override
    public StateMap getState(final String componentId) {
        final StateMap existing = states.get(componentId);
        return existing == null ? new StandardStateMap(Collections.emptyMap(), -1) : existing;
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final String componentId) {
        final StateMap existing = getState(componentId);
        if (oldValue.getVersion() == existing.getVersion() && oldValue.toMap().equals(existing.toMap())) {
            setState(newValue, componentId);
            return true;
        }

        return false;
    }

    @Override
    public void clear(final String componentId) {
        states.remove(componentId);
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
