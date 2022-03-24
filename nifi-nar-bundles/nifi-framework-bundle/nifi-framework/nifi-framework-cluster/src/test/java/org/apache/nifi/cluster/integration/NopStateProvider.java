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

package org.apache.nifi.cluster.integration;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.controller.state.StandardStateMap;

public class NopStateProvider implements StateProvider {
    private final String id = UUID.randomUUID().toString();
    private final Map<String, Map<String, String>> componentStateMap = new HashMap<>();

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        return Collections.emptyList();
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) {
        return null;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public void initialize(StateProviderInitializationContext context) throws IOException {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public synchronized void setState(Map<String, String> state, String componentId) throws IOException {
        final Map<String, String> stateMap = componentStateMap.computeIfAbsent(componentId, compId -> new HashMap<String, String>());
        stateMap.clear();
        stateMap.putAll(state);
    }

    @Override
    public synchronized StateMap getState(String componentId) throws IOException {
        return new StandardStateMap(componentStateMap.computeIfAbsent(componentId, compId -> new HashMap<String, String>()), 0L);
    }

    @Override
    public synchronized boolean replace(StateMap oldValue, Map<String, String> newValue, String componentId) throws IOException {
        return false;
    }

    @Override
    public void clear(String componentId) throws IOException {
    }

    @Override
    public void onComponentRemoved(String componentId) throws IOException {
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
        return new Scope[] {Scope.LOCAL};
    }

}
