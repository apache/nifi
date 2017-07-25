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

package org.apache.nifi.registry.variable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;

public class StandardComponentVariableRegistry implements ComponentVariableRegistry {
    private volatile VariableRegistry parent;

    public StandardComponentVariableRegistry(final VariableRegistry parent) {
        this.parent = Objects.requireNonNull(parent);
    }

    @Override
    public Map<VariableDescriptor, String> getVariableMap() {
        return Collections.emptyMap();
    }

    @Override
    public VariableRegistry getParent() {
        return parent;
    }

    @Override
    public void setParent(final VariableRegistry parentRegistry) {
        this.parent = parentRegistry;
    }

    @Override
    public VariableDescriptor getVariableKey(final String name) {
        if (name == null) {
            return null;
        }

        final VariableDescriptor spec = new VariableDescriptor(name);
        for (final Map.Entry<VariableDescriptor, String> entry : getVariableMap().entrySet()) {
            if (entry.getKey().equals(spec)) {
                return entry.getKey();
            }
        }

        return null;
    }

    @Override
    public String getVariableValue(final String name) {
        if (name == null) {
            return null;
        }

        final VariableDescriptor descriptor = new VariableDescriptor(name);
        final String value = getVariableMap().get(descriptor);
        if (value != null) {
            return value;
        }

        return parent.getVariableValue(descriptor);
    }

    @Override
    public String getVariableValue(final VariableDescriptor descriptor) {
        if (descriptor == null) {
            return null;
        }

        final String value = getVariableMap().get(descriptor);
        if (value != null) {
            return value;
        }

        return parent.getVariableValue(descriptor);
    }
}
