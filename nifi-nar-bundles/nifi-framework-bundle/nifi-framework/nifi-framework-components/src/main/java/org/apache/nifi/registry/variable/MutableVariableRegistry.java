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
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;

public class MutableVariableRegistry extends StandardComponentVariableRegistry implements VariableRegistry {
    private volatile Map<VariableDescriptor, String> variableMap = new HashMap<>();

    public MutableVariableRegistry(final VariableRegistry parent) {
        super(parent);
    }

    @Override
    public Map<VariableDescriptor, String> getVariableMap() {
        return variableMap;
    }

    public void setVariables(final Map<VariableDescriptor, String> variables) {
        final Map<VariableDescriptor, String> curVariableMap = this.variableMap;
        final Map<VariableDescriptor, String> updatedVariableMap = new HashMap<>(curVariableMap);
        for (final Map.Entry<VariableDescriptor, String> entry : variables.entrySet()) {
            if (entry.getValue() == null) {
                updatedVariableMap.remove(entry.getKey());
            } else {
                updatedVariableMap.put(entry.getKey(), entry.getValue());
            }
        }

        this.variableMap = Collections.unmodifiableMap(updatedVariableMap);
    }
}
