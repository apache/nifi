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
package org.apache.nifi.registry;

import java.util.Map;
import java.util.Set;

/***
 * This implementation of variable registry uses the ImmutableMultiMap which stores one or more
 * registries that can be searched, accessed and appended.  NOTE: Duplicate values within
 * or between added registries will be stored however on retrieval the first value encountered will be returned.
 * */
public class MultiMapVariableRegistry implements VariableRegistry {

    protected final ImmutableMultiMap<String> registry;

    MultiMapVariableRegistry() {
        this.registry = new ImmutableMultiMap<>();
    }

    @SafeVarargs
    MultiMapVariableRegistry(Map<String,String>...maps){
        this();
        if(maps != null) {
            for (Map<String, String> map : maps) {
                addVariables(map);
            }
        }
    }

    public void addVariables(Map<String, String> map) {
        this.registry.addMap(map);
    }

    @Override
    public void addRegistry(VariableRegistry variableRegistry) {
        if(variableRegistry != null && !variableRegistry.getVariables().isEmpty()) {
            this.registry.addMap(variableRegistry.getVariables());
        }
    }

    @Override
    public Map<String, String> getVariables() {
        return registry;
    }

    @Override
    public String getVariableValue(String variable) {
        return registry.get(variable);
    }

    @Override
    public Set<String> getVariableNames() {
        return this.registry.keySet();
    }
}
