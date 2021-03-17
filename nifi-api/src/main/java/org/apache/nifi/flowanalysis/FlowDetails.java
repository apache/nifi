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
package org.apache.nifi.flowanalysis;

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FlowDetails {
    private final Map<Class<? extends VersionedComponent>, Set<? extends VersionedComponent>> typeToComponents;

    public FlowDetails() {
        typeToComponents = new HashMap<>();
        typeToComponents.put(VersionedProcessor.class, new HashSet<>());
        typeToComponents.put(VersionedControllerService.class, new HashSet<>());
    }

    public Set<VersionedProcessor> getProcessors() {
        return (Set<VersionedProcessor>) typeToComponents.get(VersionedProcessor.class);
    }

    public Set<VersionedControllerService> getControllerServices() {
        return (Set<VersionedControllerService>) typeToComponents.get(VersionedControllerService.class);
    }

    public Set<String> targetIds() {
        Set<String> targetIds = typeToComponents.values().stream()
            .flatMap(Collection::stream)
            .map(VersionedComponent::getIdentifier)
            .collect(Collectors.toSet());

        return targetIds;
    }

    public int size() {
        int size = typeToComponents.values().stream()
            .mapToInt(Collection::size)
            .sum();

        return size;
    }
}
