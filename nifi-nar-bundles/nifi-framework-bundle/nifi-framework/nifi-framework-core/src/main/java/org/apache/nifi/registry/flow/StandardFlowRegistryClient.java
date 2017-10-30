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

package org.apache.nifi.registry.flow;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StandardFlowRegistryClient implements FlowRegistryClient {
    private ConcurrentMap<String, FlowRegistry> registryById = new ConcurrentHashMap<>();

    @Override
    public FlowRegistry getFlowRegistry(String registryId) {
        return registryById.get(registryId);
    }

    @Override
    public Set<String> getRegistryIdentifiers() {
        return registryById.keySet();
    }

    @Override
    public void addFlowRegistry(final FlowRegistry registry) {
        final FlowRegistry existing = registryById.putIfAbsent(registry.getIdentifier(), registry);
        if (existing != null) {
            throw new IllegalStateException("Cannot add Flow Registry " + registry + " because a Flow Registry already exists with the ID " + registry.getIdentifier());
        }
    }

    @Override
    public FlowRegistry addFlowRegistry(final String registryId, final String registryName, final String registryUrl, final String description) {
        final URI uri = URI.create(registryUrl);
        final String uriScheme = uri.getScheme();

        final FlowRegistry registry;
        if (uriScheme.equalsIgnoreCase("file")) {
            try {
                registry = new FileBasedFlowRegistry(registryId, registryUrl);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create Flow Registry for URI " + registryUrl, e);
            }

            registry.setName(registryName);
            registry.setDescription(description);
        } else {
            throw new IllegalArgumentException("Cannot create Flow Registry with URI of " + registryUrl
                + " because there are no known implementations of Flow Registries that can handle URIs of scheme " + uriScheme);
        }

        addFlowRegistry(registry);
        return registry;
    }

    @Override
    public FlowRegistry removeFlowRegistry(final String registryId) {
        return registryById.remove(registryId);
    }
}
