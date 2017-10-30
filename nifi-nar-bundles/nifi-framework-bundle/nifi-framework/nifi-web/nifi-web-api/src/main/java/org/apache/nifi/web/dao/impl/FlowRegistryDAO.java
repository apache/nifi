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

package org.apache.nifi.web.dao.impl;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.RegistryDTO;
import org.apache.nifi.web.dao.RegistryDAO;

public class FlowRegistryDAO implements RegistryDAO {
    private FlowRegistryClient flowRegistryClient;

    @Override
    public FlowRegistry createFlowRegistry(final RegistryDTO registryDto) {
        return flowRegistryClient.addFlowRegistry(registryDto.getId(), registryDto.getName(), registryDto.getUri(), registryDto.getDescription());
    }

    @Override
    public FlowRegistry getFlowRegistry(final String registryId) {
        final FlowRegistry registry = flowRegistryClient.getFlowRegistry(registryId);
        if (registry == null) {
            throw new ResourceNotFoundException("Unable to find Flow Registry with id '" + registryId + "'");
        }

        return registry;
    }

    @Override
    public Set<FlowRegistry> getFlowRegistries() {
        return flowRegistryClient.getRegistryIdentifiers().stream()
            .map(flowRegistryClient::getFlowRegistry)
            .collect(Collectors.toSet());
    }

    @Override
    public FlowRegistry removeFlowRegistry(final String registryId) {
        final FlowRegistry registry = flowRegistryClient.removeFlowRegistry(registryId);
        if (registry == null) {
            throw new ResourceNotFoundException("Unable to find Flow Registry with id '" + registryId + "'");
        }
        return registry;
    }

    public void setFlowRegistryClient(FlowRegistryClient client) {
        this.flowRegistryClient = client;
    }
}
