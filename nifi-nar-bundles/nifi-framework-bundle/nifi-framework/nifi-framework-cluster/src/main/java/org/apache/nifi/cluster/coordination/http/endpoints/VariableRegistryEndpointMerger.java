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

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.AffectedComponentEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.VariableDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.VariableEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class VariableRegistryEndpointMerger extends AbstractSingleEntityEndpoint<VariableRegistryEntity> implements EndpointResponseMerger {
    public static final Pattern VARIABLE_REGISTRY_UPDATE_REQUEST_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/variable-registry");

    private final AffectedComponentEntityMerger affectedComponentEntityMerger = new AffectedComponentEntityMerger();

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && (VARIABLE_REGISTRY_UPDATE_REQUEST_URI_PATTERN.matcher(uri.getPath()).matches())) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<VariableRegistryEntity> getEntityClass() {
        return VariableRegistryEntity.class;
    }

    @Override
    protected void mergeResponses(final VariableRegistryEntity clientEntity, final Map<NodeIdentifier, VariableRegistryEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        final VariableRegistryDTO clientVariableRegistry = clientEntity.getVariableRegistry();
        final Set<VariableEntity> clientVariableEntities = clientVariableRegistry.getVariables();

        if (clientVariableEntities != null) {
            for (final Iterator<VariableEntity> i = clientVariableEntities.iterator(); i.hasNext();) {
                final VariableEntity clientVariableEntity = i.next();
                final VariableDTO clientVariable = clientVariableEntity.getVariable();

                final Map<NodeIdentifier, Set<AffectedComponentEntity>> nodeAffectedComponentEntities = new HashMap<>();

                boolean retainClientVariable = true;
                for (final Map.Entry<NodeIdentifier, VariableRegistryEntity> nodeEntry : entityMap.entrySet()) {
                    final VariableRegistryEntity nodeVariableRegistry = nodeEntry.getValue();
                    final Set<VariableEntity> nodeVariableEntities = nodeVariableRegistry.getVariableRegistry().getVariables();

                    // if this node has no variables, then the current client variable should be removed
                    if (nodeVariableEntities == null) {
                        retainClientVariable = false;
                        break;
                    }

                    boolean variableFound = false;
                    for (final VariableEntity nodeVariableEntity : nodeVariableEntities) {
                        final VariableDTO nodeVariable = nodeVariableEntity.getVariable();

                        // identify the current clientVariable for each node
                        if (clientVariable.getProcessGroupId().equals(nodeVariable.getProcessGroupId()) && clientVariable.getName().equals(nodeVariable.getName())) {
                            variableFound = true;

                            if (Boolean.FALSE.equals(nodeVariableEntity.getCanWrite())) {
                                clientVariableEntity.setCanWrite(false);
                            }

                            nodeAffectedComponentEntities.put(nodeEntry.getKey(), nodeVariableEntity.getVariable().getAffectedComponents());
                            break;
                        }
                    }

                    if (!variableFound) {
                        retainClientVariable = false;
                        break;
                    }
                }

                if (!retainClientVariable) {
                    i.remove();
                } else {
                    final Set<AffectedComponentEntity> clientAffectedComponentEntities = clientVariableEntity.getVariable().getAffectedComponents();
                    affectedComponentEntityMerger.mergeAffectedComponents(clientAffectedComponentEntities, nodeAffectedComponentEntities);
                }
            }
        }
    }
}
