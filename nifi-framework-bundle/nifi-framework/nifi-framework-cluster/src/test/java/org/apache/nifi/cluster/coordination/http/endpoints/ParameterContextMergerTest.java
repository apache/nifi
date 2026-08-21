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

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ParameterContextMergerTest {

    @Test
    void testMergePreservesNullReferencingComponentsWhenExcludedByEveryNode() {
        final Map<NodeIdentifier, ParameterContextDTO> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("node1", 8000), createParameterContextDto("param1", null));
        entityMap.put(getNodeIdentifier("node2", 8010), createParameterContextDto("param1", null));

        final ParameterContextDTO target = createParameterContextDto("param1", null);

        ParameterContextMerger.merge(target, entityMap);

        final ParameterDTO mergedParameter = target.getParameters().iterator().next().getParameter();
        assertNull(mergedParameter.getReferencingComponents(),
                "Referencing components should remain null when every node excluded them from its response, rather than being coerced into an empty collection");
    }

    @Test
    void testMergeCombinesReferencingComponentsAcrossNodesWhenIncluded() {
        final Map<NodeIdentifier, ParameterContextDTO> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("node1", 8000), createParameterContextDto("param1", Set.of(createAffectedComponent("component1"))));
        entityMap.put(getNodeIdentifier("node2", 8010), createParameterContextDto("param1", Set.of(createAffectedComponent("component2"))));

        final ParameterContextDTO target = createParameterContextDto("param1", Set.of());

        ParameterContextMerger.merge(target, entityMap);

        final ParameterDTO mergedParameter = target.getParameters().iterator().next().getParameter();
        assertNotNull(mergedParameter.getReferencingComponents());
        assertEquals(2, mergedParameter.getReferencingComponents().size());
    }

    private ParameterContextDTO createParameterContextDto(final String parameterName, final Set<AffectedComponentEntity> referencingComponents) {
        final ParameterDTO parameterDto = new ParameterDTO();
        parameterDto.setName(parameterName);
        parameterDto.setReferencingComponents(referencingComponents);

        final ParameterEntity parameterEntity = new ParameterEntity();
        parameterEntity.setParameter(parameterDto);
        parameterEntity.setCanWrite(true);

        final ParameterContextDTO contextDto = new ParameterContextDTO();
        contextDto.setId("context1");
        contextDto.setParameters(new HashSet<>(Set.of(parameterEntity)));
        contextDto.setBoundProcessGroups(new HashSet<>());

        return contextDto;
    }

    private AffectedComponentEntity createAffectedComponent(final String id) {
        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setId(id);
        return entity;
    }

    private NodeIdentifier getNodeIdentifier(final String id, final int port) {
        return new NodeIdentifier(id, "localhost", port, "localhost", port + 1, "localhost", port + 2, port + 3, true);
    }
}
