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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleViolationDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.entity.FlowAnalysisResultEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlowAnalysisResultEntityMergerTest {
    public static final NodeIdentifier NODE_ID_1 = nodeIdOf("id1");
    public static final NodeIdentifier NODE_ID_2 = nodeIdOf("id2");

    private FlowAnalysisResultEntityMerger testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new FlowAnalysisResultEntityMerger();
    }

    @Test
    void differentViolationsAreMerged() {
        // GIVEN
        final FlowAnalysisResultEntity clientEntity = resultEntityOf(
                listOf(ruleOf("ruleId")),
                listOf(ruleViolationOf("ruleId", true, true))
        );

        final Map<NodeIdentifier, FlowAnalysisResultEntity> entityMap = resultEntityMapOf(
                resultEntityOf(
                        listOf(ruleOf("ruleId1")),
                        listOf(ruleViolationOf("ruleId1", true, true))
                ),
                resultEntityOf(
                        listOf(ruleOf("ruleId2")),
                        listOf(ruleViolationOf("ruleId2", true, true))
                )
        );

        final FlowAnalysisResultEntity expectedClientEntity = resultEntityOf(
                listOf(ruleOf("ruleId"), ruleOf("ruleId1"), ruleOf("ruleId2")),
                listOf(
                        ruleViolationOf("ruleId", true, true),
                        ruleViolationOf("ruleId1", true, true),
                        ruleViolationOf("ruleId2", true, true)
                )
        );

        testMerge(clientEntity, entityMap, expectedClientEntity);
    }

    @Test
    void violationThatCannotBeReadOnAnyNodeIsOmitted() {
        // GIVEN
        final String ruleId = "ruleWithViolationThatCantBeReadOnOneNode";

        final FlowAnalysisResultEntity clientEntity = resultEntityOf(
                listOf(ruleOf(ruleId)),
                listOf(ruleViolationOf(ruleId, true, true))
        );

        final Map<NodeIdentifier, FlowAnalysisResultEntity> entityMap = resultEntityMapOf(
                resultEntityOf(
                        listOf(ruleOf(ruleId)),
                        listOf(ruleViolationOf(ruleId, false, true))
                ),
                resultEntityOf(
                        listOf(ruleOf(ruleId)),
                        listOf(ruleViolationOf(ruleId, true, true))
                )
        );

        final FlowAnalysisResultEntity expectedClientEntity = resultEntityOf(
                listOf(ruleOf(ruleId)),
                listOf()
        );

        testMerge(clientEntity, entityMap, expectedClientEntity);
    }

    @Test
    void evenWhenViolationIsOmittedTheRuleIsNot() {
        // GIVEN
        final FlowAnalysisResultEntity clientEntity = resultEntityOf(
                listOf(),
                listOf()
        );

        final Map<NodeIdentifier, FlowAnalysisResultEntity> entityMap = resultEntityMapOf(
                resultEntityOf(
                        listOf(ruleOf("notOmittedRuleButOmittedViolation")),
                        listOf(ruleViolationOf("notOmittedRuleButOmittedViolation", false, true))
                ),
                resultEntityOf(
                        listOf(),
                        listOf()
                )
        );

        final FlowAnalysisResultEntity expectedClientEntity = resultEntityOf(
                listOf(ruleOf("notOmittedRuleButOmittedViolation")),
                listOf()
        );

        testMerge(clientEntity, entityMap, expectedClientEntity);
    }

    @Test
    void violationThatCannotBeWrittenIsNotOmitted() {
        // GIVEN
        final String ruleId = "ruleWithViolationThatCantBeWrittenOnOneNode";

        final FlowAnalysisResultEntity clientEntity = resultEntityOf(
                listOf(ruleOf(ruleId)),
                listOf(ruleViolationOf(ruleId, true, false))
        );

        final Map<NodeIdentifier, FlowAnalysisResultEntity> entityMap = resultEntityMapOf(
                resultEntityOf(
                        listOf(ruleOf(ruleId)),
                        listOf(ruleViolationOf(ruleId, true, false))
                ),
                resultEntityOf(
                        listOf(ruleOf(ruleId)),
                        listOf(ruleViolationOf(ruleId, true, false))
                )
        );

        final FlowAnalysisResultEntity expectedClientEntity = clientEntity;

        testMerge(clientEntity, entityMap, expectedClientEntity);
    }

    private void testMerge(final FlowAnalysisResultEntity clientEntity, final Map<NodeIdentifier, FlowAnalysisResultEntity> entityMap, final FlowAnalysisResultEntity expectedClientEntity) {
        // GIVEN
        final List<Function<FlowAnalysisRuleDTO, Object>> rulePropertiesProviders = Arrays.asList(FlowAnalysisRuleDTO::getId);
        final List<Function<FlowAnalysisRuleViolationDTO, Object>> list = Arrays.asList(
                FlowAnalysisRuleViolationDTO::getRuleId,
                FlowAnalysisRuleViolationDTO::isEnabled,
                ruleViolation -> ruleViolation.getSubjectPermissionDto().getCanRead(),
                ruleViolation -> ruleViolation.getSubjectPermissionDto().getCanWrite()
        );
        final List<Function<FlowAnalysisResultEntity, Object>> resultEntityEqualsPropertiesProviders = Arrays.asList(
                resultEntity -> new HashSet<>(EqualsWrapper.wrapList(resultEntity.getRules(), rulePropertiesProviders)),
                resultEntity -> new HashSet<>(EqualsWrapper.wrapList(resultEntity.getRuleViolations(), list))
        );

        // WHEN
        testSubject.merge(clientEntity, entityMap);

        // THEN
        assertEquals(new EqualsWrapper<>(
                expectedClientEntity,
                resultEntityEqualsPropertiesProviders
        ), new EqualsWrapper<>(
                clientEntity,
                resultEntityEqualsPropertiesProviders
        ));
    }

    private static NodeIdentifier nodeIdOf(final String nodeId) {
        final NodeIdentifier nodeIdentifier = new NodeIdentifier(nodeId, "unimportant", 1, "unimportant", 1, "unimportant", 1, 1, false);
        return nodeIdentifier;
    }

    private static FlowAnalysisRuleDTO ruleOf(final String ruleId) {
        final FlowAnalysisRuleDTO rule = new FlowAnalysisRuleDTO();

        rule.setId(ruleId);

        return rule;
    }

    private static FlowAnalysisRuleViolationDTO ruleViolationOf(
            final String ruleId,
            final boolean canRead,
            final boolean canWrite
    ) {
        final FlowAnalysisRuleViolationDTO ruleViolation = new FlowAnalysisRuleViolationDTO();

        ruleViolation.setRuleId(ruleId);
        ruleViolation.setSubjectPermissionDto(permissionOf(canRead, canWrite));

        return ruleViolation;
    }

    private static PermissionsDTO permissionOf(final boolean canRead, final boolean canWrite) {
        final PermissionsDTO subjectPermissionDto = new PermissionsDTO();

        subjectPermissionDto.setCanRead(canRead);
        subjectPermissionDto.setCanWrite(canWrite);

        return subjectPermissionDto;
    }

    private static FlowAnalysisResultEntity resultEntityOf(final List<FlowAnalysisRuleDTO> rules, final List<FlowAnalysisRuleViolationDTO> ruleViolations) {
        final FlowAnalysisResultEntity clientEntity = new FlowAnalysisResultEntity();

        clientEntity.setRules(rules);
        clientEntity.setRuleViolations(ruleViolations);

        return clientEntity;
    }

    private static Map<NodeIdentifier, FlowAnalysisResultEntity> resultEntityMapOf(final FlowAnalysisResultEntity clientEntity1, final FlowAnalysisResultEntity clientEntity2) {
        final Map<NodeIdentifier, FlowAnalysisResultEntity> entityMap = new HashMap<>();

        entityMap.put(NODE_ID_1, clientEntity1);
        entityMap.put(NODE_ID_2, clientEntity2);

        return entityMap;
    }

    private static <T> List<T> listOf(final T... items) {
        final List<T> itemSet = new ArrayList<>();
        for (final T item : items) {
            itemSet.add(item);

        }
        return itemSet;
    }
}
