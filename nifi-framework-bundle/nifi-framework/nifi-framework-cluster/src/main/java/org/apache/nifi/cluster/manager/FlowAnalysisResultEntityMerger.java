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
import org.apache.nifi.validation.RuleViolationKey;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleViolationDTO;
import org.apache.nifi.web.api.entity.FlowAnalysisResultEntity;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FlowAnalysisResultEntityMerger {
    public void merge(FlowAnalysisResultEntity clientEntity, Map<NodeIdentifier, FlowAnalysisResultEntity> entityMap) {
        List<FlowAnalysisRuleDTO> aggregateRules = clientEntity.getRules();
        entityMap.values().stream()
                .map(FlowAnalysisResultEntity::getRules)
                .forEach(aggregateRules::addAll);

        Map<RuleViolationKey, FlowAnalysisRuleViolationDTO> mergedViolations = clientEntity.getRuleViolations().stream().collect(Collectors.toMap(
                violation -> new RuleViolationKey(
                        violation.getScope(),
                        violation.getSubjectId(),
                        violation.getRuleId(),
                        violation.getIssueId()
                ),
                violation -> violation
        ));

        for (final Map.Entry<NodeIdentifier, FlowAnalysisResultEntity> entry : entityMap.entrySet()) {
            final FlowAnalysisResultEntity entity = entry.getValue();

            entity.getRuleViolations().forEach(violation -> mergedViolations
                    .compute(
                            new RuleViolationKey(
                                    violation.getScope(),
                                    violation.getSubjectId(),
                                    violation.getRuleId(),
                                    violation.getIssueId()
                            ),
                            (ruleViolationKey, storedViolation) -> {
                                if (storedViolation != null) {
                                    PermissionsDtoMerger.mergePermissions(violation.getSubjectPermissionDto(), storedViolation.getSubjectPermissionDto());
                                }

                                return violation;
                            }
                    )
            );
        }

        List<FlowAnalysisRuleViolationDTO> authorizedViolations = mergedViolations.values().stream()
                .filter(violation -> violation.getSubjectPermissionDto().getCanRead())
                .collect(Collectors.toList());

        clientEntity.setRuleViolations(authorizedViolations);
    }
}
