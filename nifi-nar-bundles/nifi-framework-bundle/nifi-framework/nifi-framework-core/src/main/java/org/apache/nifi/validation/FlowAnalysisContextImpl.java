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
package org.apache.nifi.validation;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FlowAnalysisContextImpl implements FlowAnalysisContext {
    private final ConcurrentMap<String, ConcurrentMap<String, RuleViolation>> idToRuleNameToRuleViolations = new ConcurrentHashMap<>();

    @Override
    public void addRuleViolation(String subjectId, RuleViolation ruleViolation) {
        idToRuleNameToRuleViolations
            .computeIfAbsent(subjectId, __ -> new ConcurrentHashMap<>())
            .compute(ruleViolation.getRuleName(), (ruleName, currentRuleViolation) -> {
                RuleViolation newRuleViolation = new RuleViolation(ruleViolation.getRuleType(), subjectId, ruleName, ruleViolation.getErrorMessage());

                if (currentRuleViolation != null) {
                    newRuleViolation.setEnabled(currentRuleViolation.isEnabled());
                }

                return newRuleViolation;
            });
    }

    @Override
    public ConcurrentMap<String, ConcurrentMap<String, RuleViolation>> getIdToRuleNameToRuleViolations() {
        return idToRuleNameToRuleViolations;
    }

    @Override
    public void updateOneRuleViolation(String subjectId, String ruleName, boolean enabled) {
        Optional.ofNullable(idToRuleNameToRuleViolations.get(subjectId))
            .map(ruleNameToRuleViolations -> ruleNameToRuleViolations.get(ruleName))
            .ifPresent(ruleViolation -> ruleViolation.setEnabled(enabled));
    }
}
