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

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * RuleViolations are stored in memory. The datastructure housing the instances is a 2-level map of maps, aiming to balance
 *  complexity and performance.
 *  <br /><br />
 *  The outer map key is the id of the component that is responsible for the violation. This way every component can fetch its "own"
 *  violations efficiently during their validation phases.
 *  The inner map key is a composite object that uniquely identifies a violation based on certain properties of the violation.
 *  <br /><br />
 *  When new violations are added the map goes through state changes vaguely similar to a mark and sweep algorithm.
 *  First, the set of violations that are supposed to be a previous version of the new set are "marked" by setting their 'available' flags to 'false'.
 *  Then we add the new violations, some of which may replace previous ones. By replacing a previous violation it automatically loses
 *  the "mark" i.e. its 'available' flag will be set to 'true'.
 *  Finally, all violations that are still "marked" are removed.
 *  <br /><br />
 *  This way operations can differentiate between "create" and "update" and can even preserve state during the update if the need
 *  arises.
 */
public class StandardRuleViolationsManager implements RuleViolationsManager {
    public StandardRuleViolationsManager() {
    }

    private final ConcurrentMap<String, ConcurrentMap<RuleViolationKey, RuleViolation>> subjectIdToRuleViolation = new ConcurrentHashMap<>();

    @Override
    public void upsertComponentViolations(String componentId, Collection<RuleViolation> violations) {
        ConcurrentMap<RuleViolationKey, RuleViolation> componentRuleViolations = subjectIdToRuleViolation
            .computeIfAbsent(componentId, __ -> new ConcurrentHashMap<>());

        synchronized (componentRuleViolations) {
            // Mark
            componentRuleViolations.values().stream()
                .filter(violation -> violation.getScope().equals(componentId))
                .forEach(violation -> violation.setAvailable(false));

            // Add/Update
            violations.forEach(violation -> componentRuleViolations
                .compute(new RuleViolationKey(violation), (ruleViolationKey, currentViolation) -> violation)
            );

            // Sweep
            componentRuleViolations.entrySet().removeIf(keyAndViolation -> {
                RuleViolation violation = keyAndViolation.getValue();

                return violation.getScope().equals(componentId)
                    && !violation.isAvailable();
            });
        }
    }

    @Override
    public synchronized void upsertGroupViolations(
        VersionedProcessGroup processGroup,
        Collection<RuleViolation> groupViolations,
        Map<VersionedComponent, Collection<RuleViolation>> componentToRuleViolations
    ) {
        // Mark
        hideGroupViolations(processGroup);

        // Add/Update
        // We have 2 sets of violations.
        //
        // 'groupViolations' are the result of rules being violated by no particular components.
        //  The responsible entity ('subject') is deemed to be a ProcessGroup. Violations in this collection can correspond to different ProcessGroups
        //  as child ProcessGroups create their own violations during the analysis of a ProcessGroup.
        //
        // 'componentToRuleViolations' are violations grouped by the components. One component can produce multiple violations.

        // Add 'groupViolations' to the stored map.
        groupViolations.forEach(groupViolation -> {
            subjectIdToRuleViolation
                .computeIfAbsent(groupViolation.getSubjectId(), __ -> new ConcurrentHashMap<>())
                .compute(new RuleViolationKey(groupViolation), (ruleViolationKey, currentViolation) -> groupViolation);
        });

        // Add 'componentToRuleViolations' to the sored map.
        componentToRuleViolations.forEach((component, componentViolations) -> {
            ConcurrentMap<RuleViolationKey, RuleViolation> componentRuleViolations = subjectIdToRuleViolation
                .computeIfAbsent(component.getIdentifier(), __ -> new ConcurrentHashMap<>());

            componentViolations.forEach(componentViolation -> componentRuleViolations
                .compute(new RuleViolationKey(componentViolation), (ruleViolationKey, currentViolation) -> componentViolation)
            );
        });

        // Sweep
        purgeGroupViolations(processGroup);
    }

    private void hideGroupViolations(VersionedProcessGroup processGroup) {
        String groupId = processGroup.getIdentifier();

        subjectIdToRuleViolation.values().stream()
            .map(Map::values).flatMap(Collection::stream)
            .filter(violation -> violation.getScope().equals(groupId))
            .forEach(violation -> violation.setAvailable(false));

        processGroup.getProcessGroups().forEach(childProcessGroup -> hideGroupViolations(childProcessGroup));
    }

    private void purgeGroupViolations(VersionedProcessGroup processGroup) {
        String groupId = processGroup.getIdentifier();

        subjectIdToRuleViolation.values().forEach(violationMap ->
            violationMap.entrySet().removeIf(keyAndViolation -> {
                RuleViolation violation = keyAndViolation.getValue();

                return violation.getScope().equals(groupId)
                    && !violation.isAvailable();
            }));

        processGroup.getProcessGroups().forEach(childProcessGroup -> purgeGroupViolations(childProcessGroup));
    }

    @Override
    public Collection<RuleViolation> getRuleViolationsForSubject(String subjectId) {
        HashSet<RuleViolation> ruleViolationsForSubject = Optional.ofNullable(subjectIdToRuleViolation.get(subjectId))
            .map(Map::values)
            .map(HashSet::new)
            .orElse(new HashSet<>());

        return ruleViolationsForSubject;
    }

    @Override
    public Collection<RuleViolation> getRuleViolationsForGroup(String groupId) {
        Set<RuleViolation> groupViolations = subjectIdToRuleViolation.values().stream()
            .map(Map::values).flatMap(Collection::stream)
            .filter(violation -> groupId.equals(violation.getGroupId()))
            .collect(Collectors.toSet());

        return groupViolations;
    }

    @Override
    public Collection<RuleViolation> getAllRuleViolations() {
        Set<RuleViolation> allRuleViolations = subjectIdToRuleViolation.values().stream()
            .map(Map::values).flatMap(Collection::stream)
            .collect(Collectors.toSet());

        return allRuleViolations;
    }

    @Override
    public void removeRuleViolationsForSubject(String subjectId) {
        subjectIdToRuleViolation.remove(subjectId);
    }

    @Override
    public void removeRuleViolationsForRule(String ruleId) {
        subjectIdToRuleViolation.values().stream()
            .forEach(
                violationMap -> violationMap
                    .entrySet()
                    .removeIf(keyAndViolation -> keyAndViolation.getValue().getRuleId().equals(ruleId))
            );
    }

    @Override
    public void cleanUp() {
        subjectIdToRuleViolation.entrySet().removeIf(subjectIdAndViolationMap -> subjectIdAndViolationMap.getValue().isEmpty());
    }
}
