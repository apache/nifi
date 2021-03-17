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
import java.util.Map;

/**
 * Manages {@link RuleViolation}s produced during flow analysis
 */
public interface RuleViolationsManager {
    /**
     * Add or update rule violations created during the analysis of a component
     *
     * @param subjectId  The id of the component that was analyzed
     * @param violations The violations to be added or updated
     */
    void upsertComponentViolations(String subjectId, Collection<RuleViolation> violations);

    /**
     * Add or update rule violations created during the analysis of a process group
     *
     * @param processGroup              The process group that was analyzed
     * @param violations                Violations to be added that scoped to a process group (the one that was analyzed or one of it's children)
     * @param componentToRuleViolations Violations to be added scoped to components under the analyzed process group (or one of it's children)
     */
    void upsertGroupViolations(VersionedProcessGroup processGroup, Collection<RuleViolation> violations, Map<VersionedComponent, Collection<RuleViolation>> componentToRuleViolations);

    /**
     * Returns rule violations tied to a component or process group with a given id
     *
     * @param subjectId The id of the component or process group
     * @return Violations tied to a component or process group with the given subjectId
     */
    Collection<RuleViolation> getRuleViolationsForSubject(String subjectId);

    /**
     * Returns a list of violations with the given groupId (non-recursive)
     *
     * @return Violations with the given groupId
     */
    Collection<RuleViolation> getRuleViolationsForGroup(String groupId);

    /**
     * @return All current rule violations
     */
    Collection<RuleViolation> getAllRuleViolations();

    /**
     * Remove all rule violations tied to a component or process group with a given id
     *
     * @param subjectId The id of the component or process group
     */
    void removeRuleViolationsForSubject(String subjectId);

    /**
     * Remove all rule violations produced by the rule with a given id
     *
     * @param ruleId The id of the rule
     */
    void removeRuleViolationsForRule(String ruleId);

    /**
     * Removes empty entries from the map storing the rule violations
     */
    void cleanUp();
}
