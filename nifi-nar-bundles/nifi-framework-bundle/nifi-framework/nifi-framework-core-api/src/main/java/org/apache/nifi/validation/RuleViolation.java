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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flowanalysis.EnforcementPolicy;

import java.util.StringJoiner;

/**
 * A result of a rule violation produced during a flow analysis.
 * Violations produced by previous analysis runs may be overridden by new ones.
 * A violation is identified by the scope, subjectId, ruleId and issueId properties.
 */
public class RuleViolation {
    private final EnforcementPolicy enforcementPolicy;
    private final String scope;
    private final String subjectId;
    private final String subjectDisplayName;
    private final ComponentType subjectComponentType;
    private final String groupId;
    private final String ruleId;
    private final String issueId;
    private final String violationMessage;
    private final String violationExplanation;

    private boolean available;

    public RuleViolation(
            EnforcementPolicy enforcementPolicy,
            String scope,
            String subjectId,
            String subjectDisplayName,
            ComponentType subjectComponentType,
            String groupId,
            String ruleId,
            String issueId,
            String violationMessage,
            String violationExplanation
    ) {
        this.enforcementPolicy = enforcementPolicy;
        this.scope = scope;
        this.subjectId = subjectId;
        this.subjectDisplayName = subjectDisplayName;
        this.subjectComponentType = subjectComponentType;
        this.groupId = groupId;
        this.ruleId = ruleId;
        this.issueId = issueId;
        this.violationMessage = violationMessage;
        this.violationExplanation = violationExplanation;
        this.available = true;
    }

    /**
     * @return the type of the rule that produced this violation
     */
    public EnforcementPolicy getEnforcementPolicy() {
        return enforcementPolicy;
    }

    /**
     * @return the scope of the analysis that produced this violation. The id of the component or the process group that was
     * the subject of the analysis.
     */
    public String getScope() {
        return scope;
    }

    /**
     * @return the id of the subject that violated the rule (component or a process group).
     */
    public String getSubjectId() {
        return subjectId;
    }

    /**
     * @return the displayed name of the subject that violated the rule
     */
    public String getSubjectDisplayName() {
        return subjectDisplayName;
    }

    /**
     * @return the type of the subject that violated the rule
     */
    public ComponentType getSubjectComponentType() {
        return subjectComponentType;
    }

    /**
     * @return group id - if this violation is a result of a component analysis, then the id of the group of the component.
     * If this violation is a result of a group analysis, then the id of that group itself.
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * @return a rule-defined id that corresponds to a unique type of issue recognized by the rule.
     */
    public String getIssueId() {
        return issueId;
    }

    /**
     * @return the id of the rule that produced this violation
     */
    public String getRuleId() {
        return ruleId;
    }

    /**
     * @return the violation message
     */
    public String getViolationMessage() {
        return violationMessage;
    }

    /**
     * @return a detailed explanation of the nature of the violation
     */
    public String getViolationExplanation() {
        return violationExplanation;
    }

    /**
     * Used by the framework internally to manage lifecycle
     */
    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    @Override
    public String toString() {
        return new StringJoiner(",\n\t", RuleViolation.class.getSimpleName() + "[\n\t", "\n]")
            .add("enforcementPolicy=" + enforcementPolicy)
            .add("scope='" + scope + "'")
            .add("subjectId='" + subjectId + "'")
            .add("subjectDisplayName='" + subjectDisplayName + "'")
            .add("subjectComponentType='" + subjectComponentType + "'")
            .add("groupId='" + groupId + "'")
            .add("issueId='" + issueId + "'")
            .add("ruleId='" + ruleId + "'")
            .add("violationMessage='" + violationMessage + "'")
            .add("violationExplanation='" + violationExplanation + "'")
            .add("available=" + available)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        RuleViolation that = (RuleViolation) o;

        return new EqualsBuilder()
            .append(enforcementPolicy, that.enforcementPolicy)
            .append(scope, that.scope)
            .append(subjectId, that.subjectId)
            .append(groupId, that.groupId)
            .append(issueId, that.issueId)
            .append(ruleId, that.ruleId)
            .append(violationMessage, that.violationMessage)
            .append(violationExplanation, that.violationExplanation)
            .append(available, that.available)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(enforcementPolicy)
            .append(scope)
            .append(subjectId)
            .append(groupId)
            .append(issueId)
            .append(ruleId)
            .append(violationMessage)
            .append(violationExplanation)
            .append(available)
            .toHashCode();
    }
}
