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
package org.apache.nifi.web.api.dto;

import jakarta.xml.bind.annotation.XmlType;

/**
 * A result of a rule violation produced during a flow analysis
 */
@XmlType(name = "flowAnalysisRuleViolation")
public class FlowAnalysisRuleViolationDTO {
    public FlowAnalysisRuleViolationDTO() {
    }

    private String enforcementPolicy;
    private String scope;
    private String subjectId;
    private String subjectDisplayName;
    private String groupId;
    private String ruleId;
    private String issueId;
    private String violationMessage;

    private String subjectComponentType;

    private PermissionsDTO subjectPermissionDto;

    private boolean enabled;

    /**
     * @return the enforcement policy of the rule that produced this result
     */
    public String getEnforcementPolicy() {
        return enforcementPolicy;
    }

    public void setEnforcementPolicy(String enforcementPolicy) {
        this.enforcementPolicy = enforcementPolicy;
    }

    /**
     * @return the scope of the result
     */
    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    /**
     * @return the id of the subject that violated the rule
     */
    public String getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(String subjectId) {
        this.subjectId = subjectId;
    }

    /**
     * @return the displayed name of the subject that violated the rule
     */
    public String getSubjectDisplayName() {
        return subjectDisplayName;
    }

    public void setSubjectDisplayName(String subjectDisplayName) {
        this.subjectDisplayName = subjectDisplayName;
    }

    /**
     * @return group id - if this violation is a result of a component analysis, then the id of the group of the component.
     * If this violation is a result of a group analysis, then the id of that group itself.
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return the id of the rule that produced this result
     */
    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    /**
     * @return a rule-defined id that corresponds to a unique type of issue recognized by the rule
     */
    public String getIssueId() {
        return issueId;
    }

    public void setIssueId(String issueId) {
        this.issueId = issueId;
    }

    /**
     * @return the violation message
     */
    public String getViolationMessage() {
        return violationMessage;
    }

    public void setViolationMessage(String violationMessage) {
        this.violationMessage = violationMessage;
    }


    /**
     * @return the type of the subject that violated the rule
     */
    public String getSubjectComponentType() {
        return subjectComponentType;
    }

    public void setSubjectComponentType(String subjectComponentType) {
        this.subjectComponentType = subjectComponentType;
    }

    /**
     * @return true if this result should be in effect, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return a permission object for the subject that violated the rule
     */
    public PermissionsDTO getSubjectPermissionDto() {
        return subjectPermissionDto;
    }

    public void setSubjectPermissionDto(PermissionsDTO subjectPermissionDto) {
        this.subjectPermissionDto = subjectPermissionDto;
    }
}
