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

import javax.xml.bind.annotation.XmlType;
import java.util.Objects;

/**
 * A result of a rule violation produced during a flow analysis
 */
@XmlType(name = "flowAnalysisResult")
public class FlowAnalysisResultDTO {
    private String ruleType;
    private String scope;
    private String subjectId;
    private String ruleId;
    private String violationMessage;

    private boolean enabled;

    /**
     * @return the type of the rule that produced this result
     */
    public String getRuleType() {
        return ruleType;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    /**
     * @return the scope of the result.
     */
    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    /**
     * @return the id of the subject that violated the rule.
     */
    public String getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(String subjectId) {
        this.subjectId = subjectId;
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
     * @return the violation message
     */
    public String getViolationMessage() {
        return violationMessage;
    }

    public void setViolationMessage(String violationMessage) {
        this.violationMessage = violationMessage;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowAnalysisResultDTO that = (FlowAnalysisResultDTO) o;
        return enabled == that.enabled
            && Objects.equals(ruleType, that.ruleType)
            && Objects.equals(scope, that.scope)
            && Objects.equals(subjectId, that.subjectId)
            && Objects.equals(ruleId, that.ruleId)
            && Objects.equals(violationMessage, that.violationMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleType, subjectId, ruleId, violationMessage, enabled, scope);
    }
}
