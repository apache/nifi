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

import org.apache.nifi.flowanalysis.FlowAnalysisRuleType;

import java.util.StringJoiner;

public class RuleViolation {
    private final FlowAnalysisRuleType ruleType;
    private final String subjectId;
    private final String ruleName;
    private final String errorMessage;

    private boolean available;
    private boolean enabled;

    public RuleViolation(FlowAnalysisRuleType ruleType, String subjectId, String ruleName, String errorMessage) {
        this.ruleType = ruleType;
        this.subjectId = subjectId;
        this.ruleName = ruleName;
        this.errorMessage = errorMessage;
        this.available = true;
        this.enabled = true;
    }

    public FlowAnalysisRuleType getRuleType() {
        return ruleType;
    }

    public String getSubjectId() {
        return subjectId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RuleViolation.class.getSimpleName() + "[", "]")
            .add("ruleType=" + ruleType)
            .add("subjectId='" + subjectId + "'")
            .add("ruleName='" + ruleName + "'")
            .add("errorMessage='" + errorMessage + "'")
            .add("available=" + available)
            .add("enabled=" + enabled)
            .toString();
    }
}
