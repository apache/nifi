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

import java.util.Objects;

/**
 * Used only as keys in a map
 */
public class RuleViolationKey {
    private final String scope;
    private final String subjectId;
    private final String ruleId;
    private final String issueId;

    public RuleViolationKey(RuleViolation violation) {
        this(violation.getScope(), violation.getSubjectId(), violation.getRuleId(), violation.getIssueId());
    }

    public RuleViolationKey(String scope, String subjectId, String ruleId, String issueId) {
        this.scope = scope;
        this.subjectId = subjectId;
        this.ruleId = ruleId;
        this.issueId = issueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RuleViolationKey that = (RuleViolationKey) o;
        return Objects.equals(scope, that.scope)
            && Objects.equals(subjectId, that.subjectId)
            && Objects.equals(ruleId, that.ruleId)
            && Objects.equals(issueId, that.issueId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, subjectId, ruleId, issueId);
    }
}
