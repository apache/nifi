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
package org.apache.nifi.flowanalysis;

import java.util.StringJoiner;

/**
 * Abstract class holding information about a {@link FlowAnalysisRule} violation.
 */
public abstract class AbstractAnalysisResult {
    protected final String issueId;
    protected final String message;
    protected final String explanation;

    protected AbstractAnalysisResult(final String issueId, final String message, final String explanation) {
        this.issueId = issueId;
        this.message = message;
        this.explanation = explanation;
    }

    /**
     * @return A rule-defined id that corresponds to a unique type of issue recognized by the rule.
     * Newer analysis runs may produce a result with the same issueId in which case the old one will
     * be overwritten (or recreated if it is the same in other aspects as well).
     * However, if the previous result was disabled the new one will be disabled as well.
     */
    public String getIssueId() {
        return issueId;
    }

    /**
     * @return the rule violation message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return a detailed explanation of the nature of the violation
     */
    public String getExplanation() {
        return explanation;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", this.getClass().getSimpleName() + "[", "]")
            .add("issueId='" + issueId + "'")
            .add("message='" + message + "'")
            .add("explanation='" + explanation + "'")
            .toString();
    }
}
