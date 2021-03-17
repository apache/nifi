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

public class FlowAnalysisResult {
    private final String subjectId;
    private final String ruleName;
    private final String message;

    public FlowAnalysisResult(String subjectId, String ruleName, String messages) {
        this.subjectId = subjectId;
        this.ruleName = ruleName;
        this.message = messages;
    }

    public String getSubjectId() {
        return subjectId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FlowAnalysisResult.class.getSimpleName() + "[", "]")
            .add("subjectId='" + subjectId + "'")
            .add("ruleName='" + ruleName + "'")
            .add("message='" + message + "'")
            .toString();
    }
}
