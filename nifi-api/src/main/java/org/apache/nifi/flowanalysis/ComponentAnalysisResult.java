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

/**
 * Holds information about a component violating a {@link FlowAnalysisRule}
 */
public class ComponentAnalysisResult extends AbstractAnalysisResult {
    /**
     * @param issueId A rule-defined id that corresponds to a unique type of issue recognized by the rule.
     *                Newer analysis runs may produce a result with the same issueId in which case the old one will
     *                be overwritten (or recreated if it is the same in other aspects as well).
     *                However, if the previous result was disabled the new one will be disabled as well.
     * @param message A violation message
     */
    public ComponentAnalysisResult(final String issueId, final String message) {
        this(issueId, message, null);
    }

    /**
     * @param issueId A rule-defined id that corresponds to a unique type of issue recognized by the rule.
     *                Newer analysis runs may produce a result with the same issueId in which case the old one will
     *                be overwritten (or recreated if it is the same in other aspects as well).
     *                However, if the previous result was disabled the new one will be disabled as well.
     * @param message A violation message
     * @param explanation A detailed explanation of the violation
     */
    public ComponentAnalysisResult(final String issueId, final String message, final String explanation) {
        super(issueId, message, explanation);
    }
}
