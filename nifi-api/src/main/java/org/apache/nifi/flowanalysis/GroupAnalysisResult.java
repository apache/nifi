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

import org.apache.nifi.flow.VersionedComponent;

import java.util.Optional;
import java.util.StringJoiner;

/**
 * Holds information about a {@link FlowAnalysisRule} violation after analyzing (a part of) the flow, represented by a process group.
 * One such analysis can result in multiple instances of this class.
 */
public class GroupAnalysisResult extends AbstractAnalysisResult {
    private final Optional<VersionedComponent> component;

    private GroupAnalysisResult(final String issueId, final String message, final String explanation, final Optional<VersionedComponent> component) {
        super(issueId, message, explanation);
        this.component = component;
    }

    /**
     * @return the component this result corresponds to or empty if this result corresponds to the entirety of the process group that was analyzed
     */
    public Optional<VersionedComponent> getComponent() {
        return component;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", this.getClass().getSimpleName() + "[", "]")
                .add("issueId='" + issueId + "'")
                .add("message='" + message + "'")
                .add("explanation='" + explanation + "'")
                .add("component='" + component + "'")
                .toString();
    }

    /**
     * Build a new analysis result tied to the currently analyzed process group
     *
     * @param issueId     A rule-defined id that corresponds to a unique type of issue recognized by the rule.
     *                    Newer analysis runs may produce a result with the same issueId in which case the old one will
     *                    be overwritten (or recreated if it is the same in other aspects as well).
     *                    However, if the previous result was disabled the new one will be disabled as well.
     * @param message     A violation message
     * @return a Builder for a new analysis result instance tied to the currently analyzed process group
     */
    public static Builder forGroup(final String issueId, final String message) {
        return new Builder(null, issueId, message);
    }

    /**
     * Build a new analysis result tied to a component.
     * Note that the result will be scoped to the process group of the component and not the currently analyzed group.
     * This means that even when a new analysis is run against that process group, this result will become obsolete.
     *
     * @param component The component that this result is tied to
     * @param issueId   A rule-defined id that corresponds to a unique type of issue recognized by the rule.
     *                  Newer analysis runs may produce a result with the same issueId in which case the old one will
     *                  be overwritten (or recreated if it is the same in other aspects as well).
     *                  However, if the previous result was disabled the new one will be disabled as well.
     * @param message   A violation message
     * @return a Builder for a new analysis result tied to a component
     */
    public static Builder forComponent(final VersionedComponent component, final String issueId, final String message) {
        return new Builder(component, issueId, message);
    }

    public static class Builder {
        private final VersionedComponent component;
        private final String issueId;
        private final String message;
        private String explanation;

        private Builder(final VersionedComponent component, final String issueId, final String message) {
            this.component = component;
            this.issueId = issueId;
            this.message = message;
        }

        /**
         * @param explanation A detailed explanation of the violation
         * @return this Builder
         */
        public Builder explanation(final String explanation) {
            this.explanation = explanation;
            return this;
        }

        /**
         * @return the flow analysis result
         */
        public GroupAnalysisResult build() {
            return new GroupAnalysisResult(issueId, message, explanation, Optional.ofNullable(component));
        }
    }
}
