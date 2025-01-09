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
package org.apache.nifi.flowanalysis.rules.util;

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlowAnalysisRuleUtils {
    /**
     * Build a map of all id/components in a ProcessGroup so that we can generate more human-readable violations.
     *
     * @param group a ProcessGroup
     * @return a map of VersionedComponents indexed by their id
     */
    public static Map<String, VersionedComponent> gatherComponents(VersionedProcessGroup group) {
        return Stream.of(
                group.getFunnels().stream(),
                group.getProcessors().stream(),
                group.getInputPorts().stream(),
                group.getOutputPorts().stream())
                .flatMap(c -> c)
                .collect(Collectors.toMap(VersionedComponent::getIdentifier, Function.identity()));
    }

    /**
     * Convert ConnectionViolations to GroupAnalysisResults.
     *
     * @param pg process group containing the connections
     * @param violations flow analysis violations
     * @return a collection of Flow Analysis Rule GroupAnalysisResult
     */
    public static Collection<GroupAnalysisResult> convertToGroupAnalysisResults(VersionedProcessGroup pg, Collection<ConnectionViolation> violations) {
        if (!violations.isEmpty()) {
            final Map<String, VersionedComponent> components = FlowAnalysisRuleUtils.gatherComponents(pg);
            final Collection<GroupAnalysisResult> results = new HashSet<>();
            violations.forEach(violation -> results.add(violation.convertToGroupAnalysisResult(components)));
            return results;
        } else {
            return Collections.emptySet();
        }
    }
}
