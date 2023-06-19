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
package org.apache.nifi.web.util;

import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;

import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class ParameterContextNameCollusionResolver {
    private static final String LINEAGE_FORMAT = "^(?<name>.+?)( \\((?<index>[0-9]+)\\))?$";
    private static final Pattern LINEAGE_PATTERN = Pattern.compile(LINEAGE_FORMAT);

    ParameterContextNameCollusionResolver(final Supplier<Collection<ParameterContextEntity>> parameterContextSource) {
        this.parameterContextSource = parameterContextSource;
    }

    private final Supplier<Collection<ParameterContextEntity>> parameterContextSource;

    public String resolveNameCollusion(final String originalParameterContextName) {
        final Matcher lineageMatcher = LINEAGE_PATTERN.matcher(originalParameterContextName);

        if (!lineageMatcher.matches()) {
            throw new IllegalArgumentException("Existing Parameter Context name \"(" + originalParameterContextName + "\") cannot be processed");
        }

        final String lineName = lineageMatcher.group("name");
        final String originalIndex = lineageMatcher.group("index");

        // Candidates cannot be cached because new context might be added between calls
        final Set<ParameterContextDTO> candidates = parameterContextSource.get()
                .stream()
                .map(pc -> pc.getComponent())
                .filter(dto -> dto.getName().startsWith(lineName))
                .collect(Collectors.toSet());

        int biggestIndex = (originalIndex == null) ? 0 : Integer.valueOf(originalIndex);

        for (final ParameterContextDTO candidate : candidates) {
            final Matcher matcher = LINEAGE_PATTERN.matcher(candidate.getName());

            if (matcher.matches() && lineName.equals(matcher.group("name"))) {
                final String indexGroup = matcher.group("index");

                if (indexGroup != null) {
                    int biggestIndexCandidate = Integer.valueOf(indexGroup);

                    if (biggestIndexCandidate > biggestIndex) {
                        biggestIndex = biggestIndexCandidate;
                    }
                }
            }
        }

        return new StringBuilder(lineName).append(" (").append(biggestIndex + 1).append(')').toString();
    }
}
