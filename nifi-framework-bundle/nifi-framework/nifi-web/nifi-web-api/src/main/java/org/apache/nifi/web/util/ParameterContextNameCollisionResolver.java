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

import org.apache.nifi.parameter.ParameterContextNameUtils;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class ParameterContextNameCollisionResolver {

    public String resolveNameCollision(final String originalParameterContextName, final Collection<ParameterContextEntity> existingContexts) {
        final String lineName = ParameterContextNameUtils.extractBaseName(originalParameterContextName);
        final int originalIndex = ParameterContextNameUtils.extractSuffixIndex(originalParameterContextName);

        // Candidates cannot be cached because new context might be added between calls
        final Set<ParameterContextDTO> candidates = existingContexts
                .stream()
                .map(ParameterContextEntity::getComponent)
                .filter(dto -> dto.getName().startsWith(lineName))
                .collect(Collectors.toSet());

        int biggestIndex = Math.max(originalIndex, 0);

        for (final ParameterContextDTO candidate : candidates) {
            try {
                final String candidateBaseName = ParameterContextNameUtils.extractBaseName(candidate.getName());
                if (lineName.equals(candidateBaseName)) {
                    final int candidateIndex = ParameterContextNameUtils.extractSuffixIndex(candidate.getName());
                    if (candidateIndex > biggestIndex) {
                        biggestIndex = candidateIndex;
                    }
                }
            } catch (final IllegalArgumentException ignored) {
                // Candidate name doesn't match expected pattern, skip it
            }
        }

        return ParameterContextNameUtils.createNameWithSuffix(lineName, biggestIndex + 1);
    }
}
