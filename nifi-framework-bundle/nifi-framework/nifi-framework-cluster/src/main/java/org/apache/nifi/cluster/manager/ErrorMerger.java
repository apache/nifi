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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ProcessorDTO;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ErrorMerger {

    private ErrorMerger() {}

    /**
     * Merges the validation or authorization errors.
     *
     * @param validationErrorMap errors for each node
     * @param nodeId node id
     * @param nodeErrors node errors
     */
    public static void mergeErrors(final Map<String, Set<NodeIdentifier>> validationErrorMap, final NodeIdentifier nodeId, final Collection<String> nodeErrors) {
        if (nodeErrors != null) {
            nodeErrors.stream().forEach(
                    err -> validationErrorMap.computeIfAbsent(err, k -> new HashSet<>())
                            .add(nodeId));
        }
    }

    /**
     * Normalizes the validation errors.
     *
     * @param errorMap validation errors for each node
     * @param totalNodes total number of nodes
     * @return the normalized validation errors
     */
    public static Set<String> normalizedMergedErrors(final Map<String, Set<NodeIdentifier>> errorMap, int totalNodes) {
        final Set<String> normalizedErrors = new HashSet<>();
        for (final Map.Entry<String, Set<NodeIdentifier>> validationEntry : errorMap.entrySet()) {
            final String msg = validationEntry.getKey();
            final Set<NodeIdentifier> nodeIds = validationEntry.getValue();

            if (nodeIds.size() == totalNodes) {
                normalizedErrors.add(msg);
            } else {
                nodeIds.forEach(id -> normalizedErrors.add(id.getApiAddress() + ":" + id.getApiPort() + " -- " + msg));
            }
        }
        return normalizedErrors;
    }

    /**
     * Determines the appropriate Validation Status to use as the aggregate for the given validation statuses
     *
     * @param validationStatuses the components' validation statuses
     * @return {@link ProcessorDTO#INVALID} if any status is invalid, else {@link ProcessorDTO#VALIDATING} if any status is validating, else {@link ProcessorDTO#VALID}
     */
    public static <T> String mergeValidationStatus(final Collection<String> validationStatuses) {
        final boolean anyValidating = validationStatuses.stream()
            .anyMatch(status -> ProcessorDTO.VALIDATING.equalsIgnoreCase(status));

        if (anyValidating) {
            return ProcessorDTO.VALIDATING;
        }

        final boolean anyInvalid = validationStatuses.stream()
            .anyMatch(status -> ProcessorDTO.INVALID.equalsIgnoreCase(status));

        if (anyInvalid) {
            return ProcessorDTO.INVALID;
        }

        return ProcessorDTO.VALID;
    }
}
