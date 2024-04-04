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

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigVerificationResultMerger {
    private final Map<String, List<ConfigVerificationResultDTO>> verificationResultDtos = new HashMap<>();

    /**
     * Adds the config verification results for one of the nodes in the cluster
     * @param nodeId the ID of the node in the cluster
     * @param nodeResults the results for the config verification
     */
    public void addNodeResults(final NodeIdentifier nodeId, final List<ConfigVerificationResultDTO> nodeResults) {
        if (nodeResults == null || nodeResults.isEmpty()) {
            return;
        }

        verificationResultDtos.put(nodeId.getApiAddress() + ":" + nodeId.getApiPort(), nodeResults);
    }

    /**
     * Computes the aggregate list of ConfigVerificationResultDTO based on all of the results added using the {link {@link #addNodeResults(NodeIdentifier, List)}} method
     * @return the aggregate results of the config verification results from all nodes
     */
    public List<ConfigVerificationResultDTO> computeAggregateResults() {
        // For each node, build up a mapping of Step Name -> Results
        final Map<String, List<ConfigVerificationResultDTO>> resultsByStepName = new HashMap<>();
        for (final Map.Entry<String, List<ConfigVerificationResultDTO>> entry : verificationResultDtos.entrySet()) {
            final String nodeId = entry.getKey();
            final List<ConfigVerificationResultDTO> nodeResults = entry.getValue();

            // If the result hasn't been set, the task is not yet complete, so we don't have to bother merging the results.
            if (nodeResults == null) {
                return null;
            }

            for (final ConfigVerificationResultDTO result : nodeResults) {
                final String stepName = result.getVerificationStepName();
                final List<ConfigVerificationResultDTO> resultList = resultsByStepName.computeIfAbsent(stepName, key -> new ArrayList<>());

                // If skipped or unsuccessful, add the node's address to the explanation
                if (!Outcome.SUCCESSFUL.name().equals(result.getOutcome())) {
                    result.setExplanation(nodeId + " - " + result.getExplanation());
                }

                resultList.add(result);
            }
        }

        // Merge together all results for each step name
        final List<ConfigVerificationResultDTO> aggregateResults = new ArrayList<>();
        for (final Map.Entry<String, List<ConfigVerificationResultDTO>> entry : resultsByStepName.entrySet()) {
            final String stepName = entry.getKey();
            final List<ConfigVerificationResultDTO> resultList = entry.getValue();

            final ConfigVerificationResultDTO firstResult = resultList.get(0); // This is safe because the list won't be added to the map unless it has at least 1 element.
            String outcome = firstResult.getOutcome();
            String explanation = firstResult.getExplanation();

            for (final ConfigVerificationResultDTO result : resultList) {
                // If any node indicates failure, the outcome is failure.
                // Otherwise, if any node indicates that a step was skipped, the outcome is skipped.
                // Otherwise, all nodes have reported the outcome is successful, so the outcome is successful.
                if (Outcome.FAILED.name().equals(result.getOutcome())) {
                    outcome = result.getOutcome();
                    explanation = result.getExplanation();
                } else if (Outcome.SKIPPED.name().equals(result.getOutcome()) && Outcome.SUCCESSFUL.name().equals(outcome)) {
                    outcome = result.getOutcome();
                    explanation = result.getExplanation();
                }
            }

            final ConfigVerificationResultDTO resultDto = new ConfigVerificationResultDTO();
            resultDto.setVerificationStepName(stepName);
            resultDto.setOutcome(outcome);
            resultDto.setExplanation(explanation);

            aggregateResults.add(resultDto);
        }

        // Determine the ordering of the original steps.
        final Map<String, Integer> stepOrders = new HashMap<>();
        for (final List<ConfigVerificationResultDTO> resultDtos : verificationResultDtos.values()) {
            for (final ConfigVerificationResultDTO resultDto : resultDtos) {
                final String stepName = resultDto.getVerificationStepName();
                stepOrders.putIfAbsent(stepName, stepOrders.size());
            }
        }

        // Sort the results by ordering them based on the order of the original steps. This will retain the original ordering of the steps.
        aggregateResults.sort(Comparator.comparing(dto -> stepOrders.get(dto.getVerificationStepName())));

        return aggregateResults;
    }
}
