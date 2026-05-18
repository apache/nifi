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

    private record NodeResult(String nodeId, ConfigVerificationResultDTO result) { }

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
     * Computes the aggregate list of ConfigVerificationResultDTO based on all of the results added using the {link {@link #addNodeResults(NodeIdentifier, List)}} method.
     *
     * <p>Node address information is only included in the explanation when nodes produce different outcomes or explanations for the same
     * verification step. When all nodes agree (including single-node deployments), the explanation is returned without any node prefix.</p>
     *
     * @return the aggregate results of the config verification results from all nodes
     */
    public List<ConfigVerificationResultDTO> computeAggregateResults() {
        // For each node, build up a mapping of Step Name -> Results
        final Map<String, List<NodeResult>> resultsByStepName = new HashMap<>();
        for (final Map.Entry<String, List<ConfigVerificationResultDTO>> entry : verificationResultDtos.entrySet()) {
            final String nodeId = entry.getKey();
            final List<ConfigVerificationResultDTO> nodeResults = entry.getValue();

            // If the result hasn't been set, the task is not yet complete, so we don't have to bother merging the results.
            if (nodeResults == null) {
                return null;
            }

            for (final ConfigVerificationResultDTO result : nodeResults) {
                resultsByStepName.computeIfAbsent(result.getVerificationStepName(), key -> new ArrayList<>())
                    .add(new NodeResult(nodeId, result));
            }
        }

        // Merge together all results for each step name
        final List<ConfigVerificationResultDTO> aggregateResults = new ArrayList<>();
        for (final Map.Entry<String, List<NodeResult>> entry : resultsByStepName.entrySet()) {
            final String stepName = entry.getKey();
            final List<NodeResult> stepResults = entry.getValue();

            // Select the worst outcome: FAILED > SKIPPED > SUCCESSFUL
            NodeResult selected = stepResults.get(0);
            for (final NodeResult nodeResult : stepResults) {
                if (Outcome.FAILED.name().equals(nodeResult.result().getOutcome())) {
                    selected = nodeResult;
                } else if (Outcome.SKIPPED.name().equals(nodeResult.result().getOutcome())
                        && Outcome.SUCCESSFUL.name().equals(selected.result().getOutcome())) {
                    selected = nodeResult;
                }
            }

            // Only include node prefix when results differ across nodes
            final long distinctResultCount = stepResults.stream()
                .map(nodeResult -> nodeResult.result().getOutcome() + " -- " + nodeResult.result().getExplanation())
                .distinct()
                .count();

            final String aggregateExplanation;
            if (distinctResultCount > 1 && !Outcome.SUCCESSFUL.name().equals(selected.result().getOutcome())) {
                aggregateExplanation = selected.nodeId() + " - " + selected.result().getExplanation();
            } else {
                aggregateExplanation = selected.result().getExplanation();
            }

            final ConfigVerificationResultDTO resultDto = new ConfigVerificationResultDTO();
            resultDto.setVerificationStepName(stepName);
            resultDto.setOutcome(selected.result().getOutcome());
            resultDto.setExplanation(aggregateExplanation);
            resultDto.setSubject(selected.result().getSubject());

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
