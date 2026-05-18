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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigVerificationResultMergerTest {

    private static final NodeIdentifier NODE_1 = new NodeIdentifier("node-1", "host-1", 8080, "host-1", 19998, null, null, null, false);
    private static final NodeIdentifier NODE_2 = new NodeIdentifier("node-2", "host-2", 8081, "host-2", 19999, null, null, null, false);

    @Test
    void testSingleNodeSuccessful() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "All good")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(Outcome.SUCCESSFUL.name(), results.get(0).getOutcome());
        assertEquals("All good", results.get(0).getExplanation());
    }

    @Test
    void testSingleNodeFailed() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.FAILED, "Connection refused")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(Outcome.FAILED.name(), results.get(0).getOutcome());
        assertEquals("Connection refused", results.get(0).getExplanation());
    }

    @Test
    void testMultiNodeAllAgreeOnFailure() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.FAILED, "Connection refused")));
        merger.addNodeResults(NODE_2, List.of(createResult("Step 1", Outcome.FAILED, "Connection refused")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(Outcome.FAILED.name(), results.get(0).getOutcome());
        assertEquals("Connection refused", results.get(0).getExplanation());
    }

    @Test
    void testMultiNodeAllAgreeOnSuccess() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "Connected")));
        merger.addNodeResults(NODE_2, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "Connected")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(Outcome.SUCCESSFUL.name(), results.get(0).getOutcome());
        assertEquals("Connected", results.get(0).getExplanation());
    }

    @Test
    void testMultiNodeDisagreeOnOutcome() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "Connected")));
        merger.addNodeResults(NODE_2, List.of(createResult("Step 1", Outcome.FAILED, "Connection refused")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(Outcome.FAILED.name(), results.get(0).getOutcome());
        assertEquals("host-2:8081 - Connection refused", results.get(0).getExplanation());
    }

    @Test
    void testMultiNodeDisagreeOnExplanation() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.FAILED, "Timeout after 5s")));
        merger.addNodeResults(NODE_2, List.of(createResult("Step 1", Outcome.FAILED, "Connection refused")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(Outcome.FAILED.name(), results.get(0).getOutcome());
        final String explanation = results.get(0).getExplanation();
        assertTrue(explanation.contains(" - "), "Node prefix should be included when explanations differ");
        assertTrue(explanation.startsWith("host-"), "Explanation should start with node address prefix");
    }

    @Test
    void testMultiNodeSkippedOverridesSuccess() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "OK")));
        merger.addNodeResults(NODE_2, List.of(createResult("Step 1", Outcome.SKIPPED, "Skipped due to config")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(Outcome.SKIPPED.name(), results.get(0).getOutcome());
        assertEquals("host-2:8081 - Skipped due to config", results.get(0).getExplanation());
    }

    @Test
    void testNullResultsFilteredByAddNodeResults() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "OK")));
        merger.addNodeResults(NODE_2, null);

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("OK", results.get(0).getExplanation());
    }

    @Test
    void testInputDtosNotMutated() {
        final ConfigVerificationResultDTO node1Result = createResult("Step 1", Outcome.FAILED, "Connection refused");
        final ConfigVerificationResultDTO node2Result = createResult("Step 1", Outcome.FAILED, "Connection refused");

        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(node1Result));
        merger.addNodeResults(NODE_2, List.of(node2Result));

        merger.computeAggregateResults();

        assertEquals("Connection refused", node1Result.getExplanation());
        assertEquals("Connection refused", node2Result.getExplanation());
    }

    @Test
    void testMultipleStepsPreserveOrder() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(
                createResult("Step A", Outcome.SUCCESSFUL, "OK"),
                createResult("Step B", Outcome.FAILED, "Bad config"),
                createResult("Step C", Outcome.SKIPPED, "Skipped")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(3, results.size());
        assertEquals("Step A", results.get(0).getVerificationStepName());
        assertEquals("Step B", results.get(1).getVerificationStepName());
        assertEquals("Step C", results.get(2).getVerificationStepName());
    }

    @Test
    void testEmptyResultsIgnored() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of());
        merger.addNodeResults(NODE_2, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "OK")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("OK", results.get(0).getExplanation());
    }

    @Test
    void testSingleNodeSubjectPreserved() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.FAILED, "Connection refused", "Hostname")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("Hostname", results.get(0).getSubject());
    }

    @Test
    void testMultiNodeAgreeSubjectPreserved() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "Connected", "Hostname")));
        merger.addNodeResults(NODE_2, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "Connected", "Hostname")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals("Hostname", results.get(0).getSubject());
    }

    @Test
    void testMultiNodeDisagreeSubjectFromSelectedResult() {
        final ConfigVerificationResultMerger merger = new ConfigVerificationResultMerger();
        merger.addNodeResults(NODE_1, List.of(createResult("Step 1", Outcome.SUCCESSFUL, "Connected", "Hostname")));
        merger.addNodeResults(NODE_2, List.of(createResult("Step 1", Outcome.FAILED, "Connection refused", "Port")));

        final List<ConfigVerificationResultDTO> results = merger.computeAggregateResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertEquals(Outcome.FAILED.name(), results.get(0).getOutcome());
        assertEquals("Port", results.get(0).getSubject());
    }

    private static ConfigVerificationResultDTO createResult(final String stepName, final Outcome outcome, final String explanation) {
        return createResult(stepName, outcome, explanation, null);
    }

    private static ConfigVerificationResultDTO createResult(final String stepName, final Outcome outcome, final String explanation, final String subject) {
        final ConfigVerificationResultDTO dto = new ConfigVerificationResultDTO();
        dto.setVerificationStepName(stepName);
        dto.setOutcome(outcome.name());
        dto.setExplanation(explanation);
        dto.setSubject(subject);
        return dto;
    }
}
