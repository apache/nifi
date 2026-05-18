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

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.BacklogDTO;
import org.apache.nifi.web.api.dto.BacklogRequestDTO;
import org.apache.nifi.web.api.entity.BacklogEntity;
import org.apache.nifi.web.api.entity.BacklogRequestEntity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BacklogRequestEndpointMergerTest {

    private static final String PROCESSOR_UUID = "12345678-1234-1234-1234-123456789012";
    private static final String CONNECTOR_UUID = "abcdef00-abcd-abcd-abcd-abcdef000000";
    private static final String REQUEST_UUID = "00000000-0000-0000-0000-000000000001";

    private final BacklogRequestEndpointMerger merger = new BacklogRequestEndpointMerger();

    @Test
    public void testCanHandleAcceptsProcessorBacklogRequestCreationUri() {
        assertTrue(merger.canHandle(URI.create("http://localhost:8080/nifi-api/processors/" + PROCESSOR_UUID + "/backlog-requests"), "POST"));
    }

    @Test
    public void testCanHandleAcceptsProcessorBacklogRequestPollingUri() {
        assertTrue(merger.canHandle(
                URI.create("http://localhost:8080/nifi-api/processors/" + PROCESSOR_UUID + "/backlog-requests/" + REQUEST_UUID), "GET"));
    }

    @Test
    public void testCanHandleAcceptsConnectorBacklogRequestUris() {
        assertTrue(merger.canHandle(URI.create("http://localhost:8080/nifi-api/connectors/" + CONNECTOR_UUID + "/backlog-requests"), "POST"));
        assertTrue(merger.canHandle(
                URI.create("http://localhost:8080/nifi-api/connectors/" + CONNECTOR_UUID + "/backlog-requests/" + REQUEST_UUID), "DELETE"));
    }

    @Test
    public void testCanHandleRejectsUnrelatedUri() {
        assertFalse(merger.canHandle(URI.create("http://localhost:8080/nifi-api/processors/" + PROCESSOR_UUID + "/backlog"), "GET"));
    }

    @Test
    public void testMergeWaitsForSlowestNodeBeforeReportingComplete() {
        final BacklogRequestEntity client = requestEntity(false, 50, null, null);
        final Map<NodeIdentifier, BacklogRequestEntity> entityMap = nodeMap(
                client,
                requestEntity(false, 20, null, null));

        merger.mergeResponses(client, entityMap, null, null);

        assertFalse(client.getRequest().isComplete());
        assertEquals(20, client.getRequest().getPercentCompleted());
        assertNull(client.getRequest().getBacklog());
    }

    @Test
    public void testMergeReportsCompleteAndAggregatesBacklogOnceEveryNodeIsComplete() {
        final BacklogRequestEntity client = requestEntity(true, 100, backlog(10L), null);
        final Map<NodeIdentifier, BacklogRequestEntity> entityMap = nodeMap(
                client,
                requestEntity(true, 100, backlog(5L), null));

        merger.mergeResponses(client, entityMap, null, null);

        assertTrue(client.getRequest().isComplete());
        assertEquals(100, client.getRequest().getPercentCompleted());
        assertEquals(15L, client.getRequest().getBacklog().getFlowFileCount());
    }

    @Test
    public void testMergeMarksFailedWhenAnyNodeFailsEvenIfOthersAreStillInProgress() {
        final BacklogRequestEntity client = requestEntity(false, 40, null, null);
        final Map<NodeIdentifier, BacklogRequestEntity> entityMap = nodeMap(
                client,
                requestEntity(true, 100, null, "Interrupted while determining backlog"));

        merger.mergeResponses(client, entityMap, null, null);

        assertTrue(client.getRequest().isComplete());
        assertEquals(100, client.getRequest().getPercentCompleted());
        assertEquals("Interrupted while determining backlog", client.getRequest().getFailureReason());
        assertNull(client.getRequest().getBacklog());
    }

    @Test
    public void testMergeCombinesDistinctFailureReasonsFromMultipleNodes() {
        final BacklogRequestEntity client = requestEntity(true, 100, null, "Source unreachable");
        final Map<NodeIdentifier, BacklogRequestEntity> entityMap = nodeMap(
                client,
                requestEntity(true, 100, null, "Interrupted while determining backlog"));

        merger.mergeResponses(client, entityMap, null, null);

        final String failureReason = client.getRequest().getFailureReason();
        assertTrue(failureReason.contains("Source unreachable"));
        assertTrue(failureReason.contains("Interrupted while determining backlog"));
    }

    @Test
    public void testMergeIgnoresProblematicNodesAndAggregatesOnlyRespondingNodes() {
        // AbstractSingleEntityEndpoint places only nodes that returned a successful response into entityMap, so a
        // node that failed to respond (a "problematic" node) is absent from entityMap entirely. This test documents
        // the merger's current behavior: it derives the merged request solely from entityMap and never inspects the
        // problematicResponses parameter, so a non-responding node is silently excluded. As a result the cluster is
        // reported complete with an aggregated backlog even though one node never reported.
        final BacklogRequestEntity client = requestEntity(true, 100, backlog(10L), null);
        final Map<NodeIdentifier, BacklogRequestEntity> entityMap = new LinkedHashMap<>();
        entityMap.put(nodeIdentifier(1), client);

        final NodeResponse problematicResponse = new NodeResponse(nodeIdentifier(2), "GET",
                URI.create("http://localhost:8082/nifi-api/processors/" + PROCESSOR_UUID + "/backlog-requests/" + REQUEST_UUID),
                new RuntimeException("Node was unreachable"));
        final Set<NodeResponse> problematicResponses = Collections.singleton(problematicResponse);

        merger.mergeResponses(client, entityMap, Collections.emptySet(), problematicResponses);

        assertTrue(client.getRequest().isComplete());
        assertEquals(100, client.getRequest().getPercentCompleted());
        assertNull(client.getRequest().getFailureReason());
        assertEquals(10L, client.getRequest().getBacklog().getFlowFileCount());
    }

    @Test
    public void testMergeEntitiesAllNullBacklogsProduceNullBacklog() {
        final BacklogEntity client = entityWith(null);
        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(entityWith(null), entityWith(null), entityWith(null)));
        assertNull(client.getBacklog());
    }

    @Test
    public void testMergeEntitiesSumsNumericDimensionsAcrossNodes() {
        final BacklogEntity client = entityWith(backlog(10L, 100L, 1000L, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(5L, 50L, 500L, "2026-05-15T09:00:00Z", "EXACT"));
        final BacklogEntity nodeThree = entityWith(backlog(2L, 20L, 200L, "2026-05-15T08:30:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo, nodeThree));

        final BacklogDTO merged = client.getBacklog();
        assertNotNull(merged);
        assertEquals(17L, merged.getFlowFileCount());
        assertEquals(170L, merged.getByteCount());
        assertEquals(1700L, merged.getRecordCount());
        assertEquals("2026-05-15T08:30:00Z", merged.getLastCaughtUp());
        assertEquals("EXACT", merged.getPrecision());
    }

    @Test
    public void testMergeEntitiesPopulatesFormattedStringsFromMergedTotals() {
        final BacklogEntity client = entityWith(backlog(1024L, 5_368_709_120L, 2050L, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(1L, 1024L, 1L, "2026-05-15T09:00:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        final BacklogDTO merged = client.getBacklog();
        assertEquals(1025L, merged.getFlowFileCount());
        // The formatted strings are produced by the same FormatUtils helpers the runtime uses, so compare against
        // those helpers directly to keep the assertions independent of the test machine's locale.
        assertEquals(FormatUtils.formatCount(1025L), merged.getFormattedFlowFileCount());
        assertEquals(5_368_710_144L, merged.getByteCount());
        assertEquals(FormatUtils.formatDataSize(5_368_710_144L), merged.getFormattedByteCount());
        assertEquals(2051L, merged.getRecordCount());
        assertEquals(FormatUtils.formatCount(2051L), merged.getFormattedRecordCount());
    }

    @Test
    public void testMergeEntitiesOmitsFormattedStringWhenDimensionIsNull() {
        final BacklogEntity client = entityWith(backlog(null, 100L, null, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(null, 50L, null, "2026-05-15T09:00:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        final BacklogDTO merged = client.getBacklog();
        assertNull(merged.getFormattedFlowFileCount());
        assertEquals(FormatUtils.formatDataSize(150L), merged.getFormattedByteCount());
        assertNull(merged.getFormattedRecordCount());
    }

    @Test
    public void testMergeEntitiesEmitsNullForDimensionWhenEveryNodeIsNull() {
        final BacklogEntity client = entityWith(backlog(null, 100L, null, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(null, 50L, null, "2026-05-15T09:00:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        final BacklogDTO merged = client.getBacklog();
        assertNull(merged.getFlowFileCount());
        assertEquals(150L, merged.getByteCount());
        assertNull(merged.getRecordCount());
    }

    @Test
    public void testMergeEntitiesTreatsNullDimensionAsZeroWhenAnyNodePopulatesIt() {
        final BacklogEntity client = entityWith(backlog(10L, null, null, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(null, null, null, "2026-05-15T09:00:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        assertEquals(10L, client.getBacklog().getFlowFileCount());
    }

    @Test
    public void testMergeEntitiesDowngradesPrecisionWhenShapeDiffers() {
        final BacklogEntity client = entityWith(backlog(10L, 100L, null, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(null, 50L, null, "2026-05-15T09:00:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        assertEquals("AT_LEAST", client.getBacklog().getPrecision());
    }

    @Test
    public void testMergeEntitiesDowngradesPrecisionWhenAnyNodeReportsAtLeast() {
        final BacklogEntity client = entityWith(backlog(10L, 100L, 1000L, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(5L, 50L, 500L, "2026-05-15T09:00:00Z", "AT_LEAST"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        assertEquals("AT_LEAST", client.getBacklog().getPrecision());
    }

    @Test
    public void testMergeEntitiesNullLastCaughtUpFromAnyNodeProducesNullMergedLastCaughtUp() {
        final BacklogEntity client = entityWith(backlog(10L, 100L, 1000L, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(5L, 50L, 500L, null, "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        assertNull(client.getBacklog().getLastCaughtUp());
        assertNull(client.getBacklog().getFormattedLastCaughtUp());
    }

    @Test
    public void testMergeEntitiesFormattedLastCaughtUpComputedFromMergedTimestamp() {
        // The earliest reported lastCaughtUp wins after merging; that wall-clock value is what
        // formattedLastCaughtUp must describe relative to "now". Stamp the timestamp well into the
        // past so the time-bucket assertion is stable regardless of when the test runs.
        final String earliest = Instant.now().minusSeconds(120L).toString();
        final String later = Instant.now().minusSeconds(30L).toString();

        final BacklogEntity client = entityWith(backlog(10L, 100L, 1000L, later, "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(2L, 20L, 200L, earliest, "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        assertEquals(earliest, client.getBacklog().getLastCaughtUp());
        // The merged backlog is not numerically caught up (FlowFiles/bytes/records > 0), so the
        // "now" collapse must not fire and the formatted value must be a relative-time string.
        final String formatted = client.getBacklog().getFormattedLastCaughtUp();
        assertNotNull(formatted);
        assertTrue(formatted.contains("min") || formatted.contains("hour"),
                "Expected merged formattedLastCaughtUp to describe a multi-minute gap, got: " + formatted);
    }

    @Test
    public void testMergeEntitiesFormattedLastCaughtUpEmitsNowWhenAllNodesCaughtUpWithinWindow() {
        final String nowIso = Instant.now().toString();
        final BacklogEntity client = entityWith(backlog(0L, 0L, 0L, nowIso, "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(0L, 0L, 0L, nowIso, "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        assertEquals("now", client.getBacklog().getFormattedLastCaughtUp());
    }

    @Test
    public void testMergeEntitiesFormattedLastCaughtUpUsesRelativeTimeWhenAnyNumericDimensionIsNonZero() {
        final String nowIso = Instant.now().toString();
        final BacklogEntity client = entityWith(backlog(0L, 0L, 1L, nowIso, "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(0L, 0L, 0L, nowIso, "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo));

        // recordCount sums to 1, so the merged backlog is not caught up; even though the timestamp
        // is within the "now" window, the formatted value must use the relative-time string.
        final String formatted = client.getBacklog().getFormattedLastCaughtUp();
        assertNotNull(formatted);
        assertTrue(formatted.endsWith("ago") || formatted.startsWith("in "), "Expected relative-time output, got: " + formatted);
    }

    @Test
    public void testMergeEntitiesExcludesNullBacklogEntitiesFromSummationButDowngradesPrecision() {
        // A node that returned backlog == null is excluded from numeric summation, but its absence
        // means the cluster total is by definition a lower bound — the missing node could be holding
        // additional work. The merged precision must therefore be AT_LEAST and the merged
        // lastCaughtUp must be cleared, because the cluster cannot claim it is caught up while any
        // node has failed to report.
        final BacklogEntity client = entityWith(backlog(10L, 100L, 1000L, "2026-05-15T10:00:00Z", "EXACT"));
        final BacklogEntity nullEntity = entityWith(null);
        final BacklogEntity nodeThree = entityWith(backlog(2L, 20L, 200L, "2026-05-15T08:30:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nullEntity, nodeThree));

        final BacklogDTO merged = client.getBacklog();
        assertEquals(12L, merged.getFlowFileCount());
        assertEquals(120L, merged.getByteCount());
        assertEquals(1200L, merged.getRecordCount());
        assertEquals("AT_LEAST", merged.getPrecision());
        assertNull(merged.getLastCaughtUp());
        assertNull(merged.getFormattedLastCaughtUp());
    }

    @Test
    public void testMergeEntitiesNullEntityElementForcesAtLeastEvenWhenOtherNodesAreExact() {
        // A null BacklogEntity in the entity list (for example because that node's response failed to
        // deserialize) is treated the same way as backlog == null: the merger has no value from that
        // node, so it cannot claim cluster-wide completeness.
        final BacklogEntity client = entityWith(backlog(7L, 70L, 700L, "2026-05-15T10:00:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, null));

        final BacklogDTO merged = client.getBacklog();
        assertEquals(7L, merged.getFlowFileCount());
        assertEquals("AT_LEAST", merged.getPrecision());
        assertNull(merged.getLastCaughtUp());
    }

    @Test
    public void testMergeEntitiesNullBacklogFromAnyNodeForbidsZeroExactCaughtUpClaim() {
        // Even if every reporting node says "zero, EXACT, caught up right now", a single
        // non-reporting node must prevent the cluster from claiming caught up.
        final String nowIso = Instant.now().toString();
        final BacklogEntity client = entityWith(backlog(0L, 0L, 0L, nowIso, "EXACT"));
        final BacklogEntity nodeTwo = entityWith(backlog(0L, 0L, 0L, nowIso, "EXACT"));
        final BacklogEntity nodeThree = entityWith(null);

        BacklogRequestEndpointMerger.mergeEntities(client, Arrays.asList(client, nodeTwo, nodeThree));

        final BacklogDTO merged = client.getBacklog();
        assertNotNull(merged);
        assertEquals(0L, merged.getFlowFileCount());
        assertEquals("AT_LEAST", merged.getPrecision());
        assertNull(merged.getLastCaughtUp());
        assertNull(merged.getFormattedLastCaughtUp());
    }

    @Test
    public void testMergeEntitiesSingleReportingNodeProducesItsBacklog() {
        // A single-node cluster (or single entity in the entities list) reports exactly what the
        // node reported. There are no non-reporting nodes to taint the precision.
        final BacklogEntity client = entityWith(backlog(3L, 30L, 300L, "2026-05-15T11:00:00Z", "EXACT"));

        BacklogRequestEndpointMerger.mergeEntities(client, Collections.singletonList(client));

        final BacklogDTO merged = client.getBacklog();
        assertNotNull(merged);
        assertEquals(3L, merged.getFlowFileCount());
        assertEquals("EXACT", merged.getPrecision());
    }

    @Test
    public void testMergeEntitiesEmptyEntityListProducesNullBacklog() {
        final BacklogEntity client = entityWith(backlog(99L, 999L, 9999L, Instant.now().toString(), "EXACT"));
        BacklogRequestEndpointMerger.mergeEntities(client, Collections.emptyList());
        assertNull(client.getBacklog());
    }

    private static BacklogEntity entityWith(final BacklogDTO dto) {
        final BacklogEntity entity = new BacklogEntity();
        entity.setBacklog(dto);
        return entity;
    }

    private static BacklogDTO backlog(final Long flowFiles, final Long bytes, final Long records, final String lastCaughtUp, final String precision) {
        final BacklogDTO dto = new BacklogDTO();
        dto.setFlowFileCount(flowFiles);
        dto.setByteCount(bytes);
        dto.setRecordCount(records);
        dto.setLastCaughtUp(lastCaughtUp);
        dto.setPrecision(precision);
        return dto;
    }

    private static Map<NodeIdentifier, BacklogRequestEntity> nodeMap(final BacklogRequestEntity clientEntity, final BacklogRequestEntity otherNodeEntity) {
        final Map<NodeIdentifier, BacklogRequestEntity> entityMap = new LinkedHashMap<>();
        entityMap.put(nodeIdentifier(1), clientEntity);
        entityMap.put(nodeIdentifier(2), otherNodeEntity);
        return entityMap;
    }

    private static NodeIdentifier nodeIdentifier(final int index) {
        return new NodeIdentifier("node-" + index, "localhost", 8000 + index, "localhost", 8100 + index,
                "localhost", 8200 + index, 8300 + index, false);
    }

    private static BacklogRequestEntity requestEntity(final boolean complete, final int percentCompleted, final BacklogDTO backlog, final String failureReason) {
        final BacklogRequestDTO dto = new BacklogRequestDTO();
        dto.setComplete(complete);
        dto.setPercentCompleted(percentCompleted);
        dto.setBacklog(backlog);
        dto.setFailureReason(failureReason);

        final BacklogRequestEntity entity = new BacklogRequestEntity();
        entity.setRequest(dto);
        return entity;
    }

    private static BacklogDTO backlog(final Long flowFileCount) {
        final BacklogDTO dto = new BacklogDTO();
        dto.setFlowFileCount(flowFileCount);
        dto.setPrecision("EXACT");
        return dto;
    }
}
