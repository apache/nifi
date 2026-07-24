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
import org.apache.nifi.components.Backlog;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.BacklogDTO;
import org.apache.nifi.web.api.dto.BacklogRequestDTO;
import org.apache.nifi.web.api.entity.BacklogEntity;
import org.apache.nifi.web.api.entity.BacklogRequestEntity;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Aggregates {@link BacklogRequestEntity} responses across cluster nodes for the asynchronous backlog request
 * endpoints — {@code POST}/{@code GET}/{@code DELETE} on {@code /nifi-api/processors/{uuid}/backlog-requests} and
 * {@code /nifi-api/connectors/{uuid}/backlog-requests}, each with an optional trailing {@code /{requestId}} —
 * each node determines its own portion of the backlog independently in the background, keyed by the same
 * request identifier across the cluster.
 *
 * <p>Aggregation rules:</p>
 * <ul>
 *     <li>If any node's request failed, the merged request is marked failed and complete, with a failure reason
 *     that lists the distinct per-node failure reasons. The merged {@code backlog} is left {@code null} because
 *     the cluster cannot report a trustworthy value when any node was unable to determine its own.</li>
 *     <li>Otherwise, the merged request is complete only once every node's request is complete, and the merged
 *     percent complete is the minimum percent complete across nodes, since the request as a whole cannot be
 *     further along than its slowest node.</li>
 *     <li>Once every node is complete without failure, the merged {@code backlog} is computed by
 *     {@link #mergeEntities(BacklogEntity, List)} using the cluster-wide aggregation rules described there.</li>
 * </ul>
 */
public class BacklogRequestEndpointMerger extends AbstractSingleEntityEndpoint<BacklogRequestEntity> {

    public static final Pattern PROCESSOR_BACKLOG_REQUEST_URI_PATTERN =
            Pattern.compile("/nifi-api/processors/[a-f0-9\\-]{36}/backlog-requests(/[a-f0-9\\-]{36})?");
    public static final Pattern CONNECTOR_BACKLOG_REQUEST_URI_PATTERN =
            Pattern.compile("/nifi-api/connectors/[a-f0-9\\-]{36}/backlog-requests(/[a-f0-9\\-]{36})?");

    private static final long NOW_WINDOW_MILLISECONDS = 3000L;

    @Override
    protected Class<BacklogRequestEntity> getEntityClass() {
        return BacklogRequestEntity.class;
    }

    @Override
    public boolean canHandle(final URI uri, final String method) {
        final String path = uri.getPath();
        return PROCESSOR_BACKLOG_REQUEST_URI_PATTERN.matcher(path).matches()
                || CONNECTOR_BACKLOG_REQUEST_URI_PATTERN.matcher(path).matches();
    }

    @Override
    protected void mergeResponses(final BacklogRequestEntity clientEntity, final Map<NodeIdentifier, BacklogRequestEntity> entityMap,
                                   final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        final BacklogRequestDTO clientDto = clientEntity.getRequest();
        final List<BacklogRequestDTO> nodeDtos = entityMap.values().stream()
                .map(BacklogRequestEntity::getRequest)
                .collect(Collectors.toList());

        final List<String> failureReasons = nodeDtos.stream()
                .map(BacklogRequestDTO::getFailureReason)
                .filter(reason -> reason != null)
                .distinct()
                .collect(Collectors.toList());

        if (!failureReasons.isEmpty()) {
            clientDto.setComplete(true);
            clientDto.setPercentCompleted(100);
            clientDto.setFailureReason(String.join("; ", failureReasons));
            clientDto.setState("Failed: " + String.join("; ", failureReasons));
            clientDto.setBacklog(null);
            return;
        }

        final boolean allComplete = nodeDtos.stream().allMatch(BacklogRequestDTO::isComplete);
        final int minPercentCompleted = nodeDtos.stream().mapToInt(BacklogRequestDTO::getPercentCompleted).min().orElse(0);

        clientDto.setComplete(allComplete);
        clientDto.setPercentCompleted(allComplete ? 100 : minPercentCompleted);

        if (!allComplete) {
            clientDto.setState("In Progress");
            clientDto.setBacklog(null);
            return;
        }

        clientDto.setState("Complete");

        final List<BacklogEntity> backlogEntities = new ArrayList<>(nodeDtos.size());
        for (final BacklogRequestDTO nodeDto : nodeDtos) {
            final BacklogEntity backlogEntity = new BacklogEntity();
            backlogEntity.setBacklog(nodeDto.getBacklog());
            backlogEntities.add(backlogEntity);
        }

        final BacklogEntity mergedBacklogEntity = new BacklogEntity();
        mergeEntities(mergedBacklogEntity, backlogEntities);
        clientDto.setBacklog(mergedBacklogEntity.getBacklog());
    }

    /**
     * Mutates {@code clientEntity} in-place so that its {@code backlog} field reflects the cluster-wide
     * aggregation across the provided entities. Visible for testing.
     *
     * <p>Each reporting node's {@link BacklogDTO} is converted back into a {@link Backlog} and the nodes are
     * combined with {@link Backlog#plus(Backlog)}, so the numeric-summation, precision, and earliest-timestamp
     * rules are defined in exactly one place — the nifi-api {@code Backlog} type — rather than being
     * re-implemented here. The cluster-specific rules below are then layered on top of that combination.</p>
     *
     * <p>Aggregation rules:</p>
     * <ul>
     *     <li>
     *         Numeric dimensions ({@code flowFileCount}, {@code byteCount}, {@code recordCount}): a dimension
     *         a node does not report is omitted rather than treated as zero, and {@link Backlog#plus(Backlog)}
     *         sums the reported values. A dimension that no reporting node populated stays absent, so the JSON
     *         output continues to omit it.
     *     </li>
     *     <li>
     *         {@code precision}: {@link Backlog#plus(Backlog)} yields {@code EXACT} only when every reporting
     *         node is {@code EXACT} and every node that reports numeric dimensions reports the same set of them;
     *         a node that reports no numeric dimensions (for example, only a {@code lastCaughtUp}) contributes
     *         no numeric uncertainty. In addition, if any node returned {@code backlog == null} the cluster
     *         total is by definition a lower bound — the unknown node could be holding additional work — so the
     *         merged precision is forced to {@code AT_LEAST}.
     *     </li>
     *     <li>
     *         {@code lastCaughtUp}: the earliest reported timestamp across reporting nodes. It is cleared to
     *         {@code null} if any reporting node returned {@code null} for {@code lastCaughtUp} <i>or</i> if any
     *         node returned {@code backlog == null}, because the cluster cannot claim it is caught up unless
     *         every node reported that it was.
     *     </li>
     *     <li>
     *         A per-node response whose {@code backlog} property is {@code null} counts as "no value to
     *         merge" for that node. If every node returns {@code backlog == null}, the merged entity is
     *         also {@code backlog == null}. Otherwise the {@code null}-backlog nodes are excluded from
     *         summation, but their absence forces the merged precision to {@code AT_LEAST} and clears
     *         the merged {@code lastCaughtUp} as described above.
     *     </li>
     * </ul>
     *
     * @param clientEntity the entity returned to the caller; its {@code backlog} is replaced
     * @param entities every per-node entity contributing to the merge, including the client entity
     */
    static void mergeEntities(final BacklogEntity clientEntity, final List<BacklogEntity> entities) {
        final List<Backlog> reportingBacklogs = new ArrayList<>(entities.size());
        boolean anyNodeMissingBacklog = false;
        boolean anyNodeMissingLastCaughtUp = false;
        for (final BacklogEntity entity : entities) {
            final BacklogDTO dto = entity == null ? null : entity.getBacklog();
            if (dto == null) {
                // A null entity or a null backlog is a non-reporting node: the cluster has no value from that
                // node and therefore cannot claim completeness for the dimensions it might be holding.
                anyNodeMissingBacklog = true;
                continue;
            }

            reportingBacklogs.add(toBacklog(dto));
            if (dto.getLastCaughtUp() == null) {
                anyNodeMissingLastCaughtUp = true;
            }
        }

        if (reportingBacklogs.isEmpty()) {
            clientEntity.setBacklog(null);
            return;
        }

        Backlog combined = reportingBacklogs.getFirst();
        for (int i = 1; i < reportingBacklogs.size(); i++) {
            combined = combined.plus(reportingBacklogs.get(i));
        }

        // A non-reporting node could be holding additional work, so its absence taints the merged counts.
        final Backlog.Precision precision = anyNodeMissingBacklog ? Backlog.Precision.AT_LEAST : combined.getPrecision();
        // The cluster can only claim it is caught up when every node reported and every reporting node supplied
        // a lastCaughtUp; otherwise the earliest-timestamp claim would rest on incomplete data.
        final boolean clusterMayBeCaughtUp = !anyNodeMissingBacklog && !anyNodeMissingLastCaughtUp;

        clientEntity.setBacklog(toDto(combined, precision, clusterMayBeCaughtUp));
    }

    private static Backlog toBacklog(final BacklogDTO dto) {
        final Backlog.Builder builder = Backlog.builder();
        if (dto.getFlowFileCount() != null) {
            builder.flowFiles(dto.getFlowFileCount());
        }

        if (dto.getByteCount() != null) {
            builder.bytes(dto.getByteCount());
        }

        if (dto.getRecordCount() != null) {
            builder.records(dto.getRecordCount());
        }

        if (dto.getLastCaughtUp() != null) {
            builder.lastCaughtUp(Instant.parse(dto.getLastCaughtUp()));
        }

        // Any value other than the exact EXACT marker, including a missing precision, is treated as a lower
        // bound so the merged result never over-claims exactness.
        final boolean exact = Backlog.Precision.EXACT.name().equals(dto.getPrecision());
        builder.precision(exact ? Backlog.Precision.EXACT : Backlog.Precision.AT_LEAST);
        return builder.build();
    }

    private static BacklogDTO toDto(final Backlog combined, final Backlog.Precision precision, final boolean clusterMayBeCaughtUp) {
        final BacklogDTO merged = new BacklogDTO();
        merged.setPrecision(precision.name());

        if (combined.getFlowFileCount().isPresent()) {
            final long flowFileCount = combined.getFlowFileCount().getAsLong();
            merged.setFlowFileCount(flowFileCount);
            merged.setFormattedFlowFileCount(FormatUtils.formatCount(flowFileCount));
        }

        if (combined.getByteCount().isPresent()) {
            final long byteCount = combined.getByteCount().getAsLong();
            merged.setByteCount(byteCount);
            merged.setFormattedByteCount(FormatUtils.formatDataSize(byteCount));
        }

        if (combined.getRecordCount().isPresent()) {
            final long recordCount = combined.getRecordCount().getAsLong();
            merged.setRecordCount(recordCount);
            merged.setFormattedRecordCount(FormatUtils.formatCount(recordCount));
        }

        if (clusterMayBeCaughtUp && combined.getLastCaughtUp().isPresent()) {
            final Instant lastCaughtUp = combined.getLastCaughtUp().get();
            merged.setLastCaughtUp(lastCaughtUp.toString());
            merged.setFormattedLastCaughtUp(computeFormattedLastCaughtUp(lastCaughtUp, merged));
        }

        return merged;
    }

    private static String computeFormattedLastCaughtUp(final Instant lastCaughtUp, final BacklogDTO merged) {
        final Instant now = Instant.now();
        if (isNumericallyCaughtUp(merged)
                && Math.abs(now.toEpochMilli() - lastCaughtUp.toEpochMilli()) <= NOW_WINDOW_MILLISECONDS) {
            return "now";
        }

        return FormatUtils.formatRelativeTime(lastCaughtUp, now);
    }

    private static boolean isNumericallyCaughtUp(final BacklogDTO dto) {
        return isZeroOrNull(dto.getFlowFileCount())
                && isZeroOrNull(dto.getByteCount())
                && isZeroOrNull(dto.getRecordCount());
    }

    private static boolean isZeroOrNull(final Long value) {
        return value == null || value.longValue() == 0L;
    }
}
