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

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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

    public static final String PRECISION_EXACT = "EXACT";
    public static final String PRECISION_AT_LEAST = "AT_LEAST";

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
     * <p>Aggregation rules:</p>
     * <ul>
     *     <li>
     *         Numeric dimensions ({@code flowFileCount}, {@code byteCount}, {@code recordCount}): treat a
     *         {@code null} per-node value as zero for summation. Emit the sum unless every reporting node
     *         had {@code null} for that dimension, in which case emit {@code null} on the merged DTO so
     *         the JSON output continues to omit the dimension.
     *     </li>
     *     <li>
     *         {@code precision}: emit {@code EXACT} only if every reporting node reported {@code EXACT},
     *         every reporting node reported the same set of populated numeric dimensions, <i>and</i> no
     *         node returned {@code backlog == null}. If any node returned {@code backlog == null} the
     *         cluster total is by definition a lower bound — the unknown node could be holding additional
     *         work — so emit {@code AT_LEAST}.
     *     </li>
     *     <li>
     *         {@code lastCaughtUp}: emit the minimum (earliest) ISO-8601 timestamp across reporting nodes.
     *         Emit {@code null} if any reporting node returned {@code null} for {@code lastCaughtUp},
     *         <i>or</i> if any node returned {@code backlog == null}. The cluster cannot claim it is
     *         caught up if any node has not reported.
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
        final List<BacklogDTO> reportingNodeDtos = new ArrayList<>(entities.size());
        boolean anyNodeMissingBacklog = false;
        for (final BacklogEntity entity : entities) {
            if (entity == null) {
                // A null entity is treated as a non-reporting node for the same reason a null backlog is:
                // the cluster has no value from that node and therefore cannot claim completeness.
                anyNodeMissingBacklog = true;
                continue;
            }

            final BacklogDTO dto = entity.getBacklog();
            if (dto == null) {
                anyNodeMissingBacklog = true;
            } else {
                reportingNodeDtos.add(dto);
            }
        }

        if (reportingNodeDtos.isEmpty()) {
            clientEntity.setBacklog(null);
            return;
        }

        final BacklogDTO merged = new BacklogDTO();
        final Long mergedFlowFileCount = sumDimension(reportingNodeDtos, BacklogDTO::getFlowFileCount);
        final Long mergedByteCount = sumDimension(reportingNodeDtos, BacklogDTO::getByteCount);
        final Long mergedRecordCount = sumDimension(reportingNodeDtos, BacklogDTO::getRecordCount);

        merged.setFlowFileCount(mergedFlowFileCount);
        if (mergedFlowFileCount != null) {
            merged.setFormattedFlowFileCount(FormatUtils.formatCount(mergedFlowFileCount));
        }

        merged.setByteCount(mergedByteCount);
        if (mergedByteCount != null) {
            merged.setFormattedByteCount(FormatUtils.formatDataSize(mergedByteCount));
        }

        merged.setRecordCount(mergedRecordCount);
        if (mergedRecordCount != null) {
            merged.setFormattedRecordCount(FormatUtils.formatCount(mergedRecordCount));
        }

        // If any node was unable to report, the cluster cannot claim "caught up" — that claim
        // requires every node to have observed itself caught up.
        final String mergedLastCaughtUp = anyNodeMissingBacklog ? null : minLastCaughtUp(reportingNodeDtos);
        merged.setLastCaughtUp(mergedLastCaughtUp);
        if (mergedLastCaughtUp != null) {
            merged.setFormattedLastCaughtUp(computeFormattedLastCaughtUp(mergedLastCaughtUp, merged));
        }

        merged.setPrecision(mergePrecision(reportingNodeDtos, anyNodeMissingBacklog));

        clientEntity.setBacklog(merged);
    }

    private static String computeFormattedLastCaughtUp(final String mergedLastCaughtUp, final BacklogDTO merged) {
        final Instant lastCaughtUp = Instant.parse(mergedLastCaughtUp);
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

    private static Long sumDimension(final List<BacklogDTO> dtos, final Function<BacklogDTO, Long> accessor) {
        long sum = 0L;
        boolean anyPresent = false;
        for (final BacklogDTO dto : dtos) {
            final Long value = accessor.apply(dto);
            if (value != null) {
                sum += value;
                anyPresent = true;
            }
        }

        return anyPresent ? sum : null;
    }

    /**
     * Returns the earliest {@code lastCaughtUp} timestamp across the supplied DTOs, formatted as
     * ISO-8601. If any DTO returns {@code null}, returns {@code null} so the merged DTO continues
     * to omit the field — the cluster cannot claim it is caught up if any node has not reported.
     */
    private static String minLastCaughtUp(final List<BacklogDTO> dtos) {
        Instant earliest = null;
        for (final BacklogDTO dto : dtos) {
            final String candidate = dto.getLastCaughtUp();
            if (candidate == null) {
                return null;
            }

            final Instant candidateInstant = Instant.parse(candidate);
            if (earliest == null || candidateInstant.isBefore(earliest)) {
                earliest = candidateInstant;
            }
        }

        return earliest == null ? null : earliest.toString();
    }

    private static String mergePrecision(final List<BacklogDTO> dtos, final boolean anyNodeMissingBacklog) {
        if (anyNodeMissingBacklog) {
            return PRECISION_AT_LEAST;
        }

        boolean allExact = true;
        boolean shapesAgree = true;

        final BacklogDTO firstDto = dtos.getFirst();
        final boolean firstFlow = firstDto.getFlowFileCount() != null;
        final boolean firstBytes = firstDto.getByteCount() != null;
        final boolean firstRecords = firstDto.getRecordCount() != null;

        for (final BacklogDTO dto : dtos) {
            if (!PRECISION_EXACT.equals(dto.getPrecision())) {
                allExact = false;
            }

            if ((dto.getFlowFileCount() != null) != firstFlow
                    || (dto.getByteCount() != null) != firstBytes
                    || (dto.getRecordCount() != null) != firstRecords) {
                shapesAgree = false;
            }
        }

        return (allExact && shapesAgree) ? PRECISION_EXACT : PRECISION_AT_LEAST;
    }
}
