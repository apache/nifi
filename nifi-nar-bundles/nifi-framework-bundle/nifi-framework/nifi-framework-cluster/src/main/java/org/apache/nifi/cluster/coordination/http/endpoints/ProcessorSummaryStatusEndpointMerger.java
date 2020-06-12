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

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.PermissionsDtoMerger;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.web.api.dto.ProcessorScheduleSummaryDTO;
import org.apache.nifi.web.api.entity.ProcessorScheduleSummariesEntity;
import org.apache.nifi.web.api.entity.ProcessorScheduleSummaryEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ProcessorSummaryStatusEndpointMerger implements EndpointResponseMerger {
    public static final String SCHEDULE_SUMMARY_URI = "/nifi-api/processors/schedule-summaries/query";

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "POST".equalsIgnoreCase(method) && SCHEDULE_SUMMARY_URI.equals(uri.getPath());
    }

    @Override
    public NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final ProcessorScheduleSummariesEntity responseEntity = clientResponse.getClientResponse().readEntity(ProcessorScheduleSummariesEntity.class);

        // Create mapping of Processor ID to its schedule Summary.
        final Map<String, ProcessorScheduleSummaryEntity> scheduleSummaries = responseEntity.getScheduleSummaries().stream()
            .collect(Collectors.toMap(entity -> entity.getScheduleSummary().getId(), entity -> entity));

        for (final NodeResponse nodeResponse : successfulResponses) {
            final ProcessorScheduleSummariesEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity :
                nodeResponse.getClientResponse().readEntity(ProcessorScheduleSummariesEntity.class);

            for (final ProcessorScheduleSummaryEntity processorEntity : nodeResponseEntity.getScheduleSummaries()) {
                final String processorId = processorEntity.getScheduleSummary().getId();

                final ProcessorScheduleSummaryEntity mergedEntity = scheduleSummaries.computeIfAbsent(processorId, id -> new ProcessorScheduleSummaryEntity());
                merge(mergedEntity, processorEntity);
            }
        }

        final ProcessorScheduleSummariesEntity mergedEntity = new ProcessorScheduleSummariesEntity();
        mergedEntity.setScheduleSummaries(new ArrayList<>(scheduleSummaries.values()));
        return new NodeResponse(clientResponse, mergedEntity);
    }

    private void merge(final ProcessorScheduleSummaryEntity target, final ProcessorScheduleSummaryEntity additional) {
        PermissionsDtoMerger.mergePermissions(target.getPermissions(), additional.getPermissions());

        final ProcessorScheduleSummaryDTO targetSummaryDto = target.getScheduleSummary();
        final ProcessorScheduleSummaryDTO additionalSummaryDto = additional.getScheduleSummary();

        // If name is null, it's because of permissions, so we want to nullify it in the target.
        if (additionalSummaryDto.getName() == null) {
            targetSummaryDto.setName(null);
        }

        targetSummaryDto.setActiveThreadCount(targetSummaryDto.getActiveThreadCount() + additionalSummaryDto.getActiveThreadCount());

        final String additionalRunStatus = additionalSummaryDto.getRunStatus();
        if (RunStatus.Running.name().equals(additionalRunStatus)) {
            targetSummaryDto.setRunStatus(RunStatus.Running.name());
        } else if (RunStatus.Validating.name().equals(additionalRunStatus)) {
            targetSummaryDto.setRunStatus(RunStatus.Validating.name());
        } else if (RunStatus.Invalid.name().equals(additionalRunStatus)) {
            targetSummaryDto.setRunStatus(RunStatus.Invalid.name());
        }

        // If validation errors is null, it's due to eprmissions, so we want to nullify it in the target.
        if (additionalSummaryDto.getValidationErrors() == null) {
            targetSummaryDto.setValidationErrors(null);
        } else if (targetSummaryDto.getValidationErrors() != null) {
            targetSummaryDto.getValidationErrors().addAll(additionalSummaryDto.getValidationErrors());
        }
    }
}
