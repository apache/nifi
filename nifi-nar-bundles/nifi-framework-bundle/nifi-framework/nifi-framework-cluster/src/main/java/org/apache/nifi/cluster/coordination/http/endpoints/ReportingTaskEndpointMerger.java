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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;

public class ReportingTaskEndpointMerger extends AbstractSingleEntityEndpoint<ReportingTaskEntity, ReportingTaskDTO> {
    public static final String REPORTING_TASKS_URI = "/nifi-api/controller/reporting-tasks/node";
    public static final Pattern REPORTING_TASK_URI_PATTERN = Pattern.compile("/nifi-api/reporting-tasks/node/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && REPORTING_TASK_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && REPORTING_TASKS_URI.equals(uri.getPath())) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<ReportingTaskEntity> getEntityClass() {
        return ReportingTaskEntity.class;
    }

    @Override
    protected ReportingTaskDTO getDto(ReportingTaskEntity entity) {
        return entity.getReportingTask();
    }

    @Override
    protected void mergeResponses(ReportingTaskDTO clientDto, Map<NodeIdentifier, ReportingTaskDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();

        int activeThreadCount = 0;
        for (final Map.Entry<NodeIdentifier, ReportingTaskDTO> nodeEntry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = nodeEntry.getKey();
            final ReportingTaskDTO nodeReportingTask = nodeEntry.getValue();

            if (nodeReportingTask.getActiveThreadCount() != null) {
                activeThreadCount += nodeReportingTask.getActiveThreadCount();
            }

            // merge the validation errors
            mergeValidationErrors(validationErrorMap, nodeId, nodeReportingTask.getValidationErrors());
        }

        // set the merged active thread counts
        clientDto.setActiveThreadCount(activeThreadCount);

        // set the merged the validation errors
        clientDto.setValidationErrors(normalizedMergedValidationErrors(validationErrorMap, dtoMap.size()));
    }

}
