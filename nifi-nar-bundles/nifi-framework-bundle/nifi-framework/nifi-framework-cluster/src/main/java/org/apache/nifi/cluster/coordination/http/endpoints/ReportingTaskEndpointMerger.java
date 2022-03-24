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
import org.apache.nifi.cluster.manager.ReportingTaskEntityMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ReportingTaskEndpointMerger  extends AbstractSingleEntityEndpoint<ReportingTaskEntity> implements EndpointResponseMerger {
    public static final String REPORTING_TASKS_URI = "/nifi-api/controller/reporting-tasks";
    public static final Pattern REPORTING_TASK_URI_PATTERN = Pattern.compile("/nifi-api/reporting-tasks/[a-f0-9\\-]{36}");
    public static final Pattern REPORTING_TASK_RUN_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/reporting-tasks/[a-f0-9\\-]{36}/run-status");
    private final ReportingTaskEntityMerger reportingTaskEntityMerger = new ReportingTaskEntityMerger();

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && REPORTING_TASK_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("PUT".equalsIgnoreCase(method) && REPORTING_TASK_RUN_STATUS_URI_PATTERN.matcher(uri.getPath()).matches()) {
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
    protected void mergeResponses(ReportingTaskEntity clientEntity, Map<NodeIdentifier, ReportingTaskEntity> entityMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        reportingTaskEntityMerger.merge(clientEntity, entityMap);
    }
}
