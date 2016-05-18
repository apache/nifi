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
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTasksEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ReportingTasksEndpointMerger extends AbstractMultiEntityEndpoint<ReportingTasksEntity, ReportingTaskEntity> {
    public static final String REPORTING_TASKS_URI = "/nifi-api/controller/reporting-tasks/node";

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && REPORTING_TASKS_URI.equals(uri.getPath());
    }

    @Override
    protected Class<ReportingTasksEntity> getEntityClass() {
        return ReportingTasksEntity.class;
    }

    @Override
    protected Set<ReportingTaskEntity> getDtos(ReportingTasksEntity entity) {
        return entity.getReportingTasks();
    }

    @Override
    protected String getComponentId(ReportingTaskEntity entity) {
        return entity.getComponent().getId();
    }

    @Override
    protected void mergeResponses(ReportingTaskEntity entity, Map<NodeIdentifier, ReportingTaskEntity> entityMap,
                                  Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {

        new ReportingTaskEndpointMerger().mergeResponses(
            entity.getComponent(),
            entityMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getComponent())),
            successfulResponses,
            problematicResponses);
    }
}
