/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ReportingTasksClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTaskRunStatusEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Jersey implementation of ReportingTasksClient.
 */
public class JerseyReportingTasksClient extends AbstractJerseyClient implements ReportingTasksClient {

    private final WebTarget reportingTasksTarget;

    public JerseyReportingTasksClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyReportingTasksClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.reportingTasksTarget = baseTarget.path("/reporting-tasks");
    }

    @Override
    public ReportingTaskEntity getReportingTask(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Reporting task id cannot be null");
        }

        return executeAction("Error retrieving status of reporting task", () -> {
            final WebTarget target = reportingTasksTarget.path(id);
            return getRequestBuilder(target).get(ReportingTaskEntity.class);
        });
    }

    @Override
    public ReportingTaskEntity activateReportingTask(final String id,
            final ReportingTaskRunStatusEntity runStatusEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Reporting task id cannot be null");
        }

        if (runStatusEntity == null) {
            throw new IllegalArgumentException("Entity cannnot be null");
        }

        return executeAction("Error starting or stopping report task", () -> {
            final WebTarget target = reportingTasksTarget
                    .path("{id}/run-status").resolveTemplate("id", id);
            return getRequestBuilder(target).put(
                    Entity.entity(runStatusEntity, MediaType.APPLICATION_JSON_TYPE),
                    ReportingTaskEntity.class);
        });
    }
}
