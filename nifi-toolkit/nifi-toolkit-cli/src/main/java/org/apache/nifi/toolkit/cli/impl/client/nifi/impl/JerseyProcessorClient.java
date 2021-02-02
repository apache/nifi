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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessorClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorRunStatusEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

public class JerseyProcessorClient extends AbstractJerseyClient implements ProcessorClient {
    private final WebTarget processGroupTarget;
    private final WebTarget processorTarget;

    public JerseyProcessorClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyProcessorClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.processGroupTarget = baseTarget.path("/process-groups/{pgId}");
        this.processorTarget = baseTarget.path("/processors/{id}");
    }

    @Override
    public ProcessorEntity createProcessor(final String parentGroupdId, final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(parentGroupdId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (processorEntity == null) {
            throw new IllegalArgumentException("Processor entity cannot be null");
        }

        return executeAction("Error creating Processor", () -> {
            final WebTarget target = processGroupTarget
                .path("/processors")
                .resolveTemplate("pgId", parentGroupdId);

            return getRequestBuilder(target).post(
                Entity.entity(processorEntity, MediaType.APPLICATION_JSON_TYPE),
                ProcessorEntity.class
            );
        });
    }

    @Override
    public ProcessorEntity getProcessor(final String processorId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processorId)) {
            throw new IllegalArgumentException("Processor ID cannot be null");
        }

        return executeAction("Error retrieving status of Processor", () -> {
            final WebTarget target = processorTarget.resolveTemplate("id", processorId);
            return getRequestBuilder(target).get(ProcessorEntity.class);
        });
    }

    @Override
    public ProcessorEntity updateProcessor(final ProcessorEntity entity) throws NiFiClientException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException("Processor entity cannot be null");
        }

        return executeAction("Error updating Processor", () -> {
            final WebTarget target = processorTarget.resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).put(
                Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                ProcessorEntity.class
            );
        });
    }

    @Override
    public ProcessorEntity startProcessor(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateProcessorState(processorId, "RUNNING", clientId, version);
    }

    @Override
    public ProcessorEntity startProcessor(final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return startProcessor(processorEntity.getId(), processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion());
    }

    @Override
    public ProcessorEntity runProcessorOnce(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateProcessorState(processorId, "RUN_ONCE", clientId, version);
    }

    @Override
    public ProcessorEntity runProcessorOnce(ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return runProcessorOnce(processorEntity.getId(), processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion());
    }

    @Override
    public ProcessorEntity stopProcessor(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateProcessorState(processorId, "STOPPED", clientId, version);
    }

    @Override
    public ProcessorEntity stopProcessor(final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return stopProcessor(processorEntity.getId(), processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion());
    }

    @Override
    public ProcessorEntity disableProcessor(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateProcessorState(processorId, "DISABLED", clientId, version);
    }

    @Override
    public ProcessorEntity disableProcessor(final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return disableProcessor(processorEntity.getId(), processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion());
    }

    @Override
    public ProcessorEntity deleteProcessor(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        if (processorId == null) {
            throw new IllegalArgumentException("Processor ID cannot be null");
        }

        return executeAction("Error deleting Processor", () -> {
            final WebTarget target = processorTarget
                .queryParam("version", version)
                .queryParam("clientId", clientId)
                .resolveTemplate("id", processorId);

            return getRequestBuilder(target).delete(ProcessorEntity.class);
        });
    }

    @Override
    public ProcessorEntity deleteProcessor(final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return deleteProcessor(processorEntity.getId(), processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion());
    }

    private ProcessorEntity updateProcessorState(final String processorId, final String desiredState, final String clientId, final long version) throws NiFiClientException, IOException {
        if (processorId == null) {
            throw new IllegalArgumentException("Processor ID cannot be null");
        }

        return executeAction("Error starting Processor", () -> {
            final WebTarget target = processorTarget
                .path("/run-status")
                .resolveTemplate("id", processorId);

            final ProcessorRunStatusEntity runStatusEntity = new ProcessorRunStatusEntity();
            runStatusEntity.setState(desiredState);

            final RevisionDTO revisionDto = new RevisionDTO();
            revisionDto.setClientId(clientId);
            revisionDto.setVersion(version);
            runStatusEntity.setRevision(revisionDto);

            return getRequestBuilder(target).put(
                Entity.entity(runStatusEntity, MediaType.APPLICATION_JSON_TYPE),
                ProcessorEntity.class
            );
        });
    }
}
