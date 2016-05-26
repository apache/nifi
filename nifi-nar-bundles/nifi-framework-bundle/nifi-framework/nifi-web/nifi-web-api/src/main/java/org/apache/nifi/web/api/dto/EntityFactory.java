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
package org.apache.nifi.web.api.dto;

import org.apache.nifi.web.api.dto.flow.FlowBreadcrumbDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.FlowBreadcrumbEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;

public final class EntityFactory {

    public ProcessGroupFlowEntity createProcessGroupFlowEntity(final ProcessGroupFlowDTO dto, final AccessPolicyDTO accessPolicy) {
        final ProcessGroupFlowEntity entity = new ProcessGroupFlowEntity();
        entity.setProcessGroupFlow(dto);
        entity.setAccessPolicy(accessPolicy);
        return entity;
    }

    public ProcessorEntity createProcessorEntity(final ProcessorDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy, final ProcessorStatusDTO status) {
        final ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public PortEntity createPortEntity(final PortDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy, final PortStatusDTO status) {
        final PortEntity entity = new PortEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            entity.setPortType(dto.getType());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public ProcessGroupEntity createProcessGroupEntity(final ProcessGroupDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy, final ProcessGroupStatusDTO status) {
        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            entity.setInputPortCount(dto.getInputPortCount());
            entity.setOutputPortCount(dto.getOutputPortCount());
            entity.setRunningCount(dto.getRunningCount());
            entity.setStoppedCount(dto.getStoppedCount());
            entity.setInvalidCount(dto.getInvalidCount());
            entity.setDisabledCount(dto.getDisabledCount());
            entity.setActiveRemotePortCount(dto.getActiveRemotePortCount());
            entity.setInactiveRemotePortCount(dto.getInactiveRemotePortCount());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public LabelEntity createLabelEntity(final LabelDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());

            final DimensionsDTO dimensions = new DimensionsDTO();
            dimensions.setHeight(dto.getHeight());
            dimensions.setWidth(dto.getWidth());
            entity.setDimensions(dimensions);

            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public FunnelEntity createFunnelEntity(final FunnelDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final FunnelEntity entity = new FunnelEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public ConnectionEntity createConnectionEntity(final ConnectionDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy, final ConnectionStatusDTO status) {
        final ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            entity.setBends(dto.getBends());
            entity.setLabelIndex(dto.getLabelIndex());
            entity.setSourceId(dto.getSource().getId());
            entity.setSourceGroupId(dto.getSource().getGroupId());
            entity.setDestinationId(dto.getDestination().getId());
            entity.setDestinationGroupId(dto.getDestination().getGroupId());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public RemoteProcessGroupEntity createRemoteProcessGroupEntity(
        final RemoteProcessGroupDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy,final RemoteProcessGroupStatusDTO status) {

        final RemoteProcessGroupEntity entity = new RemoteProcessGroupEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            entity.setInputPortCount(dto.getInputPortCount());
            entity.setOutputPortCount(dto.getOutputPortCount());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public RemoteProcessGroupPortEntity createRemoteProcessGroupPortEntity(final RemoteProcessGroupPortDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setRemoteProcessGroupPort(dto);
            }
        }

        return entity;
    }

    public SnippetEntity createSnippetEntity(final SnippetDTO dto) {
        final SnippetEntity entity = new SnippetEntity();
        entity.setSnippet(dto);
        return entity;
    }

    public ReportingTaskEntity createReportingTaskEntity(final ReportingTaskDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }

        return entity;
    }

    public ControllerServiceEntity createControllerServiceEntity(final ControllerServiceDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public ControllerServiceReferencingComponentEntity createControllerServiceReferencingComponentEntity(
        final ControllerServiceReferencingComponentDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {

        final ControllerServiceReferencingComponentEntity entity = new ControllerServiceReferencingComponentEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }

        return entity;
    }

    public FlowBreadcrumbEntity createFlowBreadcrumbEntity(final FlowBreadcrumbDTO dto, final AccessPolicyDTO accessPolicy) {
        final FlowBreadcrumbEntity entity = new FlowBreadcrumbEntity();
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setBreadcrumb(dto);
            }
        }
        return entity;
    }
}
