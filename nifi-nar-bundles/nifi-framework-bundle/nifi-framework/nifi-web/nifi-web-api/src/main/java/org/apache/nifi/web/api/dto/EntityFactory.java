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

import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

public final class EntityFactory {

    public ProcessorEntity createProcessorEntity(final ProcessorDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final ProcessorEntity entity = new ProcessorEntity();
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

    public PortEntity createPortEntity(final PortDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final PortEntity entity = new PortEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            entity.setPortType(dto.getType());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public ProcessGroupEntity createProcessGroupEntity(final ProcessGroupDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final ProcessGroupEntity entity = new ProcessGroupEntity();
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

    public ConnectionEntity createConnectionEntity(final ConnectionDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
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

    public ControllerServiceEntity createControllerServiceEntity(final ControllerServiceDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setAccessPolicy(accessPolicy);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            if (accessPolicy != null && accessPolicy.getCanRead()) {
                entity.setControllerService(dto);
            }
        }
        return entity;
    }

    public RemoteProcessGroupEntity createRemoteProcessGroupEntity(final RemoteProcessGroupDTO dto, final RevisionDTO revision, final AccessPolicyDTO accessPolicy) {
        final RemoteProcessGroupEntity entity = new RemoteProcessGroupEntity();
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

}
