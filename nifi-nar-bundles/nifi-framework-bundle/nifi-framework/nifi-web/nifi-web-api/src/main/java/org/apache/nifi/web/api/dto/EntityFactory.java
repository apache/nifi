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

import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.diagnostics.ProcessorDiagnosticsDTO;
import org.apache.nifi.web.api.dto.flow.FlowBreadcrumbDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ControllerServiceStatusDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.PortStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ReportingTaskStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.AccessPolicySummaryEntity;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.apache.nifi.web.api.entity.BucketEntity;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ComponentReferenceEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.FlowBreadcrumbEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.PortStatusEntity;
import org.apache.nifi.web.api.entity.PortStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.RegistryClientEntity;
import org.apache.nifi.web.api.entity.RegistryEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;

import java.util.Date;
import java.util.List;

public final class EntityFactory {

    public ProcessorDiagnosticsEntity createProcessorDiagnosticsEntity(final ProcessorDiagnosticsDTO dto, final RevisionDTO revision, final PermissionsDTO processorPermissions,
        final ProcessorStatusDTO status, final List<BulletinEntity> bulletins) {
        final ProcessorDiagnosticsEntity entity = new ProcessorDiagnosticsEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(processorPermissions);
            entity.setId(dto.getProcessor().getId());
            if (processorPermissions != null && processorPermissions.getCanRead()) {
                entity.setComponent(dto);
                entity.setBulletins(bulletins);
            }
        }

        entity.setBulletins(bulletins);
        return entity;
    }

    public StatusHistoryEntity createStatusHistoryEntity(final StatusHistoryDTO statusHistory, final PermissionsDTO permissions) {
        final StatusHistoryEntity entity = new StatusHistoryEntity();
        entity.setCanRead(permissions.getCanRead());
        entity.setStatusHistory(statusHistory); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public ProcessorStatusEntity createProcessorStatusEntity(final ProcessorStatusDTO status, final PermissionsDTO permissions) {
        final ProcessorStatusEntity entity = new ProcessorStatusEntity();
        entity.setCanRead(permissions.getCanRead());
        entity.setProcessorStatus(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public ProcessorStatusSnapshotEntity createProcessorStatusSnapshotEntity(final ProcessorStatusSnapshotDTO status, final PermissionsDTO permissions) {
        final ProcessorStatusSnapshotEntity entity = new ProcessorStatusSnapshotEntity();
        entity.setId(status.getId());
        entity.setCanRead(permissions.getCanRead());
        entity.setProcessorStatusSnapshot(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public ConnectionStatusEntity createConnectionStatusEntity(final ConnectionStatusDTO status, final PermissionsDTO permissions) {
        final ConnectionStatusEntity entity = new ConnectionStatusEntity();
        entity.setCanRead(permissions.getCanRead());
        entity.setConnectionStatus(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public ConnectionStatusSnapshotEntity createConnectionStatusSnapshotEntity(final ConnectionStatusSnapshotDTO status, final PermissionsDTO permissions) {
        final ConnectionStatusSnapshotEntity entity = new ConnectionStatusSnapshotEntity();
        entity.setId(status.getId());
        entity.setCanRead(permissions.getCanRead());
        entity.setConnectionStatusSnapshot(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public ProcessGroupStatusEntity createProcessGroupStatusEntity(final ProcessGroupStatusDTO status, final PermissionsDTO permissions) {
        final ProcessGroupStatusEntity entity = new ProcessGroupStatusEntity();
        entity.setCanRead(permissions.getCanRead());
        entity.setProcessGroupStatus(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public ProcessGroupStatusSnapshotEntity createProcessGroupStatusSnapshotEntity(final ProcessGroupStatusSnapshotDTO status, final PermissionsDTO permissions) {
        final ProcessGroupStatusSnapshotEntity entity = new ProcessGroupStatusSnapshotEntity();
        entity.setId(status.getId());
        entity.setCanRead(permissions.getCanRead());
        entity.setProcessGroupStatusSnapshot(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public RemoteProcessGroupStatusEntity createRemoteProcessGroupStatusEntity(final RemoteProcessGroupStatusDTO status, final PermissionsDTO permissions) {
        final RemoteProcessGroupStatusEntity entity = new RemoteProcessGroupStatusEntity();
        entity.setCanRead(permissions.getCanRead());
        entity.setRemoteProcessGroupStatus(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public RemoteProcessGroupStatusSnapshotEntity createRemoteProcessGroupStatusSnapshotEntity(final RemoteProcessGroupStatusSnapshotDTO status, final PermissionsDTO permissions) {
        final RemoteProcessGroupStatusSnapshotEntity entity = new RemoteProcessGroupStatusSnapshotEntity();
        entity.setId(status.getId());
        entity.setCanRead(permissions.getCanRead());
        entity.setRemoteProcessGroupStatusSnapshot(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public PortStatusEntity createPortStatusEntity(final PortStatusDTO status, final PermissionsDTO permissions) {
        final PortStatusEntity entity = new PortStatusEntity();
        entity.setCanRead(permissions.getCanRead());
        entity.setPortStatus(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public PortStatusSnapshotEntity createPortStatusSnapshotEntity(final PortStatusSnapshotDTO status, final PermissionsDTO permissions) {
        final PortStatusSnapshotEntity entity = new PortStatusSnapshotEntity();
        entity.setId(status.getId());
        entity.setCanRead(permissions.getCanRead());
        entity.setPortStatusSnapshot(status); // always set the status, as it's always allowed... just need to provide permission context for merging responses
        return entity;
    }

    public ControllerConfigurationEntity createControllerConfigurationEntity(final ControllerConfigurationDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final ControllerConfigurationEntity entity = new ControllerConfigurationEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public ProcessGroupFlowEntity createProcessGroupFlowEntity(final ProcessGroupFlowDTO dto, final PermissionsDTO permissions) {
        final ProcessGroupFlowEntity entity = new ProcessGroupFlowEntity();
        entity.setProcessGroupFlow(dto);
        entity.setPermissions(permissions);
        return entity;
    }

    public ProcessorEntity createProcessorEntity(final ProcessorDTO dto, final RevisionDTO revision, final PermissionsDTO permissions, final PermissionsDTO operatePermissions,
        final ProcessorStatusDTO status, final List<BulletinEntity> bulletins) {

        final ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setOperatePermissions(operatePermissions);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setInputRequirement(dto.getInputRequirement());
            entity.setPosition(dto.getPosition());
            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
                entity.setBulletins(bulletins);
            }
        }
        return entity;
    }

    public PortEntity createPortEntity(final PortDTO dto, final RevisionDTO revision, final PermissionsDTO permissions, final PermissionsDTO operatePermissions,
                                       final PortStatusDTO status, final List<BulletinEntity> bulletins) {
        final PortEntity entity = new PortEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setOperatePermissions(operatePermissions);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            entity.setPortType(dto.getType());
            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
                entity.setBulletins(bulletins);
            }
        }
        return entity;
    }

    public ProcessGroupEntity createProcessGroupEntity(final ProcessGroupDTO dto, final RevisionDTO revision, final PermissionsDTO permissions,
                                                       final ProcessGroupStatusDTO status, final List<BulletinEntity> bulletins) {

        final ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
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

            entity.setUpToDateCount(dto.getUpToDateCount());
            entity.setLocallyModifiedCount(dto.getLocallyModifiedCount());
            entity.setStaleCount(dto.getStaleCount());
            entity.setLocallyModifiedAndStaleCount(dto.getLocallyModifiedAndStaleCount());
            entity.setSyncFailureCount(dto.getSyncFailureCount());

            if (dto.getVersionControlInformation() != null) {
                entity.setVersionedFlowState(dto.getVersionControlInformation().getState());
            }

            entity.setBulletins(bulletins); // include bulletins as authorized descendant component bulletins should be available
            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public LabelEntity createLabelEntity(final LabelDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());

            final DimensionsDTO dimensions = new DimensionsDTO();
            dimensions.setHeight(dto.getHeight());
            dimensions.setWidth(dto.getWidth());
            entity.setDimensions(dimensions);

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public UserEntity createUserEntity(final UserDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final UserEntity entity = new UserEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public TenantEntity createTenantEntity(final TenantDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final TenantEntity entity = new TenantEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public AccessPolicySummaryEntity createAccessPolicySummaryEntity(final AccessPolicySummaryDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final AccessPolicySummaryEntity entity = new AccessPolicySummaryEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public ComponentReferenceEntity createComponentReferenceEntity(final ComponentReferenceDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final ComponentReferenceEntity entity = new ComponentReferenceEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());
            entity.setParentGroupId(dto.getParentGroupId());

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public AffectedComponentEntity createAffectedComponentEntity(final AffectedComponentDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public UserGroupEntity createUserGroupEntity(final UserGroupDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final UserGroupEntity entity = new UserGroupEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public AccessPolicyEntity createAccessPolicyEntity(final AccessPolicyDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final AccessPolicyEntity entity = new AccessPolicyEntity();
        entity.setRevision(revision);
        entity.setGenerated(new Date());
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public FunnelEntity createFunnelEntity(final FunnelDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final FunnelEntity entity = new FunnelEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public ConnectionEntity createConnectionEntity(final ConnectionDTO dto, final RevisionDTO revision, final PermissionsDTO permissions, final ConnectionStatusDTO status) {
        final ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            entity.setBends(dto.getBends());
            entity.setLabelIndex(dto.getLabelIndex());
            entity.setzIndex(dto.getzIndex());
            entity.setSourceId(dto.getSource().getId());
            entity.setSourceGroupId(dto.getSource().getGroupId());
            entity.setSourceType(dto.getSource().getType());
            entity.setDestinationId(dto.getDestination().getId());
            entity.setDestinationGroupId(dto.getDestination().getGroupId());
            entity.setDestinationType(dto.getDestination().getType());
            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }
        return entity;
    }

    public RemoteProcessGroupEntity createRemoteProcessGroupEntity(final RemoteProcessGroupDTO dto, final RevisionDTO revision,
                                                                   final PermissionsDTO permissions, final PermissionsDTO operatePermissions,
                                                                   final RemoteProcessGroupStatusDTO status, final List<BulletinEntity> bulletins) {
        final RemoteProcessGroupEntity entity = new RemoteProcessGroupEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setOperatePermissions(operatePermissions);
            entity.setStatus(status);
            entity.setId(dto.getId());
            entity.setPosition(dto.getPosition());
            entity.setInputPortCount(dto.getInputPortCount());
            entity.setOutputPortCount(dto.getOutputPortCount());
            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
                entity.setBulletins(bulletins);
            }
        }
        return entity;
    }

    public RemoteProcessGroupPortEntity createRemoteProcessGroupPortEntity(final RemoteProcessGroupPortDTO dto, final RevisionDTO revision,
                                                                           final PermissionsDTO permissions, final PermissionsDTO operatePermissions) {
        final RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setOperatePermissions(operatePermissions);
            entity.setId(dto.getId());
            if (permissions != null && permissions.getCanRead()) {
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

    public ReportingTaskEntity createReportingTaskEntity(final ReportingTaskDTO dto, final RevisionDTO revision,
                                                         final PermissionsDTO permissions, final PermissionsDTO operatePermissions, final List<BulletinEntity> bulletins) {
        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setOperatePermissions(operatePermissions);
            entity.setId(dto.getId());

            final ReportingTaskStatusDTO status = new ReportingTaskStatusDTO();
            status.setRunStatus(dto.getState());
            status.setValidationStatus(dto.getValidationStatus());
            status.setActiveThreadCount(dto.getActiveThreadCount());
            entity.setStatus(status);

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
                entity.setBulletins(bulletins);
            }
        }

        return entity;
    }

    public VariableRegistryEntity createVariableRegistryEntity(final VariableRegistryDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final VariableRegistryEntity entity = new VariableRegistryEntity();
        entity.setProcessGroupRevision(revision);
        if (dto != null) {
            if (permissions != null && permissions.getCanRead()) {
                entity.setVariableRegistry(dto);
            }
        }

        return entity;
    }

    public ControllerServiceEntity createControllerServiceEntity(final ControllerServiceDTO dto, final RevisionDTO revision,
                                                                 final PermissionsDTO permissions, final PermissionsDTO operatePermissions, final List<BulletinEntity> bulletins) {
        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setOperatePermissions(operatePermissions);
            entity.setId(dto.getId());
            entity.setParentGroupId(dto.getParentGroupId());
            entity.setPosition(dto.getPosition());

            final ControllerServiceStatusDTO status = new ControllerServiceStatusDTO();
            status.setRunStatus(dto.getState());
            status.setValidationStatus(dto.getValidationStatus());
            entity.setStatus(status);

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
                entity.setBulletins(bulletins);
            }
        }
        return entity;
    }

    public ControllerServiceReferencingComponentEntity createControllerServiceReferencingComponentEntity(final String id,
        final ControllerServiceReferencingComponentDTO dto, final RevisionDTO revision, final PermissionsDTO permissions, final PermissionsDTO operatePermissions) {
        final ControllerServiceReferencingComponentEntity entity = new ControllerServiceReferencingComponentEntity();
        entity.setRevision(revision);
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setOperatePermissions(operatePermissions);
            entity.setId(dto.getId());
            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }

        return entity;
    }

    public FlowBreadcrumbEntity createFlowBreadcrumbEntity(final FlowBreadcrumbDTO dto, final PermissionsDTO permissions) {
        final FlowBreadcrumbEntity entity = new FlowBreadcrumbEntity();
        if (dto != null) {
            entity.setPermissions(permissions);
            entity.setId(dto.getId());

            if (dto.getVersionControlInformation() != null) {
                entity.setVersionedFlowState(dto.getVersionControlInformation().getState());
            }

            if (permissions != null && permissions.getCanRead()) {
                entity.setBreadcrumb(dto);
            }
        }
        return entity;
    }

    public AllowableValueEntity createAllowableValueEntity(final AllowableValueDTO dto, final boolean canRead) {
        final AllowableValueEntity entity = new AllowableValueEntity();
        entity.setCanRead(canRead);
        entity.setAllowableValue(dto);
        return entity;
    }

    public ActionEntity createActionEntity(final ActionDTO dto, final boolean canRead) {
        final ActionEntity entity = new ActionEntity();
        if (dto != null) {
            entity.setId(dto.getId());
            entity.setSourceId(dto.getSourceId());
            entity.setTimestamp(dto.getTimestamp());
            entity.setCanRead(canRead);
            if (canRead) {
                entity.setAction(dto);
            }
        }
        return entity;
    }

    public BulletinEntity createBulletinEntity(final BulletinDTO dto, final boolean canRead) {
        final BulletinEntity entity = new BulletinEntity();
        if (dto != null) {
            entity.setId(dto.getId());
            entity.setSourceId(dto.getSourceId());
            entity.setGroupId(dto.getGroupId());
            entity.setTimestamp(dto.getTimestamp());
            entity.setNodeAddress(dto.getNodeAddress());
            entity.setCanRead(canRead);
            if (canRead) {
                entity.setBulletin(dto);
            }
        }
        return entity;
    }

    public VersionControlInformationEntity createVersionControlInformationEntity(final VersionControlInformationDTO dto, final RevisionDTO processGroupRevision) {
        final VersionControlInformationEntity entity = new VersionControlInformationEntity();
        entity.setVersionControlInformation(dto);
        entity.setProcessGroupRevision(processGroupRevision);
        return entity;
    }

    public RegistryClientEntity createRegistryClientEntity(final RegistryDTO dto, final RevisionDTO revision, final PermissionsDTO permissions) {
        final RegistryClientEntity entity = new RegistryClientEntity();
        entity.setRevision(revision);
        entity.setPermissions(permissions);

        if (dto != null) {
            entity.setId(dto.getId());

            if (permissions != null && permissions.getCanRead()) {
                entity.setComponent(dto);
            }
        }

        return entity;
    }

    public RegistryEntity createRegistryEntity(final RegistryDTO dto) {
        final RegistryEntity entity = new RegistryEntity();

        if (dto != null) {
            entity.setRegistry(dto);
        }

        return entity;
    }

    public BucketEntity createBucketEntity(final BucketDTO dto, final PermissionsDTO permissions) {
        final BucketEntity entity = new BucketEntity();
        entity.setId(dto.getId());
        entity.setPermissions(permissions);

        if (permissions != null && permissions.getCanRead()) {
            entity.setBucket(dto);
        }

        return entity;
    }

}
