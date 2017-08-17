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
package org.apache.nifi.web;

import com.google.common.collect.Sets;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.FlowChangePurgeDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserContextKeys;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.EnforcePolicyPermissionsThroughBaseResource;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.coordination.heartbeat.NodeHeartbeat;
import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.event.NodeEvent;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDeletionException;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.AccessPolicySummaryDTO;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ComponentHistoryDTO;
import org.apache.nifi.web.api.dto.ComponentReferenceDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.CountersSnapshotDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.dto.FlowConfigurationDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PreviousValueDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.PropertyHistoryDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.ResourceDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.AccessPolicySummaryEntity;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ComponentReferenceEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ControllerBulletinsEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.FlowConfigurationEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.PortStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.VariableEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.ControllerServiceDAO;
import org.apache.nifi.web.dao.FunnelDAO;
import org.apache.nifi.web.dao.LabelDAO;
import org.apache.nifi.web.dao.PortDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.apache.nifi.web.dao.SnippetDAO;
import org.apache.nifi.web.dao.TemplateDAO;
import org.apache.nifi.web.dao.UserDAO;
import org.apache.nifi.web.dao.UserGroupDAO;
import org.apache.nifi.web.revision.DeleteRevisionTask;
import org.apache.nifi.web.revision.ExpiredRevisionClaimException;
import org.apache.nifi.web.revision.RevisionClaim;
import org.apache.nifi.web.revision.RevisionManager;
import org.apache.nifi.web.revision.RevisionUpdate;
import org.apache.nifi.web.revision.StandardRevisionClaim;
import org.apache.nifi.web.revision.StandardRevisionUpdate;
import org.apache.nifi.web.revision.UpdateRevisionTask;
import org.apache.nifi.web.util.SnippetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Implementation of NiFiServiceFacade that performs revision checking.
 */
public class StandardNiFiServiceFacade implements NiFiServiceFacade {
    private static final Logger logger = LoggerFactory.getLogger(StandardNiFiServiceFacade.class);

    // nifi core components
    private ControllerFacade controllerFacade;
    private SnippetUtils snippetUtils;

    // revision manager
    private RevisionManager revisionManager;
    private BulletinRepository bulletinRepository;

    // data access objects
    private ProcessorDAO processorDAO;
    private ProcessGroupDAO processGroupDAO;
    private RemoteProcessGroupDAO remoteProcessGroupDAO;
    private LabelDAO labelDAO;
    private FunnelDAO funnelDAO;
    private SnippetDAO snippetDAO;
    private PortDAO inputPortDAO;
    private PortDAO outputPortDAO;
    private ConnectionDAO connectionDAO;
    private ControllerServiceDAO controllerServiceDAO;
    private ReportingTaskDAO reportingTaskDAO;
    private TemplateDAO templateDAO;
    private UserDAO userDAO;
    private UserGroupDAO userGroupDAO;
    private AccessPolicyDAO accessPolicyDAO;
    private ClusterCoordinator clusterCoordinator;
    private HeartbeatMonitor heartbeatMonitor;
    private LeaderElectionManager leaderElectionManager;

    // administrative services
    private AuditService auditService;

    // properties
    private NiFiProperties properties;
    private DtoFactory dtoFactory;
    private EntityFactory entityFactory;

    private Authorizer authorizer;

    private AuthorizableLookup authorizableLookup;

    // -----------------------------------------
    // Synchronization methods
    // -----------------------------------------
    @Override
    public void authorizeAccess(final AuthorizeAccess authorizeAccess) {
        authorizeAccess.authorize(authorizableLookup);
    }

    @Override
    public void verifyRevision(final Revision revision, final NiFiUser user) {
        final Revision curRevision = revisionManager.getRevision(revision.getComponentId());
        if (revision.equals(curRevision)) {
            return;
        }

        throw new InvalidRevisionException(revision + " is not the most up-to-date revision. This component appears to have been modified");
    }

    @Override
    public void verifyRevisions(final Set<Revision> revisions, final NiFiUser user) {
        for (final Revision revision : revisions) {
            verifyRevision(revision, user);
        }
    }

    @Override
    public Set<Revision> getRevisionsFromGroup(final String groupId, final Function<ProcessGroup, Set<String>> getComponents) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        final Set<String> componentIds = getComponents.apply(group);
        return componentIds.stream().map(id -> revisionManager.getRevision(id)).collect(Collectors.toSet());
    }

    @Override
    public Set<Revision> getRevisionsFromSnippet(final String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);
        final Set<String> componentIds = new HashSet<>();
        componentIds.addAll(snippet.getProcessors().keySet());
        componentIds.addAll(snippet.getFunnels().keySet());
        componentIds.addAll(snippet.getLabels().keySet());
        componentIds.addAll(snippet.getConnections().keySet());
        componentIds.addAll(snippet.getInputPorts().keySet());
        componentIds.addAll(snippet.getOutputPorts().keySet());
        componentIds.addAll(snippet.getProcessGroups().keySet());
        componentIds.addAll(snippet.getRemoteProcessGroups().keySet());
        return componentIds.stream().map(id -> revisionManager.getRevision(id)).collect(Collectors.toSet());
    }

    // -----------------------------------------
    // Verification Operations
    // -----------------------------------------

    @Override
    public void verifyListQueue(final String connectionId) {
        connectionDAO.verifyList(connectionId);
    }

    @Override
    public void verifyCreateConnection(final String groupId, final ConnectionDTO connectionDTO) {
        connectionDAO.verifyCreate(groupId, connectionDTO);
    }

    @Override
    public void verifyUpdateConnection(final ConnectionDTO connectionDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (connectionDAO.hasConnection(connectionDTO.getId())) {
            connectionDAO.verifyUpdate(connectionDTO);
        } else {
            connectionDAO.verifyCreate(connectionDTO.getParentGroupId(), connectionDTO);
        }
    }

    @Override
    public void verifyDeleteConnection(final String connectionId) {
        connectionDAO.verifyDelete(connectionId);
    }

    @Override
    public void verifyDeleteFunnel(final String funnelId) {
        funnelDAO.verifyDelete(funnelId);
    }

    @Override
    public void verifyUpdateInputPort(final PortDTO inputPortDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (inputPortDAO.hasPort(inputPortDTO.getId())) {
            inputPortDAO.verifyUpdate(inputPortDTO);
        }
    }

    @Override
    public void verifyDeleteInputPort(final String inputPortId) {
        inputPortDAO.verifyDelete(inputPortId);
    }

    @Override
    public void verifyUpdateOutputPort(final PortDTO outputPortDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (outputPortDAO.hasPort(outputPortDTO.getId())) {
            outputPortDAO.verifyUpdate(outputPortDTO);
        }
    }

    @Override
    public void verifyDeleteOutputPort(final String outputPortId) {
        outputPortDAO.verifyDelete(outputPortId);
    }

    @Override
    public void verifyCreateProcessor(ProcessorDTO processorDTO) {
        processorDAO.verifyCreate(processorDTO);
    }

    @Override
    public void verifyUpdateProcessor(final ProcessorDTO processorDTO) {
        // if group does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (processorDAO.hasProcessor(processorDTO.getId())) {
            processorDAO.verifyUpdate(processorDTO);
        } else {
            verifyCreateProcessor(processorDTO);
        }
    }

    @Override
    public void verifyDeleteProcessor(final String processorId) {
        processorDAO.verifyDelete(processorId);
    }

    @Override
    public void verifyScheduleComponents(final String groupId, final ScheduledState state, final Set<String> componentIds) {
        processGroupDAO.verifyScheduleComponents(groupId, state, componentIds);
    }

    @Override
    public void verifyActivateControllerServices(final String groupId, final ControllerServiceState state, final Set<String> serviceIds) {
        processGroupDAO.verifyActivateControllerServices(groupId, state, serviceIds);
    }

    @Override
    public void verifyDeleteProcessGroup(final String groupId) {
        processGroupDAO.verifyDelete(groupId);
    }

    @Override
    public void verifyUpdateRemoteProcessGroup(final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // if remote group does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupDTO.getId())) {
            remoteProcessGroupDAO.verifyUpdate(remoteProcessGroupDTO);
        }
    }

    @Override
    public void verifyUpdateRemoteProcessGroupInputPort(final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        remoteProcessGroupDAO.verifyUpdateInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
    }

    @Override
    public void verifyUpdateRemoteProcessGroupOutputPort(final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        remoteProcessGroupDAO.verifyUpdateOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
    }

    @Override
    public void verifyDeleteRemoteProcessGroup(final String remoteProcessGroupId) {
        remoteProcessGroupDAO.verifyDelete(remoteProcessGroupId);
    }

    @Override
    public void verifyCreateControllerService(ControllerServiceDTO controllerServiceDTO) {
        controllerServiceDAO.verifyCreate(controllerServiceDTO);
    }

    @Override
    public void verifyUpdateControllerService(final ControllerServiceDTO controllerServiceDTO) {
        // if service does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (controllerServiceDAO.hasControllerService(controllerServiceDTO.getId())) {
            controllerServiceDAO.verifyUpdate(controllerServiceDTO);
        } else {
            verifyCreateControllerService(controllerServiceDTO);
        }
    }

    @Override
    public void verifyUpdateControllerServiceReferencingComponents(final String controllerServiceId, final ScheduledState scheduledState, final ControllerServiceState controllerServiceState) {
        controllerServiceDAO.verifyUpdateReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
    }

    @Override
    public void verifyDeleteControllerService(final String controllerServiceId) {
        controllerServiceDAO.verifyDelete(controllerServiceId);
    }

    @Override
    public void verifyCreateReportingTask(ReportingTaskDTO reportingTaskDTO) {
        reportingTaskDAO.verifyCreate(reportingTaskDTO);
    }

    @Override
    public void verifyUpdateReportingTask(final ReportingTaskDTO reportingTaskDTO) {
        // if tasks does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (reportingTaskDAO.hasReportingTask(reportingTaskDTO.getId())) {
            reportingTaskDAO.verifyUpdate(reportingTaskDTO);
        } else {
            verifyCreateReportingTask(reportingTaskDTO);
        }
    }

    @Override
    public void verifyDeleteReportingTask(final String reportingTaskId) {
        reportingTaskDAO.verifyDelete(reportingTaskId);
    }

    // -----------------------------------------
    // Write Operations
    // -----------------------------------------

    @Override
    public AccessPolicyEntity updateAccessPolicy(final Revision revision, final AccessPolicyDTO accessPolicyDTO) {
        final Authorizable authorizable = authorizableLookup.getAccessPolicyById(accessPolicyDTO.getId());
        final RevisionUpdate<AccessPolicyDTO> snapshot = updateComponent(revision,
                authorizable,
                () -> accessPolicyDAO.updateAccessPolicy(accessPolicyDTO),
                accessPolicy -> {
                    final Set<TenantEntity> users = accessPolicy.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet());
                    final Set<TenantEntity> userGroups = accessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet());
                    final ComponentReferenceEntity componentReference = createComponentReferenceEntity(accessPolicy.getResource());
                    return dtoFactory.createAccessPolicyDto(accessPolicy, userGroups, users, componentReference);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizable);
        return entityFactory.createAccessPolicyEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public UserEntity updateUser(final Revision revision, final UserDTO userDTO) {
        final Authorizable usersAuthorizable = authorizableLookup.getTenant();
        final Set<Group> groups = userGroupDAO.getUserGroupsForUser(userDTO.getId());
        final Set<AccessPolicy> policies = userGroupDAO.getAccessPoliciesForUser(userDTO.getId());
        final RevisionUpdate<UserDTO> snapshot = updateComponent(revision,
                usersAuthorizable,
                () -> userDAO.updateUser(userDTO),
                user -> {
                    final Set<TenantEntity> tenantEntities = groups.stream().map(g -> g.getIdentifier()).map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet());
                    final Set<AccessPolicySummaryEntity> policyEntities = policies.stream().map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
                    return dtoFactory.createUserDto(user, tenantEntities, policyEntities);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(usersAuthorizable);
        return entityFactory.createUserEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public UserGroupEntity updateUserGroup(final Revision revision, final UserGroupDTO userGroupDTO) {
        final Authorizable userGroupsAuthorizable = authorizableLookup.getTenant();
        final Set<AccessPolicy> policies = userGroupDAO.getAccessPoliciesForUserGroup(userGroupDTO.getId());
        final RevisionUpdate<UserGroupDTO> snapshot = updateComponent(revision,
                userGroupsAuthorizable,
                () -> userGroupDAO.updateUserGroup(userGroupDTO),
                userGroup -> {
                    final Set<TenantEntity> tenantEntities = userGroup.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet());
                    final Set<AccessPolicySummaryEntity> policyEntities = policies.stream().map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
                    return dtoFactory.createUserGroupDto(userGroup, tenantEntities, policyEntities);
                }
        );

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(userGroupsAuthorizable);
        return entityFactory.createUserGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public ConnectionEntity updateConnection(final Revision revision, final ConnectionDTO connectionDTO) {
        final Connection connectionNode = connectionDAO.getConnection(connectionDTO.getId());

        final RevisionUpdate<ConnectionDTO> snapshot = updateComponent(
                revision,
                connectionNode,
                () -> connectionDAO.updateConnection(connectionDTO),
                connection -> dtoFactory.createConnectionDto(connection));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connectionNode);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionNode.getIdentifier()));
        return entityFactory.createConnectionEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status);
    }

    @Override
    public ProcessorEntity updateProcessor(final Revision revision, final ProcessorDTO processorDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ProcessorNode processorNode = processorDAO.getProcessor(processorDTO.getId());
        final RevisionUpdate<ProcessorDTO> snapshot = updateComponent(revision,
                processorNode,
                () -> processorDAO.updateProcessor(processorDTO),
                proc -> dtoFactory.createProcessorDto(proc));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processorNode);
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processorNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processorNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessorEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public LabelEntity updateLabel(final Revision revision, final LabelDTO labelDTO) {
        final Label labelNode = labelDAO.getLabel(labelDTO.getId());
        final RevisionUpdate<LabelDTO> snapshot = updateComponent(revision,
                labelNode,
                () -> labelDAO.updateLabel(labelDTO),
                label -> dtoFactory.createLabelDto(label));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(labelNode);
        return entityFactory.createLabelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public FunnelEntity updateFunnel(final Revision revision, final FunnelDTO funnelDTO) {
        final Funnel funnelNode = funnelDAO.getFunnel(funnelDTO.getId());
        final RevisionUpdate<FunnelDTO> snapshot = updateComponent(revision,
                funnelNode,
                () -> funnelDAO.updateFunnel(funnelDTO),
                funnel -> dtoFactory.createFunnelDto(funnel));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(funnelNode);
        return entityFactory.createFunnelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }


    /**
     * Updates a component with the given revision, using the provided supplier to call
     * into the appropriate DAO and the provided function to convert the component into a DTO.
     *
     * @param revision    the current revision
     * @param daoUpdate   a Supplier that will update the component via the appropriate DAO
     * @param dtoCreation a Function to convert a component into a dao
     * @param <D>         the DTO Type of the updated component
     * @param <C>         the Component Type of the updated component
     * @return A RevisionUpdate that represents the new configuration
     */
    private <D, C> RevisionUpdate<D> updateComponent(final Revision revision, final Authorizable authorizable, final Supplier<C> daoUpdate, final Function<C, D> dtoCreation) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        return updateComponent(user, revision, authorizable, daoUpdate, dtoCreation);
    }

    private <D, C> RevisionUpdate<D> updateComponent(final NiFiUser user, final Revision revision, final Authorizable authorizable, final Supplier<C> daoUpdate, final Function<C, D> dtoCreation) {
        try {
            final RevisionUpdate<D> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(revision), user, new UpdateRevisionTask<D>() {
                @Override
                public RevisionUpdate<D> update() {
                    // get the updated component
                    final C component = daoUpdate.get();

                    // save updated controller
                    controllerFacade.save();

                    final D dto = dtoCreation.apply(component);

                    final Revision updatedRevision = revisionManager.getRevision(revision.getComponentId()).incrementRevision(revision.getClientId());
                    final FlowModification lastModification = new FlowModification(updatedRevision, user.getIdentity());
                    return new StandardRevisionUpdate<>(dto, lastModification);
                }
            });

            return updatedComponent;
        } catch (final ExpiredRevisionClaimException erce) {
            throw new InvalidRevisionException("Failed to update component " + authorizable, erce);
        }
    }


    @Override
    public void verifyUpdateSnippet(final SnippetDTO snippetDto, final Set<String> affectedComponentIds) {
        // if snippet does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (snippetDAO.hasSnippet(snippetDto.getId())) {
            snippetDAO.verifyUpdateSnippetComponent(snippetDto);
        }
    }

    @Override
    public SnippetEntity updateSnippet(final Set<Revision> revisions, final SnippetDTO snippetDto) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revisions);

        final RevisionUpdate<SnippetDTO> snapshot;
        try {
            snapshot = revisionManager.updateRevision(revisionClaim, user, new UpdateRevisionTask<SnippetDTO>() {
                @Override
                public RevisionUpdate<SnippetDTO> update() {
                    // get the updated component
                    final Snippet snippet = snippetDAO.updateSnippetComponents(snippetDto);

                    // drop the snippet
                    snippetDAO.dropSnippet(snippet.getId());

                    // save updated controller
                    controllerFacade.save();

                    // increment the revisions
                    final Set<Revision> updatedRevisions = revisions.stream().map(revision -> {
                        final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                        return currentRevision.incrementRevision(revision.getClientId());
                    }).collect(Collectors.toSet());

                    final SnippetDTO dto = dtoFactory.createSnippetDto(snippet);
                    return new StandardRevisionUpdate<>(dto, null, updatedRevisions);
                }
            });
        } catch (final ExpiredRevisionClaimException e) {
            throw new InvalidRevisionException("Failed to update Snippet", e);
        }

        return entityFactory.createSnippetEntity(snapshot.getComponent());
    }

    @Override
    public PortEntity updateInputPort(final Revision revision, final PortDTO inputPortDTO) {
        final Port inputPortNode = inputPortDAO.getPort(inputPortDTO.getId());
        final RevisionUpdate<PortDTO> snapshot = updateComponent(revision,
                inputPortNode,
                () -> inputPortDAO.updatePort(inputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(inputPortNode);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(inputPortNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public PortEntity updateOutputPort(final Revision revision, final PortDTO outputPortDTO) {
        final Port outputPortNode = outputPortDAO.getPort(outputPortDTO.getId());
        final RevisionUpdate<PortDTO> snapshot = updateComponent(revision,
                outputPortNode,
                () -> outputPortDAO.updatePort(outputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(outputPortNode);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(outputPortNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public RemoteProcessGroupEntity updateRemoteProcessGroup(final Revision revision, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final RevisionUpdate<RemoteProcessGroupDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroup(remoteProcessGroupDTO),
                remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroupNode);
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(remoteProcessGroupNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(remoteProcessGroupNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createRemoteProcessGroupEntity(snapshot.getComponent(), updateRevision, permissions, status, bulletinEntities);
    }

    @Override
    public RemoteProcessGroupPortEntity updateRemoteProcessGroupInputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupPortDTO.getGroupId());
        final RevisionUpdate<RemoteProcessGroupPortDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroupInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
                remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createRemoteProcessGroupPortEntity(snapshot.getComponent(), updatedRevision, permissions);
    }

    @Override
    public RemoteProcessGroupPortEntity updateRemoteProcessGroupOutputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupPortDTO.getGroupId());
        final RevisionUpdate<RemoteProcessGroupPortDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroupOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
                remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createRemoteProcessGroupPortEntity(snapshot.getComponent(), updatedRevision, permissions);
    }

    @Override
    public Set<AffectedComponentDTO> getActiveComponentsAffectedByVariableRegistryUpdate(final VariableRegistryDTO variableRegistryDto) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(variableRegistryDto.getProcessGroupId());
        if (group == null) {
            throw new ResourceNotFoundException("Could not find Process Group with ID " + variableRegistryDto.getProcessGroupId());
        }

        final Map<String, String> variableMap = new HashMap<>();
        variableRegistryDto.getVariables().stream() // have to use forEach here instead of using Collectors.toMap because value may be null
            .map(VariableEntity::getVariable)
            .forEach(var -> variableMap.put(var.getName(), var.getValue()));

        final Set<AffectedComponentDTO> affectedComponentDtos = new HashSet<>();

        final Set<String> updatedVariableNames = getUpdatedVariables(group, variableMap);
        for (final String variableName : updatedVariableNames) {
            final Set<ConfiguredComponent> affectedComponents = group.getComponentsAffectedByVariable(variableName);

            for (final ConfiguredComponent component : affectedComponents) {
                if (component instanceof ProcessorNode) {
                    final ProcessorNode procNode = (ProcessorNode) component;
                    if (procNode.isRunning()) {
                        affectedComponentDtos.add(dtoFactory.createAffectedComponentDto(procNode));
                    }
                } else if (component instanceof ControllerServiceNode) {
                    final ControllerServiceNode serviceNode = (ControllerServiceNode) component;
                    if (serviceNode.isActive()) {
                        affectedComponentDtos.add(dtoFactory.createAffectedComponentDto(serviceNode));
                    }
                } else {
                    throw new RuntimeException("Found unexpected type of Component [" + component.getCanonicalClassName() + "] dependending on variable");
                }
            }
        }

        return affectedComponentDtos;
    }

    @Override
    public Set<AffectedComponentEntity> getComponentsAffectedByVariableRegistryUpdate(final VariableRegistryDTO variableRegistryDto) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(variableRegistryDto.getProcessGroupId());
        if (group == null) {
            throw new ResourceNotFoundException("Could not find Process Group with ID " + variableRegistryDto.getProcessGroupId());
        }

        final Map<String, String> variableMap = new HashMap<>();
        variableRegistryDto.getVariables().stream() // have to use forEach here instead of using Collectors.toMap because value may be null
                .map(VariableEntity::getVariable)
                .forEach(var -> variableMap.put(var.getName(), var.getValue()));

        final Set<AffectedComponentEntity> affectedComponentEntities = new HashSet<>();

        final Set<String> updatedVariableNames = getUpdatedVariables(group, variableMap);
        for (final String variableName : updatedVariableNames) {
            final Set<ConfiguredComponent> affectedComponents = group.getComponentsAffectedByVariable(variableName);
            affectedComponentEntities.addAll(dtoFactory.createAffectedComponentEntities(affectedComponents, revisionManager));
        }

        return affectedComponentEntities;
    }

    private Set<String> getUpdatedVariables(final ProcessGroup group, final Map<String, String> newVariableValues) {
        final Set<String> updatedVariableNames = new HashSet<>();

        final ComponentVariableRegistry registry = group.getVariableRegistry();
        for (final Map.Entry<String, String> entry : newVariableValues.entrySet()) {
            final String varName = entry.getKey();
            final String newValue = entry.getValue();

            final String curValue = registry.getVariableValue(varName);
            if (!Objects.equals(newValue, curValue)) {
                updatedVariableNames.add(varName);
            }
        }

        return updatedVariableNames;
    }


    @Override
    public VariableRegistryEntity updateVariableRegistry(Revision revision, VariableRegistryDTO variableRegistryDto) {
        return updateVariableRegistry(NiFiUserUtils.getNiFiUser(), revision, variableRegistryDto);
    }

    @Override
    public VariableRegistryEntity updateVariableRegistry(NiFiUser user, Revision revision, VariableRegistryDTO variableRegistryDto) {
        final ProcessGroup processGroupNode = processGroupDAO.getProcessGroup(variableRegistryDto.getProcessGroupId());
        final RevisionUpdate<VariableRegistryDTO> snapshot = updateComponent(user, revision,
            processGroupNode,
            () -> processGroupDAO.updateVariableRegistry(variableRegistryDto),
            processGroup -> dtoFactory.createVariableRegistryDto(processGroup, revisionManager));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createVariableRegistryEntity(snapshot.getComponent(), updatedRevision, permissions);
    }


    @Override
    public ProcessGroupEntity updateProcessGroup(final Revision revision, final ProcessGroupDTO processGroupDTO) {
        final ProcessGroup processGroupNode = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final RevisionUpdate<ProcessGroupDTO> snapshot = updateComponent(revision,
                processGroupNode,
                () -> processGroupDAO.updateProcessGroup(processGroupDTO),
                processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroupNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processGroupNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessGroupEntity(snapshot.getComponent(), updatedRevision, permissions, status, bulletinEntities);
    }

    @Override
    public void verifyUpdateProcessGroup(ProcessGroupDTO processGroupDTO) {
        if (processGroupDAO.hasProcessGroup(processGroupDTO.getId())) {
            processGroupDAO.verifyUpdate(processGroupDTO);
        }
    }

    @Override
    public ScheduleComponentsEntity scheduleComponents(final String processGroupId, final ScheduledState state, final Map<String, Revision> componentRevisions) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        return scheduleComponents(user, processGroupId, state, componentRevisions);
    }

    @Override
    public ScheduleComponentsEntity scheduleComponents(final NiFiUser user, final String processGroupId, final ScheduledState state, final Map<String, Revision> componentRevisions) {

        final RevisionUpdate<ScheduleComponentsEntity> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(componentRevisions.values()), user, new
                UpdateRevisionTask<ScheduleComponentsEntity>() {
                    @Override
                    public RevisionUpdate<ScheduleComponentsEntity> update() {
                        // schedule the components
                processGroupDAO.scheduleComponents(processGroupId, state, componentRevisions.keySet());

                        // update the revisions
                        final Map<String, Revision> updatedRevisions = new HashMap<>();
                        for (final Revision revision : componentRevisions.values()) {
                            final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                            updatedRevisions.put(revision.getComponentId(), currentRevision.incrementRevision(revision.getClientId()));
                        }

                        // save
                        controllerFacade.save();

                        // gather details for response
                        final ScheduleComponentsEntity entity = new ScheduleComponentsEntity();
                        entity.setId(processGroupId);
                        entity.setState(state.name());
                        return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
                    }
                });

        return updatedComponent.getComponent();
    }

    @Override
    public ActivateControllerServicesEntity activateControllerServices(final String processGroupId, final ControllerServiceState state, final Map<String, Revision> serviceRevisions) {

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        return activateControllerServices(user, processGroupId, state, serviceRevisions);
    }

    @Override
    public ActivateControllerServicesEntity activateControllerServices(final NiFiUser user, final String processGroupId, final ControllerServiceState state,
        final Map<String, Revision> serviceRevisions) {

        final RevisionUpdate<ActivateControllerServicesEntity> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(serviceRevisions.values()), user,
            new UpdateRevisionTask<ActivateControllerServicesEntity>() {
                @Override
                public RevisionUpdate<ActivateControllerServicesEntity> update() {
                    // schedule the components
                    processGroupDAO.activateControllerServices(processGroupId, state, serviceRevisions.keySet());

                    // update the revisions
                    final Map<String, Revision> updatedRevisions = new HashMap<>();
                    for (final Revision revision : serviceRevisions.values()) {
                        final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                        updatedRevisions.put(revision.getComponentId(), currentRevision.incrementRevision(revision.getClientId()));
                    }

                    // save
                    controllerFacade.save();

                    // gather details for response
                    final ActivateControllerServicesEntity entity = new ActivateControllerServicesEntity();
                    entity.setId(processGroupId);
                    entity.setState(state.name());
                    return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
                }
            });

        return updatedComponent.getComponent();
    }


    @Override
    public ControllerConfigurationEntity updateControllerConfiguration(final Revision revision, final ControllerConfigurationDTO controllerConfigurationDTO) {
        final RevisionUpdate<ControllerConfigurationDTO> updatedComponent = updateComponent(
                revision,
                controllerFacade,
                () -> {
                    if (controllerConfigurationDTO.getMaxTimerDrivenThreadCount() != null) {
                        controllerFacade.setMaxTimerDrivenThreadCount(controllerConfigurationDTO.getMaxTimerDrivenThreadCount());
                    }
                    if (controllerConfigurationDTO.getMaxEventDrivenThreadCount() != null) {
                        controllerFacade.setMaxEventDrivenThreadCount(controllerConfigurationDTO.getMaxEventDrivenThreadCount());
                    }

                    return controllerConfigurationDTO;
                },
                controller -> dtoFactory.createControllerConfigurationDto(controllerFacade));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerFacade);
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(updatedComponent.getLastModification());
        return entityFactory.createControllerConfigurationEntity(updatedComponent.getComponent(), updateRevision, permissions);
    }

    @Override
    public NodeDTO updateNode(final NodeDTO nodeDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }
        final String userDn = user.getIdentity();

        final NodeIdentifier nodeId = clusterCoordinator.getNodeIdentifier(nodeDTO.getNodeId());
        if (nodeId == null) {
            throw new UnknownNodeException("No node exists with ID " + nodeDTO.getNodeId());
        }


        if (NodeConnectionState.CONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterCoordinator.requestNodeConnect(nodeId, userDn);
        } else if (NodeConnectionState.DISCONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterCoordinator.requestNodeDisconnect(nodeId, DisconnectionCode.USER_DISCONNECTED,
                    "User " + userDn + " requested that node be disconnected from cluster");
        }

        return getNode(nodeId);
    }

    @Override
    public CounterDTO updateCounter(final String counterId) {
        return dtoFactory.createCounterDto(controllerFacade.resetCounter(counterId));
    }

    @Override
    public void verifyCanClearProcessorState(final String processorId) {
        processorDAO.verifyClearState(processorId);
    }

    @Override
    public void clearProcessorState(final String processorId) {
        processorDAO.clearState(processorId);
    }

    @Override
    public void verifyCanClearControllerServiceState(final String controllerServiceId) {
        controllerServiceDAO.verifyClearState(controllerServiceId);
    }

    @Override
    public void clearControllerServiceState(final String controllerServiceId) {
        controllerServiceDAO.clearState(controllerServiceId);
    }

    @Override
    public void verifyCanClearReportingTaskState(final String reportingTaskId) {
        reportingTaskDAO.verifyClearState(reportingTaskId);
    }

    @Override
    public void clearReportingTaskState(final String reportingTaskId) {
        reportingTaskDAO.clearState(reportingTaskId);
    }

    @Override
    public ConnectionEntity deleteConnection(final Revision revision, final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionDTO snapshot = deleteComponent(
                revision,
                connection.getResource(),
                () -> connectionDAO.deleteConnection(connectionId),
                false, // no policies to remove
                dtoFactory.createConnectionDto(connection));

        return entityFactory.createConnectionEntity(snapshot, null, permissions, null);
    }

    @Override
    public DropRequestDTO deleteFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.deleteFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO deleteFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.deleteFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public ProcessorEntity deleteProcessor(final Revision revision, final String processorId) {
        final ProcessorNode processor = processorDAO.getProcessor(processorId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final ProcessorDTO snapshot = deleteComponent(
                revision,
                processor.getResource(),
                () -> processorDAO.deleteProcessor(processorId),
                true,
                dtoFactory.createProcessorDto(processor));

        return entityFactory.createProcessorEntity(snapshot, null, permissions, null, null);
    }

    @Override
    public LabelEntity deleteLabel(final Revision revision, final String labelId) {
        final Label label = labelDAO.getLabel(labelId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(label);
        final LabelDTO snapshot = deleteComponent(
                revision,
                label.getResource(),
                () -> labelDAO.deleteLabel(labelId),
                true,
                dtoFactory.createLabelDto(label));

        return entityFactory.createLabelEntity(snapshot, null, permissions);
    }

    @Override
    public UserEntity deleteUser(final Revision revision, final String userId) {
        final User user = userDAO.getUser(userId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        final Set<TenantEntity> userGroups = user != null ? userGroupDAO.getUserGroupsForUser(userId).stream()
                .map(g -> g.getIdentifier()).map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()) : null;
        final Set<AccessPolicySummaryEntity> policyEntities = user != null ? userGroupDAO.getAccessPoliciesForUser(userId).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet()) : null;

        final String resourceIdentifier = ResourceFactory.getTenantResource().getIdentifier() + "/" + userId;
        final UserDTO snapshot = deleteComponent(
                revision,
                new Resource() {
                    @Override
                    public String getIdentifier() {
                        return resourceIdentifier;
                    }

                    @Override
                    public String getName() {
                        return resourceIdentifier;
                    }

                    @Override
                    public String getSafeDescription() {
                        return "User " + userId;
                    }
                },
                () -> userDAO.deleteUser(userId),
                false, // no user specific policies to remove
                dtoFactory.createUserDto(user, userGroups, policyEntities));

        return entityFactory.createUserEntity(snapshot, null, permissions);
    }

    @Override
    public UserGroupEntity deleteUserGroup(final Revision revision, final String userGroupId) {
        final Group userGroup = userGroupDAO.getUserGroup(userGroupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        final Set<TenantEntity> users = userGroup != null ? userGroup.getUsers().stream()
                .map(mapUserIdToTenantEntity()).collect(Collectors.toSet()) : null;
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUserGroup(userGroup.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());

        final String resourceIdentifier = ResourceFactory.getTenantResource().getIdentifier() + "/" + userGroupId;
        final UserGroupDTO snapshot = deleteComponent(
                revision,
                new Resource() {
                    @Override
                    public String getIdentifier() {
                        return resourceIdentifier;
                    }

                    @Override
                    public String getName() {
                        return resourceIdentifier;
                    }

                    @Override
                    public String getSafeDescription() {
                        return "User Group " + userGroupId;
                    }
                },
                () -> userGroupDAO.deleteUserGroup(userGroupId),
                false, // no user group specific policies to remove
                dtoFactory.createUserGroupDto(userGroup, users, policyEntities));

        return entityFactory.createUserGroupEntity(snapshot, null, permissions);
    }

    @Override
    public AccessPolicyEntity deleteAccessPolicy(final Revision revision, final String accessPolicyId) {
        final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(accessPolicyId);
        final ComponentReferenceEntity componentReference = createComponentReferenceEntity(accessPolicy.getResource());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getAccessPolicyById(accessPolicyId));
        final Set<TenantEntity> userGroups = accessPolicy != null ? accessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()) : null;
        final Set<TenantEntity> users = accessPolicy != null ? accessPolicy.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet()) : null;
        final AccessPolicyDTO snapshot = deleteComponent(
                revision,
                new Resource() {
                    @Override
                    public String getIdentifier() {
                        return accessPolicy.getResource();
                    }

                    @Override
                    public String getName() {
                        return accessPolicy.getResource();
                    }

                    @Override
                    public String getSafeDescription() {
                        return "Policy " + accessPolicyId;
                    }
                },
                () -> accessPolicyDAO.deleteAccessPolicy(accessPolicyId),
                false, // no need to clean up any policies as it's already been removed above
                dtoFactory.createAccessPolicyDto(accessPolicy, userGroups, users, componentReference));

        return entityFactory.createAccessPolicyEntity(snapshot, null, permissions);
    }

    @Override
    public FunnelEntity deleteFunnel(final Revision revision, final String funnelId) {
        final Funnel funnel = funnelDAO.getFunnel(funnelId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(funnel);
        final FunnelDTO snapshot = deleteComponent(
                revision,
                funnel.getResource(),
                () -> funnelDAO.deleteFunnel(funnelId),
                true,
                dtoFactory.createFunnelDto(funnel));

        return entityFactory.createFunnelEntity(snapshot, null, permissions);
    }

    /**
     * Deletes a component using the Optimistic Locking Manager
     *
     * @param revision     the current revision
     * @param resource the resource being removed
     * @param deleteAction the action that deletes the component via the appropriate DAO object
     * @param cleanUpPolicies whether or not the policies for this resource should be removed as well - not necessary when there are
     *                        no component specific policies or if the policies of the component are inherited
     * @return a dto that represents the new configuration
     */
    private <D, C> D deleteComponent(final Revision revision, final Resource resource, final Runnable deleteAction, final boolean cleanUpPolicies, final D dto) {
        final RevisionClaim claim = new StandardRevisionClaim(revision);
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return revisionManager.deleteRevision(claim, user, new DeleteRevisionTask<D>() {
            @Override
            public D performTask() {
                logger.debug("Attempting to delete component {} with claim {}", resource.getIdentifier(), claim);

                // run the delete action
                deleteAction.run();

                // save the flow
                controllerFacade.save();
                logger.debug("Deletion of component {} was successful", resource.getIdentifier());

                if (cleanUpPolicies) {
                    cleanUpPolicies(resource);
                }

                return dto;
            }
        });
    }

    /**
     * Clean up the policies for the specified component resource.
     *
     * @param componentResource the resource for the component
     */
    private void cleanUpPolicies(final Resource componentResource) {
        // ensure the authorizer supports configuration
        if (accessPolicyDAO.supportsConfigurableAuthorizer()) {
            final List<Resource> resources = new ArrayList<>();
            resources.add(componentResource);
            resources.add(ResourceFactory.getDataResource(componentResource));
            resources.add(ResourceFactory.getDataTransferResource(componentResource));
            resources.add(ResourceFactory.getPolicyResource(componentResource));

            for (final Resource resource : resources) {
                for (final RequestAction action : RequestAction.values()) {
                    try {
                        // since the component is being deleted, also delete any relevant access policies
                        final AccessPolicy readPolicy = accessPolicyDAO.getAccessPolicy(action, resource.getIdentifier());
                        if (readPolicy != null) {
                            accessPolicyDAO.deleteAccessPolicy(readPolicy.getIdentifier());
                        }
                    } catch (final Exception e) {
                        logger.warn(String.format("Unable to remove access policy for %s %s after component removal.", action, resource.getIdentifier()), e);
                    }
                }
            }
        }
    }

    @Override
    public void verifyDeleteSnippet(final String snippetId, final Set<String> affectedComponentIds) {
        snippetDAO.verifyDeleteSnippetComponents(snippetId);
    }

    @Override
    public SnippetEntity deleteSnippet(final Set<Revision> revisions, final String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);

        // grab the resources in the snippet so we can delete the policies afterwards
        final Set<Resource> snippetResources = new HashSet<>();
        snippet.getProcessors().keySet().forEach(id -> snippetResources.add(processorDAO.getProcessor(id).getResource()));
        snippet.getInputPorts().keySet().forEach(id -> snippetResources.add(inputPortDAO.getPort(id).getResource()));
        snippet.getOutputPorts().keySet().forEach(id -> snippetResources.add(outputPortDAO.getPort(id).getResource()));
        snippet.getFunnels().keySet().forEach(id -> snippetResources.add(funnelDAO.getFunnel(id).getResource()));
        snippet.getLabels().keySet().forEach(id -> snippetResources.add(labelDAO.getLabel(id).getResource()));
        snippet.getRemoteProcessGroups().keySet().forEach(id -> snippetResources.add(remoteProcessGroupDAO.getRemoteProcessGroup(id).getResource()));
        snippet.getProcessGroups().keySet().forEach(id -> {
            final ProcessGroup processGroup = processGroupDAO.getProcessGroup(id);

            // add the process group
            snippetResources.add(processGroup.getResource());

            // add each encapsulated component
            processGroup.findAllProcessors().forEach(processor -> snippetResources.add(processor.getResource()));
            processGroup.findAllInputPorts().forEach(inputPort -> snippetResources.add(inputPort.getResource()));
            processGroup.findAllOutputPorts().forEach(outputPort -> snippetResources.add(outputPort.getResource()));
            processGroup.findAllFunnels().forEach(funnel -> snippetResources.add(funnel.getResource()));
            processGroup.findAllLabels().forEach(label -> snippetResources.add(label.getResource()));
            processGroup.findAllProcessGroups().forEach(childGroup -> snippetResources.add(childGroup.getResource()));
            processGroup.findAllRemoteProcessGroups().forEach(remoteProcessGroup -> snippetResources.add(remoteProcessGroup.getResource()));
            processGroup.findAllTemplates().forEach(template -> snippetResources.add(template.getResource()));
            processGroup.findAllControllerServices().forEach(controllerService -> snippetResources.add(controllerService.getResource()));
        });

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionClaim claim = new StandardRevisionClaim(revisions);
        final SnippetDTO dto = revisionManager.deleteRevision(claim, user, new DeleteRevisionTask<SnippetDTO>() {
            @Override
            public SnippetDTO performTask() {
                // delete the components in the snippet
                snippetDAO.deleteSnippetComponents(snippetId);

                // drop the snippet
                snippetDAO.dropSnippet(snippetId);

                // save
                controllerFacade.save();

                // create the dto for the snippet that was just removed
                return dtoFactory.createSnippetDto(snippet);
            }
        });

        // clean up component policies
        snippetResources.forEach(resource -> cleanUpPolicies(resource));

        return entityFactory.createSnippetEntity(dto);
    }

    @Override
    public PortEntity deleteInputPort(final Revision revision, final String inputPortId) {
        final Port port = inputPortDAO.getPort(inputPortId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PortDTO snapshot = deleteComponent(
                revision,
                port.getResource(),
                () -> inputPortDAO.deletePort(inputPortId),
                true,
                dtoFactory.createPortDto(port));

        return entityFactory.createPortEntity(snapshot, null, permissions, null, null);
    }

    @Override
    public PortEntity deleteOutputPort(final Revision revision, final String outputPortId) {
        final Port port = outputPortDAO.getPort(outputPortId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PortDTO snapshot = deleteComponent(
                revision,
                port.getResource(),
                () -> outputPortDAO.deletePort(outputPortId),
                true,
                dtoFactory.createPortDto(port));

        return entityFactory.createPortEntity(snapshot, null, permissions, null, null);
    }

    @Override
    public ProcessGroupEntity deleteProcessGroup(final Revision revision, final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);

        // grab the resources in the snippet so we can delete the policies afterwards
        final Set<Resource> groupResources = new HashSet<>();
        processGroup.findAllProcessors().forEach(processor -> groupResources.add(processor.getResource()));
        processGroup.findAllInputPorts().forEach(inputPort -> groupResources.add(inputPort.getResource()));
        processGroup.findAllOutputPorts().forEach(outputPort -> groupResources.add(outputPort.getResource()));
        processGroup.findAllFunnels().forEach(funnel -> groupResources.add(funnel.getResource()));
        processGroup.findAllLabels().forEach(label -> groupResources.add(label.getResource()));
        processGroup.findAllProcessGroups().forEach(childGroup -> groupResources.add(childGroup.getResource()));
        processGroup.findAllRemoteProcessGroups().forEach(remoteProcessGroup -> groupResources.add(remoteProcessGroup.getResource()));
        processGroup.findAllTemplates().forEach(template -> groupResources.add(template.getResource()));
        processGroup.findAllControllerServices().forEach(controllerService -> groupResources.add(controllerService.getResource()));

        final ProcessGroupDTO snapshot = deleteComponent(
                revision,
                processGroup.getResource(),
                () -> processGroupDAO.deleteProcessGroup(groupId),
                true,
                dtoFactory.createProcessGroupDto(processGroup));

        // delete all applicable component policies
        groupResources.forEach(groupResource -> cleanUpPolicies(groupResource));

        return entityFactory.createProcessGroupEntity(snapshot, null, permissions, null, null);
    }

    @Override
    public RemoteProcessGroupEntity deleteRemoteProcessGroup(final Revision revision, final String remoteProcessGroupId) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroup);
        final RemoteProcessGroupDTO snapshot = deleteComponent(
                revision,
                remoteProcessGroup.getResource(),
                () -> remoteProcessGroupDAO.deleteRemoteProcessGroup(remoteProcessGroupId),
                true,
                dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        return entityFactory.createRemoteProcessGroupEntity(snapshot, null, permissions, null, null);
    }

    @Override
    public void deleteTemplate(final String id) {
        // delete the template and save the flow
        templateDAO.deleteTemplate(id);
        controllerFacade.save();
    }

    @Override
    public ConnectionEntity createConnection(final Revision revision, final String groupId, final ConnectionDTO connectionDTO) {
        final RevisionUpdate<ConnectionDTO> snapshot = createComponent(
                revision,
                connectionDTO,
                () -> connectionDAO.createConnection(groupId, connectionDTO),
                connection -> dtoFactory.createConnectionDto(connection));

        final Connection connection = connectionDAO.getConnection(connectionDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionDTO.getId()));
        return entityFactory.createConnectionEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status);
    }

    @Override
    public DropRequestDTO createFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.createFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO createFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.createFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public ProcessorEntity createProcessor(final Revision revision, final String groupId, final ProcessorDTO processorDTO) {
        final RevisionUpdate<ProcessorDTO> snapshot = createComponent(
                revision,
                processorDTO,
                () -> processorDAO.createProcessor(groupId, processorDTO),
                processor -> dtoFactory.createProcessorDto(processor));

        final ProcessorNode processor = processorDAO.getProcessor(processorDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processorDTO.getId()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processorDTO.getId()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessorEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public LabelEntity createLabel(final Revision revision, final String groupId, final LabelDTO labelDTO) {
        final RevisionUpdate<LabelDTO> snapshot = createComponent(
                revision,
                labelDTO,
                () -> labelDAO.createLabel(groupId, labelDTO),
                label -> dtoFactory.createLabelDto(label));

        final Label label = labelDAO.getLabel(labelDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(label);
        return entityFactory.createLabelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    /**
     * Creates a component using the optimistic locking manager.
     *
     * @param componentDto the DTO that will be used to create the component
     * @param daoCreation  A Supplier that will create the NiFi Component to use
     * @param dtoCreation  a Function that will convert the NiFi Component into a corresponding DTO
     * @param <D>          the DTO Type
     * @param <C>          the NiFi Component Type
     * @return a RevisionUpdate that represents the updated configuration
     */
    private <D, C> RevisionUpdate<D> createComponent(final Revision revision, final ComponentDTO componentDto, final Supplier<C> daoCreation, final Function<C, D> dtoCreation) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // read lock on the containing group
        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        // update revision through revision manager
        return revisionManager.updateRevision(claim, user, () -> {
            // add the component
            final C component = daoCreation.get();

            // save the flow
            controllerFacade.save();

            final D dto = dtoCreation.apply(component);
            final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
            return new StandardRevisionUpdate<D>(dto, lastMod);
        });
    }

    @Override
    public BulletinEntity createBulletin(final BulletinDTO bulletinDTO, final Boolean canRead){
        final Bulletin bulletin = BulletinFactory.createBulletin(bulletinDTO.getCategory(),bulletinDTO.getLevel(),bulletinDTO.getMessage());
        bulletinRepository.addBulletin(bulletin);
        return entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin),canRead);
    }

    @Override
    public FunnelEntity createFunnel(final Revision revision, final String groupId, final FunnelDTO funnelDTO) {
        final RevisionUpdate<FunnelDTO> snapshot = createComponent(
                revision,
                funnelDTO,
                () -> funnelDAO.createFunnel(groupId, funnelDTO),
                funnel -> dtoFactory.createFunnelDto(funnel));

        final Funnel funnel = funnelDAO.getFunnel(funnelDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(funnel);
        return entityFactory.createFunnelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public AccessPolicyEntity createAccessPolicy(final Revision revision, final AccessPolicyDTO accessPolicyDTO) {
        final Authorizable tenantAuthorizable = authorizableLookup.getTenant();
        final String creator = NiFiUserUtils.getNiFiUserIdentity();

        final AccessPolicy newAccessPolicy = accessPolicyDAO.createAccessPolicy(accessPolicyDTO);
        final ComponentReferenceEntity componentReference = createComponentReferenceEntity(newAccessPolicy.getResource());
        final AccessPolicyDTO newAccessPolicyDto = dtoFactory.createAccessPolicyDto(newAccessPolicy,
                newAccessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()),
                newAccessPolicy.getUsers().stream().map(userId -> {
                    final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userId));
                    return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(userDAO.getUser(userId)), userRevision,
                            dtoFactory.createPermissionsDto(tenantAuthorizable));
                }).collect(Collectors.toSet()), componentReference);

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getAccessPolicyById(accessPolicyDTO.getId()));
        return entityFactory.createAccessPolicyEntity(newAccessPolicyDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), permissions);
    }

    @Override
    public UserEntity createUser(final Revision revision, final UserDTO userDTO) {
        final String creator = NiFiUserUtils.getNiFiUserIdentity();
        final User newUser = userDAO.createUser(userDTO);
        final Set<TenantEntity> tenantEntities = userGroupDAO.getUserGroupsForUser(newUser.getIdentifier()).stream()
                .map(g -> g.getIdentifier()).map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet());
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUser(newUser.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
        final UserDTO newUserDto = dtoFactory.createUserDto(newUser, tenantEntities, policyEntities);

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        return entityFactory.createUserEntity(newUserDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), permissions);
    }

    private ComponentReferenceEntity createComponentReferenceEntity(final String resource) {
        ComponentReferenceEntity componentReferenceEntity = null;
        try {
            // get the component authorizable
            Authorizable componentAuthorizable = authorizableLookup.getAuthorizableFromResource(resource);

            // if this represents an authorizable whose policy permissions are enforced through the base resource,
            // get the underlying base authorizable for the component reference
            if (componentAuthorizable instanceof EnforcePolicyPermissionsThroughBaseResource) {
                componentAuthorizable = ((EnforcePolicyPermissionsThroughBaseResource) componentAuthorizable).getBaseAuthorizable();
            }

            final ComponentReferenceDTO componentReference = dtoFactory.createComponentReferenceDto(componentAuthorizable);
            if (componentReference != null) {
                final PermissionsDTO componentReferencePermissions = dtoFactory.createPermissionsDto(componentAuthorizable);
                final RevisionDTO componentReferenceRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(componentReference.getId()));
                componentReferenceEntity = entityFactory.createComponentReferenceEntity(componentReference, componentReferenceRevision, componentReferencePermissions);
            }
        } catch (final ResourceNotFoundException e) {
            // component not found for the specified resource
        }

        return componentReferenceEntity;
    }

    private AccessPolicySummaryEntity createAccessPolicySummaryEntity(final AccessPolicy ap) {
        final ComponentReferenceEntity componentReference = createComponentReferenceEntity(ap.getResource());
        final AccessPolicySummaryDTO apSummary = dtoFactory.createAccessPolicySummaryDto(ap, componentReference);
        final PermissionsDTO apPermissions = dtoFactory.createPermissionsDto(authorizableLookup.getAccessPolicyById(ap.getIdentifier()));
        final RevisionDTO apRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(ap.getIdentifier()));
        return entityFactory.createAccessPolicySummaryEntity(apSummary, apRevision, apPermissions);
    }

    @Override
    public UserGroupEntity createUserGroup(final Revision revision, final UserGroupDTO userGroupDTO) {
        final String creator = NiFiUserUtils.getNiFiUserIdentity();
        final Group newUserGroup = userGroupDAO.createUserGroup(userGroupDTO);
        final Set<TenantEntity> tenantEntities = newUserGroup.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet());
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUserGroup(newUserGroup.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
        final UserGroupDTO newUserGroupDto = dtoFactory.createUserGroupDto(newUserGroup, tenantEntities, policyEntities);

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        return entityFactory.createUserGroupEntity(newUserGroupDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), permissions);
    }

    private void validateSnippetContents(final FlowSnippetDTO flow) {
        // validate any processors
        if (flow.getProcessors() != null) {
            for (final ProcessorDTO processorDTO : flow.getProcessors()) {
                final ProcessorNode processorNode = processorDAO.getProcessor(processorDTO.getId());
                final Collection<ValidationResult> validationErrors = processorNode.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    processorDTO.setValidationErrors(errors);
                }
            }
        }

        if (flow.getInputPorts() != null) {
            for (final PortDTO portDTO : flow.getInputPorts()) {
                final Port port = inputPortDAO.getPort(portDTO.getId());
                final Collection<ValidationResult> validationErrors = port.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    portDTO.setValidationErrors(errors);
                }
            }
        }

        if (flow.getOutputPorts() != null) {
            for (final PortDTO portDTO : flow.getOutputPorts()) {
                final Port port = outputPortDAO.getPort(portDTO.getId());
                final Collection<ValidationResult> validationErrors = port.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    portDTO.setValidationErrors(errors);
                }
            }
        }

        // get any remote process group issues
        if (flow.getRemoteProcessGroups() != null) {
            for (final RemoteProcessGroupDTO remoteProcessGroupDTO : flow.getRemoteProcessGroups()) {
                final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());

                if (remoteProcessGroup.getAuthorizationIssue() != null) {
                    remoteProcessGroupDTO.setAuthorizationIssues(Arrays.asList(remoteProcessGroup.getAuthorizationIssue()));
                }
            }
        }
    }

    @Override
    public FlowEntity copySnippet(final String groupId, final String snippetId, final Double originX, final Double originY, final String idGenerationSeed) {
        // create the new snippet
        final FlowSnippetDTO snippet = snippetDAO.copySnippet(groupId, snippetId, originX, originY, idGenerationSeed);

        // save the flow
        controllerFacade.save();

        // drop the snippet
        snippetDAO.dropSnippet(snippetId);

        // post process new flow snippet
        final FlowDTO flowDto = postProcessNewFlowSnippet(groupId, snippet);

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setFlow(flowDto);
        return flowEntity;
    }

    @Override
    public SnippetEntity createSnippet(final SnippetDTO snippetDTO) {
        // add the component
        final Snippet snippet = snippetDAO.createSnippet(snippetDTO);

        // save the flow
        controllerFacade.save();

        final SnippetDTO dto = dtoFactory.createSnippetDto(snippet);
        final RevisionUpdate<SnippetDTO> snapshot = new StandardRevisionUpdate<SnippetDTO>(dto, null);

        return entityFactory.createSnippetEntity(snapshot.getComponent());
    }

    @Override
    public PortEntity createInputPort(final Revision revision, final String groupId, final PortDTO inputPortDTO) {
        final RevisionUpdate<PortDTO> snapshot = createComponent(
                revision,
                inputPortDTO,
                () -> inputPortDAO.createPort(groupId, inputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final Port port = inputPortDAO.getPort(inputPortDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public PortEntity createOutputPort(final Revision revision, final String groupId, final PortDTO outputPortDTO) {
        final RevisionUpdate<PortDTO> snapshot = createComponent(
                revision,
                outputPortDTO,
                () -> outputPortDAO.createPort(groupId, outputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final Port port = outputPortDAO.getPort(outputPortDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final Revision revision, final String parentGroupId, final ProcessGroupDTO processGroupDTO) {
        final RevisionUpdate<ProcessGroupDTO> snapshot = createComponent(
                revision,
                processGroupDTO,
                () -> processGroupDAO.createProcessGroup(parentGroupId, processGroupDTO),
                processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroup.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processGroup.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public RemoteProcessGroupEntity createRemoteProcessGroup(final Revision revision, final String groupId, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final RevisionUpdate<RemoteProcessGroupDTO> snapshot = createComponent(
                revision,
                remoteProcessGroupDTO,
                () -> remoteProcessGroupDAO.createRemoteProcessGroup(groupId, remoteProcessGroupDTO),
                remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroup);
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(remoteProcessGroup.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(remoteProcessGroup.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createRemoteProcessGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public void verifyCanAddTemplate(String groupId, String name) {
        templateDAO.verifyCanAddTemplate(name, groupId);
    }

    @Override
    public void verifyComponentTypes(FlowSnippetDTO snippet) {
        templateDAO.verifyComponentTypes(snippet);
    }

    @Override
    public TemplateDTO createTemplate(final String name, final String description, final String snippetId, final String groupId, final Optional<String> idGenerationSeed) {
        // get the specified snippet
        final Snippet snippet = snippetDAO.getSnippet(snippetId);

        // create the template
        final TemplateDTO templateDTO = new TemplateDTO();
        templateDTO.setName(name);
        templateDTO.setDescription(description);
        templateDTO.setTimestamp(new Date());
        templateDTO.setSnippet(snippetUtils.populateFlowSnippet(snippet, true, true, true));
        templateDTO.setEncodingVersion(TemplateDTO.MAX_ENCODING_VERSION);

        // set the id based on the specified seed
        final String uuid = idGenerationSeed.isPresent() ? (UUID.nameUUIDFromBytes(idGenerationSeed.get().getBytes(StandardCharsets.UTF_8))).toString() : UUID.randomUUID().toString();
        templateDTO.setId(uuid);

        // create the template
        final Template template = templateDAO.createTemplate(templateDTO, groupId);

        // drop the snippet
        snippetDAO.dropSnippet(snippetId);

        // save the flow
        controllerFacade.save();

        return dtoFactory.createTemplateDTO(template);
    }

    /**
     * Ensures default values are populated for all components in this snippet. This is necessary to handle old templates without default values
     * and when existing properties have default values introduced.
     *
     * @param snippet snippet
     */
    private void ensureDefaultPropertyValuesArePopulated(final FlowSnippetDTO snippet) {
        if (snippet != null) {
            if (snippet.getControllerServices() != null) {
                snippet.getControllerServices().forEach(dto -> {
                    if (dto.getProperties() == null) {
                        dto.setProperties(new LinkedHashMap<>());
                    }

                    try {
                        final ConfigurableComponent configurableComponent = controllerFacade.getTemporaryComponent(dto.getType(), dto.getBundle());
                        configurableComponent.getPropertyDescriptors().forEach(descriptor -> {
                            if (dto.getProperties().get(descriptor.getName()) == null) {
                                dto.getProperties().put(descriptor.getName(), descriptor.getDefaultValue());
                            }
                        });
                    } catch (final Exception e) {
                        logger.warn(String.format("Unable to create ControllerService of type %s to populate default values.", dto.getType()));
                    }
                });
            }

            if (snippet.getProcessors() != null) {
                snippet.getProcessors().forEach(dto -> {
                    if (dto.getConfig() == null) {
                        dto.setConfig(new ProcessorConfigDTO());
                    }

                    final ProcessorConfigDTO config = dto.getConfig();
                    if (config.getProperties() == null) {
                        config.setProperties(new LinkedHashMap<>());
                    }

                    try {
                        final ConfigurableComponent configurableComponent = controllerFacade.getTemporaryComponent(dto.getType(), dto.getBundle());
                        configurableComponent.getPropertyDescriptors().forEach(descriptor -> {
                            if (config.getProperties().get(descriptor.getName()) == null) {
                                config.getProperties().put(descriptor.getName(), descriptor.getDefaultValue());
                            }
                        });
                    } catch (final Exception e) {
                        logger.warn(String.format("Unable to create Processor of type %s to populate default values.", dto.getType()));
                    }
                });
            }

            if (snippet.getProcessGroups() != null) {
                snippet.getProcessGroups().forEach(processGroup -> {
                    ensureDefaultPropertyValuesArePopulated(processGroup.getContents());
                });
            }
        }
    }

    @Override
    public TemplateDTO importTemplate(final TemplateDTO templateDTO, final String groupId, final Optional<String> idGenerationSeed) {
        // ensure id is set
        final String uuid = idGenerationSeed.isPresent() ? (UUID.nameUUIDFromBytes(idGenerationSeed.get().getBytes(StandardCharsets.UTF_8))).toString() : UUID.randomUUID().toString();
        templateDTO.setId(uuid);

        // mark the timestamp
        templateDTO.setTimestamp(new Date());

        // ensure default values are populated
        ensureDefaultPropertyValuesArePopulated(templateDTO.getSnippet());

        // import the template
        final Template template = templateDAO.importTemplate(templateDTO, groupId);

        // save the flow
        controllerFacade.save();

        // return the template dto
        return dtoFactory.createTemplateDTO(template);
    }

    /**
     * Post processes a new flow snippet including validation, removing the snippet, and DTO conversion.
     *
     * @param groupId group id
     * @param snippet snippet
     * @return flow dto
     */
    private FlowDTO postProcessNewFlowSnippet(final String groupId, final FlowSnippetDTO snippet) {
        // validate the new snippet
        validateSnippetContents(snippet);

        // identify all components added
        final Set<String> identifiers = new HashSet<>();
        snippet.getProcessors().stream()
                .map(proc -> proc.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getConnections().stream()
                .map(conn -> conn.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getInputPorts().stream()
                .map(port -> port.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getOutputPorts().stream()
                .map(port -> port.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getProcessGroups().stream()
                .map(group -> group.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getRemoteProcessGroups().stream()
                .map(remoteGroup -> remoteGroup.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getRemoteProcessGroups().stream()
                .filter(remoteGroup -> remoteGroup.getContents() != null && remoteGroup.getContents().getInputPorts() != null)
                .flatMap(remoteGroup -> remoteGroup.getContents().getInputPorts().stream())
                .map(remoteInputPort -> remoteInputPort.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getRemoteProcessGroups().stream()
                .filter(remoteGroup -> remoteGroup.getContents() != null && remoteGroup.getContents().getOutputPorts() != null)
                .flatMap(remoteGroup -> remoteGroup.getContents().getOutputPorts().stream())
                .map(remoteOutputPort -> remoteOutputPort.getId())
                .forEach(id -> identifiers.add(id));
        snippet.getLabels().stream()
                .map(label -> label.getId())
                .forEach(id -> identifiers.add(id));

        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        final ProcessGroupStatus groupStatus = controllerFacade.getProcessGroupStatus(groupId);
        return dtoFactory.createFlowDto(group, groupStatus, snippet, revisionManager, this::getProcessGroupBulletins);
    }

    @Override
    public FlowEntity createTemplateInstance(final String groupId, final Double originX, final Double originY, final String templateEncodingVersion,
                                             final FlowSnippetDTO requestSnippet, final String idGenerationSeed) {

        // instantiate the template - there is no need to make another copy of the flow snippet since the actual template
        // was copied and this dto is only used to instantiate it's components (which as already completed)
        final FlowSnippetDTO snippet = templateDAO.instantiateTemplate(groupId, originX, originY, templateEncodingVersion, requestSnippet, idGenerationSeed);

        // save the flow
        controllerFacade.save();

        // post process the new flow snippet
        final FlowDTO flowDto = postProcessNewFlowSnippet(groupId, snippet);

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setFlow(flowDto);
        return flowEntity;
    }

    @Override
    public ControllerServiceEntity createControllerService(final Revision revision, final String groupId, final ControllerServiceDTO controllerServiceDTO) {
        controllerServiceDTO.setParentGroupId(groupId);

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        final RevisionUpdate<ControllerServiceDTO> snapshot;
        if (groupId == null) {
                // update revision through revision manager
                snapshot = revisionManager.updateRevision(claim, user, () -> {
                    // Unfortunately, we can not use the createComponent() method here because createComponent() wants to obtain the read lock
                    // on the group. The Controller Service may or may not have a Process Group (it won't if it's controller-scoped).
                    final ControllerServiceNode controllerService = controllerServiceDAO.createControllerService(controllerServiceDTO);
                    final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(controllerService);

                    controllerFacade.save();

                    final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
                    return new StandardRevisionUpdate<ControllerServiceDTO>(dto, lastMod);
                });
        } else {
            snapshot = revisionManager.updateRevision(claim, user, () -> {
                final ControllerServiceNode controllerService = controllerServiceDAO.createControllerService(controllerServiceDTO);
                final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(controllerService);

                controllerFacade.save();

                final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
                return new StandardRevisionUpdate<ControllerServiceDTO>(dto, lastMod);
            });
        }

        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerService);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(controllerServiceDTO.getId()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, bulletinEntities);
    }

    @Override
    public ControllerServiceEntity updateControllerService(final Revision revision, final ControllerServiceDTO controllerServiceDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final RevisionUpdate<ControllerServiceDTO> snapshot = updateComponent(revision,
                controllerService,
                () -> controllerServiceDAO.updateControllerService(controllerServiceDTO),
                cs -> {
                    final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(cs);
                    final ControllerServiceReference ref = controllerService.getReferences();
                    final ControllerServiceReferencingComponentsEntity referencingComponentsEntity =
                            createControllerServiceReferencingComponentsEntity(ref, Sets.newHashSet(controllerService.getIdentifier()));
                    dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());
                    return dto;
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerService);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(controllerServiceDTO.getId()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, bulletinEntities);
    }

    private Set<ConfiguredComponent> findAllReferencingComponents(final ControllerServiceReference reference) {
        final Set<ConfiguredComponent> referencingComponents = new HashSet<>(reference.getReferencingComponents());

        for (final ConfiguredComponent referencingComponent : reference.getReferencingComponents()) {
            if (referencingComponent instanceof ControllerServiceNode) {
                referencingComponents.addAll(findAllReferencingComponents(((ControllerServiceNode) referencingComponent).getReferences()));
            }
        }

        return referencingComponents;
    }

    @Override
    public ControllerServiceReferencingComponentsEntity updateControllerServiceReferencingComponents(
            final Map<String, Revision> referenceRevisions, final String controllerServiceId, final ScheduledState scheduledState, final ControllerServiceState controllerServiceState) {

        final RevisionClaim claim = new StandardRevisionClaim(referenceRevisions.values());

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionUpdate<ControllerServiceReferencingComponentsEntity> update = revisionManager.updateRevision(claim, user,
                new UpdateRevisionTask<ControllerServiceReferencingComponentsEntity>() {
                    @Override
                    public RevisionUpdate<ControllerServiceReferencingComponentsEntity> update() {
                        final Set<ConfiguredComponent> updated = controllerServiceDAO.updateControllerServiceReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
                        final ControllerServiceReference updatedReference = controllerServiceDAO.getControllerService(controllerServiceId).getReferences();

                        // get the revisions of the updated components
                        final Map<String, Revision> updatedRevisions = new HashMap<>();
                        for (final ConfiguredComponent component : updated) {
                            final Revision currentRevision = revisionManager.getRevision(component.getIdentifier());
                            final Revision requestRevision = referenceRevisions.get(component.getIdentifier());
                            updatedRevisions.put(component.getIdentifier(), currentRevision.incrementRevision(requestRevision.getClientId()));
                        }

                        // ensure the revision for all referencing components is included regardless of whether they were updated in this request
                        for (final ConfiguredComponent component : findAllReferencingComponents(updatedReference)) {
                            updatedRevisions.putIfAbsent(component.getIdentifier(), revisionManager.getRevision(component.getIdentifier()));
                        }

                        final ControllerServiceReferencingComponentsEntity entity = createControllerServiceReferencingComponentsEntity(updatedReference, updatedRevisions);
                        return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
                    }
                });

        return update.getComponent();
    }

    /**
     * Finds the identifiers for all components referencing a ControllerService.
     *
     * @param reference      ControllerServiceReference
     * @param visited        ControllerServices we've already visited
     */
    private void findControllerServiceReferencingComponentIdentifiers(final ControllerServiceReference reference, final Set<ControllerServiceNode> visited) {
        for (final ConfiguredComponent component : reference.getReferencingComponents()) {

            // if this is a ControllerService consider it's referencing components
            if (component instanceof ControllerServiceNode) {
                final ControllerServiceNode node = (ControllerServiceNode) component;
                if (!visited.contains(node)) {
                    findControllerServiceReferencingComponentIdentifiers(node.getReferences(), visited);
                }
                visited.add(node);
            }
        }
    }

    /**
     * Creates entities for components referencing a ControllerService using their current revision.
     *
     * @param reference ControllerServiceReference
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(final ControllerServiceReference reference, final Set<String> lockedIds) {
        final Set<ControllerServiceNode> visited = new HashSet<>();
        visited.add(reference.getReferencedComponent());
        findControllerServiceReferencingComponentIdentifiers(reference, visited);

        final Map<String, Revision> referencingRevisions = new HashMap<>();
        for (final ConfiguredComponent component : reference.getReferencingComponents()) {
            referencingRevisions.put(component.getIdentifier(), revisionManager.getRevision(component.getIdentifier()));
        }

        return createControllerServiceReferencingComponentsEntity(reference, referencingRevisions);
    }

    /**
     * Creates entities for components referencing a ControllerService using the specified revisions.
     *
     * @param reference ControllerServiceReference
     * @param revisions The revisions
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(
            final ControllerServiceReference reference, final Map<String, Revision> revisions) {
        final Set<ControllerServiceNode> visited = new HashSet<>();
        visited.add(reference.getReferencedComponent());
        return createControllerServiceReferencingComponentsEntity(reference, revisions, visited);
    }

    /**
     * Creates entities for components referencing a ControllerServcie using the specified revisions.
     *
     * @param reference ControllerServiceReference
     * @param revisions The revisions
     * @param visited   Which services we've already considered (in case of cycle)
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(
            final ControllerServiceReference reference, final Map<String, Revision> revisions, final Set<ControllerServiceNode> visited) {

        final String modifier = NiFiUserUtils.getNiFiUserIdentity();
        final Set<ConfiguredComponent> referencingComponents = reference.getReferencingComponents();

        final Set<ControllerServiceReferencingComponentEntity> componentEntities = new HashSet<>();
        for (final ConfiguredComponent refComponent : referencingComponents) {
            PermissionsDTO permissions = null;
            if (refComponent instanceof Authorizable) {
                permissions = dtoFactory.createPermissionsDto(refComponent);
            }

            final Revision revision = revisions.get(refComponent.getIdentifier());
            final FlowModification flowMod = new FlowModification(revision, modifier);
            final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(flowMod);
            final ControllerServiceReferencingComponentDTO dto = dtoFactory.createControllerServiceReferencingComponentDTO(refComponent);

            if (refComponent instanceof ControllerServiceNode) {
                final ControllerServiceNode node = (ControllerServiceNode) refComponent;

                // indicate if we've hit a cycle
                dto.setReferenceCycle(visited.contains(node));

                // mark node as visited before building the reference cycle
                visited.add(node);

                // if we haven't encountered this service before include it's referencing components
                if (!dto.getReferenceCycle()) {
                    final ControllerServiceReference refReferences = node.getReferences();
                    final Map<String, Revision> referencingRevisions = new HashMap<>(revisions);
                    for (final ConfiguredComponent component : refReferences.getReferencingComponents()) {
                        referencingRevisions.putIfAbsent(component.getIdentifier(), revisionManager.getRevision(component.getIdentifier()));
                    }
                    final ControllerServiceReferencingComponentsEntity references = createControllerServiceReferencingComponentsEntity(refReferences, referencingRevisions, visited);
                    dto.setReferencingComponents(references.getControllerServiceReferencingComponents());
                }
            }

            componentEntities.add(entityFactory.createControllerServiceReferencingComponentEntity(dto, revisionDto, permissions));
        }

        final ControllerServiceReferencingComponentsEntity entity = new ControllerServiceReferencingComponentsEntity();
        entity.setControllerServiceReferencingComponents(componentEntities);
        return entity;
    }

    @Override
    public ControllerServiceEntity deleteControllerService(final Revision revision, final String controllerServiceId) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerService);
        final ControllerServiceDTO snapshot = deleteComponent(
                revision,
                controllerService.getResource(),
                () -> controllerServiceDAO.deleteControllerService(controllerServiceId),
                true,
                dtoFactory.createControllerServiceDto(controllerService));

        return entityFactory.createControllerServiceEntity(snapshot, null, permissions, null);
    }


    @Override
    public ReportingTaskEntity createReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        // update revision through revision manager
        final RevisionUpdate<ReportingTaskDTO> snapshot = revisionManager.updateRevision(claim, user, () -> {
            // create the reporting task
            final ReportingTaskNode reportingTask = reportingTaskDAO.createReportingTask(reportingTaskDTO);

            // save the update
            controllerFacade.save();

            final ReportingTaskDTO dto = dtoFactory.createReportingTaskDto(reportingTask);
            final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
            return new StandardRevisionUpdate<ReportingTaskDTO>(dto, lastMod);
        });

        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reportingTask);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createReportingTaskEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, bulletinEntities);
    }

    @Override
    public ReportingTaskEntity updateReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskDTO.getId());
        final RevisionUpdate<ReportingTaskDTO> snapshot = updateComponent(revision,
                reportingTask,
                () -> reportingTaskDAO.updateReportingTask(reportingTaskDTO),
                rt -> dtoFactory.createReportingTaskDto(rt));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reportingTask);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createReportingTaskEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, bulletinEntities);
    }

    @Override
    public ReportingTaskEntity deleteReportingTask(final Revision revision, final String reportingTaskId) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reportingTask);
        final ReportingTaskDTO snapshot = deleteComponent(
                revision,
                reportingTask.getResource(),
                () -> reportingTaskDAO.deleteReportingTask(reportingTaskId),
                true,
                dtoFactory.createReportingTaskDto(reportingTask));

        return entityFactory.createReportingTaskEntity(snapshot, null, permissions, null);
    }

    @Override
    public void deleteActions(final Date endDate) {
        // get the user from the request
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the purge details
        final FlowChangePurgeDetails details = new FlowChangePurgeDetails();
        details.setEndDate(endDate);

        // create a purge action to record that records are being removed
        final FlowChangeAction purgeAction = new FlowChangeAction();
        purgeAction.setUserIdentity(user.getIdentity());
        purgeAction.setOperation(Operation.Purge);
        purgeAction.setTimestamp(new Date());
        purgeAction.setSourceId("Flow Controller");
        purgeAction.setSourceName("History");
        purgeAction.setSourceType(Component.Controller);
        purgeAction.setActionDetails(details);

        // purge corresponding actions
        auditService.purgeActions(endDate, purgeAction);
    }

    @Override
    public ProvenanceDTO submitProvenance(final ProvenanceDTO query) {
        return controllerFacade.submitProvenance(query);
    }

    @Override
    public void deleteProvenance(final String queryId) {
        controllerFacade.deleteProvenanceQuery(queryId);
    }

    @Override
    public LineageDTO submitLineage(final LineageDTO lineage) {
        return controllerFacade.submitLineage(lineage);
    }

    @Override
    public void deleteLineage(final String lineageId) {
        controllerFacade.deleteLineage(lineageId);
    }

    @Override
    public ProvenanceEventDTO submitReplay(final Long eventId) {
        return controllerFacade.submitReplay(eventId);
    }

    // -----------------------------------------
    // Read Operations
    // -----------------------------------------

    @Override
    public SearchResultsDTO searchController(final String query) {
        return controllerFacade.search(query);
    }

    @Override
    public DownloadableContent getContent(final String connectionId, final String flowFileUuid, final String uri) {
        return connectionDAO.getContent(connectionId, flowFileUuid, uri);
    }

    @Override
    public DownloadableContent getContent(final Long eventId, final String uri, final ContentDirection contentDirection) {
        return controllerFacade.getContent(eventId, uri, contentDirection);
    }

    @Override
    public ProvenanceDTO getProvenance(final String queryId, final Boolean summarize, final Boolean incrementalResults) {
        return controllerFacade.getProvenanceQuery(queryId, summarize, incrementalResults);
    }

    @Override
    public LineageDTO getLineage(final String lineageId) {
        return controllerFacade.getLineage(lineageId);
    }

    @Override
    public ProvenanceOptionsDTO getProvenanceSearchOptions() {
        return controllerFacade.getProvenanceSearchOptions();
    }

    @Override
    public ProvenanceEventDTO getProvenanceEvent(final Long id) {
        return controllerFacade.getProvenanceEvent(id);
    }

    @Override
    public ProcessGroupStatusEntity getProcessGroupStatus(final String groupId, final boolean recursive) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        final ProcessGroupStatusDTO dto = dtoFactory.createProcessGroupStatusDto(processGroup, controllerFacade.getProcessGroupStatus(groupId));

        // prune the response as necessary
        if (!recursive) {
            pruneChildGroups(dto.getAggregateSnapshot());
            if (dto.getNodeSnapshots() != null) {
                for (final NodeProcessGroupStatusSnapshotDTO nodeSnapshot : dto.getNodeSnapshots()) {
                    pruneChildGroups(nodeSnapshot.getStatusSnapshot());
                }
            }
        }

        return entityFactory.createProcessGroupStatusEntity(dto, permissions);
    }

    private void pruneChildGroups(final ProcessGroupStatusSnapshotDTO snapshot) {
        for (final ProcessGroupStatusSnapshotEntity childProcessGroupStatusEntity : snapshot.getProcessGroupStatusSnapshots()) {
            final ProcessGroupStatusSnapshotDTO childProcessGroupStatus = childProcessGroupStatusEntity.getProcessGroupStatusSnapshot();
            childProcessGroupStatus.setConnectionStatusSnapshots(null);
            childProcessGroupStatus.setProcessGroupStatusSnapshots(null);
            childProcessGroupStatus.setInputPortStatusSnapshots(null);
            childProcessGroupStatus.setOutputPortStatusSnapshots(null);
            childProcessGroupStatus.setProcessorStatusSnapshots(null);
            childProcessGroupStatus.setRemoteProcessGroupStatusSnapshots(null);
        }
    }

    @Override
    public ControllerStatusDTO getControllerStatus() {
        return controllerFacade.getControllerStatus();
    }

    @Override
    public ComponentStateDTO getProcessorState(final String processorId) {
        final StateMap clusterState = isClustered() ? processorDAO.getState(processorId, Scope.CLUSTER) : null;
        final StateMap localState = processorDAO.getState(processorId, Scope.LOCAL);

        // processor will be non null as it was already found when getting the state
        final ProcessorNode processor = processorDAO.getProcessor(processorId);
        return dtoFactory.createComponentStateDTO(processorId, processor.getProcessor().getClass(), localState, clusterState);
    }

    @Override
    public ComponentStateDTO getControllerServiceState(final String controllerServiceId) {
        final StateMap clusterState = isClustered() ? controllerServiceDAO.getState(controllerServiceId, Scope.CLUSTER) : null;
        final StateMap localState = controllerServiceDAO.getState(controllerServiceId, Scope.LOCAL);

        // controller service will be non null as it was already found when getting the state
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        return dtoFactory.createComponentStateDTO(controllerServiceId, controllerService.getControllerServiceImplementation().getClass(), localState, clusterState);
    }

    @Override
    public ComponentStateDTO getReportingTaskState(final String reportingTaskId) {
        final StateMap clusterState = isClustered() ? reportingTaskDAO.getState(reportingTaskId, Scope.CLUSTER) : null;
        final StateMap localState = reportingTaskDAO.getState(reportingTaskId, Scope.LOCAL);

        // reporting task will be non null as it was already found when getting the state
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        return dtoFactory.createComponentStateDTO(reportingTaskId, reportingTask.getReportingTask().getClass(), localState, clusterState);
    }

    @Override
    public CountersDTO getCounters() {
        final List<Counter> counters = controllerFacade.getCounters();
        final Set<CounterDTO> counterDTOs = new LinkedHashSet<>(counters.size());
        for (final Counter counter : counters) {
            counterDTOs.add(dtoFactory.createCounterDto(counter));
        }

        final CountersSnapshotDTO snapshotDto = dtoFactory.createCountersDto(counterDTOs);
        final CountersDTO countersDto = new CountersDTO();
        countersDto.setAggregateSnapshot(snapshotDto);

        return countersDto;
    }

    private ConnectionEntity createConnectionEntity(final Connection connection) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(connection.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connection.getIdentifier()));
        return entityFactory.createConnectionEntity(dtoFactory.createConnectionDto(connection), revision, permissions, status);
    }

    @Override
    public Set<ConnectionEntity> getConnections(final String groupId) {
        final Set<Connection> connections = connectionDAO.getConnections(groupId);
        return connections.stream()
            .map(connection -> createConnectionEntity(connection))
            .collect(Collectors.toSet());
    }

    @Override
    public ConnectionEntity getConnection(final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        return createConnectionEntity(connection);
    }

    @Override
    public DropRequestDTO getFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.getFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO getFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.getFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public FlowFileDTO getFlowFile(final String connectionId, final String flowFileUuid) {
        return dtoFactory.createFlowFileDTO(connectionDAO.getFlowFile(connectionId, flowFileUuid));
    }

    @Override
    public ConnectionStatusEntity getConnectionStatus(final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionStatusDTO dto = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionId));
        return entityFactory.createConnectionStatusEntity(dto, permissions);
    }

    @Override
    public StatusHistoryEntity getConnectionStatusHistory(final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final StatusHistoryDTO dto = controllerFacade.getConnectionStatusHistory(connectionId);
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    private ProcessorEntity createProcessorEntity(final ProcessorNode processor) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(processor.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processor.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processor.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessorEntity(dtoFactory.createProcessorDto(processor), revision, permissions, status, bulletinEntities);
    }

    @Override
    public Set<ProcessorEntity> getProcessors(final String groupId, final boolean includeDescendants) {
        final Set<ProcessorNode> processors = processorDAO.getProcessors(groupId, includeDescendants);
        return processors.stream()
            .map(processor -> createProcessorEntity(processor))
            .collect(Collectors.toSet());
    }

    @Override
    public TemplateDTO exportTemplate(final String id) {
        final Template template = templateDAO.getTemplate(id);
        final TemplateDTO templateDetails = template.getDetails();

        final TemplateDTO templateDTO = dtoFactory.createTemplateDTO(template);
        templateDTO.setSnippet(dtoFactory.copySnippetContents(templateDetails.getSnippet()));
        return templateDTO;
    }

    @Override
    public TemplateDTO getTemplate(final String id) {
        return dtoFactory.createTemplateDTO(templateDAO.getTemplate(id));
    }

    @Override
    public Set<TemplateEntity> getTemplates() {
        return templateDAO.getTemplates().stream()
                .map(template -> {
                    final TemplateDTO dto = dtoFactory.createTemplateDTO(template);
                    final PermissionsDTO permissions = dtoFactory.createPermissionsDto(template);

                    final TemplateEntity entity = new TemplateEntity();
                    entity.setId(dto.getId());
                    entity.setPermissions(permissions);
                    entity.setTemplate(dto);
                    return entity;
                }).collect(Collectors.toSet());
    }

    @Override
    public Set<DocumentedTypeDTO> getWorkQueuePrioritizerTypes() {
        return controllerFacade.getFlowFileComparatorTypes();
    }

    @Override
    public Set<DocumentedTypeDTO> getProcessorTypes(final String bundleGroup, final String bundleArtifact, final String type) {
        return controllerFacade.getFlowFileProcessorTypes(bundleGroup, bundleArtifact, type);
    }

    @Override
    public Set<DocumentedTypeDTO> getControllerServiceTypes(final String serviceType, final String serviceBundleGroup, final String serviceBundleArtifact, final String serviceBundleVersion,
                                                            final String bundleGroup, final String bundleArtifact, final String type) {
        return controllerFacade.getControllerServiceTypes(serviceType, serviceBundleGroup, serviceBundleArtifact, serviceBundleVersion, bundleGroup, bundleArtifact, type);
    }

    @Override
    public Set<DocumentedTypeDTO> getReportingTaskTypes(final String bundleGroup, final String bundleArtifact, final String type) {
        return controllerFacade.getReportingTaskTypes(bundleGroup, bundleArtifact, type);
    }

    @Override
    public ProcessorEntity getProcessor(final String id) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        return createProcessorEntity(processor);
    }

    @Override
    public PropertyDescriptorDTO getProcessorPropertyDescriptor(final String id, final String property) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        PropertyDescriptor descriptor = processor.getPropertyDescriptor(property);

        // return an invalid descriptor if the processor doesn't support this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor, processor.getProcessGroup().getIdentifier());
    }

    @Override
    public ProcessorStatusEntity getProcessorStatus(final String id) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final ProcessorStatusDTO dto = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(id));
        return entityFactory.createProcessorStatusEntity(dto, permissions);
    }

    @Override
    public StatusHistoryEntity getProcessorStatusHistory(final String id) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final StatusHistoryDTO dto = controllerFacade.getProcessorStatusHistory(id);
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    private boolean authorizeBulletin(final Bulletin bulletin) {
        final String sourceId = bulletin.getSourceId();
        final ComponentType type = bulletin.getSourceType();

        final Authorizable authorizable;
        try {
            switch (type) {
                case PROCESSOR:
                    authorizable = authorizableLookup.getProcessor(sourceId).getAuthorizable();
                    break;
                case REPORTING_TASK:
                    authorizable = authorizableLookup.getReportingTask(sourceId).getAuthorizable();
                    break;
                case CONTROLLER_SERVICE:
                    authorizable = authorizableLookup.getControllerService(sourceId).getAuthorizable();
                    break;
                case FLOW_CONTROLLER:
                    authorizable = controllerFacade;
                    break;
                case INPUT_PORT:
                    authorizable = authorizableLookup.getInputPort(sourceId);
                    break;
                case OUTPUT_PORT:
                    authorizable = authorizableLookup.getOutputPort(sourceId);
                    break;
                case REMOTE_PROCESS_GROUP:
                    authorizable = authorizableLookup.getRemoteProcessGroup(sourceId);
                    break;
                default:
                    throw new WebApplicationException(Response.serverError().entity("An unexpected type of component is the source of this bulletin.").build());
            }
        } catch (final ResourceNotFoundException e) {
            // if the underlying component is gone, disallow
            return false;
        }

        // perform the authorization
        final AuthorizationResult result = authorizable.checkAuthorization(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        return Result.Approved.equals(result.getResult());
    }

    @Override
    public BulletinBoardDTO getBulletinBoard(final BulletinQueryDTO query) {
        // build the query
        final BulletinQuery.Builder queryBuilder = new BulletinQuery.Builder()
                .groupIdMatches(query.getGroupId())
                .sourceIdMatches(query.getSourceId())
                .nameMatches(query.getName())
                .messageMatches(query.getMessage())
                .after(query.getAfter())
                .limit(query.getLimit());

        // perform the query
        final List<Bulletin> results = bulletinRepository.findBulletins(queryBuilder.build());

        // perform the query and generate the results - iterating in reverse order since we are
        // getting the most recent results by ordering by timestamp desc above. this gets the
        // exact results we want but in reverse order
        final List<BulletinEntity> bulletinEntities = new ArrayList<>();
        for (final ListIterator<Bulletin> bulletinIter = results.listIterator(results.size()); bulletinIter.hasPrevious(); ) {
            final Bulletin bulletin = bulletinIter.previous();
            bulletinEntities.add(entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), authorizeBulletin(bulletin)));
        }

        // create the bulletin board
        final BulletinBoardDTO bulletinBoard = new BulletinBoardDTO();
        bulletinBoard.setBulletins(bulletinEntities);
        bulletinBoard.setGenerated(new Date());
        return bulletinBoard;
    }

    @Override
    public SystemDiagnosticsDTO getSystemDiagnostics() {
        final SystemDiagnostics sysDiagnostics = controllerFacade.getSystemDiagnostics();
        return dtoFactory.createSystemDiagnosticsDto(sysDiagnostics);
    }

    @Override
    public List<ResourceDTO> getResources() {
        final List<Resource> resources = controllerFacade.getResources();
        final List<ResourceDTO> resourceDtos = new ArrayList<>(resources.size());
        for (final Resource resource : resources) {
            resourceDtos.add(dtoFactory.createResourceDto(resource));
        }
        return resourceDtos;
    }

    /**
     * Ensures the specified user has permission to access the specified port. This method does
     * not utilize the DataTransferAuthorizable as that will enforce the entire chain is
     * authorized for the transfer. This method is only invoked when obtaining the site to site
     * details so the entire chain isn't necessary.
     */
    private boolean isUserAuthorized(final NiFiUser user, final RootGroupPort port) {
        final boolean isSiteToSiteSecure = Boolean.TRUE.equals(properties.isSiteToSiteSecure());

        // if site to site is not secure, allow all users
        if (!isSiteToSiteSecure) {
            return true;
        }

        final Map<String, String> userContext;
        if (user.getClientAddress() != null && !user.getClientAddress().trim().isEmpty()) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getDataTransferResource(port.getResource()))
                .identity(user.getIdentity())
                .groups(user.getGroups())
                .anonymous(user.isAnonymous())
                .accessAttempt(false)
                .action(RequestAction.WRITE)
                .userContext(userContext)
                .explanationSupplier(() -> "Unable to retrieve port details.")
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        return Result.Approved.equals(result.getResult());
    }

    @Override
    public ControllerDTO getSiteToSiteDetails() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // serialize the input ports this NiFi has access to
        final Set<PortDTO> inputPortDtos = new LinkedHashSet<>();
        final Set<RootGroupPort> inputPorts = controllerFacade.getInputPorts();
        for (final RootGroupPort inputPort : inputPorts) {
            if (isUserAuthorized(user, inputPort)) {
                final PortDTO dto = new PortDTO();
                dto.setId(inputPort.getIdentifier());
                dto.setName(inputPort.getName());
                dto.setComments(inputPort.getComments());
                dto.setState(inputPort.getScheduledState().toString());
                inputPortDtos.add(dto);
            }
        }

        // serialize the output ports this NiFi has access to
        final Set<PortDTO> outputPortDtos = new LinkedHashSet<>();
        for (final RootGroupPort outputPort : controllerFacade.getOutputPorts()) {
            if (isUserAuthorized(user, outputPort)) {
                final PortDTO dto = new PortDTO();
                dto.setId(outputPort.getIdentifier());
                dto.setName(outputPort.getName());
                dto.setComments(outputPort.getComments());
                dto.setState(outputPort.getScheduledState().toString());
                outputPortDtos.add(dto);
            }
        }

        // get the root group
        final ProcessGroup rootGroup = processGroupDAO.getProcessGroup(controllerFacade.getRootGroupId());
        final ProcessGroupCounts counts = rootGroup.getCounts();

        // create the controller dto
        final ControllerDTO controllerDTO = new ControllerDTO();
        controllerDTO.setId(controllerFacade.getRootGroupId());
        controllerDTO.setInstanceId(controllerFacade.getInstanceId());
        controllerDTO.setInputPorts(inputPortDtos);
        controllerDTO.setOutputPorts(outputPortDtos);
        controllerDTO.setInputPortCount(inputPortDtos.size());
        controllerDTO.setOutputPortCount(outputPortDtos.size());
        controllerDTO.setRunningCount(counts.getRunningCount());
        controllerDTO.setStoppedCount(counts.getStoppedCount());
        controllerDTO.setInvalidCount(counts.getInvalidCount());
        controllerDTO.setDisabledCount(counts.getDisabledCount());

        // determine the site to site configuration
        controllerDTO.setRemoteSiteListeningPort(controllerFacade.getRemoteSiteListeningPort());
        controllerDTO.setRemoteSiteHttpListeningPort(controllerFacade.getRemoteSiteListeningHttpPort());
        controllerDTO.setSiteToSiteSecure(controllerFacade.isRemoteSiteCommsSecure());

        return controllerDTO;
    }

    @Override
    public ControllerConfigurationEntity getControllerConfiguration() {
        final Revision rev = revisionManager.getRevision(FlowController.class.getSimpleName());
        final ControllerConfigurationDTO dto = dtoFactory.createControllerConfigurationDto(controllerFacade);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerFacade);
        final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
        return entityFactory.createControllerConfigurationEntity(dto, revision, permissions);
    }

    @Override
    public ControllerBulletinsEntity getControllerBulletins() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final ControllerBulletinsEntity controllerBulletinsEntity = new ControllerBulletinsEntity();

        final List<BulletinEntity> controllerBulletinEntities = new ArrayList<>();

        final Authorizable controllerAuthorizable = authorizableLookup.getController();
        final boolean authorized = controllerAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForController());
        controllerBulletinEntities.addAll(bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, authorized)).collect(Collectors.toList()));

        // get the controller service bulletins
        final BulletinQuery controllerServiceQuery = new BulletinQuery.Builder().sourceType(ComponentType.CONTROLLER_SERVICE).build();
        final List<Bulletin> allControllerServiceBulletins = bulletinRepository.findBulletins(controllerServiceQuery);
        final List<BulletinEntity> controllerServiceBulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : allControllerServiceBulletins) {
            try {
                final Authorizable controllerServiceAuthorizable = authorizableLookup.getControllerService(bulletin.getSourceId()).getAuthorizable();
                final boolean controllerServiceAuthorized = controllerServiceAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);

                final BulletinEntity controllerServiceBulletin = entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), controllerServiceAuthorized);
                controllerServiceBulletinEntities.add(controllerServiceBulletin);
                controllerBulletinEntities.add(controllerServiceBulletin);
            } catch (final ResourceNotFoundException e) {
                // controller service missing.. skip
            }
        }
        controllerBulletinsEntity.setControllerServiceBulletins(controllerServiceBulletinEntities);

        // get the reporting task bulletins
        final BulletinQuery reportingTaskQuery = new BulletinQuery.Builder().sourceType(ComponentType.REPORTING_TASK).build();
        final List<Bulletin> allReportingTaskBulletins = bulletinRepository.findBulletins(reportingTaskQuery);
        final List<BulletinEntity> reportingTaskBulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : allReportingTaskBulletins) {
            try {
                final Authorizable reportingTaskAuthorizable = authorizableLookup.getReportingTask(bulletin.getSourceId()).getAuthorizable();
                final boolean reportingTaskAuthorizableAuthorized = reportingTaskAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);

                final BulletinEntity reportingTaskBulletin = entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), reportingTaskAuthorizableAuthorized);
                reportingTaskBulletinEntities.add(reportingTaskBulletin);
                controllerBulletinEntities.add(reportingTaskBulletin);
            } catch (final ResourceNotFoundException e) {
                // reporting task missing.. skip
            }
        }
        controllerBulletinsEntity.setReportingTaskBulletins(reportingTaskBulletinEntities);

        controllerBulletinsEntity.setBulletins(pruneAndSortBulletins(controllerBulletinEntities, BulletinRepository.MAX_BULLETINS_FOR_CONTROLLER));
        return controllerBulletinsEntity;
    }

    @Override
    public FlowConfigurationEntity getFlowConfiguration() {
        final FlowConfigurationDTO dto = dtoFactory.createFlowConfigurationDto(properties.getAutoRefreshInterval());
        final FlowConfigurationEntity entity = new FlowConfigurationEntity();
        entity.setFlowConfiguration(dto);
        return entity;
    }

    @Override
    public AccessPolicyEntity getAccessPolicy(final String accessPolicyId) {
        final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(accessPolicyId);
        return createAccessPolicyEntity(accessPolicy);
    }

    @Override
    public AccessPolicyEntity getAccessPolicy(final RequestAction requestAction, final String resource) {
        Authorizable authorizable;
        try {
            authorizable = authorizableLookup.getAuthorizableFromResource(resource);
        } catch (final ResourceNotFoundException e) {
            // unable to find the underlying authorizable... user authorized based on top level /policies... create
            // an anonymous authorizable to attempt to locate an existing policy for this resource
            authorizable = new Authorizable() {
                @Override
                public Authorizable getParentAuthorizable() {
                    return null;
                }

                @Override
                public Resource getResource() {
                    return new Resource() {
                        @Override
                        public String getIdentifier() {
                            return resource;
                        }

                        @Override
                        public String getName() {
                            return resource;
                        }

                        @Override
                        public String getSafeDescription() {
                            return "Policy " + resource;
                        }
                    };
                }
            };
        }

        final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(requestAction, authorizable);
        return createAccessPolicyEntity(accessPolicy);
    }

    private AccessPolicyEntity createAccessPolicyEntity(final AccessPolicy accessPolicy) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(accessPolicy.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getAccessPolicyById(accessPolicy.getIdentifier()));
        final ComponentReferenceEntity componentReference = createComponentReferenceEntity(accessPolicy.getResource());
        return entityFactory.createAccessPolicyEntity(
                dtoFactory.createAccessPolicyDto(accessPolicy,
                        accessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet()),
                        accessPolicy.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet()), componentReference),
                revision, permissions);
    }

    @Override
    public UserEntity getUser(final String userId) {
        final User user = userDAO.getUser(userId);
        return createUserEntity(user);
    }

    @Override
    public Set<UserEntity> getUsers() {
        final Set<User> users = userDAO.getUsers();
        return users.stream()
            .map(user -> createUserEntity(user))
            .collect(Collectors.toSet());
    }

    private UserEntity createUserEntity(final User user) {
        final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(user.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        final Set<TenantEntity> userGroups = userGroupDAO.getUserGroupsForUser(user.getIdentifier()).stream()
                .map(g -> g.getIdentifier()).map(mapUserGroupIdToTenantEntity()).collect(Collectors.toSet());
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUser(user.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
        return entityFactory.createUserEntity(dtoFactory.createUserDto(user, userGroups, policyEntities), userRevision, permissions);
    }

    private UserGroupEntity createUserGroupEntity(final Group userGroup) {
        final RevisionDTO userGroupRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userGroup.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        final Set<TenantEntity> users = userGroup.getUsers().stream().map(mapUserIdToTenantEntity()).collect(Collectors.toSet());
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUserGroup(userGroup.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
        return entityFactory.createUserGroupEntity(dtoFactory.createUserGroupDto(userGroup, users, policyEntities), userGroupRevision, permissions);
    }

    @Override
    public UserGroupEntity getUserGroup(final String userGroupId) {
        final Group userGroup = userGroupDAO.getUserGroup(userGroupId);
        return createUserGroupEntity(userGroup);
    }

    @Override
    public Set<UserGroupEntity> getUserGroups() {
        final Set<Group> userGroups = userGroupDAO.getUserGroups();
        return userGroups.stream()
            .map(userGroup -> createUserGroupEntity(userGroup))
            .collect(Collectors.toSet());
    }

    private LabelEntity createLabelEntity(final Label label) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(label.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(label);
        return entityFactory.createLabelEntity(dtoFactory.createLabelDto(label), revision, permissions);
    }

    @Override
    public Set<LabelEntity> getLabels(final String groupId) {
        final Set<Label> labels = labelDAO.getLabels(groupId);
        return labels.stream()
            .map(label -> createLabelEntity(label))
            .collect(Collectors.toSet());
    }

    @Override
    public LabelEntity getLabel(final String labelId) {
        final Label label = labelDAO.getLabel(labelId);
        return createLabelEntity(label);
    }

    private FunnelEntity createFunnelEntity(final Funnel funnel) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(funnel.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(funnel);
        return entityFactory.createFunnelEntity(dtoFactory.createFunnelDto(funnel), revision, permissions);
    }

    @Override
    public Set<FunnelEntity> getFunnels(final String groupId) {
        final Set<Funnel> funnels = funnelDAO.getFunnels(groupId);
        return funnels.stream()
            .map(funnel -> createFunnelEntity(funnel))
            .collect(Collectors.toSet());
    }

    @Override
    public FunnelEntity getFunnel(final String funnelId) {
        final Funnel funnel = funnelDAO.getFunnel(funnelId);
        return createFunnelEntity(funnel);
    }

    private PortEntity createInputPortEntity(final Port port) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(port.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, permissions, status, bulletinEntities);
    }

    private PortEntity createOutputPortEntity(final Port port) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(port.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, permissions, status, bulletinEntities);
    }

    @Override
    public Set<PortEntity> getInputPorts(final String groupId) {
        final Set<Port> inputPorts = inputPortDAO.getPorts(groupId);
        return inputPorts.stream()
            .map(port -> createInputPortEntity(port))
            .collect(Collectors.toSet());
    }

    @Override
    public Set<PortEntity> getOutputPorts(final String groupId) {
        final Set<Port> ports = outputPortDAO.getPorts(groupId);
        return ports.stream()
            .map(port -> createOutputPortEntity(port))
            .collect(Collectors.toSet());
    }

    private ProcessGroupEntity createProcessGroupEntity(final ProcessGroup group) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(group.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(group);
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(group.getIdentifier()));
        final List<BulletinEntity> bulletins = getProcessGroupBulletins(group);
        return entityFactory.createProcessGroupEntity(dtoFactory.createProcessGroupDto(group), revision, permissions, status, bulletins);
    }

    private List<BulletinEntity> getProcessGroupBulletins(final ProcessGroup group) {
        final List<Bulletin> bulletins = new ArrayList<>(bulletinRepository.findBulletinsForGroupBySource(group.getIdentifier()));

        for (final ProcessGroup descendantGroup : group.findAllProcessGroups()) {
            bulletins.addAll(bulletinRepository.findBulletinsForGroupBySource(descendantGroup.getIdentifier()));
        }

        List<BulletinEntity> bulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : bulletins) {
            bulletinEntities.add(entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), authorizeBulletin(bulletin)));
        }

        return pruneAndSortBulletins(bulletinEntities, BulletinRepository.MAX_BULLETINS_PER_COMPONENT);
    }

    private List<BulletinEntity> pruneAndSortBulletins(final List<BulletinEntity> bulletinEntities, final int maxBulletins) {
        // sort the bulletins
        Collections.sort(bulletinEntities, new Comparator<BulletinEntity>() {
            @Override
            public int compare(BulletinEntity o1, BulletinEntity o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }

                return -Long.compare(o1.getId(), o2.getId());
            }
        });

        // prune the response to only include the max number of bulletins
        if (bulletinEntities.size() > maxBulletins) {
            return bulletinEntities.subList(0, maxBulletins);
        } else {
            return bulletinEntities;
        }
    }

    @Override
    public Set<ProcessGroupEntity> getProcessGroups(final String parentGroupId) {
        final Set<ProcessGroup> groups = processGroupDAO.getProcessGroups(parentGroupId);
        return groups.stream()
            .map(group -> createProcessGroupEntity(group))
            .collect(Collectors.toSet());
    }

    private RemoteProcessGroupEntity createRemoteGroupEntity(final RemoteProcessGroup rpg) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(rpg.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(rpg);
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(rpg.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(rpg.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createRemoteProcessGroupEntity(dtoFactory.createRemoteProcessGroupDto(rpg), revision, permissions, status, bulletinEntities);
    }

    @Override
    public Set<RemoteProcessGroupEntity> getRemoteProcessGroups(final String groupId) {
        final Set<RemoteProcessGroup> rpgs = remoteProcessGroupDAO.getRemoteProcessGroups(groupId);
        return rpgs.stream()
            .map(rpg -> createRemoteGroupEntity(rpg))
            .collect(Collectors.toSet());
    }

    @Override
    public PortEntity getInputPort(final String inputPortId) {
        final Port port = inputPortDAO.getPort(inputPortId);
        return createInputPortEntity(port);
    }

    @Override
    public PortStatusEntity getInputPortStatus(final String inputPortId) {
        final Port inputPort = inputPortDAO.getPort(inputPortId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(inputPort);
        final PortStatusDTO dto = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortId));
        return entityFactory.createPortStatusEntity(dto, permissions);
    }

    @Override
    public PortEntity getOutputPort(final String outputPortId) {
        final Port port = outputPortDAO.getPort(outputPortId);
        return createOutputPortEntity(port);
    }

    @Override
    public PortStatusEntity getOutputPortStatus(final String outputPortId) {
        final Port outputPort = outputPortDAO.getPort(outputPortId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(outputPort);
        final PortStatusDTO dto = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortId));
        return entityFactory.createPortStatusEntity(dto, permissions);
    }

    @Override
    public RemoteProcessGroupEntity getRemoteProcessGroup(final String remoteProcessGroupId) {
        final RemoteProcessGroup rpg = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        return createRemoteGroupEntity(rpg);
    }

    @Override
    public RemoteProcessGroupStatusEntity getRemoteProcessGroupStatus(final String id) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(id);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroup);
        final RemoteProcessGroupStatusDTO dto = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(id));
        return entityFactory.createRemoteProcessGroupStatusEntity(dto, permissions);
    }

    @Override
    public StatusHistoryEntity getRemoteProcessGroupStatusHistory(final String id) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(id);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroup);
        final StatusHistoryDTO dto = controllerFacade.getRemoteProcessGroupStatusHistory(id);
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    @Override
    public CurrentUserEntity getCurrentUser() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final CurrentUserEntity entity = new CurrentUserEntity();
        entity.setIdentity(user.getIdentity());
        entity.setAnonymous(user.isAnonymous());
        entity.setProvenancePermissions(dtoFactory.createPermissionsDto(authorizableLookup.getProvenance()));
        entity.setCountersPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getCounters()));
        entity.setTenantsPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getTenant()));
        entity.setControllerPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getController()));
        entity.setPoliciesPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getPolicies()));
        entity.setSystemPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getSystem()));
        entity.setRestrictedComponentsPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getRestrictedComponents()));
        return entity;
    }

    @Override
    public ProcessGroupFlowEntity getProcessGroupFlow(final String groupId) {
        // get all identifiers for every child component
        final Set<String> identifiers = new HashSet<>();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        processGroup.getProcessors().stream()
            .map(proc -> proc.getIdentifier())
            .forEach(id -> identifiers.add(id));
        processGroup.getConnections().stream()
            .map(conn -> conn.getIdentifier())
            .forEach(id -> identifiers.add(id));
        processGroup.getInputPorts().stream()
            .map(port -> port.getIdentifier())
            .forEach(id -> identifiers.add(id));
        processGroup.getOutputPorts().stream()
            .map(port -> port.getIdentifier())
            .forEach(id -> identifiers.add(id));
        processGroup.getProcessGroups().stream()
            .map(group -> group.getIdentifier())
            .forEach(id -> identifiers.add(id));
        processGroup.getRemoteProcessGroups().stream()
            .map(remoteGroup -> remoteGroup.getIdentifier())
            .forEach(id -> identifiers.add(id));
        processGroup.getRemoteProcessGroups().stream()
            .flatMap(remoteGroup -> remoteGroup.getInputPorts().stream())
            .map(remoteInputPort -> remoteInputPort.getIdentifier())
            .forEach(id -> identifiers.add(id));
        processGroup.getRemoteProcessGroups().stream()
            .flatMap(remoteGroup -> remoteGroup.getOutputPorts().stream())
            .map(remoteOutputPort -> remoteOutputPort.getIdentifier())
            .forEach(id -> identifiers.add(id));

        // read lock on every component being accessed in the dto conversion
        final ProcessGroupStatus groupStatus = controllerFacade.getProcessGroupStatus(groupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        return entityFactory.createProcessGroupFlowEntity(dtoFactory.createProcessGroupFlowDto(processGroup, groupStatus, revisionManager, this::getProcessGroupBulletins), permissions);
    }

    @Override
    public ProcessGroupEntity getProcessGroup(final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        return createProcessGroupEntity(processGroup);
    }

    private ControllerServiceEntity createControllerServiceEntity(final ControllerServiceNode serviceNode, final Set<String> serviceIds, final NiFiUser user) {
        final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(serviceNode);

        final ControllerServiceReference ref = serviceNode.getReferences();
        final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = createControllerServiceReferencingComponentsEntity(ref, serviceIds);
        dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());

        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(serviceNode.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(serviceNode, user);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(serviceNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createControllerServiceEntity(dto, revision, permissions, bulletinEntities);
    }

    @Override
    public VariableRegistryEntity getVariableRegistry(final String groupId, final boolean includeAncestorGroups) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        if (processGroup == null) {
            throw new ResourceNotFoundException("Could not find group with ID " + groupId);
        }

        return createVariableRegistryEntity(processGroup, includeAncestorGroups);
    }

    private VariableRegistryEntity createVariableRegistryEntity(final ProcessGroup processGroup, final boolean includeAncestorGroups) {
        final VariableRegistryDTO registryDto = dtoFactory.createVariableRegistryDto(processGroup, revisionManager);
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(processGroup.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);

        if (includeAncestorGroups) {
            ProcessGroup parent = processGroup.getParent();
            while (parent != null) {
                final PermissionsDTO parentPerms = dtoFactory.createPermissionsDto(parent);
                if (Boolean.TRUE.equals(parentPerms.getCanRead())) {
                    final VariableRegistryDTO parentRegistryDto = dtoFactory.createVariableRegistryDto(parent, revisionManager);
                    final Set<VariableEntity> parentVariables = parentRegistryDto.getVariables();
                    registryDto.getVariables().addAll(parentVariables);
                }

                parent = parent.getParent();
            }
        }

        return entityFactory.createVariableRegistryEntity(registryDto, revision, permissions);
    }

    @Override
    public VariableRegistryEntity populateAffectedComponents(final VariableRegistryDTO variableRegistryDto) {
        final String groupId = variableRegistryDto.getProcessGroupId();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        if (processGroup == null) {
            throw new ResourceNotFoundException("Could not find group with ID " + groupId);
        }

        final VariableRegistryDTO registryDto = dtoFactory.populateAffectedComponents(variableRegistryDto, processGroup, revisionManager);
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(processGroup.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        return entityFactory.createVariableRegistryEntity(registryDto, revision, permissions);
    }

    @Override
    public Set<ControllerServiceEntity> getControllerServices(final String groupId, final boolean includeAncestorGroups, final boolean includeDescendantGroups) {
        return getControllerServices(groupId, includeAncestorGroups, includeDescendantGroups, NiFiUserUtils.getNiFiUser());
    }

    @Override
    public Set<ControllerServiceEntity> getControllerServices(final String groupId, final boolean includeAncestorGroups, final boolean includeDescendantGroups, final NiFiUser user) {
        final Set<ControllerServiceNode> serviceNodes = controllerServiceDAO.getControllerServices(groupId, includeAncestorGroups, includeDescendantGroups);
        final Set<String> serviceIds = serviceNodes.stream().map(service -> service.getIdentifier()).collect(Collectors.toSet());

        return serviceNodes.stream()
            .map(serviceNode -> createControllerServiceEntity(serviceNode, serviceIds, user))
            .collect(Collectors.toSet());
    }

    @Override
    public ControllerServiceEntity getControllerService(final String controllerServiceId) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        return createControllerServiceEntity(controllerService, Sets.newHashSet(controllerServiceId), NiFiUserUtils.getNiFiUser());
    }

    @Override
    public PropertyDescriptorDTO getControllerServicePropertyDescriptor(final String id, final String property) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(id);
        PropertyDescriptor descriptor = controllerService.getControllerServiceImplementation().getPropertyDescriptor(property);

        // return an invalid descriptor if the controller service doesn't support this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        final String groupId = controllerService.getProcessGroup() == null ? null : controllerService.getProcessGroup().getIdentifier();
        return dtoFactory.createPropertyDescriptorDto(descriptor, groupId);
    }

    @Override
    public ControllerServiceReferencingComponentsEntity getControllerServiceReferencingComponents(final String controllerServiceId) {
        final ControllerServiceNode service = controllerServiceDAO.getControllerService(controllerServiceId);
        final ControllerServiceReference ref = service.getReferences();
        return createControllerServiceReferencingComponentsEntity(ref, Sets.newHashSet(controllerServiceId));
    }

    private ReportingTaskEntity createReportingTaskEntity(final ReportingTaskNode reportingTask) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(reportingTask.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reportingTask);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createReportingTaskEntity(dtoFactory.createReportingTaskDto(reportingTask), revision, permissions, bulletinEntities);
    }

    @Override
    public Set<ReportingTaskEntity> getReportingTasks() {
        final Set<ReportingTaskNode> reportingTasks = reportingTaskDAO.getReportingTasks();
        return reportingTasks.stream()
            .map(reportingTask -> createReportingTaskEntity(reportingTask))
            .collect(Collectors.toSet());
    }

    @Override
    public ReportingTaskEntity getReportingTask(final String reportingTaskId) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        return createReportingTaskEntity(reportingTask);
    }

    @Override
    public PropertyDescriptorDTO getReportingTaskPropertyDescriptor(final String id, final String property) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(id);
        PropertyDescriptor descriptor = reportingTask.getReportingTask().getPropertyDescriptor(property);

        // return an invalid descriptor if the reporting task doesn't support this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor, null);
    }

    @Override
    public StatusHistoryEntity getProcessGroupStatusHistory(final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        final StatusHistoryDTO dto = controllerFacade.getProcessGroupStatusHistory(groupId);
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    private AuthorizationResult authorizeAction(final Action action) {
        final String sourceId = action.getSourceId();
        final Component type = action.getSourceType();

        Authorizable authorizable;
        try {
            switch (type) {
                case Processor:
                    authorizable = authorizableLookup.getProcessor(sourceId).getAuthorizable();
                    break;
                case ReportingTask:
                    authorizable = authorizableLookup.getReportingTask(sourceId).getAuthorizable();
                    break;
                case ControllerService:
                    authorizable = authorizableLookup.getControllerService(sourceId).getAuthorizable();
                    break;
                case Controller:
                    authorizable = controllerFacade;
                    break;
                case InputPort:
                    authorizable = authorizableLookup.getInputPort(sourceId);
                    break;
                case OutputPort:
                    authorizable = authorizableLookup.getOutputPort(sourceId);
                    break;
                case ProcessGroup:
                    authorizable = authorizableLookup.getProcessGroup(sourceId).getAuthorizable();
                    break;
                case RemoteProcessGroup:
                    authorizable = authorizableLookup.getRemoteProcessGroup(sourceId);
                    break;
                case Funnel:
                    authorizable = authorizableLookup.getFunnel(sourceId);
                    break;
                case Connection:
                    authorizable = authorizableLookup.getConnection(sourceId).getAuthorizable();
                    break;
                case AccessPolicy:
                    authorizable = authorizableLookup.getAccessPolicyById(sourceId);
                    break;
                case User:
                case UserGroup:
                    authorizable = authorizableLookup.getTenant();
                    break;
                default:
                    throw new WebApplicationException(Response.serverError().entity("An unexpected type of component is the source of this action.").build());
            }
        } catch (final ResourceNotFoundException e) {
            // if the underlying component is gone, use the controller to see if permissions should be allowed
            authorizable = controllerFacade;
        }

        // perform the authorization
        return authorizable.checkAuthorization(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
    }

    @Override
    public HistoryDTO getActions(final HistoryQueryDTO historyQueryDto) {
        // extract the query criteria
        final HistoryQuery historyQuery = new HistoryQuery();
        historyQuery.setStartDate(historyQueryDto.getStartDate());
        historyQuery.setEndDate(historyQueryDto.getEndDate());
        historyQuery.setSourceId(historyQueryDto.getSourceId());
        historyQuery.setUserIdentity(historyQueryDto.getUserIdentity());
        historyQuery.setOffset(historyQueryDto.getOffset());
        historyQuery.setCount(historyQueryDto.getCount());
        historyQuery.setSortColumn(historyQueryDto.getSortColumn());
        historyQuery.setSortOrder(historyQueryDto.getSortOrder());

        // perform the query
        final History history = auditService.getActions(historyQuery);

        // only retain authorized actions
        final HistoryDTO historyDto = dtoFactory.createHistoryDto(history);
        if (history.getActions() != null) {
            final List<ActionEntity> actionEntities = new ArrayList<>();
            for (final Action action : history.getActions()) {
                final AuthorizationResult result = authorizeAction(action);
                actionEntities.add(entityFactory.createActionEntity(dtoFactory.createActionDto(action), Result.Approved.equals(result.getResult())));
            }
            historyDto.setActions(actionEntities);
        }

        // create the response
        return historyDto;
    }

    @Override
    public ActionEntity getAction(final Integer actionId) {
        // get the action
        final Action action = auditService.getAction(actionId);

        // ensure the action was found
        if (action == null) {
            throw new ResourceNotFoundException(String.format("Unable to find action with id '%s'.", actionId));
        }

        final AuthorizationResult result = authorizeAction(action);
        final boolean authorized = Result.Approved.equals(result.getResult());
        if (!authorized) {
            throw new AccessDeniedException(result.getExplanation());
        }

        // return the action
        return entityFactory.createActionEntity(dtoFactory.createActionDto(action), authorized);
    }

    @Override
    public ComponentHistoryDTO getComponentHistory(final String componentId) {
        final Map<String, PropertyHistoryDTO> propertyHistoryDtos = new LinkedHashMap<>();
        final Map<String, List<PreviousValue>> propertyHistory = auditService.getPreviousValues(componentId);

        for (final Map.Entry<String, List<PreviousValue>> entry : propertyHistory.entrySet()) {
            final List<PreviousValueDTO> previousValueDtos = new ArrayList<>();

            for (final PreviousValue previousValue : entry.getValue()) {
                final PreviousValueDTO dto = new PreviousValueDTO();
                dto.setPreviousValue(previousValue.getPreviousValue());
                dto.setTimestamp(previousValue.getTimestamp());
                dto.setUserIdentity(previousValue.getUserIdentity());
                previousValueDtos.add(dto);
            }

            if (!previousValueDtos.isEmpty()) {
                final PropertyHistoryDTO propertyHistoryDto = new PropertyHistoryDTO();
                propertyHistoryDto.setPreviousValues(previousValueDtos);
                propertyHistoryDtos.put(entry.getKey(), propertyHistoryDto);
            }
        }

        final ComponentHistoryDTO history = new ComponentHistoryDTO();
        history.setComponentId(componentId);
        history.setPropertyHistory(propertyHistoryDtos);

        return history;
    }

    @Override
    public boolean isClustered() {
        return controllerFacade.isClustered();
    }

    @Override
    public String getNodeId() {
        final NodeIdentifier nodeId = controllerFacade.getNodeId();
        if (nodeId != null) {
            return nodeId.getId();
        } else {
            return null;
        }
    }

    @Override
    public ClusterDTO getCluster() {
        // create cluster summary dto
        final ClusterDTO clusterDto = new ClusterDTO();

        // set current time
        clusterDto.setGenerated(new Date());

        // create node dtos
        final List<NodeDTO> nodeDtos = clusterCoordinator.getNodeIdentifiers().stream()
            .map(nodeId -> getNode(nodeId))
            .collect(Collectors.toList());
        clusterDto.setNodes(nodeDtos);

        return clusterDto;
    }

    @Override
    public NodeDTO getNode(final String nodeId) {
        final NodeIdentifier nodeIdentifier = clusterCoordinator.getNodeIdentifier(nodeId);
        return getNode(nodeIdentifier);
    }

    private NodeDTO getNode(final NodeIdentifier nodeId) {
        final NodeConnectionStatus nodeStatus = clusterCoordinator.getConnectionStatus(nodeId);
        final List<NodeEvent> events = clusterCoordinator.getNodeEvents(nodeId);
        final Set<String> roles = getRoles(nodeId);
        final NodeHeartbeat heartbeat = heartbeatMonitor.getLatestHeartbeat(nodeId);
        return dtoFactory.createNodeDTO(nodeId, nodeStatus, heartbeat, events, roles);
    }

    private Set<String> getRoles(final NodeIdentifier nodeId) {
        final Set<String> roles = new HashSet<>();
        final String nodeAddress = nodeId.getSocketAddress() + ":" + nodeId.getSocketPort();

        for (final String roleName : ClusterRoles.getAllRoles()) {
            final String leader = leaderElectionManager.getLeader(roleName);
            if (leader == null) {
                continue;
            }

            if (leader.equals(nodeAddress)) {
                roles.add(roleName);
            }
        }

        return roles;
    }

    @Override
    public void deleteNode(final String nodeId) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        final String userDn = user.getIdentity();
        final NodeIdentifier nodeIdentifier = clusterCoordinator.getNodeIdentifier(nodeId);
        if (nodeIdentifier == null) {
            throw new UnknownNodeException("Cannot remove Node with ID " + nodeId + " because it is not part of the cluster");
        }

        final NodeConnectionStatus nodeConnectionStatus = clusterCoordinator.getConnectionStatus(nodeIdentifier);
        if (!nodeConnectionStatus.getState().equals(NodeConnectionState.DISCONNECTED)) {
            throw new IllegalNodeDeletionException("Cannot remove Node with ID " + nodeId + " because it is not disconnected, current state = " + nodeConnectionStatus.getState());
        }

        clusterCoordinator.removeNode(nodeIdentifier, userDn);
        heartbeatMonitor.removeHeartbeat(nodeIdentifier);
    }

    /* reusable function declarations for converting ids to tenant entities */
    private Function<String, TenantEntity> mapUserGroupIdToTenantEntity() {
        return userGroupId -> {
            final RevisionDTO userGroupRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userGroupId));
            return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(userGroupDAO.getUserGroup(userGroupId)), userGroupRevision,
                    dtoFactory.createPermissionsDto(authorizableLookup.getTenant()));
        };
    }

    private Function<String, TenantEntity> mapUserIdToTenantEntity() {
        return userId -> {
            final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userId));
            return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(userDAO.getUser(userId)), userRevision,
                    dtoFactory.createPermissionsDto(authorizableLookup.getTenant()));
        };
    }


    /* setters */
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    public void setControllerFacade(final ControllerFacade controllerFacade) {
        this.controllerFacade = controllerFacade;
    }

    public void setRemoteProcessGroupDAO(final RemoteProcessGroupDAO remoteProcessGroupDAO) {
        this.remoteProcessGroupDAO = remoteProcessGroupDAO;
    }

    public void setLabelDAO(final LabelDAO labelDAO) {
        this.labelDAO = labelDAO;
    }

    public void setFunnelDAO(final FunnelDAO funnelDAO) {
        this.funnelDAO = funnelDAO;
    }

    public void setSnippetDAO(final SnippetDAO snippetDAO) {
        this.snippetDAO = snippetDAO;
    }

    public void setProcessorDAO(final ProcessorDAO processorDAO) {
        this.processorDAO = processorDAO;
    }

    public void setConnectionDAO(final ConnectionDAO connectionDAO) {
        this.connectionDAO = connectionDAO;
    }

    public void setAuditService(final AuditService auditService) {
        this.auditService = auditService;
    }

    public void setRevisionManager(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setEntityFactory(final EntityFactory entityFactory) {
        this.entityFactory = entityFactory;
    }

    public void setInputPortDAO(final PortDAO inputPortDAO) {
        this.inputPortDAO = inputPortDAO;
    }

    public void setOutputPortDAO(final PortDAO outputPortDAO) {
        this.outputPortDAO = outputPortDAO;
    }

    public void setProcessGroupDAO(final ProcessGroupDAO processGroupDAO) {
        this.processGroupDAO = processGroupDAO;
    }

    public void setControllerServiceDAO(final ControllerServiceDAO controllerServiceDAO) {
        this.controllerServiceDAO = controllerServiceDAO;
    }

    public void setReportingTaskDAO(final ReportingTaskDAO reportingTaskDAO) {
        this.reportingTaskDAO = reportingTaskDAO;
    }

    public void setTemplateDAO(final TemplateDAO templateDAO) {
        this.templateDAO = templateDAO;
    }

    public void setSnippetUtils(final SnippetUtils snippetUtils) {
        this.snippetUtils = snippetUtils;
    }

    public void setAuthorizableLookup(final AuthorizableLookup authorizableLookup) {
        this.authorizableLookup = authorizableLookup;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setUserDAO(final UserDAO userDAO) {
        this.userDAO = userDAO;
    }

    public void setUserGroupDAO(final UserGroupDAO userGroupDAO) {
        this.userGroupDAO = userGroupDAO;
    }

    public void setAccessPolicyDAO(final AccessPolicyDAO accessPolicyDAO) {
        this.accessPolicyDAO = accessPolicyDAO;
    }

    public void setClusterCoordinator(final ClusterCoordinator coordinator) {
        this.clusterCoordinator = coordinator;
    }

    public void setHeartbeatMonitor(final HeartbeatMonitor heartbeatMonitor) {
        this.heartbeatMonitor = heartbeatMonitor;
    }

    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }

    public void setLeaderElectionManager(final LeaderElectionManager leaderElectionManager) {
        this.leaderElectionManager = leaderElectionManager;
    }
}
