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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.FlowChangePurgeDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.KeyService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.heartbeat.NodeHeartbeat;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
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
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ComponentHistoryDTO;
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
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
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
import org.apache.nifi.web.api.dto.action.ActionDTO;
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
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.controller.ControllerFacade;
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
import org.apache.nifi.web.revision.DeleteRevisionTask;
import org.apache.nifi.web.revision.ExpiredRevisionClaimException;
import org.apache.nifi.web.revision.ReadOnlyRevisionCallback;
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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
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

    // administrative services
    private AuditService auditService;
    private KeyService keyService;

    // cluster manager
    private WebClusterManager clusterManager;

    // properties
    private NiFiProperties properties;
    private DtoFactory dtoFactory;
    private EntityFactory entityFactory;

    private Authorizer authorizer;

    private final AuthorizableLookup authorizableLookup = new AuthorizableLookup() {
        @Override
        public Authorizable getProcessor(final String id) {
            return processorDAO.getProcessor(id);
        }

        @Override
        public Authorizable getInputPort(final String id) {
            return inputPortDAO.getPort(id);
        }

        @Override
        public Authorizable getOutputPort(final String id) {
            return outputPortDAO.getPort(id);
        }

        @Override
        public Authorizable getConnection(final String id) {
            return connectionDAO.getConnection(id);
        }

        @Override
        public Authorizable getProcessGroup(final String id) {
            return processGroupDAO.getProcessGroup(id);
        }

        @Override
        public Authorizable getRemoteProcessGroup(final String id) {
            return remoteProcessGroupDAO.getRemoteProcessGroup(id);
        }

        @Override
        public Authorizable getRemoteProcessGroupInputPort(final String remoteProcessGroupId, final String id) {
            final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
            return remoteProcessGroup.getInputPort(id);
        }

        @Override
        public Authorizable getRemoteProcessGroupOutputPort(final String remoteProcessGroupId, final String id) {
            final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
            return remoteProcessGroup.getOutputPort(id);
        }

        @Override
        public Authorizable getLabel(final String id) {
            return labelDAO.getLabel(id);
        }

        @Override
        public Authorizable getFunnel(final String id) {
            return funnelDAO.getFunnel(id);
        }

        @Override
        public Authorizable getControllerService(final String id) {
            return controllerServiceDAO.getControllerService(id);
        }

        @Override
        public Authorizable getControllerServiceReferencingComponent(String controllerSeriveId, String id) {
            final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerSeriveId);
            final ControllerServiceReference referencingComponents = controllerService.getReferences();

            ConfiguredComponent reference = null;
            for (final ConfiguredComponent component : referencingComponents.getReferencingComponents()) {
                if (component.getIdentifier().equals(id)) {
                    reference = component;
                    break;
                }
            }

            if (reference == null) {
                throw new ResourceNotFoundException("Unable to find referencing component with id " + id);
            }

            return reference;
        }

        @Override
        public Authorizable getReportingTask(final String id) {
            return reportingTaskDAO.getReportingTask(id);
        }

        @Override
        public Snippet getSnippet(final String id) {
            return snippetDAO.getSnippet(id);
        }

        @Override
        public Authorizable getTemplate(final String id) {
            return templateDAO.getTemplate(id);
        }

        @Override
        public Authorizable getConnectable(String id) {
            final ProcessGroup group = processGroupDAO.getProcessGroup(controllerFacade.getRootGroupId());
            return group.findConnectable(id);
        }
    };

    // -----------------------------------------
    // Synchronization methods
    // -----------------------------------------

    public void authorizeAccess(AuthorizeAccess authorizeAccess) {
        authorizeAccess.authorize(authorizableLookup);
    }

    @Override
    public void claimRevision(Revision revision, NiFiUser user) {
        revisionManager.requestClaim(revision, user);
    }

    @Override
    public void claimRevisions(Set<Revision> revisions, NiFiUser user) {
        revisionManager.requestClaim(revisions, user);
    }

    @Override
    public void cancelRevision(Revision revision) {
        revisionManager.cancelClaim(revision);
    }

    @Override
    public void cancelRevisions(Set<Revision> revisions) {
        revisionManager.cancelClaims(revisions);
    }

    @Override
    public Set<Revision> getRevisionsFromGroup(String groupId, Function<ProcessGroup, Set<String>> getComponents) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        final Set<String> componentIds = revisionManager.get(group.getIdentifier(), rev -> getComponents.apply(group));
        return componentIds.stream().map(id -> revisionManager.getRevision(id)).collect(Collectors.toSet());
    }

    @Override
    public Set<Revision> getRevisionsFromSnippet(String snippetId) {
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
    public void verifyListQueue(String connectionId) {
        connectionDAO.verifyList(connectionId);
    }

    @Override
    public void verifyCreateConnection(String groupId, ConnectionDTO connectionDTO) {
        connectionDAO.verifyCreate(groupId, connectionDTO);
    }

    @Override
    public void verifyUpdateConnection(ConnectionDTO connectionDTO) {
        try {
            // if connection does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (connectionDAO.hasConnection(connectionDTO.getId())) {
                connectionDAO.verifyUpdate(connectionDTO);
            } else {
                connectionDAO.verifyCreate(connectionDTO.getParentGroupId(), connectionDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(connectionDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteConnection(String connectionId) {
        connectionDAO.verifyDelete(connectionId);
    }

    @Override
    public void verifyDeleteFunnel(String funnelId) {
        funnelDAO.verifyDelete(funnelId);
    }

    @Override
    public void verifyUpdateInputPort(PortDTO inputPortDTO) {
        try {
            // if connection does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (inputPortDAO.hasPort(inputPortDTO.getId())) {
                inputPortDAO.verifyUpdate(inputPortDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(inputPortDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteInputPort(String inputPortId) {
        inputPortDAO.verifyDelete(inputPortId);
    }

    @Override
    public void verifyUpdateOutputPort(PortDTO outputPortDTO) {
        try {
            // if connection does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (outputPortDAO.hasPort(outputPortDTO.getId())) {
                outputPortDAO.verifyUpdate(outputPortDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(outputPortDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteOutputPort(String outputPortId) {
        outputPortDAO.verifyDelete(outputPortId);
    }

    @Override
    public void verifyUpdateProcessor(ProcessorDTO processorDTO) {
        try {
            // if group does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (processorDAO.hasProcessor(processorDTO.getId())) {
                processorDAO.verifyUpdate(processorDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(processorDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteProcessor(String processorId) {
        processorDAO.verifyDelete(processorId);
    }

    @Override
    public void verifyScheduleComponents(String groupId, ScheduledState state, Set<String> componentIds) {
        try {
            processGroupDAO.verifyScheduleComponents(groupId, state, componentIds);
        } catch (final Exception e) {
            componentIds.forEach(id -> revisionManager.cancelClaim(id));
            throw e;
        }
    }

    @Override
    public void verifyDeleteProcessGroup(String groupId) {
        processGroupDAO.verifyDelete(groupId);
    }

    @Override
    public void verifyUpdateRemoteProcessGroup(RemoteProcessGroupDTO remoteProcessGroupDTO) {
        try {
            // if remote group does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupDTO.getId())) {
                remoteProcessGroupDAO.verifyUpdate(remoteProcessGroupDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(remoteProcessGroupDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyUpdateRemoteProcessGroupInputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        try {
            remoteProcessGroupDAO.verifyUpdateInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
        } catch (final Exception e) {
            revisionManager.cancelClaim(remoteProcessGroupId);
            throw e;
        }
    }

    @Override
    public void verifyUpdateRemoteProcessGroupOutputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        try {
            remoteProcessGroupDAO.verifyUpdateOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
        } catch (final Exception e) {
            revisionManager.cancelClaim(remoteProcessGroupId);
            throw e;
        }
    }

    @Override
    public void verifyDeleteRemoteProcessGroup(String remoteProcessGroupId) {
        remoteProcessGroupDAO.verifyDelete(remoteProcessGroupId);
    }

    @Override
    public void verifyUpdateControllerService(ControllerServiceDTO controllerServiceDTO) {
        try {
            // if service does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (controllerServiceDAO.hasControllerService(controllerServiceDTO.getId())) {
                controllerServiceDAO.verifyUpdate(controllerServiceDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(controllerServiceDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyUpdateControllerServiceReferencingComponents(String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState) {
        try {
            controllerServiceDAO.verifyUpdateReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
        } catch (final Exception e) {
            revisionManager.cancelClaim(controllerServiceId);
            throw e;
        }
    }

    @Override
    public void verifyDeleteControllerService(String controllerServiceId) {
        controllerServiceDAO.verifyDelete(controllerServiceId);
    }

    @Override
    public void verifyUpdateReportingTask(ReportingTaskDTO reportingTaskDTO) {
        try {
            // if tasks does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (reportingTaskDAO.hasReportingTask(reportingTaskDTO.getId())) {
                reportingTaskDAO.verifyUpdate(reportingTaskDTO);
            }
        } catch (final Exception e) {
            revisionManager.cancelClaim(reportingTaskDTO.getId());
            throw e;
        }
    }

    @Override
    public void verifyDeleteReportingTask(String reportingTaskId) {
        reportingTaskDAO.verifyDelete(reportingTaskId);
    }

    // -----------------------------------------
    // Write Operations
    // -----------------------------------------
    @Override
    public UpdateResult<ConnectionEntity> updateConnection(final Revision revision, final ConnectionDTO connectionDTO) {
        // if connection does not exist, then create new connection
        if (connectionDAO.hasConnection(connectionDTO.getId()) == false) {
            return new UpdateResult<>(createConnection(connectionDTO.getParentGroupId(), connectionDTO), true);
        }

        final Connection connectionNode = connectionDAO.getConnection(connectionDTO.getId());

        final RevisionUpdate<ConnectionDTO> snapshot = updateComponent(
                revision,
                connectionNode,
                () -> connectionDAO.updateConnection(connectionDTO),
                connection -> dtoFactory.createConnectionDto(connection));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connectionNode);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionNode.getIdentifier()));
        return new UpdateResult<>(entityFactory.createConnectionEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status), false);
    }

    @Override
    public UpdateResult<ProcessorEntity> updateProcessor(final Revision revision, final ProcessorDTO processorDTO) {
        // if processor does not exist, then create new processor
        if (processorDAO.hasProcessor(processorDTO.getId()) == false) {
            return new UpdateResult<>(createProcessor(processorDTO.getParentGroupId(), processorDTO), true);
        }

        // get the component, ensure we have access to it, and perform the update request
        final ProcessorNode processorNode = processorDAO.getProcessor(processorDTO.getId());
        final RevisionUpdate<ProcessorDTO> snapshot = updateComponent(revision,
                processorNode,
                () -> processorDAO.updateProcessor(processorDTO),
                proc -> dtoFactory.createProcessorDto(proc));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processorNode);
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processorNode.getIdentifier()));
        return new UpdateResult<>(entityFactory.createProcessorEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status), false);
    }

    @Override
    public UpdateResult<LabelEntity> updateLabel(final Revision revision, final LabelDTO labelDTO) {
        // if label does not exist, then create new label
        if (labelDAO.hasLabel(labelDTO.getId()) == false) {
            return new UpdateResult<>(createLabel(labelDTO.getParentGroupId(), labelDTO), false);
        }

        final Label labelNode = labelDAO.getLabel(labelDTO.getId());
        final RevisionUpdate<LabelDTO> snapshot = updateComponent(revision,
                labelNode,
                () -> labelDAO.updateLabel(labelDTO),
                label -> dtoFactory.createLabelDto(label));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(labelNode);
        return new UpdateResult<>(entityFactory.createLabelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy), false);
    }

    @Override
    public UpdateResult<FunnelEntity> updateFunnel(final Revision revision, final FunnelDTO funnelDTO) {
        // if label does not exist, then create new label
        if (funnelDAO.hasFunnel(funnelDTO.getId()) == false) {
            return new UpdateResult<>(createFunnel(funnelDTO.getParentGroupId(), funnelDTO), true);
        }

        final Funnel funnelNode = funnelDAO.getFunnel(funnelDTO.getId());
        final RevisionUpdate<FunnelDTO> snapshot = updateComponent(revision,
                funnelNode,
                () -> funnelDAO.updateFunnel(funnelDTO),
                funnel -> dtoFactory.createFunnelDto(funnel));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnelNode);
        return new UpdateResult<>(entityFactory.createFunnelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy), false);
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
        final String modifier = user.getUserName();
        try {
            final RevisionUpdate<D> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(revision), user, new UpdateRevisionTask<D>() {
                @Override
                public RevisionUpdate<D> update() {
                    // get the updated component
                    final C component = daoUpdate.get();

                    // save updated controller
                    controllerFacade.save();

                    final Revision updatedRevision = incrementRevision(revision);
                    final D dto = dtoCreation.apply(component);

                    final FlowModification lastModification = new FlowModification(updatedRevision, modifier);
                    return new StandardRevisionUpdate<>(dto, lastModification);
                }
            });

            return updatedComponent;
        } catch (final ExpiredRevisionClaimException erce) {
            throw new InvalidRevisionException("Failed to update component " + authorizable, erce);
        }
    }


    @Override
    public void verifyUpdateSnippet(SnippetDTO snippetDto, final Set<String> affectedComponentIds) {
        try {
            // if snippet does not exist, then the update request is likely creating it
            // so we don't verify since it will fail
            if (snippetDAO.hasSnippet(snippetDto.getId())) {
                snippetDAO.verifyUpdateSnippetComponent(snippetDto);
            }
        } catch (final Exception e) {
            affectedComponentIds.forEach(id -> revisionManager.cancelClaim(snippetDto.getId()));
            throw e;
        }
    }

    @Override
    public SnippetEntity updateSnippet(final Set<Revision> revisions, final SnippetDTO snippetDto) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revisions);

        RevisionUpdate<SnippetDTO> snapshot;
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
                    final Set<Revision> updatedRevisions = revisions.stream().map(revision -> incrementRevision(revision)).collect(Collectors.toSet());

                    final SnippetDTO dto = dtoFactory.createSnippetDto(snippet);
                    return new StandardRevisionUpdate<>(dto, null, updatedRevisions);
                }
            });
        } catch (ExpiredRevisionClaimException e) {
            throw new InvalidRevisionException("Failed to update Snippet", e);
        }

        return entityFactory.createSnippetEntity(snapshot.getComponent());
    }

    @Override
    public UpdateResult<PortEntity> updateInputPort(final Revision revision, final PortDTO inputPortDTO) {
        // if input port does not exist, then create new input port
        if (inputPortDAO.hasPort(inputPortDTO.getId()) == false) {
            return new UpdateResult<>(createInputPort(inputPortDTO.getParentGroupId(), inputPortDTO), true);
        }

        final Port inputPortNode = inputPortDAO.getPort(inputPortDTO.getId());
        final RevisionUpdate<PortDTO> snapshot = updateComponent(revision,
                inputPortNode,
                () -> inputPortDAO.updatePort(inputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(inputPortNode);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortNode.getIdentifier()));
        return new UpdateResult<>(entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status), false);
    }

    @Override
    public UpdateResult<PortEntity> updateOutputPort(final Revision revision, final PortDTO outputPortDTO) {
        // if output port does not exist, then create new output port
        if (outputPortDAO.hasPort(outputPortDTO.getId()) == false) {
            return new UpdateResult<>(createOutputPort(outputPortDTO.getParentGroupId(), outputPortDTO), true);
        }

        final Port outputPortNode = outputPortDAO.getPort(outputPortDTO.getId());
        final RevisionUpdate<PortDTO> snapshot = updateComponent(revision,
                outputPortNode,
                () -> outputPortDAO.updatePort(outputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(outputPortNode);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortNode.getIdentifier()));
        return new UpdateResult<>(entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status), false);
    }

    @Override
    public UpdateResult<RemoteProcessGroupEntity> updateRemoteProcessGroup(final Revision revision, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // if controller reference does not exist, then create new controller reference
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupDTO.getId()) == false) {
            return new UpdateResult<>(createRemoteProcessGroup(remoteProcessGroupDTO.getParentGroupId(), remoteProcessGroupDTO), true);
        }

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final RevisionUpdate<RemoteProcessGroupDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroup(remoteProcessGroupDTO),
                remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroupNode);
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(remoteProcessGroupNode.getIdentifier()));
        return new UpdateResult<>(entityFactory.createRemoteProcessGroupEntity(snapshot.getComponent(), updateRevision, accessPolicy, status), false);
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

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createRemoteProcessGroupPortEntity(snapshot.getComponent(), updatedRevision, accessPolicy);
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

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createRemoteProcessGroupPortEntity(snapshot.getComponent(), updatedRevision, accessPolicy);
    }

    @Override
    public UpdateResult<ProcessGroupEntity> updateProcessGroup(final Revision revision, final ProcessGroupDTO processGroupDTO) {
        // if process group does not exist, then create new process group
        if (processGroupDAO.hasProcessGroup(processGroupDTO.getId()) == false) {
            if (processGroupDTO.getParentGroupId() == null) {
                throw new IllegalArgumentException("Unable to create the specified process group since the parent group was not specified.");
            } else {
                return new UpdateResult<>(createProcessGroup(processGroupDTO.getParentGroupId(), processGroupDTO), true);
            }
        }

        final ProcessGroup processGroupNode = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final RevisionUpdate<ProcessGroupDTO> snapshot = updateComponent(revision,
                processGroupNode,
                () -> processGroupDAO.updateProcessGroup(processGroupDTO),
                processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroupNode.getIdentifier()));
        return new UpdateResult<>(entityFactory.createProcessGroupEntity(snapshot.getComponent(), updatedRevision, accessPolicy, status), false);
    }

    @Override
    public ScheduleComponentsEntity scheduleComponents(final String processGroupId, final ScheduledState state, final Map<String, Revision> componentRevisions) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
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
                            final Revision updatedRevision = incrementRevision(currentRevision);
                            updatedRevisions.put(revision.getComponentId(), updatedRevision);
                        }

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
            controller -> dtoFactory.createControllerConfigurationDto(controllerFacade, properties.getAutoRefreshInterval()));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerFacade);
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(updatedComponent.getLastModification());
        return entityFactory.createControllerConfigurationEntity(updatedComponent.getComponent(), updateRevision, accessPolicy);
    }

    @Override
    public NodeDTO updateNode(NodeDTO nodeDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }
        final String userDn = user.getIdentity();

        if (Node.Status.CONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterManager.requestReconnection(nodeDTO.getNodeId(), userDn);
        } else if (Node.Status.DISCONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterManager.requestDisconnection(nodeDTO.getNodeId(), userDn);
        }

        final String nodeId = nodeDTO.getNodeId();

        NodeIdentifier nodeIdentifier = null;
        for (final Node node : clusterManager.getNodes()) {
            if (node.getNodeId().getId().equals(nodeId)) {
                nodeIdentifier = node.getNodeId();
                break;
            }
        }

        final NodeHeartbeat nodeHeartbeat = nodeIdentifier == null ? null : clusterManager.getLatestHeartbeat(nodeIdentifier);
        return dtoFactory.createNodeDTO(clusterManager.getNode(nodeId), nodeHeartbeat, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId));
    }

    @Override
    public CounterDTO updateCounter(String counterId) {
        return dtoFactory.createCounterDto(controllerFacade.resetCounter(counterId));
    }

    @Override
    public void verifyCanClearProcessorState(final String processorId) {
        try {
            processorDAO.verifyClearState(processorId);
        } catch (final Exception e) {
            revisionManager.cancelClaim(processorId);
            throw e;
        }
    }

    @Override
    public void clearProcessorState(final String processorId) {
        clearComponentState(processorId, () -> processorDAO.clearState(processorId));
    }

    private void clearComponentState(final String componentId, final Runnable clearState) {
        revisionManager.get(componentId, rev -> {
            clearState.run();
            return null;
        });
    }

    @Override
    public void verifyCanClearControllerServiceState(final String controllerServiceId) {
        try {
            controllerServiceDAO.verifyClearState(controllerServiceId);
        } catch (final Exception e) {
            revisionManager.cancelClaim(controllerServiceId);
            throw e;
        }
    }

    @Override
    public void clearControllerServiceState(final String controllerServiceId) {
        clearComponentState(controllerServiceId, () -> controllerServiceDAO.clearState(controllerServiceId));
    }

    @Override
    public void verifyCanClearReportingTaskState(final String reportingTaskId) {
        try {
            reportingTaskDAO.verifyClearState(reportingTaskId);
        } catch (final Exception e) {
            revisionManager.cancelClaim(reportingTaskId);
            throw e;
        }
    }

    @Override
    public void clearReportingTaskState(final String reportingTaskId) {
        clearComponentState(reportingTaskId, () -> reportingTaskDAO.clearState(reportingTaskId));
    }

    @Override
    public ConnectionEntity deleteConnection(final Revision revision, final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ConnectionDTO snapshot = deleteComponent(
                revision,
                connection,
                () -> connectionDAO.deleteConnection(connectionId),
                dtoFactory.createConnectionDto(connection));

        return entityFactory.createConnectionEntity(snapshot, null, null, null);
    }

    @Override
    public DropRequestDTO deleteFlowFileDropRequest(String connectionId, String dropRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        connection.authorize(authorizer, RequestAction.WRITE);

        return dtoFactory.createDropRequestDTO(connectionDAO.deleteFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO deleteFlowFileListingRequest(String connectionId, String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        connection.authorize(authorizer, RequestAction.WRITE);

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
        final ProcessorDTO snapshot = deleteComponent(
                revision,
                processor,
                () -> processorDAO.deleteProcessor(processorId),
                dtoFactory.createProcessorDto(processor));

        return entityFactory.createProcessorEntity(snapshot, null, null, null);
    }

    @Override
    public LabelEntity deleteLabel(final Revision revision, final String labelId) {
        final Label label = labelDAO.getLabel(labelId);
        final LabelDTO snapshot = deleteComponent(
                revision,
                label,
                () -> labelDAO.deleteLabel(labelId),
                dtoFactory.createLabelDto(label));

        return entityFactory.createLabelEntity(snapshot, null, null);
    }

    @Override
    public FunnelEntity deleteFunnel(final Revision revision, final String funnelId) {
        final Funnel funnel = funnelDAO.getFunnel(funnelId);
        final FunnelDTO snapshot = deleteComponent(
                revision,
                funnel,
                () -> funnelDAO.deleteFunnel(funnelId),
                dtoFactory.createFunnelDto(funnel));

        return entityFactory.createFunnelEntity(snapshot, null, null);
    }

    /**
     * Deletes a component using the Optimistic Locking Manager
     *
     * @param revision     the current revision
     * @param deleteAction the action that deletes the component via the appropriate DAO object
     * @return a dto that represents the new configuration
     */
    private <D, C> D deleteComponent(final Revision revision, final Authorizable authorizable, final Runnable deleteAction, final D dto) {
        final RevisionClaim claim = new StandardRevisionClaim(revision);
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return revisionManager.deleteRevision(claim, user, new DeleteRevisionTask<D>() {
            @Override
            public D performTask() {
                logger.debug("Attempting to delete component {} with claim {}", authorizable, claim);

                deleteAction.run();

                // save the flow
                controllerFacade.save();
                logger.debug("Deletion of component {} was successful", authorizable);

                return dto;
            }
        });
    }

    @Override
    public void verifyDeleteSnippet(String snippetId, Set<String> affectedComponentIds) {
        try {
            snippetDAO.verifyDeleteSnippetComponents(snippetId);
        } catch (final Exception e) {
            affectedComponentIds.forEach(id -> revisionManager.cancelClaim(id));
            throw e;
        }
    }

    @Override
    public SnippetEntity deleteSnippet(final Set<Revision> revisions, final String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);

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

        return entityFactory.createSnippetEntity(dto);
    }

    @Override
    public PortEntity deleteInputPort(final Revision revision, final String inputPortId) {
        final Port port = inputPortDAO.getPort(inputPortId);
        final PortDTO snapshot = deleteComponent(
                revision,
                port,
                () -> inputPortDAO.deletePort(inputPortId),
                dtoFactory.createPortDto(port));

        return entityFactory.createPortEntity(snapshot, null, null, null);
    }

    @Override
    public PortEntity deleteOutputPort(final Revision revision, final String outputPortId) {
        final Port port = outputPortDAO.getPort(outputPortId);
        final PortDTO snapshot = deleteComponent(
                revision,
                port,
                () -> outputPortDAO.deletePort(outputPortId),
                dtoFactory.createPortDto(port));

        return entityFactory.createPortEntity(snapshot, null, null, null);
    }

    @Override
    public ProcessGroupEntity deleteProcessGroup(final Revision revision, final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final ProcessGroupDTO snapshot = deleteComponent(
                revision,
                processGroup,
                () -> processGroupDAO.deleteProcessGroup(groupId),
                dtoFactory.createProcessGroupDto(processGroup));

        return entityFactory.createProcessGroupEntity(snapshot, null, null, null);
    }

    @Override
    public RemoteProcessGroupEntity deleteRemoteProcessGroup(final Revision revision, final String remoteProcessGroupId) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        final RemoteProcessGroupDTO snapshot = deleteComponent(
                revision,
                remoteProcessGroup,
                () -> remoteProcessGroupDAO.deleteRemoteProcessGroup(remoteProcessGroupId),
                dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        return entityFactory.createRemoteProcessGroupEntity(snapshot, null, null, null);
    }

    @Override
    public void deleteTemplate(String id) {
        // create the template
        templateDAO.deleteTemplate(id);
    }

    @Override
    public ConnectionEntity createConnection(final String groupId, final ConnectionDTO connectionDTO) {
        final RevisionUpdate<ConnectionDTO> snapshot = createComponent(
                connectionDTO,
                () -> connectionDAO.createConnection(groupId, connectionDTO),
                connection -> dtoFactory.createConnectionDto(connection));

        final Connection connection = connectionDAO.getConnection(connectionDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connection);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionDTO.getId()));
        return entityFactory.createConnectionEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status);
    }

    @Override
    public DropRequestDTO createFlowFileDropRequest(String connectionId, String dropRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        connection.authorize(authorizer, RequestAction.WRITE);
        return dtoFactory.createDropRequestDTO(connectionDAO.createFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO createFlowFileListingRequest(String connectionId, String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        connection.authorize(authorizer, RequestAction.WRITE);

        // create the listing request
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
    public ProcessorEntity createProcessor(final String groupId, final ProcessorDTO processorDTO) {
        final RevisionUpdate<ProcessorDTO> snapshot = createComponent(
                processorDTO,
                () -> processorDAO.createProcessor(groupId, processorDTO),
                processor -> dtoFactory.createProcessorDto(processor));

        final ProcessorNode processor = processorDAO.getProcessor(processorDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processor);
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processorDTO.getId()));
        return entityFactory.createProcessorEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status);
    }

    @Override
    public LabelEntity createLabel(final String groupId, final LabelDTO labelDTO) {
        final RevisionUpdate<LabelDTO> snapshot = createComponent(
                labelDTO,
                () -> labelDAO.createLabel(groupId, labelDTO),
                label -> dtoFactory.createLabelDto(label));

        final Label label = labelDAO.getLabel(labelDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(label);
        return entityFactory.createLabelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
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
    private <D, C> RevisionUpdate<D> createComponent(final ComponentDTO componentDto, final Supplier<C> daoCreation, final Function<C, D> dtoCreation) {
        final String modifier = NiFiUserUtils.getNiFiUserName();

        // ensure id is set
        if (StringUtils.isBlank(componentDto.getId())) {
            componentDto.setId(UUID.randomUUID().toString());
        }

        final String groupId = componentDto.getParentGroupId();
        return revisionManager.get(groupId, rev -> {
            // add the component
            final C component = daoCreation.get();

            // save the flow
            controllerFacade.save();

            final D dto = dtoCreation.apply(component);
            final FlowModification lastMod = new FlowModification(new Revision(0L, rev.getClientId(), componentDto.getId()), modifier);
            return new StandardRevisionUpdate<D>(dto, lastMod);
        });
    }


    @Override
    public FunnelEntity createFunnel(final String groupId, final FunnelDTO funnelDTO) {
        final RevisionUpdate<FunnelDTO> snapshot = createComponent(
                funnelDTO,
                () -> funnelDAO.createFunnel(groupId, funnelDTO),
                funnel -> dtoFactory.createFunnelDto(funnel));

        final Funnel funnel = funnelDAO.getFunnel(funnelDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnel);
        return entityFactory.createFunnelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }

    private void validateSnippetContents(final FlowSnippetDTO flow) {
        // validate any processors
        if (flow.getProcessors() != null) {
            for (final ProcessorDTO processorDTO : flow.getProcessors()) {
                final ProcessorNode processorNode = revisionManager.get(processorDTO.getId(), rev -> processorDAO.getProcessor(processorDTO.getId()));
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
                final Port port = revisionManager.get(portDTO.getId(), rev -> inputPortDAO.getPort(portDTO.getId()));
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
                final Port port = revisionManager.get(portDTO.getId(), rev -> outputPortDAO.getPort(portDTO.getId()));
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
                final RemoteProcessGroup remoteProcessGroup = revisionManager.get(
                        remoteProcessGroupDTO.getId(), rev -> remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId()));

                if (remoteProcessGroup.getAuthorizationIssue() != null) {
                    remoteProcessGroupDTO.setAuthorizationIssues(Arrays.asList(remoteProcessGroup.getAuthorizationIssue()));
                }
            }
        }
    }

    @Override
    public FlowEntity copySnippet(final String groupId, final String snippetId, final Double originX, final Double originY, final String idGenerationSeed) {
        final FlowDTO flowDto = revisionManager.get(groupId,
                rev -> {
                    // create the new snippet
                    final FlowSnippetDTO snippet = snippetDAO.copySnippet(groupId, snippetId, originX, originY, idGenerationSeed);

                    // save the flow
                    controllerFacade.save();

                    // drop the snippet
                    snippetDAO.dropSnippet(snippetId);

                    // post process new flow snippet
                    return postProcessNewFlowSnippet(groupId, snippet);
                });

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setFlow(flowDto);
        return flowEntity;
    }

    private <T> ConfigurationSnapshot<T> createConfigSnapshot(final RevisionUpdate<T> update) {
        return new ConfigurationSnapshot<>(update.getLastModification().getRevision().getVersion(), update.getComponent());
    }

    @Override
    public SnippetEntity createSnippet(final SnippetDTO snippetDTO) {
        final String groupId = snippetDTO.getParentGroupId();
        final RevisionUpdate<SnippetDTO> snapshot = revisionManager.get(groupId, rev -> {
            // add the component
            final Snippet snippet = snippetDAO.createSnippet(snippetDTO);

            // save the flow
            controllerFacade.save();

            final SnippetDTO dto = dtoFactory.createSnippetDto(snippet);
            return new StandardRevisionUpdate<SnippetDTO>(dto, null);
        });

        return entityFactory.createSnippetEntity(snapshot.getComponent());
    }

    @Override
    public PortEntity createInputPort(final String groupId, final PortDTO inputPortDTO) {
        final RevisionUpdate<PortDTO> snapshot = createComponent(
                inputPortDTO,
                () -> inputPortDAO.createPort(groupId, inputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final Port port = inputPortDAO.getPort(inputPortDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(port.getIdentifier()));
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status);
    }

    @Override
    public PortEntity createOutputPort(final String groupId, final PortDTO outputPortDTO) {
        final RevisionUpdate<PortDTO> snapshot = createComponent(
                outputPortDTO,
                () -> outputPortDAO.createPort(groupId, outputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final Port port = outputPortDAO.getPort(outputPortDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(port.getIdentifier()));
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status);
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final String parentGroupId, final ProcessGroupDTO processGroupDTO) {
        final RevisionUpdate<ProcessGroupDTO> snapshot = createComponent(
                processGroupDTO,
                () -> processGroupDAO.createProcessGroup(parentGroupId, processGroupDTO),
                processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroup);
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroup.getIdentifier()));
        return entityFactory.createProcessGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status);
    }

    @Override
    public RemoteProcessGroupEntity createRemoteProcessGroup(final String groupId, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final RevisionUpdate<RemoteProcessGroupDTO> snapshot = createComponent(
                remoteProcessGroupDTO,
                () -> remoteProcessGroupDAO.createRemoteProcessGroup(groupId, remoteProcessGroupDTO),
                remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroup);
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(remoteProcessGroup.getIdentifier()));
        return entityFactory.createRemoteProcessGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy, status);
    }

    @Override
    public TemplateDTO createTemplate(String name, String description, String snippetId, String groupId, Optional<String> idGenerationSeed) {
        // get the specified snippet
        Snippet snippet = snippetDAO.getSnippet(snippetId);

        // create the template
        TemplateDTO templateDTO = new TemplateDTO();
        templateDTO.setName(name);
        templateDTO.setDescription(description);
        templateDTO.setTimestamp(new Date());
        templateDTO.setSnippet(snippetUtils.populateFlowSnippet(snippet, true, true));
        templateDTO.setEncodingVersion(TemplateDTO.MAX_ENCODING_VERSION);

        // set the id based on the specified seed
        final String uuid = idGenerationSeed.isPresent() ? (UUID.nameUUIDFromBytes(idGenerationSeed.get().getBytes(StandardCharsets.UTF_8))).toString() : UUID.randomUUID().toString();
        templateDTO.setId(uuid);

        // create the template
        Template template = templateDAO.createTemplate(templateDTO, groupId);

        // drop the snippet
        snippetDAO.dropSnippet(snippetId);

        return dtoFactory.createTemplateDTO(template);
    }

    @Override
    public TemplateDTO importTemplate(TemplateDTO templateDTO, String groupId, Optional<String> idGenerationSeed) {
        // ensure id is set
        final String uuid = idGenerationSeed.isPresent() ? (UUID.nameUUIDFromBytes(idGenerationSeed.get().getBytes(StandardCharsets.UTF_8))).toString() : UUID.randomUUID().toString();
        templateDTO.setId(uuid);

        // mark the timestamp
        templateDTO.setTimestamp(new Date());

        // import the template
        final Template template = templateDAO.importTemplate(templateDTO, groupId);

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

        return revisionManager.get(identifiers,
                () -> {
                    final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
                    final ProcessGroupStatus groupStatus = controllerFacade.getProcessGroupStatus(groupId);
                    return dtoFactory.createFlowDto(group, groupStatus, snippet, revisionManager);
                });
    }

    @Override
    public FlowEntity createTemplateInstance(final String groupId, final Double originX, final Double originY, final String templateId, final String idGenerationSeed) {
        final FlowDTO flowDto = revisionManager.get(groupId, rev -> {
            // instantiate the template - there is no need to make another copy of the flow snippet since the actual template
            // was copied and this dto is only used to instantiate it's components (which as already completed)
            final FlowSnippetDTO snippet = templateDAO.instantiateTemplate(groupId, originX, originY, templateId, idGenerationSeed);

            // save the flow
            controllerFacade.save();

            // post process the new flow snippet
            return postProcessNewFlowSnippet(groupId, snippet);
        });

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setFlow(flowDto);
        return flowEntity;
    }

    @Override
    public ProcessGroupEntity createArchive() {
        try {
            controllerFacade.createArchive();
        } catch (IOException e) {
            logger.error("Failed to create an archive", e);
        }

        return getProcessGroup("root");
    }

    @Override
    public ProcessorEntity setProcessorAnnotationData(final Revision revision, final String processorId, final String annotationData) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final String modifier = user.getUserName();

        final RevisionUpdate<ProcessorEntity> update = revisionManager.updateRevision(new StandardRevisionClaim(revision), user, new UpdateRevisionTask<ProcessorEntity>() {
            @Override
            public RevisionUpdate<ProcessorEntity> update() {
                // create the processor config
                final ProcessorConfigDTO config = new ProcessorConfigDTO();
                config.setAnnotationData(annotationData);

                // create the processor dto
                final ProcessorDTO processorDTO = new ProcessorDTO();
                processorDTO.setId(processorId);
                processorDTO.setConfig(config);

                // update the processor configuration
                final ProcessorNode processor = processorDAO.updateProcessor(processorDTO);

                final ProcessorDTO updatedProcDto = dtoFactory.createProcessorDto(processor);

                // save the flow
                controllerFacade.save();

                final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processor);
                final FlowModification lastMod = new FlowModification(incrementRevision(revision), modifier);
                final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processor.getIdentifier()));
                final ProcessorEntity entity = entityFactory.createProcessorEntity(updatedProcDto, dtoFactory.createRevisionDTO(lastMod), accessPolicy, status);

                return new StandardRevisionUpdate<>(entity, lastMod);
            }
        });

        return update.getComponent();
    }

    @Override
    public ControllerServiceEntity createControllerService(final String groupId, final ControllerServiceDTO controllerServiceDTO) {
        final String normalizedGroupId = groupId == null ? controllerFacade.getRootGroupId() : groupId;
        controllerServiceDTO.setParentGroupId(normalizedGroupId);

        final RevisionUpdate<ControllerServiceDTO> snapshot = createComponent(
                controllerServiceDTO,
                () -> {
                    // create the controller service
                    final ControllerServiceNode controllerService = controllerServiceDAO.createControllerService(controllerServiceDTO);

                // TODO - this logic should be part of the controllerServiceDAO
                final ProcessGroup group = processGroupDAO.getProcessGroup(normalizedGroupId);
                group.addControllerService(controllerService);
                return controllerService;
            },
            controllerService -> dtoFactory.createControllerServiceDto(controllerService));

        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerService);
        return entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy);
    }

    @Override
    public UpdateResult<ControllerServiceEntity> updateControllerService(final Revision revision, final ControllerServiceDTO controllerServiceDTO) {
        // if controller service does not exist, then create new controller service
        if (controllerServiceDAO.hasControllerService(controllerServiceDTO.getId()) == false) {
            return new UpdateResult<>(createControllerService(controllerServiceDTO.getParentGroupId(), controllerServiceDTO), true);
        }

        // get the component, ensure we have access to it, and perform the update request
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final RevisionUpdate<ControllerServiceDTO> snapshot = updateComponent(revision,
                controllerService,
                () -> controllerServiceDAO.updateControllerService(controllerServiceDTO),
                cs -> dtoFactory.createControllerServiceDto(cs));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerService);
        return new UpdateResult<>(entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy), false);
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
                        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
                        final ControllerServiceReference reference = controllerService.getReferences();
                        for (final ConfiguredComponent component : reference.getReferencingComponents()) {
                            if (component instanceof Authorizable) {
                                // ensure we can write the referencing components
                                ((Authorizable) component).authorize(authorizer, RequestAction.WRITE);
                            }
                        }

                        final Set<ConfiguredComponent> updated = controllerServiceDAO.updateControllerServiceReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
                        final ControllerServiceReference updatedReference = controllerServiceDAO.getControllerService(controllerServiceId).getReferences();

                        final Map<String, Revision> updatedRevisions = new HashMap<>();
                        for (final Revision refRevision : referenceRevisions.values()) {
                            updatedRevisions.put(refRevision.getComponentId(), refRevision);
                        }

                        for (final ConfiguredComponent component : updated) {
                            final Revision currentRevision = revisionManager.getRevision(component.getIdentifier());
                            final Revision updatedRevision = incrementRevision(currentRevision);
                            updatedRevisions.put(component.getIdentifier(), updatedRevision);
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
     * @param referencingIds Collection of identifiers
     * @param visited        ControllerServices we've already visited
     */
    private void findControllerServiceReferencingComponentIdentifiers(final ControllerServiceReference reference, final Set<String> referencingIds, final Set<ControllerServiceNode> visited) {
        for (final ConfiguredComponent component : reference.getReferencingComponents()) {
            referencingIds.add(component.getIdentifier());

            // if this is a ControllerService consider it's referencing components
            if (component instanceof ControllerServiceNode) {
                final ControllerServiceNode node = (ControllerServiceNode) component;
                if (!visited.contains(node)) {
                    findControllerServiceReferencingComponentIdentifiers(node.getReferences(), referencingIds, visited);
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
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(final ControllerServiceReference reference) {
        final Set<String> referencingIds = new HashSet<>();
        final Set<ControllerServiceNode> visited = new HashSet<>();
        visited.add(reference.getReferencedComponent());
        findControllerServiceReferencingComponentIdentifiers(reference, referencingIds, visited);

        return revisionManager.get(referencingIds, () -> {
            final Map<String, Revision> referencingRevisions = new HashMap<>();
            for (final ConfiguredComponent component : reference.getReferencingComponents()) {
                referencingRevisions.put(component.getIdentifier(), revisionManager.getRevision(component.getIdentifier()));
            }
            return createControllerServiceReferencingComponentsEntity(reference, referencingRevisions);
        });
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
        return createControllerServiceReferencingComponentsEntity(reference, revisions, new HashSet<>());
    }

    /**
     * Creates entities for compnents referencing a ControllerServcie using the specified revisions.
     *
     * @param reference ControllerServiceReference
     * @param revisions The revisions
     * @param visited   Which services we've already considered (in case of cycle)
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(
            final ControllerServiceReference reference, final Map<String, Revision> revisions, final Set<ControllerServiceNode> visited) {

        final String modifier = NiFiUserUtils.getNiFiUserName();
        final Set<ConfiguredComponent> referencingComponents = reference.getReferencingComponents();

        final Set<ControllerServiceReferencingComponentEntity> componentEntities = new HashSet<>();
        for (final ConfiguredComponent refComponent : referencingComponents) {
            AccessPolicyDTO accessPolicy = null;
            if (refComponent instanceof Authorizable) {
                accessPolicy = dtoFactory.createAccessPolicyDto((Authorizable) refComponent);
            }

            final Revision revision = revisions.get(refComponent.getIdentifier());
            final FlowModification flowMod = new FlowModification(revision, modifier);
            final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(flowMod);
            final ControllerServiceReferencingComponentDTO dto = dtoFactory.createControllerServiceReferencingComponentDTO(refComponent);

            if (refComponent instanceof ControllerServiceNode) {
                final ControllerServiceNode node = (ControllerServiceNode) refComponent;

                // indicate if we've hit a cycle
                dto.setReferenceCycle(visited.contains(node));

                // if we haven't encountered this service before include it's referencing components
                if (!dto.getReferenceCycle()) {
                    final ControllerServiceReferencingComponentsEntity references = createControllerServiceReferencingComponentsEntity(node.getReferences(), revisions, visited);
                    dto.setReferencingComponents(references.getControllerServiceReferencingComponents());
                }

                // mark node as visited
                visited.add(node);
            }

            componentEntities.add(entityFactory.createControllerServiceReferencingComponentEntity(dto, revisionDto, accessPolicy));
        }

        final ControllerServiceReferencingComponentsEntity entity = new ControllerServiceReferencingComponentsEntity();
        entity.setControllerServiceReferencingComponents(componentEntities);
        return entity;
    }

    @Override
    public ControllerServiceEntity deleteControllerService(final Revision revision, final String controllerServiceId) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        final ControllerServiceDTO snapshot = deleteComponent(
                revision,
                controllerService,
                () -> controllerServiceDAO.deleteControllerService(controllerServiceId),
                dtoFactory.createControllerServiceDto(controllerService));

        return entityFactory.createControllerServiceEntity(snapshot, null, null);
    }


    @Override
    public ReportingTaskEntity createReportingTask(final ReportingTaskDTO reportingTaskDTO) {
        final String modifier = NiFiUserUtils.getNiFiUserName();

        // ensure id is set
        if (StringUtils.isBlank(reportingTaskDTO.getId())) {
            reportingTaskDTO.setId(UUID.randomUUID().toString());
        }

        return revisionManager.get(controllerFacade.getInstanceId(), rev -> {
            // ensure access to the controller
            controllerFacade.authorize(authorizer, RequestAction.WRITE);

            // create the reporting task
            final ReportingTaskNode reportingTask = reportingTaskDAO.createReportingTask(reportingTaskDTO);

            // save the update
            controllerFacade.save();

            final ReportingTaskDTO dto = dtoFactory.createReportingTaskDto(reportingTask);
            final FlowModification lastMod = new FlowModification(new Revision(0L, rev.getClientId(), dto.getId()), modifier);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(reportingTask);
            return entityFactory.createReportingTaskEntity(dto, dtoFactory.createRevisionDTO(lastMod), accessPolicy);
        });
    }

    @Override
    public UpdateResult<ReportingTaskEntity> updateReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        // if reporting task does not exist, then create new reporting task
        if (reportingTaskDAO.hasReportingTask(reportingTaskDTO.getId()) == false) {
            return new UpdateResult<>(createReportingTask(reportingTaskDTO), true);
        }

        // get the component, ensure we have access to it, and perform the update request
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskDTO.getId());
        final RevisionUpdate<ReportingTaskDTO> snapshot = updateComponent(revision,
                reportingTask,
                () -> reportingTaskDAO.updateReportingTask(reportingTaskDTO),
                rt -> dtoFactory.createReportingTaskDto(rt));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(reportingTask);
        return new UpdateResult<>(entityFactory.createReportingTaskEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), accessPolicy), false);
    }

    @Override
    public ReportingTaskEntity deleteReportingTask(final Revision revision, final String reportingTaskId) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        final ReportingTaskDTO snapshot = deleteComponent(
                revision,
                reportingTask,
                () -> reportingTaskDAO.deleteReportingTask(reportingTaskId),
                dtoFactory.createReportingTaskDto(reportingTask));

        return entityFactory.createReportingTaskEntity(snapshot, null, null);
    }

    @Override
    public void deleteActions(Date endDate) {
        // get the user from the request
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the purge details
        FlowChangePurgeDetails details = new FlowChangePurgeDetails();
        details.setEndDate(endDate);

        // create a purge action to record that records are being removed
        FlowChangeAction purgeAction = new FlowChangeAction();
        purgeAction.setUserIdentity(user.getIdentity());
        purgeAction.setUserName(user.getUserName());
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
    public ProvenanceDTO submitProvenance(ProvenanceDTO query) {
        return controllerFacade.submitProvenance(query);
    }

    @Override
    public void deleteProvenance(String queryId) {
        controllerFacade.deleteProvenanceQuery(queryId);
    }

    @Override
    public LineageDTO submitLineage(LineageDTO lineage) {
        return controllerFacade.submitLineage(lineage);
    }

    @Override
    public void deleteLineage(String lineageId) {
        controllerFacade.deleteLineage(lineageId);
    }

    @Override
    public ProvenanceEventDTO submitReplay(Long eventId) {
        return controllerFacade.submitReplay(eventId);
    }

    // -----------------------------------------
    // Read Operations
    // -----------------------------------------

    @Override
    public SearchResultsDTO searchController(String query) {
        return controllerFacade.search(query);
    }

    @Override
    public DownloadableContent getContent(String connectionId, String flowFileUuid, String uri) {
        return connectionDAO.getContent(connectionId, flowFileUuid, uri);
    }

    @Override
    public DownloadableContent getContent(Long eventId, String uri, ContentDirection contentDirection) {
        return controllerFacade.getContent(eventId, uri, contentDirection);
    }

    @Override
    public ProvenanceDTO getProvenance(String queryId) {
        return controllerFacade.getProvenanceQuery(queryId);
    }

    @Override
    public LineageDTO getLineage(String lineageId) {
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
    public ProcessGroupStatusDTO getProcessGroupStatus(String groupId) {
        return dtoFactory.createProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(groupId));
    }

    @Override
    public ControllerStatusDTO getControllerStatus() {
        return controllerFacade.getControllerStatus();
    }

    @Override
    public ComponentStateDTO getProcessorState(String processorId) {
        return revisionManager.get(processorId, new ReadOnlyRevisionCallback<ComponentStateDTO>() {
            @Override
            public ComponentStateDTO withRevision(Revision revision) {
                final StateMap clusterState = isClustered() ? processorDAO.getState(processorId, Scope.CLUSTER) : null;
                final StateMap localState = processorDAO.getState(processorId, Scope.LOCAL);

                // processor will be non null as it was already found when getting the state
                final ProcessorNode processor = processorDAO.getProcessor(processorId);
                return dtoFactory.createComponentStateDTO(processorId, processor.getProcessor().getClass(), localState, clusterState);
            }
        });
    }

    @Override
    public ComponentStateDTO getControllerServiceState(String controllerServiceId) {
        return revisionManager.get(controllerServiceId, new ReadOnlyRevisionCallback<ComponentStateDTO>() {
            @Override
            public ComponentStateDTO withRevision(Revision revision) {
                final StateMap clusterState = isClustered() ? controllerServiceDAO.getState(controllerServiceId, Scope.CLUSTER) : null;
                final StateMap localState = controllerServiceDAO.getState(controllerServiceId, Scope.LOCAL);

                // controller service will be non null as it was already found when getting the state
                final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
                return dtoFactory.createComponentStateDTO(controllerServiceId, controllerService.getControllerServiceImplementation().getClass(), localState, clusterState);
            }
        });
    }

    @Override
    public ComponentStateDTO getReportingTaskState(String reportingTaskId) {
        return revisionManager.get(reportingTaskId, new ReadOnlyRevisionCallback<ComponentStateDTO>() {
            @Override
            public ComponentStateDTO withRevision(Revision revision) {
                final StateMap clusterState = isClustered() ? reportingTaskDAO.getState(reportingTaskId, Scope.CLUSTER) : null;
                final StateMap localState = reportingTaskDAO.getState(reportingTaskId, Scope.LOCAL);

                // reporting task will be non null as it was already found when getting the state
                final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
                return dtoFactory.createComponentStateDTO(reportingTaskId, reportingTask.getReportingTask().getClass(), localState, clusterState);
            }
        });
    }

    @Override
    public CountersDTO getCounters() {
        List<Counter> counters = controllerFacade.getCounters();
        Set<CounterDTO> counterDTOs = new LinkedHashSet<>(counters.size());
        for (Counter counter : counters) {
            counterDTOs.add(dtoFactory.createCounterDto(counter));
        }

        final CountersSnapshotDTO snapshotDto = dtoFactory.createCountersDto(counterDTOs);
        final CountersDTO countersDto = new CountersDTO();
        countersDto.setAggregateSnapshot(snapshotDto);

        return countersDto;
    }

    @Override
    public Set<ConnectionEntity> getConnections(String groupId) {
        return revisionManager.get(groupId, rev -> {
            final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
            group.authorize(authorizer, RequestAction.READ);

            final Set<Connection> connections = connectionDAO.getConnections(groupId);
            final Set<String> connectionIds = connections.stream().map(connection -> connection.getIdentifier()).collect(Collectors.toSet());
            return revisionManager.get(connectionIds, () -> {
                return connections.stream()
                        .map(connection -> {
                            final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(connection.getIdentifier()));
                            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connection);
                            final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connection.getIdentifier()));
                            return entityFactory.createConnectionEntity(dtoFactory.createConnectionDto(connection), revision, accessPolicy, status);
                        })
                        .collect(Collectors.toSet());
            });
        });
    }

    @Override
    public ConnectionEntity getConnection(String connectionId) {
        return revisionManager.get(connectionId, rev -> {
            final Connection connection = connectionDAO.getConnection(connectionId);
            connection.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connection);
            final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionId));
            return entityFactory.createConnectionEntity(dtoFactory.createConnectionDto(connectionDAO.getConnection(connectionId)), revision, accessPolicy, status);
        });
    }

    @Override
    public DropRequestDTO getFlowFileDropRequest(String connectionId, String dropRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        connection.authorize(authorizer, RequestAction.WRITE);
        return dtoFactory.createDropRequestDTO(connectionDAO.getFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO getFlowFileListingRequest(String connectionId, String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        connection.authorize(authorizer, RequestAction.WRITE);

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
    public FlowFileDTO getFlowFile(String connectionId, String flowFileUuid) {
        return dtoFactory.createFlowFileDTO(connectionDAO.getFlowFile(connectionId, flowFileUuid));
    }

    @Override
    public ConnectionStatusDTO getConnectionStatus(String connectionId) {
        return revisionManager.get(connectionId, rev -> dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionId)));
    }

    @Override
    public StatusHistoryDTO getConnectionStatusHistory(String connectionId) {
        return revisionManager.get(connectionId, rev -> controllerFacade.getConnectionStatusHistory(connectionId));
    }

    @Override
    public Set<ProcessorEntity> getProcessors(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        final Set<ProcessorNode> processors = processorDAO.getProcessors(groupId);
        final Set<String> ids = processors.stream().map(proc -> proc.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return processors.stream()
                    .map(processor -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(processor.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processor);
                        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processor.getIdentifier()));
                        return entityFactory.createProcessorEntity(dtoFactory.createProcessorDto(processor), revision, accessPolicy, status);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public TemplateDTO exportTemplate(String id) {
        Template template = templateDAO.getTemplate(id);
        TemplateDTO templateDetails = template.getDetails();

        TemplateDTO templateDTO = dtoFactory.createTemplateDTO(template);
        templateDTO.setSnippet(dtoFactory.copySnippetContents(templateDetails.getSnippet()));
        return templateDTO;
    }

    @Override
    public TemplateDTO getTemplate(String id) {
        return dtoFactory.createTemplateDTO(templateDAO.getTemplate(id));
    }

    @Override
    public Set<TemplateDTO> getTemplates() {
        Set<TemplateDTO> templateDtos = new LinkedHashSet<>();
        for (Template template : templateDAO.getTemplates()) {
            templateDtos.add(dtoFactory.createTemplateDTO(template));
        }
        return templateDtos;
    }

    @Override
    public Set<DocumentedTypeDTO> getWorkQueuePrioritizerTypes() {
        return controllerFacade.getFlowFileComparatorTypes();
    }

    @Override
    public Set<DocumentedTypeDTO> getProcessorTypes() {
        return controllerFacade.getFlowFileProcessorTypes();
    }

    @Override
    public Set<DocumentedTypeDTO> getControllerServiceTypes(final String serviceType) {
        return controllerFacade.getControllerServiceTypes(serviceType);
    }

    @Override
    public Set<DocumentedTypeDTO> getReportingTaskTypes() {
        return controllerFacade.getReportingTaskTypes();
    }

    @Override
    public ProcessorEntity getProcessor(String id) {
        return revisionManager.get(id, rev -> {
            final ProcessorNode processor = processorDAO.getProcessor(id);
            processor.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(id));
            return entityFactory.createProcessorEntity(dtoFactory.createProcessorDto(processor), revision, dtoFactory.createAccessPolicyDto(processor), status);
        });
    }

    @Override
    public PropertyDescriptorDTO getProcessorPropertyDescriptor(String id, String property) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        PropertyDescriptor descriptor = processor.getPropertyDescriptor(property);

        // return an invalid descriptor if the processor doesn't suppor this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor, processor.getProcessGroup().getIdentifier());
    }

    @Override
    public ProcessorStatusDTO getProcessorStatus(String id) {
        return revisionManager.get(id, rev -> dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(id)));
    }

    @Override
    public StatusHistoryDTO getProcessorStatusHistory(String id) {
        return controllerFacade.getProcessorStatusHistory(id);
    }

    @Override
    public BulletinBoardDTO getBulletinBoard(BulletinQueryDTO query) {
        // build the query
        final BulletinQuery.Builder queryBuilder = new BulletinQuery.Builder()
                .groupIdMatches(query.getGroupId())
                .sourceIdMatches(query.getSourceId())
                .nameMatches(query.getName())
                .messageMatches(query.getMessage())
                .after(query.getAfter())
                .limit(query.getLimit());

        // get the bulletin repository
        final BulletinRepository bulletinRepository;
        if (properties.isClusterManager()) {
            bulletinRepository = clusterManager.getBulletinRepository();
        } else {
            bulletinRepository = controllerFacade.getBulletinRepository();
        }

        // perform the query
        final List<Bulletin> results = bulletinRepository.findBulletins(queryBuilder.build());

        // perform the query and generate the results - iterating in reverse order since we are
        // getting the most recent results by ordering by timestamp desc above. this gets the
        // exact results we want but in reverse order
        final List<BulletinDTO> bulletins = new ArrayList<>();
        for (final ListIterator<Bulletin> bulletinIter = results.listIterator(results.size()); bulletinIter.hasPrevious(); ) {
            bulletins.add(dtoFactory.createBulletinDto(bulletinIter.previous()));
        }

        // create the bulletin board
        BulletinBoardDTO bulletinBoard = new BulletinBoardDTO();
        bulletinBoard.setBulletins(bulletins);
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
     * Ensures the specified user has permission to access the specified port.
     */
    private boolean isUserAuthorized(final NiFiUser user, final RootGroupPort port) {
        final boolean isSiteToSiteSecure = Boolean.TRUE.equals(properties.isSiteToSiteSecure());

        // if site to site is not secure, allow all users
        if (!isSiteToSiteSecure) {
            return true;
        }

        // TODO - defer to authorizer to see if user is able to retrieve site-to-site details for the specified port
        return true;
    }

    @Override
    public ControllerDTO getController() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // TODO - defer to authorizer to see if user is able to retrieve site-to-site details

        // TODO - filter response for access to specific ports

        // serialize the input ports this NiFi has access to
        final Set<PortDTO> inputPortDtos = new LinkedHashSet<>();
        final Set<RootGroupPort> inputPorts = controllerFacade.getInputPorts();
        final Set<String> inputPortIds = inputPorts.stream().map(port -> port.getIdentifier()).collect(Collectors.toSet());
        revisionManager.get(inputPortIds, () -> {
            for (RootGroupPort inputPort : inputPorts) {
                if (isUserAuthorized(user, inputPort)) {
                    final PortDTO dto = new PortDTO();
                    dto.setId(inputPort.getIdentifier());
                    dto.setName(inputPort.getName());
                    dto.setComments(inputPort.getComments());
                    dto.setState(inputPort.getScheduledState().toString());
                    inputPortDtos.add(dto);
                }
            }
            return null;
        });

        // serialize the output ports this NiFi has access to
        final Set<PortDTO> outputPortDtos = new LinkedHashSet<>();
        final Set<RootGroupPort> outputPorts = controllerFacade.getOutputPorts();
        final Set<String> outputPortIds = outputPorts.stream().map(port -> port.getIdentifier()).collect(Collectors.toSet());
        revisionManager.get(outputPortIds, () -> {
            for (RootGroupPort outputPort : controllerFacade.getOutputPorts()) {
                if (isUserAuthorized(user, outputPort)) {
                    final PortDTO dto = new PortDTO();
                    dto.setId(outputPort.getIdentifier());
                    dto.setName(outputPort.getName());
                    dto.setComments(outputPort.getComments());
                    dto.setState(outputPort.getScheduledState().toString());
                    outputPortDtos.add(dto);
                }
            }

            return null;
        });

        // get the root group
        final String rootGroupId = controllerFacade.getRootGroupId();
        final ProcessGroupCounts counts = revisionManager.get(rootGroupId, rev -> {
            final ProcessGroup rootGroup = processGroupDAO.getProcessGroup(controllerFacade.getRootGroupId());
            return rootGroup.getCounts();
        });

        // create the controller dto
        final ControllerDTO controllerDTO = new ControllerDTO();
        controllerDTO.setId(controllerFacade.getRootGroupId());
        controllerDTO.setInstanceId(controllerFacade.getInstanceId());
        controllerDTO.setInputPorts(inputPortDtos);
        controllerDTO.setOutputPorts(outputPortDtos);
        controllerDTO.setInputPortCount(inputPorts.size());
        controllerDTO.setOutputPortCount(outputPortDtos.size());
        controllerDTO.setRunningCount(counts.getRunningCount());
        controllerDTO.setStoppedCount(counts.getStoppedCount());
        controllerDTO.setInvalidCount(counts.getInvalidCount());
        controllerDTO.setDisabledCount(counts.getDisabledCount());

        // determine the site to site configuration
        if (isClustered()) {
            controllerDTO.setRemoteSiteListeningPort(controllerFacade.getClusterManagerRemoteSiteListeningPort());
            controllerDTO.setSiteToSiteSecure(controllerFacade.isClusterManagerRemoteSiteCommsSecure());
        } else {
            controllerDTO.setRemoteSiteListeningPort(controllerFacade.getRemoteSiteListeningPort());
            controllerDTO.setSiteToSiteSecure(controllerFacade.isRemoteSiteCommsSecure());
        }

        return controllerDTO;
    }

    @Override
    public ControllerConfigurationEntity getControllerConfiguration() {
        return revisionManager.get(FlowController.class.getSimpleName(), rev -> {
            final ControllerConfigurationDTO dto = dtoFactory.createControllerConfigurationDto(controllerFacade, properties.getAutoRefreshInterval());
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerFacade);
            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            return entityFactory.createControllerConfigurationEntity(dto, revision, accessPolicy);
        });
    }

    @Override
    public Set<LabelEntity> getLabels(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        final Set<Label> labels = labelDAO.getLabels(groupId);
        final Set<String> ids = labels.stream().map(label -> label.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return labels.stream()
                    .map(label -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(label.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(label);
                        return entityFactory.createLabelEntity(dtoFactory.createLabelDto(label), revision, accessPolicy);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public LabelEntity getLabel(String labelId) {
        return revisionManager.get(labelId, rev -> {
            final Label label = labelDAO.getLabel(labelId);
            label.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(label);
            return entityFactory.createLabelEntity(dtoFactory.createLabelDto(label), revision, accessPolicy);
        });
    }

    @Override
    public Set<FunnelEntity> getFunnels(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        final Set<Funnel> funnels = funnelDAO.getFunnels(groupId);
        final Set<String> funnelIds = funnels.stream().map(funnel -> funnel.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(funnelIds, () -> {
            return funnels.stream()
                    .map(funnel -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(funnel.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnel);
                        return entityFactory.createFunnelEntity(dtoFactory.createFunnelDto(funnel), revision, accessPolicy);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public FunnelEntity getFunnel(String funnelId) {
        return revisionManager.get(funnelId, rev -> {
            final Funnel funnel = funnelDAO.getFunnel(funnelId);
            funnel.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnel);
            return entityFactory.createFunnelEntity(dtoFactory.createFunnelDto(funnel), revision, accessPolicy);
        });
    }

    @Override
    public Set<PortEntity> getInputPorts(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        final Set<Port> inputPorts = inputPortDAO.getPorts(groupId);
        final Set<String> portIds = inputPorts.stream().map(port -> port.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(portIds, () -> {
            return inputPorts.stream()
                    .map(port -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(port.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
                        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(port.getIdentifier()));
                        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, accessPolicy, status);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public Set<PortEntity> getOutputPorts(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        final Set<Port> ports = outputPortDAO.getPorts(groupId);
        final Set<String> ids = ports.stream().map(port -> port.getIdentifier()).collect(Collectors.toSet());

        return revisionManager.get(ids, () -> {
            return ports.stream()
                    .map(port -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(port.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
                        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(port.getIdentifier()));
                        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, accessPolicy, status);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public Set<ProcessGroupEntity> getProcessGroups(String parentGroupId) {
        final ProcessGroup parentGroup = processGroupDAO.getProcessGroup(parentGroupId);
        parentGroup.authorize(authorizer, RequestAction.READ);

        final Set<ProcessGroup> groups = processGroupDAO.getProcessGroups(parentGroupId);
        final Set<String> ids = groups.stream().map(group -> group.getIdentifier()).collect(Collectors.toSet());
        return revisionManager.get(ids, () -> {
            return groups.stream()
                    .map(group -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(group.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(group);
                        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(group.getIdentifier()));
                        return entityFactory.createProcessGroupEntity(dtoFactory.createProcessGroupDto(group), revision, accessPolicy, status);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public Set<RemoteProcessGroupEntity> getRemoteProcessGroups(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        final Set<RemoteProcessGroup> rpgs = remoteProcessGroupDAO.getRemoteProcessGroups(groupId);
        final Set<String> ids = rpgs.stream().map(rpg -> rpg.getIdentifier()).collect(Collectors.toSet());

        return revisionManager.get(ids, () -> {
            return rpgs.stream()
                    .map(rpg -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(rpg.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(rpg);
                        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(rpg.getIdentifier()));
                        return entityFactory.createRemoteProcessGroupEntity(dtoFactory.createRemoteProcessGroupDto(rpg), revision, accessPolicy, status);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public PortEntity getInputPort(String inputPortId) {
        return revisionManager.get(inputPortId, rev -> {
            final Port port = inputPortDAO.getPort(inputPortId);
            port.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
            final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortId));
            return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, accessPolicy, status);
        });
    }

    @Override
    public PortStatusDTO getInputPortStatus(String inputPortId) {
        return revisionManager.get(inputPortId, rev -> dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortId)));
    }

    @Override
    public PortEntity getOutputPort(String outputPortId) {
        return revisionManager.get(outputPortId, rev -> {
            final Port port = outputPortDAO.getPort(outputPortId);
            port.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
            final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortId));
            return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, accessPolicy, status);
        });
    }

    @Override
    public PortStatusDTO getOutputPortStatus(String outputPortId) {
        return revisionManager.get(outputPortId, rev -> dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(outputPortId)));
    }

    @Override
    public RemoteProcessGroupEntity getRemoteProcessGroup(String remoteProcessGroupId) {
        return revisionManager.get(remoteProcessGroupId, rev -> {
            final RemoteProcessGroup rpg = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
            rpg.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(rpg);
            final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(rpg.getIdentifier()));
            return entityFactory.createRemoteProcessGroupEntity(dtoFactory.createRemoteProcessGroupDto(rpg), revision, accessPolicy, status);
        });
    }

    @Override
    public RemoteProcessGroupStatusDTO getRemoteProcessGroupStatus(String id) {
        return revisionManager.get(id, rev -> dtoFactory.createRemoteProcessGroupStatusDto(controllerFacade.getRemoteProcessGroupStatus(id)));
    }

    @Override
    public StatusHistoryDTO getRemoteProcessGroupStatusHistory(String id) {
        return controllerFacade.getRemoteProcessGroupStatusHistory(id);
    }

    @Override
    public ProcessGroupFlowEntity getProcessGroupFlow(String groupId, boolean recurse) {
        return revisionManager.get(groupId,
                rev -> {
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
                    return revisionManager.get(identifiers,
                            () -> {
                                final ProcessGroupStatus groupStatus = controllerFacade.getProcessGroupStatus(groupId);
                                final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroup);
                                return entityFactory.createProcessGroupFlowEntity(dtoFactory.createProcessGroupFlowDto(processGroup, groupStatus, revisionManager), accessPolicy);
                            });
                });
    }

    @Override
    public ProcessGroupEntity getProcessGroup(String groupId) {
        return revisionManager.get(groupId, rev -> {
            final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
            processGroup.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroup);
            final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(groupId));
            return entityFactory.createProcessGroupEntity(dtoFactory.createProcessGroupDto(processGroup), revision, accessPolicy, status);
        });
    }

    @Override
    public Set<ControllerServiceEntity> getControllerServices(String groupId) {
        // TODO - move this logic into the ControllerServiceDAO

        final Set<ControllerServiceNode> serviceNodes;
        final Set<String> serviceIds;
        if (groupId == null) {
            // TODO - controller services scoped by the controller
            serviceNodes = controllerServiceDAO.getControllerServices();
            serviceIds = serviceNodes.stream().map(service -> service.getIdentifier()).collect(Collectors.toSet());
        } else {
            final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
            serviceNodes = group.getControllerServices(true);
            serviceIds = serviceNodes.stream().map(service -> service.getIdentifier()).collect(Collectors.toSet());
        }

        return revisionManager.get(serviceIds, () -> {
            return serviceNodes.stream()
                    .map(serviceNode -> {
                        final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(serviceNode);

                        final ControllerServiceReference ref = serviceNode.getReferences();
                        final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = createControllerServiceReferencingComponentsEntity(ref);
                        dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());

                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(serviceNode.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(serviceNode);
                        return entityFactory.createControllerServiceEntity(dto, revision, accessPolicy);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public ControllerServiceEntity getControllerService(String controllerServiceId) {
        return revisionManager.get(controllerServiceId, rev -> {
            final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
            controllerService.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(controllerService);
            final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(controllerService);

            final ControllerServiceReference ref = controllerService.getReferences();
            final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = createControllerServiceReferencingComponentsEntity(ref);
            dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());

            return entityFactory.createControllerServiceEntity(dto, revision, accessPolicy);
        });
    }

    @Override
    public PropertyDescriptorDTO getControllerServicePropertyDescriptor(String id, String property) {
        return revisionManager.get(id, rev -> {
            final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(id);
            PropertyDescriptor descriptor = controllerService.getControllerServiceImplementation().getPropertyDescriptor(property);

            // return an invalid descriptor if the controller service doesn't support this property
            if (descriptor == null) {
                descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
            }

            return dtoFactory.createPropertyDescriptorDto(descriptor, controllerService.getProcessGroup().getIdentifier());
        });
    }

    @Override
    public ControllerServiceReferencingComponentsEntity getControllerServiceReferencingComponents(String controllerServiceId) {
        return revisionManager.get(controllerServiceId, rev -> {
            final ControllerServiceNode service = controllerServiceDAO.getControllerService(controllerServiceId);
            final ControllerServiceReference ref = service.getReferences();
            return createControllerServiceReferencingComponentsEntity(ref);
        });
    }

    @Override
    public Set<ReportingTaskEntity> getReportingTasks() {
        final Set<ReportingTaskNode> reportingTasks = reportingTaskDAO.getReportingTasks();
        final Set<String> ids = reportingTasks.stream().map(task -> task.getIdentifier()).collect(Collectors.toSet());

        return revisionManager.get(ids, () -> {
            return reportingTasks.stream()
                    .map(reportingTask -> {
                        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(reportingTask.getIdentifier()));
                        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(reportingTask);
                        return entityFactory.createReportingTaskEntity(dtoFactory.createReportingTaskDto(reportingTask), revision, accessPolicy);
                    })
                    .collect(Collectors.toSet());
        });
    }

    @Override
    public ReportingTaskEntity getReportingTask(String reportingTaskId) {
        return revisionManager.get(reportingTaskId, rev -> {
            final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
            reportingTask.authorize(authorizer, RequestAction.READ);

            final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
            final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(reportingTask);
            return entityFactory.createReportingTaskEntity(dtoFactory.createReportingTaskDto(reportingTask), revision, accessPolicy);
        });
    }

    @Override
    public PropertyDescriptorDTO getReportingTaskPropertyDescriptor(String id, String property) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(id);
        PropertyDescriptor descriptor = reportingTask.getReportingTask().getPropertyDescriptor(property);

        // return an invalid descriptor if the reporting task doesn't support this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor, "root");
    }

    @Override
    public StatusHistoryDTO getProcessGroupStatusHistory(String groupId) {
        return controllerFacade.getProcessGroupStatusHistory(groupId);
    }

    @Override
    public HistoryDTO getActions(HistoryQueryDTO historyQueryDto) {
        // extract the query criteria
        HistoryQuery historyQuery = new HistoryQuery();
        historyQuery.setStartDate(historyQueryDto.getStartDate());
        historyQuery.setEndDate(historyQueryDto.getEndDate());
        historyQuery.setSourceId(historyQueryDto.getSourceId());
        historyQuery.setUserName(historyQueryDto.getUserName());
        historyQuery.setOffset(historyQueryDto.getOffset());
        historyQuery.setCount(historyQueryDto.getCount());
        historyQuery.setSortColumn(historyQueryDto.getSortColumn());
        historyQuery.setSortOrder(historyQueryDto.getSortOrder());

        // perform the query
        History history = auditService.getActions(historyQuery);

        // create the response
        return dtoFactory.createHistoryDto(history);
    }

    @Override
    public ActionDTO getAction(Integer actionId) {
        // get the action
        Action action = auditService.getAction(actionId);

        // ensure the action was found
        if (action == null) {
            throw new ResourceNotFoundException(String.format("Unable to find action with id '%s'.", actionId));
        }

        // return the action
        return dtoFactory.createActionDto(action);
    }

    @Override
    public ComponentHistoryDTO getComponentHistory(String componentId) {
        final Map<String, PropertyHistoryDTO> propertyHistoryDtos = new LinkedHashMap<>();
        final Map<String, List<PreviousValue>> propertyHistory = auditService.getPreviousValues(componentId);

        for (final Map.Entry<String, List<PreviousValue>> entry : propertyHistory.entrySet()) {
            final List<PreviousValueDTO> previousValueDtos = new ArrayList<>();

            for (final PreviousValue previousValue : entry.getValue()) {
                final PreviousValueDTO dto = new PreviousValueDTO();
                dto.setPreviousValue(previousValue.getPreviousValue());
                dto.setTimestamp(previousValue.getTimestamp());
                dto.setUserName(previousValue.getUserName());
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
        final Collection<NodeDTO> nodeDtos = new ArrayList<>();
        clusterDto.setNodes(nodeDtos);
        for (final Node node : clusterManager.getNodes()) {
            // create and add node dto
            final String nodeId = node.getNodeId().getId();
            nodeDtos.add(dtoFactory.createNodeDTO(node, clusterManager.getLatestHeartbeat(node.getNodeId()), clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));
        }

        return clusterDto;
    }

    @Override
    public NodeDTO getNode(String nodeId) {
        final Node node = clusterManager.getNode(nodeId);
        if (node == null) {
            throw new UnknownNodeException("Node does not exist.");
        } else {
            return dtoFactory.createNodeDTO(node, clusterManager.getLatestHeartbeat(node.getNodeId()), clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId));
        }
    }

    @Override
    public void deleteNode(String nodeId) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        final String userDn = user.getIdentity();
        clusterManager.deleteNode(nodeId, userDn);
    }

    /* setters */
    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setControllerFacade(ControllerFacade controllerFacade) {
        this.controllerFacade = controllerFacade;
    }

    public void setRemoteProcessGroupDAO(RemoteProcessGroupDAO remoteProcessGroupDAO) {
        this.remoteProcessGroupDAO = remoteProcessGroupDAO;
    }

    public void setLabelDAO(LabelDAO labelDAO) {
        this.labelDAO = labelDAO;
    }

    public void setFunnelDAO(FunnelDAO funnelDAO) {
        this.funnelDAO = funnelDAO;
    }

    public void setSnippetDAO(SnippetDAO snippetDAO) {
        this.snippetDAO = snippetDAO;
    }

    public void setProcessorDAO(ProcessorDAO processorDAO) {
        this.processorDAO = processorDAO;
    }

    public void setConnectionDAO(ConnectionDAO connectionDAO) {
        this.connectionDAO = connectionDAO;
    }

    public void setAuditService(AuditService auditService) {
        this.auditService = auditService;
    }

    public void setKeyService(KeyService keyService) {
        this.keyService = keyService;
    }

    public void setRevisionManager(RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setDtoFactory(DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setEntityFactory(EntityFactory entityFactory) {
        this.entityFactory = entityFactory;
    }

    public void setInputPortDAO(PortDAO inputPortDAO) {
        this.inputPortDAO = inputPortDAO;
    }

    public void setOutputPortDAO(PortDAO outputPortDAO) {
        this.outputPortDAO = outputPortDAO;
    }

    public void setProcessGroupDAO(ProcessGroupDAO processGroupDAO) {
        this.processGroupDAO = processGroupDAO;
    }

    public void setControllerServiceDAO(ControllerServiceDAO controllerServiceDAO) {
        this.controllerServiceDAO = controllerServiceDAO;
    }

    public void setReportingTaskDAO(ReportingTaskDAO reportingTaskDAO) {
        this.reportingTaskDAO = reportingTaskDAO;
    }

    public void setTemplateDAO(TemplateDAO templateDAO) {
        this.templateDAO = templateDAO;
    }

    public void setSnippetUtils(SnippetUtils snippetUtils) {
        this.snippetUtils = snippetUtils;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    private boolean isPrimaryNode(String nodeId) {
        final Node primaryNode = clusterManager.getPrimaryNode();
        return (primaryNode != null && primaryNode.getNodeId().getId().equals(nodeId));
    }

    private AccessPolicyDTO allAccess() {
        final AccessPolicyDTO dto = new AccessPolicyDTO();
        dto.setCanRead(true);
        dto.setCanWrite(true);
        return dto;
    }
}
