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
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
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
import org.apache.nifi.controller.Counter;
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
import org.apache.nifi.util.FormatUtils;
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
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
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
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
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
import org.apache.nifi.web.util.SnippetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of NiFiServiceFacade that performs revision checking.
 */
public class StandardNiFiServiceFacade implements NiFiServiceFacade {

    private static final Logger logger = LoggerFactory.getLogger(StandardNiFiServiceFacade.class);

    // nifi core components
    private ControllerFacade controllerFacade;
    private SnippetUtils snippetUtils;

    // optimistic locking manager
    private OptimisticLockingManager optimisticLockingManager;

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
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (connectionDAO.hasConnection(connectionDTO.getId())) {
            connectionDAO.verifyUpdate(connectionDTO);
        } else {
            connectionDAO.verifyCreate(connectionDTO.getParentGroupId(), connectionDTO);
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
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (inputPortDAO.hasPort(inputPortDTO.getId())) {
            inputPortDAO.verifyUpdate(inputPortDTO);
        }
    }

    @Override
    public void verifyDeleteInputPort(String inputPortId) {
        inputPortDAO.verifyDelete(inputPortId);
    }

    @Override
    public void verifyUpdateOutputPort(PortDTO outputPortDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (outputPortDAO.hasPort(outputPortDTO.getId())) {
            outputPortDAO.verifyUpdate(outputPortDTO);
        }
    }

    @Override
    public void verifyDeleteOutputPort(String outputPortId) {
        outputPortDAO.verifyDelete(outputPortId);
    }

    @Override
    public void verifyUpdateProcessor(ProcessorDTO processorDTO) {
        // if group does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (processorDAO.hasProcessor(processorDTO.getId())) {
            processorDAO.verifyUpdate(processorDTO);
        }
    }

    @Override
    public void verifyDeleteProcessor(String processorId) {
        processorDAO.verifyDelete(processorId);
    }

    @Override
    public void verifyUpdateProcessGroup(ProcessGroupDTO processGroupDTO) {
        // if group does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (processGroupDAO.hasProcessGroup(processGroupDTO.getId())) {
            processGroupDAO.verifyUpdate(processGroupDTO);
        }
    }

    @Override
    public void verifyDeleteProcessGroup(String groupId) {
        processGroupDAO.verifyDelete(groupId);
    }

    @Override
    public void verifyUpdateRemoteProcessGroup(RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // if remote group does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupDTO.getId())) {
            remoteProcessGroupDAO.verifyUpdate(remoteProcessGroupDTO);
        }
    }

    @Override
    public void verifyUpdateRemoteProcessGroupInputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        remoteProcessGroupDAO.verifyUpdateInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
    }

    @Override
    public void verifyUpdateRemoteProcessGroupOutputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        remoteProcessGroupDAO.verifyUpdateOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
    }

    @Override
    public void verifyDeleteRemoteProcessGroup(String remoteProcessGroupId) {
        remoteProcessGroupDAO.verifyDelete(remoteProcessGroupId);
    }

    @Override
    public void verifyUpdateControllerService(ControllerServiceDTO controllerServiceDTO) {
        // if service does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (controllerServiceDAO.hasControllerService(controllerServiceDTO.getId())) {
            controllerServiceDAO.verifyUpdate(controllerServiceDTO);
        }
    }

    @Override
    public void verifyUpdateControllerServiceReferencingComponents(String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState) {
        controllerServiceDAO.verifyUpdateReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
    }

    @Override
    public void verifyDeleteControllerService(String controllerServiceId) {
        controllerServiceDAO.verifyDelete(controllerServiceId);
    }

    @Override
    public void verifyUpdateReportingTask(ReportingTaskDTO reportingTaskDTO) {
        // if tasks does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (reportingTaskDAO.hasReportingTask(reportingTaskDTO.getId())) {
            reportingTaskDAO.verifyUpdate(reportingTaskDTO);
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
            return new UpdateResult<>(createConnection(revision, connectionDTO.getParentGroupId(), connectionDTO), true);
        }

        final Connection connectionNode = connectionDAO.getConnection(connectionDTO.getId());
        final ConfigurationSnapshot<ConnectionDTO> snapshot = updateComponent(
            revision,
            connectionNode,
            () -> connectionDAO.updateConnection(connectionDTO),
            connection -> dtoFactory.createConnectionDto(connection));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connectionNode);
        return new UpdateResult<>(entityFactory.createConnectionEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy), false);
    }

    @Override
    public UpdateResult<ProcessorEntity> updateProcessor(final Revision revision, final ProcessorDTO processorDTO) {
        // if processor does not exist, then create new processor
        if (processorDAO.hasProcessor(processorDTO.getId()) == false) {
            return new UpdateResult<>(createProcessor(revision, processorDTO.getParentGroupId(), processorDTO), true);
        }

        // get the component, ensure we have access to it, and perform the update request
        final ProcessorNode processorNode = processorDAO.getProcessor(processorDTO.getId());
        final ConfigurationSnapshot<ProcessorDTO> snapshot = updateComponent(revision,
            processorNode,
            () -> processorDAO.updateProcessor(processorDTO),
            proc -> dtoFactory.createProcessorDto(proc));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processorNode);
        return new UpdateResult<>(entityFactory.createProcessorEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy), false);
    }

    @Override
    public UpdateResult<LabelEntity> updateLabel(final Revision revision, final LabelDTO labelDTO) {
        // if label does not exist, then create new label
        if (labelDAO.hasLabel(labelDTO.getId()) == false) {
            return new UpdateResult<>(createLabel(revision, labelDTO.getParentGroupId(), labelDTO), false);
        }

        final Label labelNode = labelDAO.getLabel(labelDTO.getId());
        final ConfigurationSnapshot<LabelDTO> snapshot = updateComponent(revision,
            labelNode,
            () -> labelDAO.updateLabel(labelDTO),
            label -> dtoFactory.createLabelDto(label));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(labelNode);
        return new UpdateResult<>(entityFactory.createLabelEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy), false);
    }

    @Override
    public UpdateResult<FunnelEntity> updateFunnel(final Revision revision, final FunnelDTO funnelDTO) {
        // if label does not exist, then create new label
        if (funnelDAO.hasFunnel(funnelDTO.getId()) == false) {
            return new UpdateResult<>(createFunnel(revision, funnelDTO.getParentGroupId(), funnelDTO), true);
        }

        final Funnel funnelNode = funnelDAO.getFunnel(funnelDTO.getId());
        final ConfigurationSnapshot<FunnelDTO> snapshot = updateComponent(revision,
            funnelNode,
            () -> funnelDAO.updateFunnel(funnelDTO),
            funnel -> dtoFactory.createFunnelDto(funnel));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnelNode);
        return new UpdateResult<>(entityFactory.createFunnelEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy), false);
    }


    /**
     * Updates a component with the given revision, using the provided supplier to call
     * into the appropriate DAO and the provided function to convert the component into a DTO.
     *
     * @param revision the current revision
     * @param daoUpdate a Supplier that will update the component via the appropriate DAO
     * @param dtoCreation a Function to convert a component into a dao
     *
     * @param <D> the DTO Type of the updated component
     * @param <C> the Component Type of the updated component
     *
     * @return A ConfigurationSnapshot that represents the new configuration
     */
    private <D, C> ConfigurationSnapshot<D> updateComponent(final Revision revision, final Authorizable authorizable, final Supplier<C> daoUpdate, final Function<C, D>
        dtoCreation) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<D>() {
            @Override
            public ConfigurationResult<D> execute() {
                // ensure write access to the flow
                authorizable.authorize(authorizer, RequestAction.WRITE);

                // also ensure read access to the flow as the component must be read in order to generate a response
                authorizable.authorize(authorizer, RequestAction.READ);

                // get the updated component
                final C component = daoUpdate.get();

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult<D>() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public D getConfiguration() {
                        return dtoCreation.apply(component);
                    }
                };
            }
        });
    }


    @Override
    public void verifyUpdateSnippet(SnippetDTO snippetDto) {
        // if snippet does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (snippetDAO.hasSnippet(snippetDto.getId())) {
            snippetDAO.verifyUpdate(snippetDto);
        }
    }

    @Override
    public ConfigurationSnapshot<SnippetDTO> updateSnippet(final Revision revision, final SnippetDTO snippetDto) {
        // if label does not exist, then create new label
        if (snippetDAO.hasSnippet(snippetDto.getId()) == false) {
            return createSnippet(revision, snippetDto);
        }

        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(snippetDto.getParentGroupId());
        return updateComponent(
            revision,
            processGroup,
            () -> snippetDAO.updateSnippet(snippetDto),
            snippet -> {
                 return dtoFactory.createSnippetDto(snippet);
            });
    }

    @Override
    public UpdateResult<PortEntity> updateInputPort(final Revision revision, final PortDTO inputPortDTO) {
        // if input port does not exist, then create new input port
        if (inputPortDAO.hasPort(inputPortDTO.getId()) == false) {
            return new UpdateResult<>(createInputPort(revision, inputPortDTO.getParentGroupId(), inputPortDTO), true);
        }

        final Port inputPortNode = inputPortDAO.getPort(inputPortDTO.getId());
        final ConfigurationSnapshot<PortDTO> snapshot = updateComponent(revision,
            inputPortNode,
            () -> inputPortDAO.updatePort(inputPortDTO),
            port -> dtoFactory.createPortDto(port));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(inputPortNode);
        return new UpdateResult<>(entityFactory.createPortEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy), false);
    }

    @Override
    public UpdateResult<PortEntity> updateOutputPort(final Revision revision, final PortDTO outputPortDTO) {
        // if output port does not exist, then create new output port
        if (outputPortDAO.hasPort(outputPortDTO.getId()) == false) {
            return new UpdateResult<>(createOutputPort(revision, outputPortDTO.getParentGroupId(), outputPortDTO), true);
        }

        final Port outputPortNode = outputPortDAO.getPort(outputPortDTO.getId());
        final ConfigurationSnapshot<PortDTO> snapshot = updateComponent(revision,
            outputPortNode,
            () -> outputPortDAO.updatePort(outputPortDTO),
            port -> dtoFactory.createPortDto(port));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(outputPortNode);
        return new UpdateResult<>(entityFactory.createPortEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy), false);
    }

    @Override
    public UpdateResult<RemoteProcessGroupEntity> updateRemoteProcessGroup(final Revision revision, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // if controller reference does not exist, then create new controller reference
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupDTO.getId()) == false) {
            return new UpdateResult<>(createRemoteProcessGroup(revision, remoteProcessGroupDTO.getParentGroupId(), remoteProcessGroupDTO), true);
        }

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final ConfigurationSnapshot<RemoteProcessGroupDTO> snapshot = updateComponent(
            revision,
            remoteProcessGroupNode,
            () -> remoteProcessGroupDAO.updateRemoteProcessGroup(remoteProcessGroupDTO),
            remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroupNode);
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId());
        return new UpdateResult<>(entityFactory.createRemoteProcessGroupEntity(snapshot.getConfiguration(), updateRevision, accessPolicy), false);
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupInputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupPortDTO.getGroupId());
        return updateComponent(
            revision,
            remoteProcessGroupNode,
            () -> remoteProcessGroupDAO.updateRemoteProcessGroupInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
            remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupOutputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupPortDTO.getGroupId());
        return updateComponent(
            revision,
            remoteProcessGroupNode,
            () -> remoteProcessGroupDAO.updateRemoteProcessGroupOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
            remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));
    }

    @Override
    public UpdateResult<ProcessGroupEntity> updateProcessGroup(final Revision revision, final ProcessGroupDTO processGroupDTO) {
        // if process group does not exist, then create new process group
        if (processGroupDAO.hasProcessGroup(processGroupDTO.getId()) == false) {
            if (processGroupDTO.getParentGroupId() == null) {
                throw new IllegalArgumentException("Unable to create the specified process group since the parent group was not specified.");
            } else {
                return new UpdateResult<>(createProcessGroup(processGroupDTO.getParentGroupId(), revision, processGroupDTO), true);
            }
        }

        final ProcessGroup processGroupNode = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final ConfigurationSnapshot<ProcessGroupDTO> snapshot = updateComponent(revision,
            processGroupNode,
            () -> processGroupDAO.updateProcessGroup(processGroupDTO),
            processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId());
        return new UpdateResult<>(entityFactory.createProcessGroupEntity(snapshot.getConfiguration(), updatedRevision, accessPolicy), false);
    }

    @Override
    public ConfigurationSnapshot<ControllerConfigurationDTO> updateControllerConfiguration(final Revision revision, final ControllerConfigurationDTO controllerConfigurationDTO) {

        return updateComponent(
            revision,
            controllerFacade,
            () -> {
                // update the controller configuration through the proxy
                if (controllerConfigurationDTO.getName() != null) {
                    controllerFacade.setName(controllerConfigurationDTO.getName());
                }
                if (controllerConfigurationDTO.getComments() != null) {
                    controllerFacade.setComments(controllerConfigurationDTO.getComments());
                }
                if (controllerConfigurationDTO.getMaxTimerDrivenThreadCount() != null) {
                    controllerFacade.setMaxTimerDrivenThreadCount(controllerConfigurationDTO.getMaxTimerDrivenThreadCount());
                }
                if (controllerConfigurationDTO.getMaxEventDrivenThreadCount() != null) {
                    controllerFacade.setMaxEventDrivenThreadCount(controllerConfigurationDTO.getMaxEventDrivenThreadCount());
                }

                return controllerConfigurationDTO;
            },
            controller -> getControllerConfiguration());
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
        processorDAO.verifyClearState(processorId);
    }

    @Override
    public ConfigurationSnapshot<Void> clearProcessorState(final Revision revision, final String processorId) {
        return clearComponentState(revision, () -> processorDAO.clearState(processorId));
    }

    private ConfigurationSnapshot<Void> clearComponentState(final Revision revision, final Runnable clearState) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // clear the state for the specified component
                clearState.run();

                return new StandardConfigurationResult<Void>(false, null);
            }
        });
    }

    @Override
    public void verifyCanClearControllerServiceState(final String controllerServiceId) {
        controllerServiceDAO.verifyClearState(controllerServiceId);
    }

    @Override
    public ConfigurationSnapshot<Void> clearControllerServiceState(final Revision revision, final String controllerServiceId) {
        return clearComponentState(revision, () -> controllerServiceDAO.clearState(controllerServiceId));
    }

    @Override
    public void verifyCanClearReportingTaskState(final String reportingTaskId) {
        reportingTaskDAO.verifyClearState(reportingTaskId);
    }

    @Override
    public ConfigurationSnapshot<Void> clearReportingTaskState(final Revision revision, final String reportingTaskId) {
        return clearComponentState(revision, () -> reportingTaskDAO.clearState(reportingTaskId));
    }

    @Override
    public ConnectionEntity deleteConnection(final Revision revision, final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ConfigurationSnapshot<Void> snapshot = deleteComponent(
            revision,
            connection,
            () -> connectionDAO.deleteConnection(connectionId));

        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connection);
        return entityFactory.createConnectionEntity(null, dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
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
        final ConfigurationSnapshot<Void> snapshot = deleteComponent(
            revision,
            processor,
            () -> processorDAO.deleteProcessor(processorId));

        return entityFactory.createProcessorEntity(null, dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), null);
    }

    @Override
    public LabelEntity deleteLabel(final Revision revision, final String labelId) {
        final Label label = labelDAO.getLabel(labelId);
        final ConfigurationSnapshot<Void> snapshot = deleteComponent(
            revision,
            label,
            () -> labelDAO.deleteLabel(labelId));

        return entityFactory.createLabelEntity(null, dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), null);
    }

    @Override
    public FunnelEntity deleteFunnel(final Revision revision, final String funnelId) {
        final Funnel funnel = funnelDAO.getFunnel(funnelId);
        final ConfigurationSnapshot<Void> snapshot = deleteComponent(
            revision,
            funnel,
            () -> funnelDAO.deleteFunnel(funnelId));

        return entityFactory.createFunnelEntity(null, dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), null);
    }

    /**
     * Deletes a component using the Optimistic Locking Manager
     *
     * @param revision the current revision
     * @param action the action that deletes the component via the appropriate DAO object
     * @return a ConfigurationSnapshot that represents the new configuration
     */
    private ConfigurationSnapshot<Void> deleteComponent(final Revision revision, final Authorizable authorizable, final Runnable action) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // ensure access to the component
                authorizable.authorize(authorizer, RequestAction.WRITE);

                action.run();

                // save the flow
                controllerFacade.save();
                return new ConfigurationResult<Void>() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public Void getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public void verifyDeleteSnippet(String id) {
        snippetDAO.verifyDelete(id);
    }

    @Override
    public ConfigurationSnapshot<Void> deleteSnippet(final Revision revision, final String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(snippet.getParentGroupId());
        return deleteComponent(revision,
            processGroup,
            () -> snippetDAO.deleteSnippet(snippetId));
    }

    @Override
    public PortEntity deleteInputPort(final Revision revision, final String inputPortId) {
        final Port port = inputPortDAO.getPort(inputPortId);
        final ConfigurationSnapshot<Void> snapshot = deleteComponent(
            revision,
            port,
            () -> inputPortDAO.deletePort(inputPortId));

        return entityFactory.createPortEntity(null, dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), null);
    }

    @Override
    public PortEntity deleteOutputPort(final Revision revision, final String outputPortId) {
        final Port port = outputPortDAO.getPort(outputPortId);
        final ConfigurationSnapshot<Void> snapshot = deleteComponent(
            revision,
            port,
            () -> outputPortDAO.deletePort(outputPortId));

        return entityFactory.createPortEntity(null, dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), null);
    }

    @Override
    public ProcessGroupEntity deleteProcessGroup(final Revision revision, final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final ConfigurationSnapshot<Void> snapshot = deleteComponent(
            revision,
            processGroup,
            () -> processGroupDAO.deleteProcessGroup(groupId));

        return entityFactory.createProcessGroupEntity(null, dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), null);
    }

    @Override
    public RemoteProcessGroupEntity deleteRemoteProcessGroup(final Revision revision, final String remoteProcessGroupId) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        final ConfigurationSnapshot<Void> snapshot = deleteComponent(
            revision,
            remoteProcessGroup,
            () -> remoteProcessGroupDAO.deleteRemoteProcessGroup(remoteProcessGroupId));

        return entityFactory.createRemoteProcessGroupEntity(null, dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), null);
    }

    @Override
    public void deleteTemplate(String id) {
        // create the template
        templateDAO.deleteTemplate(id);
    }

    @Override
    public ConnectionEntity createConnection(final Revision revision, final String groupId, final ConnectionDTO connectionDTO) {
        final ConfigurationSnapshot<ConnectionDTO> snapshot = createComponent(
            revision,
            connectionDTO,
            () -> connectionDAO.createConnection(groupId, connectionDTO),
            connection -> dtoFactory.createConnectionDto(connection));

        final Connection connection = connectionDAO.getConnection(connectionDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(connection);
        return entityFactory.createConnectionEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
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
    public ProcessorEntity createProcessor(final Revision revision, final String groupId, final ProcessorDTO processorDTO) {
        final ConfigurationSnapshot<ProcessorDTO> snapshot = createComponent(
            revision,
            processorDTO,
            () -> processorDAO.createProcessor(groupId, processorDTO),
            processor -> dtoFactory.createProcessorDto(processor));

        final ProcessorNode processor = processorDAO.getProcessor(processorDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processor);
        return entityFactory.createProcessorEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
    }

    @Override
    public LabelEntity createLabel(final Revision revision, final String groupId, final LabelDTO labelDTO) {
        final ConfigurationSnapshot<LabelDTO> snapshot = createComponent(
            revision,
            labelDTO,
            () -> labelDAO.createLabel(groupId, labelDTO),
            label -> dtoFactory.createLabelDto(label));

        final Label label = labelDAO.getLabel(labelDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(label);
        return entityFactory.createLabelEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
    }

    /**
     * Creates a component using the optimistic locking manager.
     *
     * @param revision the current revision
     * @param componentDto the DTO that will be used to create the component
     * @param daoCreation A Supplier that will create the NiFi Component to use
     * @param dtoCreation a Function that will convert the NiFi Component into a corresponding DTO
     *
     * @param <D> the DTO Type
     * @param <C> the NiFi Component Type
     *
     * @return a ConfigurationSnapshot that represents the updated configuration
     */
    private <D, C> ConfigurationSnapshot<D> createComponent(final Revision revision, final ComponentDTO componentDto,
        final Supplier<C> daoCreation, final Function<C, D> dtoCreation) {

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<D>() {
            @Override
            public ConfigurationResult<D> execute() {
                // ensure id is set
                if (StringUtils.isBlank(componentDto.getId())) {
                    componentDto.setId(UUID.randomUUID().toString());
                }

                // ensure access to process group
                final ProcessGroup parent = processGroupDAO.getProcessGroup(componentDto.getParentGroupId());
                parent.authorize(authorizer, RequestAction.WRITE);

                // add the component
                final C component = daoCreation.get();

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult<D>() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public D getConfiguration() {
                        return dtoCreation.apply(component);
                    }
                };
            }
        });
    }



    @Override
    public FunnelEntity createFunnel(final Revision revision, final String groupId, final FunnelDTO funnelDTO) {
        final ConfigurationSnapshot<FunnelDTO> snapshot = createComponent(
            revision,
            funnelDTO,
            () -> funnelDAO.createFunnel(groupId, funnelDTO),
            funnel -> dtoFactory.createFunnelDto(funnel));

        final Funnel funnel = funnelDAO.getFunnel(funnelDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(funnel);
        return entityFactory.createFunnelEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
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
    public ConfigurationSnapshot<FlowDTO> copySnippet(final Revision revision, final String groupId, final String snippetId, final Double originX, final Double originY) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<FlowDTO>() {
            @Override
            public ConfigurationResult<FlowDTO> execute() {
                String id = snippetId;

                // create the new snippet
                final FlowSnippetDTO snippet = snippetDAO.copySnippet(groupId, id, originX, originY);

                // validate the new snippet
                validateSnippetContents(snippet);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult<FlowDTO>() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public FlowDTO getConfiguration() {
                        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
                        return dtoFactory.createFlowDto(group, snippet);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<SnippetDTO> createSnippet(final Revision revision, final SnippetDTO snippetDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<SnippetDTO>() {
            @Override
            public ConfigurationResult<SnippetDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(snippetDTO.getId())) {
                    snippetDTO.setId(UUID.randomUUID().toString());
                }

                // add the snippet
                final Snippet snippet = snippetDAO.createSnippet(snippetDTO);
                final SnippetDTO responseSnippetDTO = dtoFactory.createSnippetDto(snippet);

                return new ConfigurationResult<SnippetDTO>() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public SnippetDTO getConfiguration() {
                        return responseSnippetDTO;
                    }
                };
            }
        });
    }

    @Override
    public PortEntity createInputPort(final Revision revision, final String groupId, final PortDTO inputPortDTO) {
        final ConfigurationSnapshot<PortDTO> snapshot = createComponent(
            revision,
            inputPortDTO,
            () -> inputPortDAO.createPort(groupId, inputPortDTO),
            port -> dtoFactory.createPortDto(port));

        final Port port = inputPortDAO.getPort(inputPortDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
        return entityFactory.createPortEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
    }

    @Override
    public PortEntity createOutputPort(final Revision revision, final String groupId, final PortDTO outputPortDTO) {
        final ConfigurationSnapshot<PortDTO> snapshot = createComponent(
            revision,
            outputPortDTO,
            () -> outputPortDAO.createPort(groupId, outputPortDTO),
            port -> dtoFactory.createPortDto(port));

        final Port port = outputPortDAO.getPort(outputPortDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(port);
        return entityFactory.createPortEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final String parentGroupId, final Revision revision, final ProcessGroupDTO processGroupDTO) {
        final ConfigurationSnapshot<ProcessGroupDTO> snapshot = createComponent(revision, processGroupDTO,
            () -> processGroupDAO.createProcessGroup(parentGroupId, processGroupDTO),
            processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(processGroup);
        return entityFactory.createProcessGroupEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
    }

    @Override
    public RemoteProcessGroupEntity createRemoteProcessGroup(final Revision revision, final String groupId, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final ConfigurationSnapshot<RemoteProcessGroupDTO> snapshot = createComponent(
            revision,
            remoteProcessGroupDTO,
            () -> remoteProcessGroupDAO.createRemoteProcessGroup(groupId, remoteProcessGroupDTO),
            remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final AccessPolicyDTO accessPolicy = dtoFactory.createAccessPolicyDto(remoteProcessGroup);
        return entityFactory.createRemoteProcessGroupEntity(snapshot.getConfiguration(), dtoFactory.createRevisionDTO(snapshot.getVersion(), revision.getClientId()), accessPolicy);
    }

    @Override
    public TemplateDTO createTemplate(String name, String description, String snippetId) {
        // get the specified snippet
        Snippet snippet = snippetDAO.getSnippet(snippetId);

        // create the template
        TemplateDTO templateDTO = new TemplateDTO();
        templateDTO.setName(name);
        templateDTO.setDescription(description);
        templateDTO.setTimestamp(new Date());
        templateDTO.setSnippet(snippetUtils.populateFlowSnippet(snippet, true, true));

        // set the id based on the specified seed
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            templateDTO.setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        }

        // create the template
        Template template = templateDAO.createTemplate(templateDTO);

        return dtoFactory.createTemplateDTO(template);
    }

    @Override
    public TemplateDTO importTemplate(TemplateDTO templateDTO) {
        // ensure id is set
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            templateDTO.setId(UUID.nameUUIDFromBytes(clusterContext.getIdGenerationSeed().getBytes(StandardCharsets.UTF_8)).toString());
        }

        // mark the timestamp
        templateDTO.setTimestamp(new Date());

        // import the template
        final Template template = templateDAO.importTemplate(templateDTO);

        // return the template dto
        return dtoFactory.createTemplateDTO(template);
    }

    @Override
    public ConfigurationSnapshot<FlowDTO> createTemplateInstance(final Revision revision, final String groupId, final Double originX, final Double originY, final String templateId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<FlowDTO>() {
            @Override
            public ConfigurationResult<FlowDTO> execute() {
                // instantiate the template - there is no need to make another copy of the flow snippet since the actual template
                // was copied and this dto is only used to instantiate it's components (which as already completed)
                final FlowSnippetDTO snippet = templateDAO.instantiateTemplate(groupId, originX, originY, templateId);

                // validate the new snippet
                validateSnippetContents(snippet);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult<FlowDTO>() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public FlowDTO getConfiguration() {
                        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
                        return dtoFactory.createFlowDto(group, snippet);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> createArchive(final Revision revision) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // create the archive
                controllerFacade.createArchive();

                return new StandardConfigurationResult<Void>(false, null);
            }
        });
    }

    @Override
    public ConfigurationSnapshot<ProcessorDTO> setProcessorAnnotationData(final Revision revision, final String processorId, final String annotationData) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ProcessorDTO>() {
            @Override
            public ConfigurationResult<ProcessorDTO> execute() {
                // create the processor config
                final ProcessorConfigDTO config = new ProcessorConfigDTO();
                config.setAnnotationData(annotationData);

                // create the processor dto
                final ProcessorDTO processorDTO = new ProcessorDTO();
                processorDTO.setId(processorId);
                processorDTO.setConfig(config);

                // update the processor configuration
                final ProcessorNode processor = processorDAO.updateProcessor(processorDTO);

                // save the flow
                controllerFacade.save();

                return new StandardConfigurationResult<>(false, dtoFactory.createProcessorDto(processor));
            }
        });
    }

    @Override
    public ConfigurationSnapshot<ControllerServiceDTO> createControllerService(final Revision revision, final ControllerServiceDTO controllerServiceDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ControllerServiceDTO>() {
            @Override
            public ConfigurationResult<ControllerServiceDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(controllerServiceDTO.getId())) {
                    controllerServiceDTO.setId(UUID.randomUUID().toString());
                }

                // create the controller service
                final ControllerServiceNode controllerService = controllerServiceDAO.createControllerService(controllerServiceDTO);

                // save the update
                if (properties.isClusterManager()) {
                    clusterManager.saveControllerServices();
                } else {
                    controllerFacade.save();
                }

                return new StandardConfigurationResult<ControllerServiceDTO>(true, dtoFactory.createControllerServiceDto(controllerService));
            }
        });
    }

    @Override
    public ConfigurationSnapshot<ControllerServiceDTO> updateControllerService(final Revision revision, final ControllerServiceDTO controllerServiceDTO) {
        // if controller service does not exist, then create new controller service
        if (controllerServiceDAO.hasControllerService(controllerServiceDTO.getId()) == false) {
            return createControllerService(revision, controllerServiceDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ControllerServiceDTO>() {
            @Override
            public ConfigurationResult<ControllerServiceDTO> execute() {
                final ControllerServiceNode controllerService = controllerServiceDAO.updateControllerService(controllerServiceDTO);

                // save the update
                if (properties.isClusterManager()) {
                    clusterManager.saveControllerServices();
                } else {
                    controllerFacade.save();
                }

                return new StandardConfigurationResult<ControllerServiceDTO>(false, dtoFactory.createControllerServiceDto(controllerService));
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Set<ControllerServiceReferencingComponentDTO>> updateControllerServiceReferencingComponents(
            final Revision revision,
            final String controllerServiceId,
            final org.apache.nifi.controller.ScheduledState scheduledState,
            final org.apache.nifi.controller.service.ControllerServiceState controllerServiceState) {

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Set<ControllerServiceReferencingComponentDTO>>() {
            @Override
            public ConfigurationResult<Set<ControllerServiceReferencingComponentDTO>> execute() {
                final ControllerServiceReference reference = controllerServiceDAO.updateControllerServiceReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);

                return new StandardConfigurationResult<Set<ControllerServiceReferencingComponentDTO>>(false, dtoFactory.createControllerServiceReferencingComponentsDto(reference));
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> deleteControllerService(final Revision revision, final String controllerServiceId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // delete the label
                controllerServiceDAO.deleteControllerService(controllerServiceId);

                // save the update
                if (properties.isClusterManager()) {
                    clusterManager.saveControllerServices();
                } else {
                    controllerFacade.save();
                }

                return new StandardConfigurationResult<Void>(false, null);
            }
        });
    }


    @Override
    public ConfigurationSnapshot<ReportingTaskDTO> createReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ReportingTaskDTO>() {
            @Override
            public ConfigurationResult<ReportingTaskDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(reportingTaskDTO.getId())) {
                    reportingTaskDTO.setId(UUID.randomUUID().toString());
                }

                // create the reporting
                final ReportingTaskNode reportingTask = reportingTaskDAO.createReportingTask(reportingTaskDTO);

                // save the update
                if (properties.isClusterManager()) {
                    clusterManager.saveReportingTasks();
                } else {
                    controllerFacade.save();
                }

                return new StandardConfigurationResult<ReportingTaskDTO>(true, dtoFactory.createReportingTaskDto(reportingTask));
            }
        });
    }

    @Override
    public ConfigurationSnapshot<ReportingTaskDTO> updateReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        // if reporting task does not exist, then create new reporting task
        if (reportingTaskDAO.hasReportingTask(reportingTaskDTO.getId()) == false) {
            return createReportingTask(revision, reportingTaskDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ReportingTaskDTO>() {
            @Override
            public ConfigurationResult<ReportingTaskDTO> execute() {
                final ReportingTaskNode reportingTask = reportingTaskDAO.updateReportingTask(reportingTaskDTO);

                // save the update
                if (properties.isClusterManager()) {
                    clusterManager.saveReportingTasks();
                } else {
                    controllerFacade.save();
                }

                return new StandardConfigurationResult<ReportingTaskDTO>(false, dtoFactory.createReportingTaskDto(reportingTask));
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> deleteReportingTask(final Revision revision, final String reportingTaskId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // delete the label
                reportingTaskDAO.deleteReportingTask(reportingTaskId);

                // save the update
                if (properties.isClusterManager()) {
                    clusterManager.saveReportingTasks();
                } else {
                    controllerFacade.save();
                }

                return new StandardConfigurationResult<Void>(false, null);
            }
        });
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
    public RevisionDTO getRevision() {
        return dtoFactory.createRevisionDTO(optimisticLockingManager.getLastModification());
    }

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
        return controllerFacade.getProcessGroupStatus(groupId);
    }

    @Override
    public ControllerStatusDTO getControllerStatus() {
        return controllerFacade.getControllerStatus();
    }

    @Override
    public ComponentStateDTO getProcessorState(String processorId) {
        final StateMap clusterState = isClustered() ? processorDAO.getState(processorId, Scope.CLUSTER) : null;
        final StateMap localState = processorDAO.getState(processorId, Scope.LOCAL);

        // processor will be non null as it was already found when getting the state
        final ProcessorNode processor = processorDAO.getProcessor(processorId);
        return dtoFactory.createComponentStateDTO(processorId, processor.getProcessor().getClass(), localState, clusterState);
    }

    @Override
    public ComponentStateDTO getControllerServiceState(String controllerServiceId) {
        final StateMap clusterState = isClustered() ? controllerServiceDAO.getState(controllerServiceId, Scope.CLUSTER) : null;
        final StateMap localState = controllerServiceDAO.getState(controllerServiceId, Scope.LOCAL);

        // controller service will be non null as it was already found when getting the state
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        return dtoFactory.createComponentStateDTO(controllerServiceId, controllerService.getControllerServiceImplementation().getClass(), localState, clusterState);
    }

    @Override
    public ComponentStateDTO getReportingTaskState(String reportingTaskId) {
        final StateMap clusterState = isClustered() ? reportingTaskDAO.getState(reportingTaskId, Scope.CLUSTER) : null;
        final StateMap localState = reportingTaskDAO.getState(reportingTaskId, Scope.LOCAL);

        // reporting task will be non null as it was already found when getting the state
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        return dtoFactory.createComponentStateDTO(reportingTaskId, reportingTask.getReportingTask().getClass(), localState, clusterState);
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
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        Set<ConnectionEntity> connections = new LinkedHashSet<>();
        for (Connection connection : connectionDAO.getConnections(groupId)) {
            connections.add(entityFactory.createConnectionEntity(dtoFactory.createConnectionDto(connection), null, dtoFactory.createAccessPolicyDto(connection)));
        }
        return connections;
    }

    @Override
    public ConnectionEntity getConnection(String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        connection.authorize(authorizer, RequestAction.READ);
        return entityFactory.createConnectionEntity(dtoFactory.createConnectionDto(connectionDAO.getConnection(connectionId)), null, dtoFactory.createAccessPolicyDto(connection));
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
        final Connection connection = connectionDAO.getConnection(connectionId);
        connection.authorize(authorizer, RequestAction.WRITE);
        return dtoFactory.createFlowFileDTO(connectionDAO.getFlowFile(connectionId, flowFileUuid));
    }

    @Override
    public ConnectionStatusDTO getConnectionStatus(String connectionId) {
        return controllerFacade.getConnectionStatus(connectionId);
    }

    @Override
    public StatusHistoryDTO getConnectionStatusHistory(String connectionId) {
        return controllerFacade.getConnectionStatusHistory(connectionId);
    }

    @Override
    public Set<ProcessorEntity> getProcessors(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        Set<ProcessorEntity> processors = new LinkedHashSet<>();
        for (ProcessorNode processor : processorDAO.getProcessors(groupId)) {
            processors.add(entityFactory.createProcessorEntity(dtoFactory.createProcessorDto(processor), null, dtoFactory.createAccessPolicyDto(processor)));
        }
        return processors;
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
        final ProcessorNode processor = processorDAO.getProcessor(id);
        processor.authorize(authorizer, RequestAction.READ);
        return entityFactory.createProcessorEntity(dtoFactory.createProcessorDto(processor), null, dtoFactory.createAccessPolicyDto(processor));
    }

    @Override
    public PropertyDescriptorDTO getProcessorPropertyDescriptor(String id, String property) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        PropertyDescriptor descriptor = processor.getPropertyDescriptor(property);

        // return an invalid descriptor if the processor doesn't suppor this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor);
    }

    @Override
    public ProcessorStatusDTO getProcessorStatus(String id) {
        return controllerFacade.getProcessorStatus(id);
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
        for (final ListIterator<Bulletin> bulletinIter = results.listIterator(results.size()); bulletinIter.hasPrevious();) {
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
        final Set<PortDTO> inputPorts = new LinkedHashSet<>();
        for (RootGroupPort inputPort : controllerFacade.getInputPorts()) {
            if (isUserAuthorized(user, inputPort)) {
                final PortDTO dto = new PortDTO();
                dto.setId(inputPort.getIdentifier());
                dto.setName(inputPort.getName());
                dto.setComments(inputPort.getComments());
                dto.setState(inputPort.getScheduledState().toString());
                inputPorts.add(dto);
            }
        }

        // serialize the output ports this NiFi has access to
        final Set<PortDTO> outputPorts = new LinkedHashSet<>();
        for (RootGroupPort outputPort : controllerFacade.getOutputPorts()) {
            if (isUserAuthorized(user, outputPort)) {
                final PortDTO dto = new PortDTO();
                dto.setId(outputPort.getIdentifier());
                dto.setName(outputPort.getName());
                dto.setComments(outputPort.getComments());
                dto.setState(outputPort.getScheduledState().toString());
                outputPorts.add(dto);
            }
        }

        // get the root group
        final ProcessGroup rootGroup = processGroupDAO.getProcessGroup(controllerFacade.getRootGroupId());
        final ProcessGroupCounts counts = rootGroup.getCounts();

        // create the controller dto
        final ControllerDTO controllerDTO = new ControllerDTO();
        controllerDTO.setId(controllerFacade.getRootGroupId());
        controllerDTO.setInstanceId(controllerFacade.getInstanceId());
        controllerDTO.setName(controllerFacade.getName());
        controllerDTO.setComments(controllerFacade.getComments());
        controllerDTO.setInputPorts(inputPorts);
        controllerDTO.setOutputPorts(outputPorts);
        controllerDTO.setInputPortCount(inputPorts.size());
        controllerDTO.setOutputPortCount(outputPorts.size());
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
    public ControllerConfigurationDTO getControllerConfiguration() {
        ControllerConfigurationDTO controllerConfig = new ControllerConfigurationDTO();
        controllerConfig.setName(controllerFacade.getName());
        controllerConfig.setComments(controllerFacade.getComments());
        controllerConfig.setMaxTimerDrivenThreadCount(controllerFacade.getMaxTimerDrivenThreadCount());
        controllerConfig.setMaxEventDrivenThreadCount(controllerFacade.getMaxEventDrivenThreadCount());

        // get the refresh interval
        final long refreshInterval = FormatUtils.getTimeDuration(properties.getAutoRefreshInterval(), TimeUnit.SECONDS);
        controllerConfig.setAutoRefreshIntervalSeconds(refreshInterval);

        final Date now = new Date();
        controllerConfig.setTimeOffset(TimeZone.getDefault().getOffset(now.getTime()));
        controllerConfig.setCurrentTime(now);

        // determine the site to site configuration
        if (isClustered()) {
            controllerConfig.setSiteToSiteSecure(controllerFacade.isClusterManagerRemoteSiteCommsSecure());
        } else {
            controllerConfig.setSiteToSiteSecure(controllerFacade.isRemoteSiteCommsSecure());
        }

        return controllerConfig;
    }

    @Override
    public Set<LabelEntity> getLabels(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        Set<LabelEntity> labels = new LinkedHashSet<>();
        for (Label label : labelDAO.getLabels(groupId)) {
            labels.add(entityFactory.createLabelEntity(dtoFactory.createLabelDto(label), null, dtoFactory.createAccessPolicyDto(label)));
        }
        return labels;
    }

    @Override
    public LabelEntity getLabel(String labelId) {
        final Label label = labelDAO.getLabel(labelId);
        label.authorize(authorizer, RequestAction.READ);
        return entityFactory.createLabelEntity(dtoFactory.createLabelDto(label), null, dtoFactory.createAccessPolicyDto(label));
    }

    @Override
    public Set<FunnelEntity> getFunnels(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        final Set<FunnelEntity> funnelDtos = new LinkedHashSet<>();
        for (Funnel funnel : funnelDAO.getFunnels(groupId)) {
            funnelDtos.add(entityFactory.createFunnelEntity(dtoFactory.createFunnelDto(funnel), null, dtoFactory.createAccessPolicyDto(funnel)));
        }
        return funnelDtos;
    }

    @Override
    public FunnelEntity getFunnel(String funnelId) {
        final Funnel funnel = funnelDAO.getFunnel(funnelId);
        funnel.authorize(authorizer, RequestAction.READ);
        return entityFactory.createFunnelEntity(dtoFactory.createFunnelDto(funnel), null, dtoFactory.createAccessPolicyDto(funnel));
    }

    @Override
    public SnippetDTO getSnippet(String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);
        final SnippetDTO snippetDTO = dtoFactory.createSnippetDto(snippet);
        return snippetDTO;
    }

    @Override
    public Set<PortEntity> getInputPorts(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        final Set<PortEntity> ports = new LinkedHashSet<>();
        for (Port port : inputPortDAO.getPorts(groupId)) {
            ports.add(entityFactory.createPortEntity(dtoFactory.createPortDto(port), null, dtoFactory.createAccessPolicyDto(port)));
        }
        return ports;
    }

    @Override
    public Set<PortEntity> getOutputPorts(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        Set<PortEntity> ports = new LinkedHashSet<>();
        for (Port port : outputPortDAO.getPorts(groupId)) {
            ports.add(entityFactory.createPortEntity(dtoFactory.createPortDto(port), null, dtoFactory.createAccessPolicyDto(port)));
        }
        return ports;
    }

    @Override
    public Set<ProcessGroupEntity> getProcessGroups(String parentGroupId) {
        final ProcessGroup parentGroup = processGroupDAO.getProcessGroup(parentGroupId);
        parentGroup.authorize(authorizer, RequestAction.READ);

        Set<ProcessGroupEntity> processGroups = new LinkedHashSet<>();
        for (ProcessGroup group : processGroupDAO.getProcessGroups(parentGroupId)) {
            processGroups.add(entityFactory.createProcessGroupEntity(dtoFactory.createProcessGroupDto(group), null, dtoFactory.createAccessPolicyDto(group)));
        }
        return processGroups;
    }

    @Override
    public Set<RemoteProcessGroupEntity> getRemoteProcessGroups(String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.authorize(authorizer, RequestAction.READ);

        Set<RemoteProcessGroupEntity> remoteProcessGroups = new LinkedHashSet<>();
        for (RemoteProcessGroup rpg : remoteProcessGroupDAO.getRemoteProcessGroups(groupId)) {
            remoteProcessGroups.add(entityFactory.createRemoteProcessGroupEntity(dtoFactory.createRemoteProcessGroupDto(rpg), null, dtoFactory.createAccessPolicyDto(rpg)));
        }
        return remoteProcessGroups;
    }

    @Override
    public PortEntity getInputPort(String inputPortId) {
        final Port port = inputPortDAO.getPort(inputPortId);
        port.authorize(authorizer, RequestAction.READ);
        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), null, dtoFactory.createAccessPolicyDto(port));
    }

    @Override
    public PortStatusDTO getInputPortStatus(String inputPortId) {
        return controllerFacade.getInputPortStatus(inputPortId);
    }

    @Override
    public PortEntity getOutputPort(String outputPortId) {
        final Port port = outputPortDAO.getPort(outputPortId);
        port.authorize(authorizer, RequestAction.READ);
        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), null, dtoFactory.createAccessPolicyDto(port));
    }

    @Override
    public PortStatusDTO getOutputPortStatus(String outputPortId) {
        return controllerFacade.getOutputPortStatus(outputPortId);
    }

    @Override
    public RemoteProcessGroupEntity getRemoteProcessGroup(String remoteProcessGroupId) {
        final RemoteProcessGroup rpg = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        rpg.authorize(authorizer, RequestAction.READ);
        return entityFactory.createRemoteProcessGroupEntity(dtoFactory.createRemoteProcessGroupDto(rpg), null, dtoFactory.createAccessPolicyDto(rpg));
    }

    @Override
    public RemoteProcessGroupStatusDTO getRemoteProcessGroupStatus(String id) {
        return controllerFacade.getRemoteProcessGroupStatus(id);
    }

    @Override
    public StatusHistoryDTO getRemoteProcessGroupStatusHistory(String id) {
        return controllerFacade.getRemoteProcessGroupStatusHistory(id);
    }

    @Override
    public ConfigurationSnapshot<ProcessGroupFlowDTO> getProcessGroupFlow(String groupId, boolean recurse) {
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        Revision revision = optimisticLockingManager.getLastModification().getRevision();
        ConfigurationSnapshot<ProcessGroupFlowDTO> response = new ConfigurationSnapshot<>(revision.getVersion(), dtoFactory.createProcessGroupFlowDto(processGroup, recurse));
        return response;
    }

    @Override
    public ProcessGroupEntity getProcessGroup(String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        processGroup.authorize(authorizer, RequestAction.READ);
        return entityFactory.createProcessGroupEntity(dtoFactory.createProcessGroupDto(processGroup), null, dtoFactory.createAccessPolicyDto(processGroup));
    }

    @Override
    public Set<ControllerServiceDTO> getControllerServices() {
        final Set<ControllerServiceDTO> controllerServiceDtos = new LinkedHashSet<>();
        for (ControllerServiceNode controllerService : controllerServiceDAO.getControllerServices()) {
            controllerServiceDtos.add(dtoFactory.createControllerServiceDto(controllerService));
        }
        return controllerServiceDtos;
    }

    @Override
    public ControllerServiceDTO getControllerService(String controllerServiceId) {
        return dtoFactory.createControllerServiceDto(controllerServiceDAO.getControllerService(controllerServiceId));
    }

    @Override
    public PropertyDescriptorDTO getControllerServicePropertyDescriptor(String id, String property) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(id);
        PropertyDescriptor descriptor = controllerService.getControllerServiceImplementation().getPropertyDescriptor(property);

        // return an invalid descriptor if the controller service doesn't support this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor);
    }

    @Override
    public Set<ControllerServiceReferencingComponentDTO> getControllerServiceReferencingComponents(String controllerServiceId) {
        final ControllerServiceNode service = controllerServiceDAO.getControllerService(controllerServiceId);
        return dtoFactory.createControllerServiceReferencingComponentsDto(service.getReferences());
    }

    @Override
    public Set<ReportingTaskDTO> getReportingTasks() {
        final Set<ReportingTaskDTO> reportingTaskDtos = new LinkedHashSet<>();
        for (ReportingTaskNode reportingTask : reportingTaskDAO.getReportingTasks()) {
            reportingTaskDtos.add(dtoFactory.createReportingTaskDto(reportingTask));
        }
        return reportingTaskDtos;
    }

    @Override
    public ReportingTaskDTO getReportingTask(String reportingTaskId) {
        return dtoFactory.createReportingTaskDto(reportingTaskDAO.getReportingTask(reportingTaskId));
    }

    @Override
    public PropertyDescriptorDTO getReportingTaskPropertyDescriptor(String id, String property) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(id);
        PropertyDescriptor descriptor = reportingTask.getReportingTask().getPropertyDescriptor(property);

        // return an invalid descriptor if the reporting task doesn't support this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor);
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

    public void setOptimisticLockingManager(OptimisticLockingManager optimisticLockingManager) {
        this.optimisticLockingManager = optimisticLockingManager;
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

    /**
     * Utility method to get the oldest of the two specified dates.
     */
    private Date getOldestDate(final Date date1, final Date date2) {
        if (date1 == null && date2 == null) {
            return null;
        } else if (date1 == null) {
            return date2;
        } else if (date2 == null) {
            return date1;
        }

        if (date1.before(date2)) {
            return date1;
        } else if (date1.after(date2)) {
            return date2;
        } else {
            return date1;
        }
    }

    /**
     * Utility method to get the newest of the two specified dates.
     */
    private Date getNewestDate(final Date date1, final Date date2) {
        if (date1 == null && date2 == null) {
            return null;
        } else if (date1 == null) {
            return date2;
        } else if (date2 == null) {
            return date1;
        }

        if (date1.before(date2)) {
            return date2;
        } else if (date1.after(date2)) {
            return date1;
        } else {
            return date1;
        }
    }
}
