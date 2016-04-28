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
import org.apache.nifi.authorization.Resource;
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
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
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
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.NiFiComponentDTO;
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
import org.apache.nifi.web.security.user.NiFiUserUtils;
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
    public ConfigurationSnapshot<ConnectionDTO> updateConnection(final Revision revision, final ConnectionDTO connectionDTO) {
        // if connection does not exist, then create new connection
        if (connectionDAO.hasConnection(connectionDTO.getId()) == false) {
            return createConnection(revision, connectionDTO.getParentGroupId(), connectionDTO);
        }

        return updateComponent(revision, () -> connectionDAO.updateConnection(connectionDTO), connection -> dtoFactory.createConnectionDto(connection));
    }

    @Override
    public ConfigurationSnapshot<ProcessorDTO> updateProcessor(final Revision revision, final ProcessorDTO processorDTO) {
        // if processor does not exist, then create new processor
        if (processorDAO.hasProcessor(processorDTO.getId()) == false) {
            return createProcessor(revision, processorDTO.getParentGroupId(), processorDTO);
        }

        return updateComponent(revision, () -> processorDAO.updateProcessor(processorDTO), proc -> dtoFactory.createProcessorDto(proc));
    }

    @Override
    public ConfigurationSnapshot<LabelDTO> updateLabel(final Revision revision, final LabelDTO labelDTO) {
        // if label does not exist, then create new label
        if (labelDAO.hasLabel(labelDTO.getId()) == false) {
            return createLabel(revision, labelDTO.getParentGroupId(), labelDTO);
        }

        return updateComponent(revision, () -> labelDAO.updateLabel(labelDTO), label -> dtoFactory.createLabelDto(label));
    }

    @Override
    public ConfigurationSnapshot<FunnelDTO> updateFunnel(final Revision revision, final FunnelDTO funnelDTO) {
        // if label does not exist, then create new label
        if (funnelDAO.hasFunnel(funnelDTO.getId()) == false) {
            return createFunnel(revision, funnelDTO.getParentGroupId(), funnelDTO);
        }

        return updateComponent(revision, () -> funnelDAO.updateFunnel(funnelDTO), funnel -> dtoFactory.createFunnelDto(funnel));
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
    private <D, C> ConfigurationSnapshot<D> updateComponent(final Revision revision, final Supplier<C> daoUpdate, final Function<C, D> dtoCreation) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<D>() {
            @Override
            public ConfigurationResult<D> execute() {
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

        return updateComponent(revision,
            () -> snippetDAO.updateSnippet(snippetDto),
            snippet -> {
                final SnippetDTO responseSnippetDto = dtoFactory.createSnippetDto(snippet);
                responseSnippetDto.setContents(snippetUtils.populateFlowSnippet(snippet, false, false));
                return responseSnippetDto;
            });
    }

    @Override
    public ConfigurationSnapshot<PortDTO> updateInputPort(final Revision revision, final PortDTO inputPortDTO) {
        // if input port does not exist, then create new input port
        if (inputPortDAO.hasPort(inputPortDTO.getId()) == false) {
            return createInputPort(revision, inputPortDTO.getParentGroupId(), inputPortDTO);
        }

        return updateComponent(revision, () -> inputPortDAO.updatePort(inputPortDTO), port -> dtoFactory.createPortDto(port));
    }

    @Override
    public ConfigurationSnapshot<PortDTO> updateOutputPort(final Revision revision, final PortDTO outputPortDTO) {
        // if output port does not exist, then create new output port
        if (outputPortDAO.hasPort(outputPortDTO.getId()) == false) {
            return createOutputPort(revision, outputPortDTO.getParentGroupId(), outputPortDTO);
        }

        return updateComponent(revision, () -> outputPortDAO.updatePort(outputPortDTO), port -> dtoFactory.createPortDto(port));
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupDTO> updateRemoteProcessGroup(final Revision revision, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // if controller reference does not exist, then create new controller reference
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupDTO.getId()) == false) {
            return createRemoteProcessGroup(revision, remoteProcessGroupDTO.getParentGroupId(), remoteProcessGroupDTO);
        }

        return updateComponent(revision,
            () -> remoteProcessGroupDAO.updateRemoteProcessGroup(remoteProcessGroupDTO),
            remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupInputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        return updateComponent(revision,
            () -> remoteProcessGroupDAO.updateRemoteProcessGroupInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
            remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupOutputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        return updateComponent(revision,
            () -> remoteProcessGroupDAO.updateRemoteProcessGroupOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
            remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));
    }

    @Override
    public ConfigurationSnapshot<ProcessGroupDTO> updateProcessGroup(final Revision revision, final ProcessGroupDTO processGroupDTO) {
        // if process group does not exist, then create new process group
        if (processGroupDAO.hasProcessGroup(processGroupDTO.getId()) == false) {
            if (processGroupDTO.getParentGroupId() == null) {
                throw new IllegalArgumentException("Unable to create the specified process group since the parent group was not specified.");
            } else {
                return createProcessGroup(processGroupDTO.getParentGroupId(), revision, processGroupDTO);
            }
        }

        return updateComponent(revision, () -> processGroupDAO.updateProcessGroup(processGroupDTO), processGroup -> dtoFactory.createProcessGroupDto(processGroup));
    }

    @Override
    public ConfigurationSnapshot<ControllerConfigurationDTO> updateControllerConfiguration(final Revision revision, final ControllerConfigurationDTO controllerConfigurationDTO) {
        return updateComponent(revision,
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
    public ConfigurationSnapshot<Void> deleteConnection(final Revision revision, final String connectionId) {
        return deleteComponent(revision, () -> connectionDAO.deleteConnection(connectionId));
    }

    @Override
    public DropRequestDTO deleteFlowFileDropRequest(String connectionId, String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.deleteFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO deleteFlowFileListingRequest(String connectionId, String listingRequestId) {
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.deleteFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        final Connection connection = connectionDAO.getConnection(connectionId);
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public ConfigurationSnapshot<Void> deleteProcessor(final Revision revision, final String processorId) {
        return deleteComponent(revision, () -> processorDAO.deleteProcessor(processorId));
    }

    @Override
    public ConfigurationSnapshot<Void> deleteLabel(final Revision revision, final String labelId) {
        return deleteComponent(revision, () -> labelDAO.deleteLabel(labelId));
    }

    @Override
    public ConfigurationSnapshot<Void> deleteFunnel(final Revision revision, final String funnelId) {
        return deleteComponent(revision, () -> funnelDAO.deleteFunnel(funnelId));
    }

    /**
     * Deletes a component using the Optimistic Locking Manager
     *
     * @param revision the current revision
     * @param action the action that deletes the component via the appropriate DAO object
     * @return a ConfigurationSnapshot that represents the new configuration
     */
    private ConfigurationSnapshot<Void> deleteComponent(final Revision revision, final Runnable action) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
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
        return deleteComponent(revision, () -> snippetDAO.deleteSnippet(snippetId));
    }

    @Override
    public ConfigurationSnapshot<Void> deleteInputPort(final Revision revision, final String inputPortId) {
        return deleteComponent(revision, () -> inputPortDAO.deletePort(inputPortId));
    }

    @Override
    public ConfigurationSnapshot<Void> deleteOutputPort(final Revision revision, final String outputPortId) {
        return deleteComponent(revision, () -> outputPortDAO.deletePort(outputPortId));
    }

    @Override
    public ConfigurationSnapshot<Void> deleteProcessGroup(final Revision revision, final String groupId) {
        return deleteComponent(revision, () -> processGroupDAO.deleteProcessGroup(groupId));
    }

    @Override
    public ConfigurationSnapshot<Void> deleteRemoteProcessGroup(final Revision revision, final String remoteProcessGroupId) {
        return deleteComponent(revision, () -> remoteProcessGroupDAO.deleteRemoteProcessGroup(remoteProcessGroupId));
    }

    @Override
    public void deleteTemplate(String id) {
        // create the template
        templateDAO.deleteTemplate(id);
    }

    @Override
    public ConfigurationSnapshot<ConnectionDTO> createConnection(final Revision revision, final String groupId, final ConnectionDTO connectionDTO) {
        return createComponent(revision, connectionDTO, () -> connectionDAO.createConnection(groupId, connectionDTO), connection -> dtoFactory.createConnectionDto(connection));
    }

    @Override
    public DropRequestDTO createFlowFileDropRequest(String connectionId, String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.createFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO createFlowFileListingRequest(String connectionId, String listingRequestId) {
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.createFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        final Connection connection = connectionDAO.getConnection(connectionId);
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public ConfigurationSnapshot<ProcessorDTO> createProcessor(final Revision revision, final String groupId, final ProcessorDTO processorDTO) {
        return createComponent(revision, processorDTO, () -> processorDAO.createProcessor(groupId, processorDTO), processor -> dtoFactory.createProcessorDto(processor));
    }

    @Override
    public ConfigurationSnapshot<LabelDTO> createLabel(final Revision revision, final String groupId, final LabelDTO labelDTO) {
        return createComponent(revision, labelDTO, () -> labelDAO.createLabel(groupId, labelDTO), label -> dtoFactory.createLabelDto(label));
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
    private <D, C> ConfigurationSnapshot<D> createComponent(final Revision revision, final NiFiComponentDTO componentDto,
        final Supplier<C> daoCreation, final Function<C, D> dtoCreation) {

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<D>() {
            @Override
            public ConfigurationResult<D> execute() {
                // ensure id is set
                if (StringUtils.isBlank(componentDto.getId())) {
                    componentDto.setId(UUID.randomUUID().toString());
                }

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
    public ConfigurationSnapshot<FunnelDTO> createFunnel(final Revision revision, final String groupId, final FunnelDTO funnelDTO) {
        return createComponent(revision, funnelDTO, () -> funnelDAO.createFunnel(groupId, funnelDTO), funnel -> dtoFactory.createFunnelDto(funnel));
    }

    private void validateSnippetContents(final FlowSnippetDTO flowSnippet) {
        // validate any processors
        if (flowSnippet.getProcessors() != null) {
            for (final ProcessorDTO processorDTO : flowSnippet.getProcessors()) {
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

        if (flowSnippet.getInputPorts() != null) {
            for (final PortDTO portDTO : flowSnippet.getInputPorts()) {
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

        if (flowSnippet.getOutputPorts() != null) {
            for (final PortDTO portDTO : flowSnippet.getOutputPorts()) {
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
        if (flowSnippet.getRemoteProcessGroups() != null) {
            for (final RemoteProcessGroupDTO remoteProcessGroupDTO : flowSnippet.getRemoteProcessGroups()) {
                final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
                if (remoteProcessGroup.getAuthorizationIssue() != null) {
                    remoteProcessGroupDTO.setAuthorizationIssues(Arrays.asList(remoteProcessGroup.getAuthorizationIssue()));
                }
            }
        }
    }

    @Override
    public ConfigurationSnapshot<FlowSnippetDTO> copySnippet(final Revision revision, final String groupId, final String snippetId, final Double originX, final Double originY) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<FlowSnippetDTO>() {
            @Override
            public ConfigurationResult<FlowSnippetDTO> execute() {
                String id = snippetId;

                // ensure id is set
                if (StringUtils.isBlank(id)) {
                    id = UUID.randomUUID().toString();
                }

                // create the new snippet
                final FlowSnippetDTO flowSnippet = snippetDAO.copySnippet(groupId, id, originX, originY);

                // validate the new snippet
                validateSnippetContents(flowSnippet);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult<FlowSnippetDTO>() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public FlowSnippetDTO getConfiguration() {
                        return flowSnippet;
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
                responseSnippetDTO.setContents(snippetUtils.populateFlowSnippet(snippet, false, false));

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
    public ConfigurationSnapshot<PortDTO> createInputPort(final Revision revision, final String groupId, final PortDTO inputPortDTO) {
        return createComponent(revision, inputPortDTO, () -> inputPortDAO.createPort(groupId, inputPortDTO), port -> dtoFactory.createPortDto(port));
    }

    @Override
    public ConfigurationSnapshot<PortDTO> createOutputPort(final Revision revision, final String groupId, final PortDTO outputPortDTO) {
        return createComponent(revision, outputPortDTO, () -> outputPortDAO.createPort(groupId, outputPortDTO), port -> dtoFactory.createPortDto(port));
    }

    @Override
    public ConfigurationSnapshot<ProcessGroupDTO> createProcessGroup(final String parentGroupId, final Revision revision, final ProcessGroupDTO processGroupDTO) {
        return createComponent(revision, processGroupDTO,
            () -> processGroupDAO.createProcessGroup(parentGroupId, processGroupDTO),
            processGroup -> dtoFactory.createProcessGroupDto(processGroup));
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupDTO> createRemoteProcessGroup(final Revision revision, final String groupId, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        return createComponent(revision, remoteProcessGroupDTO,
            () -> remoteProcessGroupDAO.createRemoteProcessGroup(groupId, remoteProcessGroupDTO),
            remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));
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
    public ConfigurationSnapshot<FlowSnippetDTO> createTemplateInstance(final Revision revision, final String groupId, final Double originX, final Double originY, final String templateId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<FlowSnippetDTO>() {
            @Override
            public ConfigurationResult<FlowSnippetDTO> execute() {
                // instantiate the template - there is no need to make another copy of the flow snippet since the actual template
                // was copied and this dto is only used to instantiate it's components (which as already completed)
                final FlowSnippetDTO flowSnippet = templateDAO.instantiateTemplate(groupId, originX, originY, templateId);

                // validate the new snippet
                validateSnippetContents(flowSnippet);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult<FlowSnippetDTO>() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public FlowSnippetDTO getConfiguration() {
                        return flowSnippet;
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
    public Set<ConnectionDTO> getConnections(String groupId) {
        Set<ConnectionDTO> connectionDtos = new LinkedHashSet<>();
        for (Connection connection : connectionDAO.getConnections(groupId)) {
            connectionDtos.add(dtoFactory.createConnectionDto(connection));
        }
        return connectionDtos;
    }

    @Override
    public ConnectionDTO getConnection(String connectionId) {
        return dtoFactory.createConnectionDto(connectionDAO.getConnection(connectionId));
    }

    @Override
    public DropRequestDTO getFlowFileDropRequest(String connectionId, String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.getFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO getFlowFileListingRequest(String connectionId, String listingRequestId) {
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.getFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        final Connection connection = connectionDAO.getConnection(connectionId);
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
        return controllerFacade.getConnectionStatus(connectionId);
    }

    @Override
    public StatusHistoryDTO getConnectionStatusHistory(String connectionId) {
        return controllerFacade.getConnectionStatusHistory(connectionId);
    }

    @Override
    public Set<ProcessorDTO> getProcessors(String groupId) {
        Set<ProcessorDTO> processorDtos = new LinkedHashSet<>();
        for (ProcessorNode processor : processorDAO.getProcessors(groupId)) {
            processorDtos.add(dtoFactory.createProcessorDto(processor));
        }
        return processorDtos;
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
    public ProcessorDTO getProcessor(String id) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        final ProcessorDTO processorDto = dtoFactory.createProcessorDto(processor);
        return processorDto;
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
    public Set<LabelDTO> getLabels(String groupId) {
        Set<LabelDTO> labelDtos = new LinkedHashSet<>();
        for (Label label : labelDAO.getLabels(groupId)) {
            labelDtos.add(dtoFactory.createLabelDto(label));
        }
        return labelDtos;
    }

    @Override
    public LabelDTO getLabel(String labelId) {
        return dtoFactory.createLabelDto(labelDAO.getLabel(labelId));
    }

    @Override
    public Set<FunnelDTO> getFunnels(String groupId) {
        Set<FunnelDTO> funnelDtos = new LinkedHashSet<>();
        for (Funnel funnel : funnelDAO.getFunnels(groupId)) {
            funnelDtos.add(dtoFactory.createFunnelDto(funnel));
        }
        return funnelDtos;
    }

    @Override
    public FunnelDTO getFunnel(String funnelId) {
        return dtoFactory.createFunnelDto(funnelDAO.getFunnel(funnelId));
    }

    @Override
    public SnippetDTO getSnippet(String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);
        final SnippetDTO snippetDTO = dtoFactory.createSnippetDto(snippet);
        snippetDTO.setContents(snippetUtils.populateFlowSnippet(snippet, false, false));
        return snippetDTO;
    }

    @Override
    public Set<PortDTO> getInputPorts(String groupId) {
        Set<PortDTO> portDtos = new LinkedHashSet<>();
        for (Port port : inputPortDAO.getPorts(groupId)) {
            portDtos.add(dtoFactory.createPortDto(port));
        }
        return portDtos;
    }

    @Override
    public Set<PortDTO> getOutputPorts(String groupId) {
        Set<PortDTO> portDtos = new LinkedHashSet<>();
        for (Port port : outputPortDAO.getPorts(groupId)) {
            portDtos.add(dtoFactory.createPortDto(port));
        }
        return portDtos;
    }

    @Override
    public Set<ProcessGroupDTO> getProcessGroups(String parentGroupId) {
        Set<ProcessGroupDTO> processGroupDtos = new LinkedHashSet<>();
        for (ProcessGroup groups : processGroupDAO.getProcessGroups(parentGroupId)) {
            processGroupDtos.add(dtoFactory.createProcessGroupDto(groups));
        }
        return processGroupDtos;
    }

    @Override
    public Set<RemoteProcessGroupDTO> getRemoteProcessGroups(String groupId) {
        Set<RemoteProcessGroupDTO> remoteProcessGroupDtos = new LinkedHashSet<>();
        for (RemoteProcessGroup remoteProcessGroup : remoteProcessGroupDAO.getRemoteProcessGroups(groupId)) {
            remoteProcessGroupDtos.add(dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));
        }
        return remoteProcessGroupDtos;
    }

    @Override
    public PortDTO getInputPort(String inputPortId) {
        return dtoFactory.createPortDto(inputPortDAO.getPort(inputPortId));
    }

    @Override
    public PortStatusDTO getInputPortStatus(String inputPortId) {
        return controllerFacade.getInputPortStatus(inputPortId);
    }

    @Override
    public PortDTO getOutputPort(String outputPortId) {
        return dtoFactory.createPortDto(outputPortDAO.getPort(outputPortId));
    }

    @Override
    public PortStatusDTO getOutputPortStatus(String outputPortId) {
        return controllerFacade.getOutputPortStatus(outputPortId);
    }

    @Override
    public RemoteProcessGroupDTO getRemoteProcessGroup(String remoteProcessGroupId) {
        return dtoFactory.createRemoteProcessGroupDto(remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId));
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
    public ConfigurationSnapshot<ProcessGroupDTO> getProcessGroup(String groupId, final boolean recurse) {
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        Revision revision = optimisticLockingManager.getLastModification().getRevision();
        ConfigurationSnapshot<ProcessGroupDTO> response = new ConfigurationSnapshot<>(revision.getVersion(), dtoFactory.createProcessGroupDto(processGroup, recurse));
        return response;
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
