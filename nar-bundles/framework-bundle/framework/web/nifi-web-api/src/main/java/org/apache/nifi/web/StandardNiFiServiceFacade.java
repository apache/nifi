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

import org.apache.nifi.web.OptimisticLockingManager;
import org.apache.nifi.web.ConfigurationSnapshot;
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
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.WebApplicationException;

import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.PurgeDetails;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.cluster.HeartbeatPayload;
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.cluster.manager.exception.IllegalClusterStateException;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.Template;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.user.NiFiUserGroup;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.NodeSystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PreviousValueDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.ProcessorHistoryDTO;
import org.apache.nifi.web.api.dto.PropertyHistoryDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ClusterConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterPortStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterRemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterStatusDTO;
import org.apache.nifi.web.api.dto.status.ClusterStatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.NodePortStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeRemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.FunnelDAO;
import org.apache.nifi.web.dao.LabelDAO;
import org.apache.nifi.web.dao.PortDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.dao.SnippetDAO;
import org.apache.nifi.web.dao.TemplateDAO;
import org.apache.nifi.web.util.DownloadableContent;
import org.apache.nifi.web.util.SnippetUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

/**
 * Implementation of NiFiServiceFacade that performs revision checking.
 */
public class StandardNiFiServiceFacade implements NiFiServiceFacade {

    private static final Logger logger = LoggerFactory.getLogger(StandardNiFiServiceFacade.class);
    private static final String INVALID_REVISION_ERROR = "Given revision %s does not match current revision %s.";
    private static final String SYNC_ERROR = "This NiFi instance has been updated by '%s'. Please refresh to synchronize the view.";

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
    private TemplateDAO templateDAO;

    // administrative services
    private AuditService auditService;
    private UserService userService;

    // cluster manager
    private WebClusterManager clusterManager;

    // properties
    private NiFiProperties properties;
    private DtoFactory dtoFactory;

    /**
     * Checks the specified revision against the current revision.
     *
     * @param revision The revision to check
     * @param clientId The client id
     * @return Whether or not the request should proceed
     * @throws NiFiCoreException If the specified revision is not current
     */
    private void checkRevision(Revision revision) {

        boolean approved = optimisticLockingManager.isCurrent(revision);

        if (!approved) {
            Revision currentRevision = optimisticLockingManager.getRevision();
            logger.debug("Revision check failed because current revision is " + currentRevision + " but supplied revision is " + revision);

            if (StringUtils.isBlank(currentRevision.getClientId()) || currentRevision.getVersion() == null) {
                throw new InvalidRevisionException(String.format(INVALID_REVISION_ERROR, revision, currentRevision));
            } else {
                throw new InvalidRevisionException(String.format(SYNC_ERROR, optimisticLockingManager.getLastModifier()));
            }
        }
    }

    /**
     * Increments the revision and updates the last modifier.
     *
     * @param revision
     * @return
     */
    private Revision updateRevision(Revision revision) {
        // update the client id and modifier
        final Revision updatedRevision = optimisticLockingManager.incrementRevision(revision.getClientId());

        // get the nifi user to extract the username
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            optimisticLockingManager.setLastModifier("unknown");
        } else {
            optimisticLockingManager.setLastModifier(user.getUserName());
        }

        return updatedRevision;
    }

    // -----------------------------------------
    // Verification Operations
    // -----------------------------------------
    @Override
    public void verifyCreateConnection(String groupId, ConnectionDTO connectionDTO) {
        connectionDAO.verifyCreate(groupId, connectionDTO);
    }

    @Override
    public void verifyUpdateConnection(String groupId, ConnectionDTO connectionDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (connectionDAO.hasConnection(groupId, connectionDTO.getId())) {
            connectionDAO.verifyUpdate(groupId, connectionDTO);
        } else {
            connectionDAO.verifyCreate(groupId, connectionDTO);
        }
    }

    @Override
    public void verifyDeleteConnection(String groupId, String connectionId) {
        connectionDAO.verifyDelete(groupId, connectionId);
    }

    @Override
    public void verifyDeleteFunnel(String groupId, String funnelId) {
        funnelDAO.verifyDelete(groupId, funnelId);
    }

    @Override
    public void verifyUpdateInputPort(String groupId, PortDTO inputPortDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (inputPortDAO.hasPort(groupId, inputPortDTO.getId())) {
            inputPortDAO.verifyUpdate(groupId, inputPortDTO);
        }
    }

    @Override
    public void verifyDeleteInputPort(String groupId, String inputPortId) {
        inputPortDAO.verifyDelete(groupId, inputPortId);
    }

    @Override
    public void verifyUpdateOutputPort(String groupId, PortDTO outputPortDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (outputPortDAO.hasPort(groupId, outputPortDTO.getId())) {
            outputPortDAO.verifyUpdate(groupId, outputPortDTO);
        }
    }

    @Override
    public void verifyDeleteOutputPort(String groupId, String outputPortId) {
        outputPortDAO.verifyDelete(groupId, outputPortId);
    }

    @Override
    public void verifyUpdateProcessor(ProcessorDTO processorDTO) {
        final String groupId = controllerFacade.findProcessGroupIdForProcessor(processorDTO.getId());

        // if processor does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (groupId != null) {
            verifyUpdateProcessor(groupId, processorDTO);
        }
    }

    @Override
    public void verifyUpdateProcessor(String groupId, ProcessorDTO processorDTO) {
        // if processor does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (processorDAO.hasProcessor(groupId, processorDTO.getId())) {
            processorDAO.verifyUpdate(groupId, processorDTO);
        }
    }

    @Override
    public void verifyDeleteProcessor(String groupId, String processorId) {
        processorDAO.verifyDelete(groupId, processorId);
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
    public void verifyUpdateRemoteProcessGroup(String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // if remote group does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(groupId, remoteProcessGroupDTO.getId())) {
            remoteProcessGroupDAO.verifyUpdate(groupId, remoteProcessGroupDTO);
        }
    }

    @Override
    public void verifyUpdateRemoteProcessGroupInputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        remoteProcessGroupDAO.verifyUpdateInputPort(groupId, remoteProcessGroupId, remoteProcessGroupPortDTO);
    }

    @Override
    public void verifyUpdateRemoteProcessGroupOutputPort(String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        remoteProcessGroupDAO.verifyUpdateOutputPort(groupId, remoteProcessGroupId, remoteProcessGroupPortDTO);
    }

    @Override
    public void verifyDeleteRemoteProcessGroup(String groupId, String remoteProcessGroupId) {
        remoteProcessGroupDAO.verifyDelete(groupId, remoteProcessGroupId);
    }

    // -----------------------------------------
    // Write Operations
    // -----------------------------------------
    @Override
    public ConfigurationSnapshot<ConnectionDTO> updateConnection(Revision revision, String groupId, ConnectionDTO connectionDTO) {

        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if connection does not exist, then create new connection
        if (connectionDAO.hasConnection(groupId, connectionDTO.getId()) == false) {
            return createConnection(revision, groupId, connectionDTO);
        }

        final Connection connection = connectionDAO.updateConnection(groupId, connectionDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<ConnectionDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createConnectionDto(connection));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<ProcessorDTO> updateProcessor(Revision revision, String groupId, ProcessorDTO processorDTO) {

        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if processor does not exist, then create new processor
        if (processorDAO.hasProcessor(groupId, processorDTO.getId()) == false) {
            return createProcessor(revision, groupId, processorDTO);
        }

        // update the processor
        ProcessorNode processor = processorDAO.updateProcessor(groupId, processorDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<ProcessorDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createProcessorDto(processor));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<LabelDTO> updateLabel(Revision revision, String groupId, LabelDTO labelDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if label does not exist, then create new label
        if (labelDAO.hasLabel(groupId, labelDTO.getId()) == false) {
            return createLabel(revision, groupId, labelDTO);
        }

        // update the existing label
        final Label label = labelDAO.updateLabel(groupId, labelDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<LabelDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createLabelDto(label));

        // save updated controller
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<FunnelDTO> updateFunnel(Revision revision, String groupId, FunnelDTO funnelDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if label does not exist, then create new label
        if (funnelDAO.hasFunnel(groupId, funnelDTO.getId()) == false) {
            return createFunnel(revision, groupId, funnelDTO);
        }

        // update the existing label
        final Funnel funnel = funnelDAO.updateFunnel(groupId, funnelDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<FunnelDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createFunnelDto(funnel));

        // save updated controller
        controllerFacade.save();

        return response;
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
    public ConfigurationSnapshot<SnippetDTO> updateSnippet(Revision revision, SnippetDTO snippetDto) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if label does not exist, then create new label
        if (snippetDAO.hasSnippet(snippetDto.getId()) == false) {
            return createSnippet(revision, snippetDto);
        }

        // update the snippet
        final Snippet snippet = snippetDAO.updateSnippet(snippetDto);

        // build the snippet dto
        final SnippetDTO responseSnippetDto = dtoFactory.createSnippetDto(snippet);
        responseSnippetDto.setContents(snippetUtils.populateFlowSnippet(snippet, false));

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<SnippetDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), responseSnippetDto);

        // save updated controller if applicable
        if (snippetDto.getParentGroupId() != null && snippet.isLinked()) {
            controllerFacade.save();
        }

        return response;
    }

    @Override
    public ConfigurationSnapshot<PortDTO> updateInputPort(Revision revision, String groupId, PortDTO inputPortDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if input port does not exist, then create new input port
        if (inputPortDAO.hasPort(groupId, inputPortDTO.getId()) == false) {
            return createInputPort(revision, groupId, inputPortDTO);
        }

        final Port inputPort = inputPortDAO.updatePort(groupId, inputPortDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<PortDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createPortDto(inputPort));

        // save updated controller
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<PortDTO> updateOutputPort(Revision revision, String groupId, PortDTO outputPortDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if output port does not exist, then create new output port
        if (outputPortDAO.hasPort(groupId, outputPortDTO.getId()) == false) {
            return createOutputPort(revision, groupId, outputPortDTO);
        }

        final Port outputPort = outputPortDAO.updatePort(groupId, outputPortDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<PortDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createPortDto(outputPort));

        // save updated controller
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupDTO> updateRemoteProcessGroup(Revision revision, String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if controller reference does not exist, then create new controller reference
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(groupId, remoteProcessGroupDTO.getId()) == false) {
            return createRemoteProcessGroup(revision, groupId, remoteProcessGroupDTO);
        }

        RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.updateRemoteProcessGroup(groupId, remoteProcessGroupDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<RemoteProcessGroupDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        // save updated controller
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupInputPort(Revision revision, String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // update the remote port
        RemoteGroupPort remoteGroupPort = remoteProcessGroupDAO.updateRemoteProcessGroupInputPort(groupId, remoteProcessGroupId, remoteProcessGroupPortDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<RemoteProcessGroupPortDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));

        // save updated controller
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupOutputPort(Revision revision, String groupId, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // update the remote port
        RemoteGroupPort remoteGroupPort = remoteProcessGroupDAO.updateRemoteProcessGroupOutputPort(groupId, remoteProcessGroupId, remoteProcessGroupPortDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<RemoteProcessGroupPortDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));

        // save updated controller
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<ProcessGroupDTO> updateProcessGroup(Revision revision, String parentGroupId, ProcessGroupDTO processGroupDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // if process group does not exist, then create new process group
        if (processGroupDAO.hasProcessGroup(processGroupDTO.getId()) == false) {
            if (parentGroupId == null) {
                throw new IllegalArgumentException("Unable to create the specified process group since the parent group was not specified.");
            } else {
                return createProcessGroup(parentGroupId, revision, processGroupDTO);
            }
        }

        // update the process group
        ProcessGroup processGroup = processGroupDAO.updateProcessGroup(processGroupDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<ProcessGroupDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createProcessGroupDto(processGroup));

        // save updated controller
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<ControllerConfigurationDTO> updateControllerConfiguration(Revision revision, ControllerConfigurationDTO controllerConfigurationDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

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

        // create the controller configuration dto
        ControllerConfigurationDTO controllerConfig = getControllerConfiguration();

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<ControllerConfigurationDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), controllerConfig);

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public NodeDTO updateNode(NodeDTO nodeDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }
        final String userDn = user.getDn();

        if (Node.Status.CONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterManager.requestReconnection(nodeDTO.getNodeId(), userDn);
        } else if (Node.Status.DISCONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterManager.requestDisconnection(nodeDTO.getNodeId(), userDn);
        } else {
            // handle primary
            final Boolean primary = nodeDTO.isPrimary();
            if (primary != null && primary) {
                clusterManager.setPrimaryNode(nodeDTO.getNodeId(), userDn);
            }
        }

        final String nodeId = nodeDTO.getNodeId();
        return dtoFactory.createNodeDTO(clusterManager.getNode(nodeId), clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId));
    }

    @Override
    public CounterDTO updateCounter(String counterId) {
        return dtoFactory.createCounterDto(controllerFacade.resetCounter(counterId));
    }

    @Override
    public ConfigurationSnapshot<Void> deleteConnection(Revision revision, String groupId, String connectionId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        connectionDAO.deleteConnection(groupId, connectionId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<Void> deleteProcessor(Revision revision, String groupId, String processorId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // delete the processor and synchronize the connection state
        processorDAO.deleteProcessor(groupId, processorId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<Void> deleteLabel(Revision revision, String groupId, String labelId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // delete the label
        labelDAO.deleteLabel(groupId, labelId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<Void> deleteFunnel(Revision revision, String groupId, String funnelId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // delete the label
        funnelDAO.deleteFunnel(groupId, funnelId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public void verifyDeleteSnippet(String id) {
        snippetDAO.verifyDelete(id);
    }

    @Override
    public ConfigurationSnapshot<Void> deleteSnippet(Revision revision, String snippetId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // determine if this snippet was linked to the data flow
        Snippet snippet = snippetDAO.getSnippet(snippetId);
        boolean linked = snippet.isLinked();

        // delete the snippet
        snippetDAO.deleteSnippet(snippetId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow if necessary
        if (linked) {
            controllerFacade.save();
        }

        return response;
    }

    @Override
    public ConfigurationSnapshot<Void> deleteInputPort(Revision revision, String groupId, String inputPortId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        inputPortDAO.deletePort(groupId, inputPortId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<Void> deleteOutputPort(Revision revision, String groupId, String outputPortId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        outputPortDAO.deletePort(groupId, outputPortId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<Void> deleteProcessGroup(Revision revision, String groupId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        processGroupDAO.deleteProcessGroup(groupId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<Void> deleteRemoteProcessGroup(Revision revision, String groupId, String remoteProcessGroupId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        remoteProcessGroupDAO.deleteRemoteProcessGroup(groupId, remoteProcessGroupId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public void deleteTemplate(String id) {
        // create the template
        templateDAO.deleteTemplate(id);
    }

    @Override
    public ConfigurationSnapshot<ConnectionDTO> createConnection(Revision revision, String groupId, ConnectionDTO connectionDTO) {

        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(connectionDTO.getId())) {
            connectionDTO.setId(UUID.randomUUID().toString());
        }

        final Connection connection = connectionDAO.createConnection(groupId, connectionDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<ConnectionDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createConnectionDto(connection));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<ProcessorDTO> createProcessor(Revision revision, String groupId, ProcessorDTO processorDTO) {

        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(processorDTO.getId())) {
            processorDTO.setId(UUID.randomUUID().toString());
        }

        // create the processor
        final ProcessorNode processor = processorDAO.createProcessor(groupId, processorDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<ProcessorDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createProcessorDto(processor));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<LabelDTO> createLabel(Revision revision, String groupId, LabelDTO labelDTO) {

        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(labelDTO.getId())) {
            labelDTO.setId(UUID.randomUUID().toString());
        }

        // add the label
        final Label label = labelDAO.createLabel(groupId, labelDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<LabelDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createLabelDto(label));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<FunnelDTO> createFunnel(Revision revision, String groupId, FunnelDTO funnelDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(funnelDTO.getId())) {
            funnelDTO.setId(UUID.randomUUID().toString());
        }

        // add the label
        final Funnel funnel = funnelDAO.createFunnel(groupId, funnelDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<FunnelDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createFunnelDto(funnel));

        // save the flow
        controllerFacade.save();

        return response;
    }

    private void validateSnippetContents(final FlowSnippetDTO flowSnippet, final String groupId) {
        // validate any processors
        if (flowSnippet.getProcessors() != null) {
            for (final ProcessorDTO processorDTO : flowSnippet.getProcessors()) {
                final ProcessorNode processorNode = processorDAO.getProcessor(groupId, processorDTO.getId());
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
                final Port port = inputPortDAO.getPort(groupId, portDTO.getId());
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
                final Port port = outputPortDAO.getPort(groupId, portDTO.getId());
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
                final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(groupId, remoteProcessGroupDTO.getId());
                if (remoteProcessGroup.getAuthorizationIssue() != null) {
                    remoteProcessGroupDTO.setAuthorizationIssues(Arrays.asList(remoteProcessGroup.getAuthorizationIssue()));
                }
            }
        }
    }

    @Override
    public ConfigurationSnapshot<FlowSnippetDTO> copySnippet(Revision revision, String groupId, String snippetId, Double originX, Double originY) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(snippetId)) {
            snippetId = UUID.randomUUID().toString();
        }

        // create the new snippet
        FlowSnippetDTO flowSnippet = snippetDAO.copySnippet(groupId, snippetId, originX, originY);

        // validate the new snippet
        validateSnippetContents(flowSnippet, groupId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<FlowSnippetDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), flowSnippet);

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<SnippetDTO> createSnippet(final Revision revision, final SnippetDTO snippetDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(snippetDTO.getId())) {
            snippetDTO.setId(UUID.randomUUID().toString());
        }

        // add the snippet
        final Snippet snippet = snippetDAO.createSnippet(snippetDTO);
        final SnippetDTO responseSnippetDTO = dtoFactory.createSnippetDto(snippet);
        responseSnippetDTO.setContents(snippetUtils.populateFlowSnippet(snippet, false));

        // create the response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<SnippetDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), responseSnippetDTO);

        return response;
    }

    @Override
    public ConfigurationSnapshot<PortDTO> createInputPort(Revision revision, String groupId, PortDTO inputPortDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(inputPortDTO.getId())) {
            inputPortDTO.setId(UUID.randomUUID().toString());
        }

        final Port inputPort = inputPortDAO.createPort(groupId, inputPortDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<PortDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createPortDto(inputPort));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<PortDTO> createOutputPort(Revision revision, String groupId, PortDTO outputPortDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(outputPortDTO.getId())) {
            outputPortDTO.setId(UUID.randomUUID().toString());
        }

        final Port outputPort = outputPortDAO.createPort(groupId, outputPortDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<PortDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createPortDto(outputPort));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<ProcessGroupDTO> createProcessGroup(String parentGroupId, Revision revision, ProcessGroupDTO processGroupDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(processGroupDTO.getId())) {
            processGroupDTO.setId(UUID.randomUUID().toString());
        }

        final ProcessGroup processGroup = processGroupDAO.createProcessGroup(parentGroupId, processGroupDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<ProcessGroupDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createProcessGroupDto(processGroup));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupDTO> createRemoteProcessGroup(Revision revision, String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // ensure id is set
        if (StringUtils.isBlank(remoteProcessGroupDTO.getId())) {
            remoteProcessGroupDTO.setId(UUID.randomUUID().toString());
        }

        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.createRemoteProcessGroup(groupId, remoteProcessGroupDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<RemoteProcessGroupDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        // save the flow
        controllerFacade.save();

        return response;
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
        templateDTO.setSnippet(snippetUtils.populateFlowSnippet(snippet, true));

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
    public ConfigurationSnapshot<FlowSnippetDTO> createTemplateInstance(Revision revision, String groupId, Double originX, Double originY, String templateId) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // instantiate the template - there is no need to make another copy of the flow snippet since the actual template
        // was copied and this dto is only used to instantiate it's components (which as already completed)
        FlowSnippetDTO flowSnippet = templateDAO.instantiateTemplate(groupId, originX, originY, templateId);

        // validate the new snippet
        validateSnippetContents(flowSnippet, groupId);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<FlowSnippetDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), flowSnippet);

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public ConfigurationSnapshot<Void> createArchive(Revision revision) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // create the archive
        controllerFacade.createArchive();

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<Void> response = new ConfigurationSnapshot<>(updatedRevision.getVersion());
        return response;
    }

    @Override
    public ConfigurationSnapshot<ProcessorDTO> setProcessorAnnotationData(Revision revision, String processorId, String annotationData) {
        // ensure the proper revision before performing the update
        checkRevision(revision);

        // create the processor config
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setAnnotationData(annotationData);

        // create the processor dto
        final ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId(processorId);
        processorDTO.setConfig(config);

        // get the parent group id for the specified processor
        String groupId = controllerFacade.findProcessGroupIdForProcessor(processorId);

        // ensure the parent group id was found
        if (groupId == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate Processor with id '%s'.", processorId));
        }

        // update the processor configuration
        ProcessorNode processor = processorDAO.updateProcessor(groupId, processorDTO);

        // update the revision and generate a response
        final Revision updatedRevision = updateRevision(revision);
        final ConfigurationSnapshot<ProcessorDTO> response = new ConfigurationSnapshot<>(updatedRevision.getVersion(), dtoFactory.createProcessorDto(processor));

        // save the flow
        controllerFacade.save();

        return response;
    }

    @Override
    public void deleteActions(Date endDate) {
        // get the user from the request
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the purge details
        PurgeDetails details = new PurgeDetails();
        details.setEndDate(endDate);

        // create a purge action to record that records are being removed
        Action purgeAction = new Action();
        purgeAction.setUserDn(user.getDn());
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
    public void invalidateUser(String userId) {
        try {
            userService.invalidateUserAccount(userId);
        } catch (final AccountNotFoundException anfe) {
            // ignore 
        }
    }

    @Override
    public void invalidateUserGroup(String userGroup, Set<String> userIds) {
        // invalidates any user currently associated with this group
        if (userGroup != null) {
            userService.invalidateUserGroupAccount(userGroup);
        }

        // invalidates any user that will be associated with this group
        if (userIds != null) {
            for (final String userId : userIds) {
                invalidateUser(userId);
            }
        }
    }

    @Override
    public UserDTO updateUser(UserDTO userDto) {
        NiFiUser user;

        // attempt to parse the user id
        final String id = userDto.getId();

        // determine the authorities that have been specified in the request
        Set<Authority> authorities = null;
        if (userDto.getAuthorities() != null) {
            authorities = Authority.convertRawAuthorities(userDto.getAuthorities());
        }

        // if the account status isn't specified or isn't changing
        final AccountStatus accountStatus = AccountStatus.valueOfStatus(userDto.getStatus());
        if (accountStatus == null || AccountStatus.ACTIVE.equals(accountStatus)) {
            // ensure that authorities have been specified (may be empty, but not null)
            if (authorities == null) {
                throw new IllegalArgumentException("Authorities must be specified when updating an account.");
            }

            // update the user account
            user = userService.update(id, authorities);
        } else if (AccountStatus.DISABLED.equals(accountStatus)) {
            // disable the account
            user = userService.disable(id);
        } else {
            throw new IllegalArgumentException("Accounts cannot be marked pending.");
        }

        return dtoFactory.createUserDTO(user);
    }

    @Override
    public void deleteUser(String userId) {
        userService.deleteUser(userId);
    }

    @Override
    public UserGroupDTO updateUserGroup(final UserGroupDTO userGroupDTO) {
        NiFiUserGroup userGroup;

        // convert the authorities
        Set<Authority> authorities = null;
        if (userGroupDTO.getAuthorities() != null) {
            authorities = Authority.convertRawAuthorities(userGroupDTO.getAuthorities());
        }

        final AccountStatus accountStatus = AccountStatus.valueOfStatus(userGroupDTO.getStatus());
        if (accountStatus == null || AccountStatus.ACTIVE.equals(accountStatus)) {
            // update the user group
            userGroup = userService.updateGroup(userGroupDTO.getGroup(), userGroupDTO.getUserIds(), authorities);
        } else if (AccountStatus.DISABLED.equals(accountStatus)) {
            // disable the accounts
            userGroup = userService.disableGroup(userGroupDTO.getGroup());
        } else {
            throw new IllegalArgumentException("Accounts cannot be marked pending.");
        }

        // generate the user group dto
        return dtoFactory.createUserGroupDTO(userGroup);
    }

    @Override
    public void removeUserFromGroup(String userId) {
        userService.ungroupUser(userId);
    }

    @Override
    public void removeUserGroup(String userGroup) {
        userService.ungroup(userGroup);
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
        return dtoFactory.createRevisionDTO(optimisticLockingManager.getRevision());
    }

    @Override
    public SearchResultsDTO searchController(String query) {
        return controllerFacade.search(query);
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
        ProcessGroupStatusDTO statusReport;
        if (properties.isClusterManager()) {
            final ProcessGroupStatus mergedProcessGroupStatus = clusterManager.getProcessGroupStatus(groupId);
            if (mergedProcessGroupStatus == null) {
                throw new ResourceNotFoundException(String.format("Unable to find status for process group %s.", groupId));
            }
            statusReport = dtoFactory.createProcessGroupStatusDto(clusterManager.getBulletinRepository(), mergedProcessGroupStatus);
        } else {
            statusReport = controllerFacade.getProcessGroupStatus(groupId);
        }
        return statusReport;
    }

    @Override
    public ControllerStatusDTO getControllerStatus() {
        final ControllerStatusDTO controllerStatus;

        if (properties.isClusterManager()) {
            final Set<Node> connectedNodes = clusterManager.getNodes(Node.Status.CONNECTED);

            if (connectedNodes.isEmpty()) {
                throw new NoConnectedNodesException();
            }

            int activeThreadCount = 0;
            long totalFlowFileObjectCount = 0;
            long totalFlowFileByteCount = 0;
            for (final Node node : connectedNodes) {
                final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
                if (nodeHeartbeatPayload == null) {
                    continue;
                }

                activeThreadCount += nodeHeartbeatPayload.getActiveThreadCount();
                totalFlowFileObjectCount += nodeHeartbeatPayload.getTotalFlowFileCount();
                totalFlowFileByteCount += nodeHeartbeatPayload.getTotalFlowFileBytes();
            }

            controllerStatus = new ControllerStatusDTO();
            controllerStatus.setActiveThreadCount(activeThreadCount);
            controllerStatus.setQueued(FormatUtils.formatCount(totalFlowFileObjectCount) + " / " + FormatUtils.formatDataSize(totalFlowFileByteCount));

            final int numNodes = clusterManager.getNodeIds().size();
            controllerStatus.setConnectedNodes(connectedNodes.size() + " / " + numNodes);

            // get the bulletins for the controller
            final BulletinRepository bulletinRepository = clusterManager.getBulletinRepository();
            final List<Bulletin> results = bulletinRepository.findBulletinsForController();
            final List<BulletinDTO> bulletinDtos = new ArrayList<>(results.size());
            for (final Bulletin bulletin : results) {
                bulletinDtos.add(dtoFactory.createBulletinDto(bulletin));
            }
            controllerStatus.setBulletins(bulletinDtos);

            // get the component counts by extracting them from the roots' group status
            final ProcessGroupStatus status = clusterManager.getProcessGroupStatus("root");
            if (status != null) {
                final ProcessGroupCounts counts = extractProcessGroupCounts(status);
                controllerStatus.setRunningCount(counts.getRunningCount());
                controllerStatus.setStoppedCount(counts.getStoppedCount());
                controllerStatus.setInvalidCount(counts.getInvalidCount());
                controllerStatus.setDisabledCount(counts.getDisabledCount());
                controllerStatus.setActiveRemotePortCount(counts.getActiveRemotePortCount());
                controllerStatus.setInactiveRemotePortCount(counts.getInactiveRemotePortCount());
            }
        } else {
            // get the controller status
            controllerStatus = controllerFacade.getControllerStatus();
        }

        // determine if there are any pending user accounts - only include if appropriate
        if (NiFiUserUtils.getAuthorities().contains(Authority.ROLE_ADMIN.toString())) {
            controllerStatus.setHasPendingAccounts(userService.hasPendingUserAccount());
        }

        return controllerStatus;
    }

    @Override
    public CountersDTO getCounters() {
        if (properties.isClusterManager()) {
            final Map<String, CounterDTO> mergedCountersMap = new HashMap<>();
            final Set<Node> connectedNodes = clusterManager.getNodes(Node.Status.CONNECTED);

            if (connectedNodes.isEmpty()) {
                throw new NoConnectedNodesException();
            }

            for (final Node node : connectedNodes) {
                final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
                if (nodeHeartbeatPayload == null) {
                    continue;
                }
                final List<Counter> nodeCounters = node.getHeartbeatPayload().getCounters();
                if (nodeCounters == null) {
                    continue;
                }

                // for each node, add its counter values to the aggregate values
                for (final Counter nodeCounter : nodeCounters) {
                    final CounterDTO mergedCounter = mergedCountersMap.get(nodeCounter.getIdentifier());

                    // either create a new aggregate counter or update the aggregate counter
                    if (mergedCounter == null) {
                        // add new counter
                        mergedCountersMap.put(nodeCounter.getIdentifier(), dtoFactory.createCounterDto(nodeCounter));
                    } else {
                        // update aggregate counter
                        mergedCounter.setValueCount(mergedCounter.getValueCount() + nodeCounter.getValue());
                        mergedCounter.setValue(FormatUtils.formatCount(mergedCounter.getValueCount()));
                    }
                }
            }

            final CountersDTO mergedCounters = new CountersDTO();
            mergedCounters.setGenerated(new Date());
            mergedCounters.setCounters(mergedCountersMap.values());
            return mergedCounters;
        } else {
            List<Counter> counters = controllerFacade.getCounters();
            Set<CounterDTO> counterDTOs = new LinkedHashSet<>(counters.size());
            for (Counter counter : counters) {
                counterDTOs.add(dtoFactory.createCounterDto(counter));
            }
            return dtoFactory.createCountersDto(counterDTOs);
        }

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
    public ConnectionDTO getConnection(String groupId, String connectionId) {
        return dtoFactory.createConnectionDto(connectionDAO.getConnection(groupId, connectionId));
    }

    @Override
    public StatusHistoryDTO getConnectionStatusHistory(String groupId, String connectionId) {
        return controllerFacade.getConnectionStatusHistory(groupId, connectionId);
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
    public ProcessorDTO getProcessor(String groupId, String id) {
        final ProcessorNode processor = processorDAO.getProcessor(groupId, id);
        final ProcessorDTO processorDto = dtoFactory.createProcessorDto(processor);
        return processorDto;
    }

    @Override
    public StatusHistoryDTO getProcessorStatusHistory(String groupId, String id) {
        return controllerFacade.getProcessorStatusHistory(groupId, id);
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
        final SystemDiagnosticsDTO dto;
        if (properties.isClusterManager()) {
            final SystemDiagnostics clusterDiagnostics = clusterManager.getSystemDiagnostics();
            if (clusterDiagnostics == null) {
                throw new IllegalStateException("Nodes are connected but no systems diagnostics have been reported.");
            }
            dto = dtoFactory.createSystemDiagnosticsDto(clusterDiagnostics);
        } else {
            final SystemDiagnostics sysDiagnostics = controllerFacade.getSystemDiagnostics();
            dto = dtoFactory.createSystemDiagnosticsDto(sysDiagnostics);
        }
        return dto;
    }

    /**
     * Ensures the specified user has permission to access the specified port.
     *
     * @param user
     * @param port
     * @return
     */
    private boolean isUserAuthorized(final NiFiUser user, final RootGroupPort port) {
        final boolean isSiteToSiteSecure = Boolean.TRUE.equals(properties.isSiteToSiteSecure());

        // if site to site is not secure, allow all users
        if (!isSiteToSiteSecure) {
            return true;
        }

        final Set<String> allowedUsers = port.getUserAccessControl();
        if (allowedUsers.contains(user.getDn())) {
            return true;
        }

        final String userGroup = user.getUserGroup();
        if (userGroup == null) {
            return false;
        }

        final Set<String> allowedGroups = port.getGroupAccessControl();
        return allowedGroups.contains(userGroup);
    }

    @Override
    public ControllerDTO getController() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // at this point we know that the user must have ROLE_NIFI because it's required 
        // get to the endpoint that calls this method but we'll check again anyways
        final Set<Authority> authorities = user.getAuthorities();
        if (!authorities.contains(Authority.ROLE_NIFI)) {
            throw new AccessDeniedException("User must have the NiFi role in order to access these details.");
        }

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
    public String getInstanceId() {
        return controllerFacade.getInstanceId();
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

        // get the content viewer url
        controllerConfig.setContentViewerUrl(properties.getProperty(NiFiProperties.CONTENT_VIEWER_URL));

        final Date now = new Date();
        controllerConfig.setTimeOffset(TimeZone.getDefault().getOffset(now.getTime()));

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
    public LabelDTO getLabel(String groupId, String labelId) {
        return dtoFactory.createLabelDto(labelDAO.getLabel(groupId, labelId));
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
    public FunnelDTO getFunnel(String groupId, String funnelId) {
        return dtoFactory.createFunnelDto(funnelDAO.getFunnel(groupId, funnelId));
    }

    @Override
    public SnippetDTO getSnippet(String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);
        final SnippetDTO snippetDTO = dtoFactory.createSnippetDto(snippet);
        snippetDTO.setContents(snippetUtils.populateFlowSnippet(snippet, false));
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
    public PortDTO getInputPort(String groupId, String inputPortId) {
        return dtoFactory.createPortDto(inputPortDAO.getPort(groupId, inputPortId));
    }

    @Override
    public PortDTO getOutputPort(String groupId, String outputPortId) {
        return dtoFactory.createPortDto(outputPortDAO.getPort(groupId, outputPortId));
    }

    @Override
    public RemoteProcessGroupDTO getRemoteProcessGroup(String groupId, String remoteProcessGroupId) {
        return dtoFactory.createRemoteProcessGroupDto(remoteProcessGroupDAO.getRemoteProcessGroup(groupId, remoteProcessGroupId));
    }

    @Override
    public StatusHistoryDTO getRemoteProcessGroupStatusHistory(String groupId, String id) {
        return controllerFacade.getRemoteProcessGroupStatusHistory(groupId, id);
    }

    @Override
    public ConfigurationSnapshot<ProcessGroupDTO> getProcessGroup(String groupId, final boolean recurse) {
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        Long version = optimisticLockingManager.getRevision().getVersion();
        ConfigurationSnapshot<ProcessGroupDTO> response = new ConfigurationSnapshot<>(version, dtoFactory.createProcessGroupDto(processGroup, recurse));
        return response;
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
    public ProcessorHistoryDTO getProcessorHistory(String processorId) {
        final Map<String, PropertyHistoryDTO> propertyHistoryDtos = new LinkedHashMap<>();
        final Map<String, List<PreviousValue>> propertyHistory = auditService.getPreviousValues(processorId);

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

        final ProcessorHistoryDTO history = new ProcessorHistoryDTO();
        history.setProcessorId(processorId);
        history.setPropertyHistory(propertyHistoryDtos);

        return history;
    }

    @Override
    public UserDTO getUser(String userId) {
        // get the user
        NiFiUser user = userService.getUserById(userId);

        // ensure the user was found
        if (user == null) {
            throw new ResourceNotFoundException(String.format("Unable to find user with id '%s'.", userId));
        }

        return dtoFactory.createUserDTO(user);
    }

    @Override
    public Collection<UserDTO> getUsers(Boolean grouped) {
        // get the users
        final Collection<NiFiUser> users = userService.getUsers();
        final Collection<UserDTO> userDTOs = new HashSet<>();

        if (grouped) {
            final Map<String, UserDTO> groupedUserDTOs = new HashMap<>();

            // group the users
            for (final NiFiUser user : users) {
                if (StringUtils.isNotBlank(user.getUserGroup())) {
                    if (groupedUserDTOs.containsKey(user.getUserGroup())) {
                        final UserDTO groupedUser = groupedUserDTOs.get(user.getUserGroup());
                        groupedUser.setId(groupedUser.getId() + "," + String.valueOf(user.getId()));
                        groupedUser.setUserName(groupedUser.getUserName() + ", " + user.getUserName());
                        groupedUser.setDn(groupedUser.getDn() + ", " + user.getDn());
                        groupedUser.setCreation(getOldestDate(groupedUser.getCreation(), user.getCreation()));
                        groupedUser.setLastAccessed(getNewestDate(groupedUser.getLastAccessed(), user.getLastAccessed()));
                        groupedUser.setLastVerified(getNewestDate(groupedUser.getLastVerified(), user.getLastVerified()));

                        // only retain the justification if al users have the same justification
                        if (groupedUser.getJustification() != null) {
                            if (!groupedUser.getStatus().equals(user.getJustification())) {
                                groupedUser.setJustification(null);
                            }
                        }

                        // only retain the status if all users have the same status
                        if (groupedUser.getStatus() != null) {
                            if (!groupedUser.getStatus().equals(user.getStatus().toString())) {
                                groupedUser.setStatus(null);
                            }
                        }

                        // only retain the authorities if all users have the same authorities
                        if (groupedUser.getAuthorities() != null) {
                            final Set<String> groupAuthorities = new HashSet<>(groupedUser.getAuthorities());
                            final Set<String> userAuthorities = Authority.convertAuthorities(user.getAuthorities());
                            if (!CollectionUtils.isEqualCollection(groupAuthorities, userAuthorities)) {
                                groupedUser.setAuthorities(null);
                            }
                        }
                    } else {
                        groupedUserDTOs.put(user.getUserGroup(), dtoFactory.createUserDTO(user));
                    }
                } else {
                    userDTOs.add(dtoFactory.createUserDTO(user));
                }
            }

            // add the grouped users
            userDTOs.addAll(groupedUserDTOs.values());
        } else {
            // convert each into a DTOs
            for (final NiFiUser user : users) {
                userDTOs.add(dtoFactory.createUserDTO(user));
            }
        }

        return userDTOs;
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
            nodeDtos.add(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));
        }

        return clusterDto;
    }

    @Override
    public NodeDTO getNode(String nodeId) {
        final Node node = clusterManager.getNode(nodeId);
        if (node == null) {
            throw new UnknownNodeException("Node does not exist.");
        } else {
            return dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId));
        }
    }

    @Override
    public void deleteNode(String nodeId) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        final String userDn = user.getDn();
        clusterManager.deleteNode(nodeId, userDn);
    }

    private ProcessorStatus findNodeProcessorStatus(final ProcessGroupStatus groupStatus, final String processorId) {
        ProcessorStatus processorStatus = null;

        for (final ProcessorStatus status : groupStatus.getProcessorStatus()) {
            if (processorId.equals(status.getId())) {
                processorStatus = status;
                break;
            }
        }

        if (processorStatus == null) {
            for (final ProcessGroupStatus status : groupStatus.getProcessGroupStatus()) {
                processorStatus = findNodeProcessorStatus(status, processorId);

                if (processorStatus != null) {
                    break;
                }
            }
        }

        return processorStatus;
    }

    // TODO Refactor!!!
    @Override
    public ClusterProcessorStatusDTO getClusterProcessorStatus(String processorId) {

        final ClusterProcessorStatusDTO clusterProcessorStatusDto = new ClusterProcessorStatusDTO();
        clusterProcessorStatusDto.setNodeProcessorStatus(new ArrayList<NodeProcessorStatusDTO>());

        // set the current time
        clusterProcessorStatusDto.setStatsLastRefreshed(new Date());

        final Set<Node> nodes = clusterManager.getNodes(Node.Status.CONNECTED);
        boolean firstNode = true;
        for (final Node node : nodes) {

            final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
            if (nodeHeartbeatPayload == null) {
                continue;
            }

            final ProcessGroupStatus nodeStats = nodeHeartbeatPayload.getProcessGroupStatus();
            if (nodeStats == null || nodeStats.getProcessorStatus() == null) {
                continue;
            }

            // attempt to find the processor stats for this node
            final ProcessorStatus processorStatus = findNodeProcessorStatus(nodeStats, processorId);

            // sanity check that we have status for this processor
            if (processorStatus == null) {
                throw new ResourceNotFoundException(String.format("Unable to find status for processor id '%s'.", processorId));
            }

            if (firstNode) {
                clusterProcessorStatusDto.setProcessorId(processorId);
                clusterProcessorStatusDto.setProcessorName(processorStatus.getName());
                clusterProcessorStatusDto.setProcessorType(processorStatus.getType());
                clusterProcessorStatusDto.setProcessorRunStatus(processorStatus.getRunStatus().toString());
                firstNode = false;
            }

            // create node processor status dto
            final NodeProcessorStatusDTO nodeProcessorStatusDTO = new NodeProcessorStatusDTO();
            clusterProcessorStatusDto.getNodeProcessorStatus().add(nodeProcessorStatusDTO);

            // populate node processor status dto
            final String nodeId = node.getNodeId().getId();
            nodeProcessorStatusDTO.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));
            nodeProcessorStatusDTO.setProcessorStatus(dtoFactory.createProcessorStatusDto(processorStatus));

        }

        return clusterProcessorStatusDto;
    }

    private ConnectionStatus findNodeConnectionStatus(final ProcessGroupStatus groupStatus, final String connectionId) {
        ConnectionStatus connectionStatus = null;

        for (final ConnectionStatus status : groupStatus.getConnectionStatus()) {
            if (connectionId.equals(status.getId())) {
                connectionStatus = status;
                break;
            }
        }

        if (connectionStatus == null) {
            for (final ProcessGroupStatus status : groupStatus.getProcessGroupStatus()) {
                connectionStatus = findNodeConnectionStatus(status, connectionId);

                if (connectionStatus != null) {
                    break;
                }
            }
        }

        return connectionStatus;
    }

    @Override
    public ClusterConnectionStatusDTO getClusterConnectionStatus(String connectionId) {
        final ClusterConnectionStatusDTO clusterConnectionStatusDto = new ClusterConnectionStatusDTO();
        clusterConnectionStatusDto.setNodeConnectionStatus(new ArrayList<NodeConnectionStatusDTO>());

        // set the current time
        clusterConnectionStatusDto.setStatsLastRefreshed(new Date());

        final Set<Node> nodes = clusterManager.getNodes(Node.Status.CONNECTED);
        boolean firstNode = true;
        for (final Node node : nodes) {

            final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
            if (nodeHeartbeatPayload == null) {
                continue;
            }

            final ProcessGroupStatus nodeStats = nodeHeartbeatPayload.getProcessGroupStatus();
            if (nodeStats == null || nodeStats.getProcessorStatus() == null) {
                continue;
            }

            // find the connection status for this node
            final ConnectionStatus connectionStatus = findNodeConnectionStatus(nodeStats, connectionId);

            // sanity check that we have status for this connection
            if (connectionStatus == null) {
                throw new ResourceNotFoundException(String.format("Unable to find status for connection id '%s'.", connectionId));
            }

            if (firstNode) {
                clusterConnectionStatusDto.setConnectionId(connectionId);
                clusterConnectionStatusDto.setConnectionName(connectionStatus.getName());
                firstNode = false;
            }

            // create node connection status dto
            final NodeConnectionStatusDTO nodeConnectionStatusDTO = new NodeConnectionStatusDTO();
            clusterConnectionStatusDto.getNodeConnectionStatus().add(nodeConnectionStatusDTO);

            // populate node processor status dto
            final String nodeId = node.getNodeId().getId();
            nodeConnectionStatusDTO.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));
            nodeConnectionStatusDTO.setConnectionStatus(dtoFactory.createConnectionStatusDto(connectionStatus));

        }

        return clusterConnectionStatusDto;
    }

    private PortStatus findNodeInputPortStatus(final ProcessGroupStatus groupStatus, final String inputPortId) {
        PortStatus portStatus = null;

        for (final PortStatus status : groupStatus.getInputPortStatus()) {
            if (inputPortId.equals(status.getId())) {
                portStatus = status;
                break;
            }
        }

        if (portStatus == null) {
            for (final ProcessGroupStatus status : groupStatus.getProcessGroupStatus()) {
                portStatus = findNodeInputPortStatus(status, inputPortId);

                if (portStatus != null) {
                    break;
                }
            }
        }

        return portStatus;
    }

    @Override
    public ClusterPortStatusDTO getClusterInputPortStatus(String inputPortId) {
        final ClusterPortStatusDTO clusterInputPortStatusDto = new ClusterPortStatusDTO();
        clusterInputPortStatusDto.setNodePortStatus(new ArrayList<NodePortStatusDTO>());

        // set the current time
        clusterInputPortStatusDto.setStatsLastRefreshed(new Date());

        final Set<Node> nodes = clusterManager.getNodes(Node.Status.CONNECTED);
        boolean firstNode = true;
        for (final Node node : nodes) {

            final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
            if (nodeHeartbeatPayload == null) {
                continue;
            }

            final ProcessGroupStatus nodeStats = nodeHeartbeatPayload.getProcessGroupStatus();
            if (nodeStats == null || nodeStats.getProcessorStatus() == null) {
                continue;
            }

            // find the input status for this node
            final PortStatus inputPortStatus = findNodeInputPortStatus(nodeStats, inputPortId);

            // sanity check that we have status for this input port
            if (inputPortStatus == null) {
                throw new ResourceNotFoundException(String.format("Unable to find status for input port id '%s'.", inputPortId));
            }

            if (firstNode) {
                clusterInputPortStatusDto.setPortId(inputPortId);
                clusterInputPortStatusDto.setPortName(inputPortStatus.getName());
                firstNode = false;
            }

            // create node port status dto
            final NodePortStatusDTO nodeInputPortStatusDTO = new NodePortStatusDTO();
            clusterInputPortStatusDto.getNodePortStatus().add(nodeInputPortStatusDTO);

            // populate node input port status dto
            final String nodeId = node.getNodeId().getId();
            nodeInputPortStatusDTO.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));
            nodeInputPortStatusDTO.setPortStatus(dtoFactory.createPortStatusDto(inputPortStatus));
        }

        return clusterInputPortStatusDto;
    }

    private PortStatus findNodeOutputPortStatus(final ProcessGroupStatus groupStatus, final String outputPortId) {
        PortStatus portStatus = null;

        for (final PortStatus status : groupStatus.getOutputPortStatus()) {
            if (outputPortId.equals(status.getId())) {
                portStatus = status;
                break;
            }
        }

        if (portStatus == null) {
            for (final ProcessGroupStatus status : groupStatus.getProcessGroupStatus()) {
                portStatus = findNodeOutputPortStatus(status, outputPortId);

                if (portStatus != null) {
                    break;
                }
            }
        }

        return portStatus;
    }

    @Override
    public ClusterPortStatusDTO getClusterOutputPortStatus(String outputPortId) {
        final ClusterPortStatusDTO clusterOutputPortStatusDto = new ClusterPortStatusDTO();
        clusterOutputPortStatusDto.setNodePortStatus(new ArrayList<NodePortStatusDTO>());

        // set the current time
        clusterOutputPortStatusDto.setStatsLastRefreshed(new Date());

        final Set<Node> nodes = clusterManager.getNodes(Node.Status.CONNECTED);
        boolean firstNode = true;
        for (final Node node : nodes) {

            final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
            if (nodeHeartbeatPayload == null) {
                continue;
            }

            final ProcessGroupStatus nodeStats = nodeHeartbeatPayload.getProcessGroupStatus();
            if (nodeStats == null || nodeStats.getProcessorStatus() == null) {
                continue;
            }

            // find the output status for this node
            final PortStatus outputPortStatus = findNodeOutputPortStatus(nodeStats, outputPortId);

            // sanity check that we have status for this output port
            if (outputPortStatus == null) {
                throw new ResourceNotFoundException(String.format("Unable to find status for output port id '%s'.", outputPortId));
            }

            if (firstNode) {
                clusterOutputPortStatusDto.setPortId(outputPortId);
                clusterOutputPortStatusDto.setPortName(outputPortStatus.getName());
                firstNode = false;
            }

            // create node port status dto
            final NodePortStatusDTO nodeOutputPortStatusDTO = new NodePortStatusDTO();
            clusterOutputPortStatusDto.getNodePortStatus().add(nodeOutputPortStatusDTO);

            // populate node output port status dto
            final String nodeId = node.getNodeId().getId();
            nodeOutputPortStatusDTO.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));
            nodeOutputPortStatusDTO.setPortStatus(dtoFactory.createPortStatusDto(outputPortStatus));
        }

        return clusterOutputPortStatusDto;
    }

    private RemoteProcessGroupStatus findNodeRemoteProcessGroupStatus(final ProcessGroupStatus groupStatus, final String remoteProcessGroupId) {
        RemoteProcessGroupStatus remoteProcessGroupStatus = null;

        for (final RemoteProcessGroupStatus status : groupStatus.getRemoteProcessGroupStatus()) {
            if (remoteProcessGroupId.equals(status.getId())) {
                remoteProcessGroupStatus = status;
                break;
            }
        }

        if (remoteProcessGroupStatus == null) {
            for (final ProcessGroupStatus status : groupStatus.getProcessGroupStatus()) {
                remoteProcessGroupStatus = findNodeRemoteProcessGroupStatus(status, remoteProcessGroupId);

                if (remoteProcessGroupStatus != null) {
                    break;
                }
            }
        }

        return remoteProcessGroupStatus;
    }

    @Override
    public ClusterRemoteProcessGroupStatusDTO getClusterRemoteProcessGroupStatus(String remoteProcessGroupId) {
        final ClusterRemoteProcessGroupStatusDTO clusterRemoteProcessGroupStatusDto = new ClusterRemoteProcessGroupStatusDTO();
        clusterRemoteProcessGroupStatusDto.setNodeRemoteProcessGroupStatus(new ArrayList<NodeRemoteProcessGroupStatusDTO>());

        // set the current time
        clusterRemoteProcessGroupStatusDto.setStatsLastRefreshed(new Date());

        final Set<Node> nodes = clusterManager.getNodes(Node.Status.CONNECTED);
        boolean firstNode = true;
        for (final Node node : nodes) {

            final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
            if (nodeHeartbeatPayload == null) {
                continue;
            }

            final ProcessGroupStatus nodeStats = nodeHeartbeatPayload.getProcessGroupStatus();
            if (nodeStats == null || nodeStats.getProcessorStatus() == null) {
                continue;
            }

            // find the remote process group for this node
            final RemoteProcessGroupStatus remoteProcessGroupStatus = findNodeRemoteProcessGroupStatus(nodeStats, remoteProcessGroupId);

            // sanity check that we have status for this remote process group
            if (remoteProcessGroupStatus == null) {
                throw new ResourceNotFoundException(String.format("Unable to find status for remote process group id '%s'.", remoteProcessGroupId));
            }

            if (firstNode) {
                clusterRemoteProcessGroupStatusDto.setRemoteProcessGroupId(remoteProcessGroupId);
                clusterRemoteProcessGroupStatusDto.setRemoteProcessGroupName(remoteProcessGroupStatus.getName());
                firstNode = false;
            }

            // create node remote process group status dto
            final NodeRemoteProcessGroupStatusDTO nodeRemoteProcessGroupStatusDTO = new NodeRemoteProcessGroupStatusDTO();
            clusterRemoteProcessGroupStatusDto.getNodeRemoteProcessGroupStatus().add(nodeRemoteProcessGroupStatusDTO);

            // populate node remote process group status dto
            final String nodeId = node.getNodeId().getId();
            nodeRemoteProcessGroupStatusDTO.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));
            nodeRemoteProcessGroupStatusDTO.setRemoteProcessGroupStatus(dtoFactory.createRemoteProcessGroupStatusDto(remoteProcessGroupStatus));
        }

        return clusterRemoteProcessGroupStatusDto;
    }

    @Override
    public ClusterStatusHistoryDTO getClusterProcessorStatusHistory(String processorId) {
        return clusterManager.getProcessorStatusHistory(processorId);
    }

    @Override
    public ClusterStatusHistoryDTO getClusterConnectionStatusHistory(String connectionId) {
        return clusterManager.getConnectionStatusHistory(connectionId);
    }

    @Override
    public ClusterStatusHistoryDTO getClusterProcessGroupStatusHistory(String processGroupId) {
        return clusterManager.getProcessGroupStatusHistory(processGroupId);
    }

    @Override
    public ClusterStatusHistoryDTO getClusterRemoteProcessGroupStatusHistory(String remoteProcessGroupId) {
        return clusterManager.getRemoteProcessGroupStatusHistory(remoteProcessGroupId);
    }

    @Override
    public NodeStatusDTO getNodeStatus(String nodeId) {
        // find the node in question
        final Node node = clusterManager.getNode(nodeId);

        // verify node state
        if (node == null) {
            throw new UnknownNodeException("Node does not exist.");
        } else if (Node.Status.CONNECTED != node.getStatus()) {
            throw new IllegalClusterStateException(
                    String.format("Node '%s:%s' is not connected to the cluster.",
                            node.getNodeId().getApiAddress(), node.getNodeId().getApiPort()));
        }

        // get the node's last heartbeat
        final NodeStatusDTO nodeStatus = new NodeStatusDTO();
        final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
        if (nodeHeartbeatPayload == null) {
            return nodeStatus;
        }

        // get the node status
        final ProcessGroupStatus nodeProcessGroupStatus = nodeHeartbeatPayload.getProcessGroupStatus();
        if (nodeProcessGroupStatus == null) {
            return nodeStatus;
        }

        final ProcessGroupStatusDTO nodeProcessGroupStatusDto = dtoFactory.createProcessGroupStatusDto(clusterManager.getBulletinRepository(), nodeProcessGroupStatus);
        nodeStatus.setControllerStatus(nodeProcessGroupStatusDto);
        nodeStatus.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));

        return nodeStatus;
    }

    @Override
    public NodeSystemDiagnosticsDTO getNodeSystemDiagnostics(String nodeId) {
        // find the node in question
        final Node node = clusterManager.getNode(nodeId);

        // verify node state
        if (node == null) {
            throw new UnknownNodeException("Node does not exist.");
        } else if (Node.Status.CONNECTED != node.getStatus()) {
            throw new IllegalClusterStateException(
                    String.format("Node '%s:%s' is not connected to the cluster.",
                            node.getNodeId().getApiAddress(), node.getNodeId().getApiPort()));
        }

        // get the node's last heartbeat
        final NodeSystemDiagnosticsDTO nodeStatus = new NodeSystemDiagnosticsDTO();
        final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
        if (nodeHeartbeatPayload == null) {
            return nodeStatus;
        }

        // get the node status
        final SystemDiagnostics nodeSystemDiagnostics = nodeHeartbeatPayload.getSystemDiagnostics();
        if (nodeSystemDiagnostics == null) {
            return nodeStatus;
        }

        // populate the dto
        nodeStatus.setControllerStatus(dtoFactory.createSystemDiagnosticsDto(nodeSystemDiagnostics));
        nodeStatus.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));

        return nodeStatus;
    }

    @Override
    public ClusterStatusDTO getClusterStatus() {

        // create cluster status dto
        final ClusterStatusDTO clusterStatusDto = new ClusterStatusDTO();

        // populate node status dtos
        final Collection<NodeStatusDTO> nodeStatusDtos = new ArrayList<>();
        clusterStatusDto.setNodeStatus(nodeStatusDtos);

        for (final Node node : clusterManager.getNodes()) {

            if (Node.Status.CONNECTED != node.getStatus()) {
                continue;
            }

            final HeartbeatPayload nodeHeartbeatPayload = node.getHeartbeatPayload();
            if (nodeHeartbeatPayload == null) {
                continue;
            }

            final ProcessGroupStatus nodeProcessGroupStatus = nodeHeartbeatPayload.getProcessGroupStatus();
            if (nodeProcessGroupStatus == null) {
                continue;
            }

            final ProcessGroupStatusDTO nodeProcessGroupStatusDto = dtoFactory.createProcessGroupStatusDto(clusterManager.getBulletinRepository(), nodeProcessGroupStatus);

            // create node status dto
            final NodeStatusDTO nodeStatusDto = new NodeStatusDTO();
            nodeStatusDtos.add(nodeStatusDto);

            // populate the status
            nodeStatusDto.setControllerStatus(nodeProcessGroupStatusDto);

            // create and add node dto
            final String nodeId = node.getNodeId().getId();
            nodeStatusDto.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));

        }

        return clusterStatusDto;
    }

    @Override
    public ProcessorDTO getProcessor(String id) {
        ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            String groupId = controllerFacade.findProcessGroupIdForProcessor(id);

            // ensure the parent group id was found
            if (groupId == null) {
                throw new ResourceNotFoundException(String.format("Unable to locate Processor with id '%s'.", id));
            }

            // get the processor
            return getProcessor(groupId, id);
        } finally {
            if (currentContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentContextClassLoader);
            }
        }
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

    public void setUserService(UserService userService) {
        this.userService = userService;
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
     *
     * @param date1
     * @param date2
     * @return
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
     *
     * @param date1
     * @param date2
     * @return
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

    /**
     * Utility method for extracting component counts from the specified group
     * status.
     *
     * @param groupStatus
     * @return
     */
    private ProcessGroupCounts extractProcessGroupCounts(ProcessGroupStatus groupStatus) {
        int running = 0;
        int stopped = 0;
        int invalid = 0;
        int disabled = 0;
        int activeRemotePorts = 0;
        int inactiveRemotePorts = 0;

        for (final ProcessorStatus processorStatus : groupStatus.getProcessorStatus()) {
            switch (processorStatus.getRunStatus()) {
                case Disabled:
                    disabled++;
                    break;
                case Running:
                    running++;
                    break;
                case Invalid:
                    invalid++;
                    break;
                default:
                    stopped++;
                    break;
            }
        }

        for (final PortStatus portStatus : groupStatus.getInputPortStatus()) {
            switch (portStatus.getRunStatus()) {
                case Disabled:
                    disabled++;
                    break;
                case Running:
                    running++;
                    break;
                case Invalid:
                    invalid++;
                    break;
                default:
                    stopped++;
                    break;
            }
        }

        for (final PortStatus portStatus : groupStatus.getOutputPortStatus()) {
            switch (portStatus.getRunStatus()) {
                case Disabled:
                    disabled++;
                    break;
                case Running:
                    running++;
                    break;
                case Invalid:
                    invalid++;
                    break;
                default:
                    stopped++;
                    break;
            }
        }

        for (final RemoteProcessGroupStatus remoteStatus : groupStatus.getRemoteProcessGroupStatus()) {
            if (remoteStatus.getActiveRemotePortCount() != null) {
                activeRemotePorts += remoteStatus.getActiveRemotePortCount();
            }
            if (remoteStatus.getInactiveRemotePortCount() != null) {
                inactiveRemotePorts += remoteStatus.getInactiveRemotePortCount();
            }
            if (CollectionUtils.isNotEmpty(remoteStatus.getAuthorizationIssues())) {
                invalid++;
            }
        }

        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            final ProcessGroupCounts childCounts = extractProcessGroupCounts(childGroupStatus);
            running += childCounts.getRunningCount();
            stopped += childCounts.getStoppedCount();
            invalid += childCounts.getInvalidCount();
            disabled += childCounts.getDisabledCount();
            activeRemotePorts += childCounts.getActiveRemotePortCount();
            inactiveRemotePorts += childCounts.getInactiveRemotePortCount();
        }

        return new ProcessGroupCounts(0, 0, running, stopped, invalid, disabled, activeRemotePorts, inactiveRemotePorts);
    }
}
