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
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.FlowChangePurgeDetails;
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
import org.apache.nifi.web.api.dto.ComponentHistoryDTO;
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
import org.apache.nifi.web.util.SnippetUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.status.ClusterProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessGroupStatusDTO;
import org.apache.nifi.web.dao.ControllerServiceDAO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

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
    private UserService userService;

    // cluster manager
    private WebClusterManager clusterManager;

    // properties
    private NiFiProperties properties;
    private DtoFactory dtoFactory;

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
    public ConfigurationSnapshot<ConnectionDTO> updateConnection(final Revision revision, final String groupId, final ConnectionDTO connectionDTO) {
        // if connection does not exist, then create new connection
        if (connectionDAO.hasConnection(groupId, connectionDTO.getId()) == false) {
            return createConnection(revision, groupId, connectionDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ConnectionDTO>() {
            @Override
            public ConfigurationResult<ConnectionDTO> execute() {
                final Connection connection = connectionDAO.updateConnection(groupId, connectionDTO);

                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ConnectionDTO getConfiguration() {
                        return dtoFactory.createConnectionDto(connection);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<ProcessorDTO> updateProcessor(final Revision revision, final String groupId, final ProcessorDTO processorDTO) {
        // if processor does not exist, then create new processor
        if (processorDAO.hasProcessor(groupId, processorDTO.getId()) == false) {
            return createProcessor(revision, groupId, processorDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ProcessorDTO>() {
            @Override
            public ConfigurationResult<ProcessorDTO> execute() {
                // update the processor
                final ProcessorNode processor = processorDAO.updateProcessor(groupId, processorDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ProcessorDTO getConfiguration() {
                        return dtoFactory.createProcessorDto(processor);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<LabelDTO> updateLabel(final Revision revision, final String groupId, final LabelDTO labelDTO) {
        // if label does not exist, then create new label
        if (labelDAO.hasLabel(groupId, labelDTO.getId()) == false) {
            return createLabel(revision, groupId, labelDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<LabelDTO>() {
            @Override
            public ConfigurationResult<LabelDTO> execute() {
                // update the existing label
                final Label label = labelDAO.updateLabel(groupId, labelDTO);

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public LabelDTO getConfiguration() {
                        return dtoFactory.createLabelDto(label);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<FunnelDTO> updateFunnel(final Revision revision, final String groupId, final FunnelDTO funnelDTO) {
        // if label does not exist, then create new label
        if (funnelDAO.hasFunnel(groupId, funnelDTO.getId()) == false) {
            return createFunnel(revision, groupId, funnelDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<FunnelDTO>() {
            @Override
            public ConfigurationResult<FunnelDTO> execute() {
                // update the existing label
                final Funnel funnel = funnelDAO.updateFunnel(groupId, funnelDTO);

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public FunnelDTO getConfiguration() {
                        return dtoFactory.createFunnelDto(funnel);
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

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<SnippetDTO>() {
            @Override
            public ConfigurationResult<SnippetDTO> execute() {
                // update the snippet
                final Snippet snippet = snippetDAO.updateSnippet(snippetDto);

                // build the snippet dto
                final SnippetDTO responseSnippetDto = dtoFactory.createSnippetDto(snippet);
                responseSnippetDto.setContents(snippetUtils.populateFlowSnippet(snippet, false, false));

                // save updated controller if applicable
                if (snippetDto.getParentGroupId() != null && snippet.isLinked()) {
                    controllerFacade.save();
                }

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public SnippetDTO getConfiguration() {
                        return responseSnippetDto;
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<PortDTO> updateInputPort(final Revision revision, final String groupId, final PortDTO inputPortDTO) {
        // if input port does not exist, then create new input port
        if (inputPortDAO.hasPort(groupId, inputPortDTO.getId()) == false) {
            return createInputPort(revision, groupId, inputPortDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<PortDTO>() {
            @Override
            public ConfigurationResult<PortDTO> execute() {
                final Port inputPort = inputPortDAO.updatePort(groupId, inputPortDTO);

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public PortDTO getConfiguration() {
                        return dtoFactory.createPortDto(inputPort);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<PortDTO> updateOutputPort(final Revision revision, final String groupId, final PortDTO outputPortDTO) {
        // if output port does not exist, then create new output port
        if (outputPortDAO.hasPort(groupId, outputPortDTO.getId()) == false) {
            return createOutputPort(revision, groupId, outputPortDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<PortDTO>() {
            @Override
            public ConfigurationResult<PortDTO> execute() {
                final Port outputPort = outputPortDAO.updatePort(groupId, outputPortDTO);

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public PortDTO getConfiguration() {
                        return dtoFactory.createPortDto(outputPort);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupDTO> updateRemoteProcessGroup(final Revision revision, final String groupId, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // if controller reference does not exist, then create new controller reference
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(groupId, remoteProcessGroupDTO.getId()) == false) {
            return createRemoteProcessGroup(revision, groupId, remoteProcessGroupDTO);
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<RemoteProcessGroupDTO>() {
            @Override
            public ConfigurationResult<RemoteProcessGroupDTO> execute() {
                final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.updateRemoteProcessGroup(groupId, remoteProcessGroupDTO);

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public RemoteProcessGroupDTO getConfiguration() {
                        return dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupInputPort(
            final Revision revision, final String groupId, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<RemoteProcessGroupPortDTO>() {
            @Override
            public ConfigurationResult<RemoteProcessGroupPortDTO> execute() {
                // update the remote port
                final RemoteGroupPort remoteGroupPort = remoteProcessGroupDAO.updateRemoteProcessGroupInputPort(groupId, remoteProcessGroupId, remoteProcessGroupPortDTO);

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public RemoteProcessGroupPortDTO getConfiguration() {
                        return dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupPortDTO> updateRemoteProcessGroupOutputPort(
            final Revision revision, final String groupId, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<RemoteProcessGroupPortDTO>() {
            @Override
            public ConfigurationResult<RemoteProcessGroupPortDTO> execute() {
                // update the remote port
                final RemoteGroupPort remoteGroupPort = remoteProcessGroupDAO.updateRemoteProcessGroupOutputPort(groupId, remoteProcessGroupId, remoteProcessGroupPortDTO);

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public RemoteProcessGroupPortDTO getConfiguration() {
                        return dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<ProcessGroupDTO> updateProcessGroup(final Revision revision, final String parentGroupId, final ProcessGroupDTO processGroupDTO) {
        // if process group does not exist, then create new process group
        if (processGroupDAO.hasProcessGroup(processGroupDTO.getId()) == false) {
            if (parentGroupId == null) {
                throw new IllegalArgumentException("Unable to create the specified process group since the parent group was not specified.");
            } else {
                return createProcessGroup(parentGroupId, revision, processGroupDTO);
            }
        }

        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ProcessGroupDTO>() {
            @Override
            public ConfigurationResult<ProcessGroupDTO> execute() {
                // update the process group
                final ProcessGroup processGroup = processGroupDAO.updateProcessGroup(processGroupDTO);

                // save updated controller
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ProcessGroupDTO getConfiguration() {
                        return dtoFactory.createProcessGroupDto(processGroup);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<ControllerConfigurationDTO> updateControllerConfiguration(final Revision revision, final ControllerConfigurationDTO controllerConfigurationDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ControllerConfigurationDTO>() {
            @Override
            public ConfigurationResult<ControllerConfigurationDTO> execute() {
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
                final ControllerConfigurationDTO controllerConfig = getControllerConfiguration();

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return controllerConfig;
                    }
                };
            }
        });
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
    public ConfigurationSnapshot<Void> deleteConnection(final Revision revision, final String groupId, final String connectionId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                connectionDAO.deleteConnection(groupId, connectionId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public DropRequestDTO deleteFlowFileDropRequest(String groupId, String connectionId, String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.deleteFlowFileDropRequest(groupId, connectionId, dropRequestId));
    }

    @Override
    public ConfigurationSnapshot<Void> deleteProcessor(final Revision revision, final String groupId, final String processorId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // delete the processor and synchronize the connection state
                processorDAO.deleteProcessor(groupId, processorId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> deleteLabel(final Revision revision, final String groupId, final String labelId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // delete the label
                labelDAO.deleteLabel(groupId, labelId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> deleteFunnel(final Revision revision, final String groupId, final String funnelId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // delete the label
                funnelDAO.deleteFunnel(groupId, funnelId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
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
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                // determine if this snippet was linked to the data flow
                Snippet snippet = snippetDAO.getSnippet(snippetId);
                boolean linked = snippet.isLinked();

                // delete the snippet
                snippetDAO.deleteSnippet(snippetId);

                // save the flow if necessary
                if (linked) {
                    controllerFacade.save();
                }

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> deleteInputPort(final Revision revision, final String groupId, final String inputPortId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                inputPortDAO.deletePort(groupId, inputPortId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> deleteOutputPort(final Revision revision, final String groupId, final String outputPortId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                outputPortDAO.deletePort(groupId, outputPortId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> deleteProcessGroup(final Revision revision, final String groupId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                processGroupDAO.deleteProcessGroup(groupId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<Void> deleteRemoteProcessGroup(final Revision revision, final String groupId, final String remoteProcessGroupId) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<Void>() {
            @Override
            public ConfigurationResult<Void> execute() {
                remoteProcessGroupDAO.deleteRemoteProcessGroup(groupId, remoteProcessGroupId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
            }
        });
    }

    @Override
    public void deleteTemplate(String id) {
        // create the template
        templateDAO.deleteTemplate(id);
    }

    @Override
    public ConfigurationSnapshot<ConnectionDTO> createConnection(final Revision revision, final String groupId, final ConnectionDTO connectionDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ConnectionDTO>() {
            @Override
            public ConfigurationResult<ConnectionDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(connectionDTO.getId())) {
                    connectionDTO.setId(UUID.randomUUID().toString());
                }

                final Connection connection = connectionDAO.createConnection(groupId, connectionDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public ConnectionDTO getConfiguration() {
                        return dtoFactory.createConnectionDto(connection);
                    }
                };
            }
        });
    }

    @Override
    public DropRequestDTO createFlowFileDropRequest(String groupId, String connectionId, String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.createFileFlowDropRequest(groupId, connectionId, dropRequestId));
    }

    @Override
    public ConfigurationSnapshot<ProcessorDTO> createProcessor(final Revision revision, final String groupId, final ProcessorDTO processorDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ProcessorDTO>() {
            @Override
            public ConfigurationResult<ProcessorDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(processorDTO.getId())) {
                    processorDTO.setId(UUID.randomUUID().toString());
                }

                // create the processor
                final ProcessorNode processor = processorDAO.createProcessor(groupId, processorDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public ProcessorDTO getConfiguration() {
                        return dtoFactory.createProcessorDto(processor);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<LabelDTO> createLabel(final Revision revision, final String groupId, final LabelDTO labelDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<LabelDTO>() {
            @Override
            public ConfigurationResult<LabelDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(labelDTO.getId())) {
                    labelDTO.setId(UUID.randomUUID().toString());
                }

                // add the label
                final Label label = labelDAO.createLabel(groupId, labelDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public LabelDTO getConfiguration() {
                        return dtoFactory.createLabelDto(label);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<FunnelDTO> createFunnel(final Revision revision, final String groupId, final FunnelDTO funnelDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<FunnelDTO>() {
            @Override
            public ConfigurationResult<FunnelDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(funnelDTO.getId())) {
                    funnelDTO.setId(UUID.randomUUID().toString());
                }

                // add the label
                final Funnel funnel = funnelDAO.createFunnel(groupId, funnelDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public FunnelDTO getConfiguration() {
                        return dtoFactory.createFunnelDto(funnel);
                    }
                };
            }
        });
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
                validateSnippetContents(flowSnippet, groupId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
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

                return new ConfigurationResult() {
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
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<PortDTO>() {
            @Override
            public ConfigurationResult<PortDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(inputPortDTO.getId())) {
                    inputPortDTO.setId(UUID.randomUUID().toString());
                }

                final Port inputPort = inputPortDAO.createPort(groupId, inputPortDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public PortDTO getConfiguration() {
                        return dtoFactory.createPortDto(inputPort);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<PortDTO> createOutputPort(final Revision revision, final String groupId, final PortDTO outputPortDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<PortDTO>() {
            @Override
            public ConfigurationResult<PortDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(outputPortDTO.getId())) {
                    outputPortDTO.setId(UUID.randomUUID().toString());
                }

                final Port outputPort = outputPortDAO.createPort(groupId, outputPortDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public PortDTO getConfiguration() {
                        return dtoFactory.createPortDto(outputPort);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<ProcessGroupDTO> createProcessGroup(final String parentGroupId, final Revision revision, final ProcessGroupDTO processGroupDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<ProcessGroupDTO>() {
            @Override
            public ConfigurationResult<ProcessGroupDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(processGroupDTO.getId())) {
                    processGroupDTO.setId(UUID.randomUUID().toString());
                }

                final ProcessGroup processGroup = processGroupDAO.createProcessGroup(parentGroupId, processGroupDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public ProcessGroupDTO getConfiguration() {
                        return dtoFactory.createProcessGroupDto(processGroup);
                    }
                };
            }
        });
    }

    @Override
    public ConfigurationSnapshot<RemoteProcessGroupDTO> createRemoteProcessGroup(final Revision revision, final String groupId, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        return optimisticLockingManager.configureFlow(revision, new ConfigurationRequest<RemoteProcessGroupDTO>() {
            @Override
            public ConfigurationResult<RemoteProcessGroupDTO> execute() {
                // ensure id is set
                if (StringUtils.isBlank(remoteProcessGroupDTO.getId())) {
                    remoteProcessGroupDTO.setId(UUID.randomUUID().toString());
                }

                final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.createRemoteProcessGroup(groupId, remoteProcessGroupDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public RemoteProcessGroupDTO getConfiguration() {
                        return dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup);
                    }
                };
            }
        });
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
                validateSnippetContents(flowSnippet, groupId);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
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

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
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

                // get the parent group id for the specified processor
                String groupId = controllerFacade.findProcessGroupIdForProcessor(processorId);

                // ensure the parent group id was found
                if (groupId == null) {
                    throw new ResourceNotFoundException(String.format("Unable to locate Processor with id '%s'.", processorId));
                }

                // update the processor configuration
                final ProcessorNode processor = processorDAO.updateProcessor(groupId, processorDTO);

                // save the flow
                controllerFacade.save();

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ProcessorDTO getConfiguration() {
                        return dtoFactory.createProcessorDto(processor);
                    }
                };
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

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public ControllerServiceDTO getConfiguration() {
                        return dtoFactory.createControllerServiceDto(controllerService);
                    }
                };
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

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerServiceDTO getConfiguration() {
                        return dtoFactory.createControllerServiceDto(controllerService);
                    }
                };
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

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public Set<ControllerServiceReferencingComponentDTO> getConfiguration() {
                        return dtoFactory.createControllerServiceReferencingComponentsDto(reference);
                    }
                };
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

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
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

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return true;
                    }

                    @Override
                    public ReportingTaskDTO getConfiguration() {
                        return dtoFactory.createReportingTaskDto(reportingTask);
                    }
                };
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

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ReportingTaskDTO getConfiguration() {
                        return dtoFactory.createReportingTaskDto(reportingTask);
                    }
                };
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

                return new ConfigurationResult() {
                    @Override
                    public boolean isNew() {
                        return false;
                    }

                    @Override
                    public ControllerConfigurationDTO getConfiguration() {
                        return null;
                    }
                };
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
        purgeAction.setUserIdentity(user.getDn());
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
        return dtoFactory.createRevisionDTO(optimisticLockingManager.getLastModification());
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
            controllerStatus.setBulletins(dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForController()));

            // get the controller service bulletins
            final BulletinQuery controllerServiceQuery = new BulletinQuery.Builder().sourceType(ComponentType.CONTROLLER_SERVICE).build();
            controllerStatus.setControllerServiceBulletins(dtoFactory.createBulletinDtos(bulletinRepository.findBulletins(controllerServiceQuery)));

            // get the reporting task bulletins
            final BulletinQuery reportingTaskQuery = new BulletinQuery.Builder().sourceType(ComponentType.REPORTING_TASK).build();
            controllerStatus.setReportingTaskBulletins(dtoFactory.createBulletinDtos(bulletinRepository.findBulletins(reportingTaskQuery)));

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
    public DropRequestDTO getFlowFileDropRequest(String groupId, String connectionId, String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.getFlowFileDropRequest(groupId, connectionId, dropRequestId));
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
    public Set<DocumentedTypeDTO> getControllerServiceTypes(final String serviceType) {
        return controllerFacade.getControllerServiceTypes(serviceType);
    }

    @Override
    public Set<DocumentedTypeDTO> getReportingTaskTypes() {
        return controllerFacade.getReportingTaskTypes();
    }

    @Override
    public ProcessorDTO getProcessor(String groupId, String id) {
        final ProcessorNode processor = processorDAO.getProcessor(groupId, id);
        final ProcessorDTO processorDto = dtoFactory.createProcessorDto(processor);
        return processorDto;
    }

    @Override
    public PropertyDescriptorDTO getProcessorPropertyDescriptor(String groupId, String id, String property) {
        final ProcessorNode processor = processorDAO.getProcessor(groupId, id);
        PropertyDescriptor descriptor = processor.getPropertyDescriptor(property);

        // return an invalid descriptor if the processor doesn't suppor this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor);
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

    private ProcessGroupStatus findNodeProcessGroupStatus(final ProcessGroupStatus groupStatus, final String processGroupId) {
        ProcessGroupStatus processGroupStatus = null;

        if (processGroupId.equals(groupStatus.getId())) {
            processGroupStatus = groupStatus;
        }

        if (processGroupStatus == null) {
            for (final ProcessGroupStatus status : groupStatus.getProcessGroupStatus()) {
                processGroupStatus = findNodeProcessGroupStatus(status, processGroupId);

                if (processGroupStatus != null) {
                    break;
                }
            }
        }

        return processGroupStatus;
    }

    @Override
    public ClusterProcessGroupStatusDTO getClusterProcessGroupStatus(String processGroupId) {

        final ClusterProcessGroupStatusDTO clusterProcessGroupStatusDto = new ClusterProcessGroupStatusDTO();
        clusterProcessGroupStatusDto.setNodeProcessGroupStatus(new ArrayList<NodeProcessGroupStatusDTO>());

        // set the current time
        clusterProcessGroupStatusDto.setStatsLastRefreshed(new Date());

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

            // attempt to find the process group stats for this node
            final ProcessGroupStatus processGroupStatus = findNodeProcessGroupStatus(nodeStats, processGroupId);

            // sanity check that we have status for this process group
            if (processGroupStatus == null) {
                throw new ResourceNotFoundException(String.format("Unable to find status for process group id '%s'.", processGroupId));
            }

            if (firstNode) {
                clusterProcessGroupStatusDto.setProcessGroupId(processGroupId);
                clusterProcessGroupStatusDto.setProcessGroupName(processGroupStatus.getName());
                firstNode = false;
            }

            // create node process group status dto
            final NodeProcessGroupStatusDTO nodeProcessGroupStatusDTO = new NodeProcessGroupStatusDTO();
            clusterProcessGroupStatusDto.getNodeProcessGroupStatus().add(nodeProcessGroupStatusDTO);

            // populate node process group status dto
            final String nodeId = node.getNodeId().getId();
            nodeProcessGroupStatusDTO.setNode(dtoFactory.createNodeDTO(node, clusterManager.getNodeEvents(nodeId), isPrimaryNode(nodeId)));
            nodeProcessGroupStatusDTO.setProcessGroupStatus(dtoFactory.createProcessGroupStatusDto(clusterManager.getBulletinRepository(), processGroupStatus));

        }

        return clusterProcessGroupStatusDto;
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

    /**
     * Utility method for extracting component counts from the specified group status.
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
