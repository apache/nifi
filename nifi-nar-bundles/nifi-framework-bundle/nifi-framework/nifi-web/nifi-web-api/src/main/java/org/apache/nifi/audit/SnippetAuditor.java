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
package org.apache.nifi.audit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.action.component.details.FlowChangeRemoteProcessGroupDetails;
import org.apache.nifi.action.details.ConnectDetails;
import org.apache.nifi.action.details.FlowChangeConnectDetails;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.FunnelDAO;
import org.apache.nifi.web.dao.PortDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.dao.SnippetDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
@Aspect
public class SnippetAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(SnippetAuditor.class);

    private PortDAO inputPortDAO;
    private PortDAO outputPortDAO;
    private RemoteProcessGroupDAO remoteProcessGroupDAO;
    private ProcessorDAO processorDAO;
    private FunnelDAO funnelDAO;
    private ConnectionDAO connectionDAO;

    private PortAuditor portAuditor;
    private RemoteProcessGroupAuditor remoteProcessGroupAuditor;
    private ProcessGroupAuditor processGroupAuditor;
    private ProcessorAuditor processorAuditor;
    private FunnelAuditor funnelAuditor;
    private RelationshipAuditor relationshipAuditor;

    /**
     * Audits copy/paste.
     *
     * @param proceedingJoinPoint join point
     * @return dto
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.SnippetDAO+) && "
            + "execution(org.apache.nifi.web.api.dto.FlowSnippetDTO copySnippet(java.lang.String, java.lang.String, java.lang.Double, java.lang.Double, java.lang.String))")
    public FlowSnippetDTO copySnippetAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // perform the underlying operation
        FlowSnippetDTO snippet = (FlowSnippetDTO) proceedingJoinPoint.proceed();
        auditSnippet(snippet);
        return snippet;
    }

    /**
     * Audits the instantiation of a template.
     *
     * @param proceedingJoinPoint join point
     * @return dto
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.TemplateDAO+) && "
            + "execution(org.apache.nifi.web.api.dto.FlowSnippetDTO instantiateTemplate("
            + "java.lang.String, java.lang.Double, java.lang.Double, java.lang.String, org.apache.nifi.web.api.dto.FlowSnippetDTO, java.lang.String))")
    public FlowSnippetDTO instantiateTemplateAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // perform the underlying operation
        FlowSnippetDTO snippet = (FlowSnippetDTO) proceedingJoinPoint.proceed();
        auditSnippet(snippet);
        return snippet;
    }

    /**
     * Audits the specified snippet.
     */
    private void auditSnippet(final FlowSnippetDTO snippet) {
        final Collection<Action> actions = new ArrayList<>();
        final Date timestamp = new Date();

        // input ports
        for (final PortDTO inputPort : snippet.getInputPorts()) {
            actions.add(generateAuditRecord(inputPort.getId(), inputPort.getName(), Component.InputPort, Operation.Add, timestamp));
        }

        // output ports
        for (final PortDTO outputPort : snippet.getOutputPorts()) {
            actions.add(generateAuditRecord(outputPort.getId(), outputPort.getName(), Component.OutputPort, Operation.Add, timestamp));
        }

        // remote processor groups
        for (final RemoteProcessGroupDTO remoteProcessGroup : snippet.getRemoteProcessGroups()) {
            FlowChangeRemoteProcessGroupDetails remoteProcessGroupDetails = new FlowChangeRemoteProcessGroupDetails();
            remoteProcessGroupDetails.setUri(remoteProcessGroup.getTargetUri());

            final FlowChangeAction action = generateAuditRecord(remoteProcessGroup.getId(), remoteProcessGroup.getName(), Component.RemoteProcessGroup, Operation.Add, timestamp);
            action.setComponentDetails(remoteProcessGroupDetails);
            actions.add(action);
        }

        // processor groups
        for (final ProcessGroupDTO processGroup : snippet.getProcessGroups()) {
            actions.add(generateAuditRecord(processGroup.getId(), processGroup.getName(), Component.ProcessGroup, Operation.Add, timestamp));
        }

        // processors
        for (final ProcessorDTO processor : snippet.getProcessors()) {
            final FlowChangeExtensionDetails processorDetails = new FlowChangeExtensionDetails();
            processorDetails.setType(StringUtils.substringAfterLast(processor.getType(), "."));

            final FlowChangeAction action = generateAuditRecord(processor.getId(), processor.getName(), Component.Processor, Operation.Add, timestamp);
            action.setComponentDetails(processorDetails);
            actions.add(action);
        }

        // funnels
        for (final FunnelDTO funnel : snippet.getFunnels()) {
            actions.add(generateAuditRecord(funnel.getId(), StringUtils.EMPTY, Component.Funnel, Operation.Add, timestamp));
        }

        // connections
        for (final ConnectionDTO connection : snippet.getConnections()) {
            final ConnectableDTO source = connection.getSource();
            final ConnectableDTO destination = connection.getDestination();

            // determine the relationships and connection name
            final String relationships = CollectionUtils.isEmpty(connection.getSelectedRelationships()) ? StringUtils.EMPTY : StringUtils.join(connection.getSelectedRelationships(), ", ");
            final String name = StringUtils.isBlank(connection.getName()) ? relationships : connection.getName();

            // create the connect details
            FlowChangeConnectDetails connectDetails = new FlowChangeConnectDetails();
            connectDetails.setSourceId(source.getId());
            connectDetails.setSourceName(source.getName());
            connectDetails.setSourceType(determineConnectableType(source));
            connectDetails.setRelationship(relationships);
            connectDetails.setDestinationId(destination.getId());
            connectDetails.setDestinationName(destination.getName());
            connectDetails.setDestinationType(determineConnectableType(destination));

            // create the audit record
            final FlowChangeAction action = generateAuditRecord(connection.getId(), name, Component.Connection, Operation.Connect, timestamp);
            action.setActionDetails(connectDetails);
            actions.add(action);
        }

        // save the actions
        if (!actions.isEmpty()) {
            saveActions(actions, logger);
        }
    }

    /**
     * Determines the type of component the specified connectable is.
     */
    private Component determineConnectableType(ConnectableDTO connectable) {
        Component component = Component.Controller;

        final String connectableType = connectable.getType();
        if (ConnectableType.PROCESSOR.name().equals(connectableType)) {
            component = Component.Processor;
        } else if (ConnectableType.INPUT_PORT.name().equals(connectableType)) {
            component = Component.InputPort;
        } else if (ConnectableType.OUTPUT_PORT.name().equals(connectableType)) {
            component = Component.OutputPort;
        } else if (ConnectableType.FUNNEL.name().equals(connectableType)) {
            component = Component.Funnel;
        } else {
            component = Component.RemoteProcessGroup;
        }

        return component;
    }

    /**
     * Generates an audit record for the creation of the specified funnel.
     */
    private FlowChangeAction generateAuditRecord(String id, String name, Component type, Operation operation, Date timestamp) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the action for adding this funnel
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(timestamp);
            action.setSourceId(id);
            action.setSourceName(name);
            action.setSourceType(type);
        }

        return action;
    }

    /**
     * Audits a bulk move.
     *
     * @param proceedingJoinPoint join point
     * @param snippetDTO dto
     * @param snippetDAO dao
     * @return snippet
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.SnippetDAO+) && "
            + "execution(org.apache.nifi.controller.Snippet updateSnippetComponents(org.apache.nifi.web.api.dto.SnippetDTO)) && "
            + "args(snippetDTO) && "
            + "target(snippetDAO)")
    public Snippet updateSnippetAdvice(ProceedingJoinPoint proceedingJoinPoint, SnippetDTO snippetDTO, SnippetDAO snippetDAO) throws Throwable {
        // get the snippet before removing it
        Snippet snippet = snippetDAO.getSnippet(snippetDTO.getId());
        final String previousGroupId = snippet.getParentGroupId();

        // perform the underlying operation
        snippet = (Snippet) proceedingJoinPoint.proceed();

        // if this snippet is linked and its parent group id has changed
        final String groupId = snippetDTO.getParentGroupId();
        if (!previousGroupId.equals(groupId)) {

            // create move audit records for all items in this snippet
            final Collection<Action> actions = new ArrayList<>();

            for (String id : snippet.getProcessors().keySet()) {
                final ProcessorNode processor = processorDAO.getProcessor(id);
                final Action action = processorAuditor.generateAuditRecord(processor, Operation.Move, createMoveDetails(previousGroupId, groupId, logger));
                if (action != null) {
                    actions.add(action);
                }
            }

            for (String id : snippet.getFunnels().keySet()) {
                final Funnel funnel = funnelDAO.getFunnel(id);
                final Action action = funnelAuditor.generateAuditRecord(funnel, Operation.Move, createMoveDetails(previousGroupId, groupId, logger));
                if (action != null) {
                    actions.add(action);
                }
            }

            for (String id : snippet.getInputPorts().keySet()) {
                final Port port = inputPortDAO.getPort(id);
                final Action action = portAuditor.generateAuditRecord(port, Operation.Move, createMoveDetails(previousGroupId, groupId, logger));
                if (action != null) {
                    actions.add(action);
                }
            }

            for (String id : snippet.getOutputPorts().keySet()) {
                final Port port = outputPortDAO.getPort(id);
                final Action action = portAuditor.generateAuditRecord(port, Operation.Move, createMoveDetails(previousGroupId, groupId, logger));
                if (action != null) {
                    actions.add(action);
                }
            }

            for (String id : snippet.getRemoteProcessGroups().keySet()) {
                final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(id);
                final Action action = remoteProcessGroupAuditor.generateAuditRecord(remoteProcessGroup, Operation.Move, createMoveDetails(previousGroupId, groupId, logger));
                if (action != null) {
                    actions.add(action);
                }
            }

            for (String id : snippet.getProcessGroups().keySet()) {
                final ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
                final ProcessGroup processGroup = processGroupDAO.getProcessGroup(id);
                final Action action = processGroupAuditor.generateAuditRecord(processGroup, Operation.Move, createMoveDetails(previousGroupId, groupId, logger));
                if (action != null) {
                    actions.add(action);
                }
            }

            for (String id : snippet.getConnections().keySet()) {
                final Connection connection = connectionDAO.getConnection(id);
                final Action action = relationshipAuditor.generateAuditRecordForConnection(connection, Operation.Move, createMoveDetails(previousGroupId, groupId, logger));
                if (action != null) {
                    actions.add(action);
                }
            }

            // save the actions
            if (CollectionUtils.isNotEmpty(actions)) {
                saveActions(actions, logger);
            }
        }

        return snippet;
    }

    /**
     * Audits bulk delete.
     *
     * @param proceedingJoinPoint join point
     * @param snippetId snippet id
     * @param snippetDAO dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.SnippetDAO+) && "
            + "execution(void deleteSnippetComponents(java.lang.String)) && "
            + "args(snippetId) && "
            + "target(snippetDAO)")
    public void removeSnippetAdvice(ProceedingJoinPoint proceedingJoinPoint, String snippetId, SnippetDAO snippetDAO) throws Throwable {
        // get the snippet before removing it
        final Snippet snippet = snippetDAO.getSnippet(snippetId);

        // locate all the components being removed
        final Set<Funnel> funnels = new HashSet<>();
        for (String id : snippet.getFunnels().keySet()) {
            funnels.add(funnelDAO.getFunnel(id));
        }

        final Set<Port> inputPorts = new HashSet<>();
        for (String id : snippet.getInputPorts().keySet()) {
            inputPorts.add(inputPortDAO.getPort(id));
        }

        final Set<Port> outputPorts = new HashSet<>();
        for (String id : snippet.getOutputPorts().keySet()) {
            outputPorts.add(outputPortDAO.getPort(id));
        }

        final Set<RemoteProcessGroup> remoteProcessGroups = new HashSet<>();
        for (String id : snippet.getRemoteProcessGroups().keySet()) {
            remoteProcessGroups.add(remoteProcessGroupDAO.getRemoteProcessGroup(id));
        }

        final Set<ProcessGroup> processGroups = new HashSet<>();
        final ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        for (String id : snippet.getProcessGroups().keySet()) {
            processGroups.add(processGroupDAO.getProcessGroup(id));
        }

        final Set<ProcessorNode> processors = new HashSet<>();
        for (String id : snippet.getProcessors().keySet()) {
            processors.add(processorDAO.getProcessor(id));
        }

        final Set<Connection> connections = new HashSet<>();
        for (String id : snippet.getConnections().keySet()) {
            connections.add(connectionDAO.getConnection(id));
        }

        // remove the snippet and components
        proceedingJoinPoint.proceed();

        final Collection<Action> actions = new ArrayList<>();

        // audit funnel removal
        for (Funnel funnel : funnels) {
            final Action action = funnelAuditor.generateAuditRecord(funnel, Operation.Remove);
            if (action != null) {
                actions.add(action);
            }
        }

        for (Port inputPort : inputPorts) {
            final Action action = portAuditor.generateAuditRecord(inputPort, Operation.Remove);
            if (action != null) {
                actions.add(action);
            }
        }

        for (Port outputPort : outputPorts) {
            final Action action = portAuditor.generateAuditRecord(outputPort, Operation.Remove);
            if (action != null) {
                actions.add(action);
            }
        }

        for (RemoteProcessGroup remoteProcessGroup : remoteProcessGroups) {
            final Action action = remoteProcessGroupAuditor.generateAuditRecord(remoteProcessGroup, Operation.Remove);
            if (action != null) {
                actions.add(action);
            }
        }

        for (ProcessGroup processGroup : processGroups) {
            final Action action = processGroupAuditor.generateAuditRecord(processGroup, Operation.Remove);
            if (action != null) {
                actions.add(action);
            }
        }

        for (ProcessorNode processor : processors) {
            final Action action = processorAuditor.generateAuditRecord(processor, Operation.Remove);
            if (action != null) {
                actions.add(action);
            }
        }

        for (Connection connection : connections) {
            final ConnectDetails connectDetails = relationshipAuditor.createConnectDetails(connection, connection.getRelationships());
            final Action action = relationshipAuditor.generateAuditRecordForConnection(connection, Operation.Disconnect, connectDetails);
            if (action != null) {
                actions.add(action);
            }
        }

        // save the actions
        if (CollectionUtils.isNotEmpty(actions)) {
            saveActions(actions, logger);
        }
    }

    /* setters */
    public void setFunnelDAO(FunnelDAO funnelDAO) {
        this.funnelDAO = funnelDAO;
    }

    public void setInputPortDAO(PortDAO inputPortDAO) {
        this.inputPortDAO = inputPortDAO;
    }

    public void setOutputPortDAO(PortDAO outputPortDAO) {
        this.outputPortDAO = outputPortDAO;
    }

    public void setPortAuditor(PortAuditor portAuditor) {
        this.portAuditor = portAuditor;
    }

    public void setFunnelAuditor(FunnelAuditor funnelAuditor) {
        this.funnelAuditor = funnelAuditor;
    }

    public void setProcessGroupAuditor(ProcessGroupAuditor processGroupAuditor) {
        this.processGroupAuditor = processGroupAuditor;
    }

    public void setRemoteProcessGroupAuditor(RemoteProcessGroupAuditor remoteProcessGroupAuditor) {
        this.remoteProcessGroupAuditor = remoteProcessGroupAuditor;
    }

    public void setRemoteProcessGroupDAO(RemoteProcessGroupDAO remoteProcessGroupDAO) {
        this.remoteProcessGroupDAO = remoteProcessGroupDAO;
    }

    public void setConnectionDAO(ConnectionDAO connectionDAO) {
        this.connectionDAO = connectionDAO;
    }

    public void setProcessorAuditor(ProcessorAuditor processorAuditor) {
        this.processorAuditor = processorAuditor;
    }

    public void setProcessorDAO(ProcessorDAO processorDAO) {
        this.processorDAO = processorDAO;
    }

    public void setRelationshipAuditor(RelationshipAuditor relationshipAuditor) {
        this.relationshipAuditor = relationshipAuditor;
    }

}
