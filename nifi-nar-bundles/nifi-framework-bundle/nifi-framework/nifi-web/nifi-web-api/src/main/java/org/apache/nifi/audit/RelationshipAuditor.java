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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConnectDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.action.details.FlowChangeConnectDetails;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Audits relationship creation/removal.
 */
@Aspect
public class RelationshipAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(RelationshipAuditor.class);

    private static final String NAME = "Name";
    private static final String FLOW_FILE_EXPIRATION = "File Expiration";
    private static final String BACK_PRESSURE_OBJECT_THRESHOLD = "Back Pressure Object Threshold";
    private static final String BACK_PRESSURE_DATA_SIZE_THRESHOLD = "Back Pressure Data Size Threshold";
    private static final String PRIORITIZERS = "Prioritizers";

    /**
     * Audits the creation of relationships via createConnection().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint join point
     * @return connection
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ConnectionDAO+) && "
            + "execution(org.apache.nifi.connectable.Connection createConnection(java.lang.String, org.apache.nifi.web.api.dto.ConnectionDTO))")
    public Connection createConnectionAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // perform the underlying operation
        Connection connection = (Connection) proceedingJoinPoint.proceed();

        // audit the connection creation
        final ConnectDetails connectDetails = createConnectDetails(connection, connection.getRelationships());
        final Action action = generateAuditRecordForConnection(connection, Operation.Connect, connectDetails);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return connection;
    }

    /**
     * Audits the creation and removal of relationships via updateConnection().
     *
     * @param proceedingJoinPoint join point
     * @param connectionDTO dto
     * @param connectionDAO dao
     * @return connection
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ConnectionDAO+) && "
            + "execution(org.apache.nifi.connectable.Connection updateConnection(org.apache.nifi.web.api.dto.ConnectionDTO)) && "
            + "args(connectionDTO) && "
            + "target(connectionDAO)")
    public Connection updateConnectionAdvice(ProceedingJoinPoint proceedingJoinPoint, ConnectionDTO connectionDTO, ConnectionDAO connectionDAO) throws Throwable {
        // get the previous configuration
        Connection connection = connectionDAO.getConnection(connectionDTO.getId());
        Connectable previousDestination = connection.getDestination();
        Collection<Relationship> previousRelationships = connection.getRelationships();
        Map<String, String> values = extractConfiguredPropertyValues(connection, connectionDTO);

        // perform the underlying operation
        connection = (Connection) proceedingJoinPoint.proceed();

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            Collection<Action> actions = new ArrayList<>();
            Map<String, String> updatedValues = extractConfiguredPropertyValues(connection, connectionDTO);

            // get the source
            Connectable source = connection.getSource();

            // get the current target
            Connectable destination = connection.getDestination();

            // determine if a new target was specified in the initial request
            if (destination != null && !previousDestination.getIdentifier().equals(destination.getIdentifier())) {
                // record the removal of all relationships from the previous target
                final ConnectDetails disconnectDetails = createConnectDetails(connection, source, previousRelationships, previousDestination);
                actions.add(generateAuditRecordForConnection(connection, Operation.Disconnect, disconnectDetails));

                // record the addition of all relationships to the new target
                final ConnectDetails connectDetails = createConnectDetails(connection, connection.getRelationships());
                actions.add(generateAuditRecordForConnection(connection, Operation.Connect, connectDetails));
            }

            // audit and relationships added/removed
            Collection<Relationship> newRelationships = connection.getRelationships();

            // identify any relationships that were added
            if (newRelationships != null) {
                List<Relationship> relationshipsToAdd = new ArrayList<>(newRelationships);
                if (previousRelationships != null) {
                    relationshipsToAdd.removeAll(previousRelationships);
                }
                if (!relationshipsToAdd.isEmpty()) {
                    final ConnectDetails connectDetails = createConnectDetails(connection, relationshipsToAdd);
                    actions.add(generateAuditRecordForConnection(connection, Operation.Connect, connectDetails));
                }
            }

            // identify any relationships that were removed
            if (previousRelationships != null) {
                List<Relationship> relationshipsToRemove = new ArrayList<>(previousRelationships);
                if (newRelationships != null) {
                    relationshipsToRemove.removeAll(newRelationships);
                }
                if (!relationshipsToRemove.isEmpty()) {
                    final ConnectDetails connectDetails = createConnectDetails(connection, relationshipsToRemove);
                    actions.add(generateAuditRecordForConnection(connection, Operation.Disconnect, connectDetails));
                }
            }

            // go through each updated value
            Date actionTimestamp = new Date();
            for (String property : updatedValues.keySet()) {
                String newValue = updatedValues.get(property);
                String oldValue = values.get(property);

                // ensure the value is changing
                if (oldValue == null || newValue == null || !newValue.equals(oldValue)) {
                    // create the config details
                    FlowChangeConfigureDetails configurationDetails = new FlowChangeConfigureDetails();
                    configurationDetails.setName(property);
                    configurationDetails.setValue(newValue);
                    configurationDetails.setPreviousValue(oldValue);

                    // create a configuration action
                    FlowChangeAction configurationAction = new FlowChangeAction();
                    configurationAction.setUserIdentity(user.getIdentity());
                    configurationAction.setOperation(Operation.Configure);
                    configurationAction.setTimestamp(actionTimestamp);
                    configurationAction.setSourceId(connection.getIdentifier());
                    configurationAction.setSourceName(connection.getName());
                    configurationAction.setSourceType(Component.Connection);
                    configurationAction.setActionDetails(configurationDetails);
                    actions.add(configurationAction);
                }

            }

            // save the actions
            if (!actions.isEmpty()) {
                saveActions(actions, logger);
            }
        }

        return connection;
    }

    /**
     * Audits the removal of relationships via deleteConnection().
     *
     * @param proceedingJoinPoint join point
     * @param id id
     * @param connectionDAO dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ConnectionDAO+) && "
            + "execution(void deleteConnection(java.lang.String)) && "
            + "args(id) && "
            + "target(connectionDAO)")
    public void removeConnectionAdvice(ProceedingJoinPoint proceedingJoinPoint, String id, ConnectionDAO connectionDAO) throws Throwable {
        // get the connection before performing the update
        Connection connection = connectionDAO.getConnection(id);

        // perform the underlying operation
        proceedingJoinPoint.proceed();

        // audit the connection creation
        final ConnectDetails connectDetails = createConnectDetails(connection, connection.getRelationships());
        final Action action = generateAuditRecordForConnection(connection, Operation.Disconnect, connectDetails);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    public ConnectDetails createConnectDetails(final Connection connection, final Collection<Relationship> relationships) {
        return createConnectDetails(connection, connection.getSource(), relationships, connection.getDestination());
    }

    /**
     * Creates action details for connect/disconnect actions.
     *
     * @param connection connection
     * @param source source
     * @param relationships relationships
     * @param destination destinations
     * @return details
     */
    public ConnectDetails createConnectDetails(final Connection connection, final Connectable source, final Collection<Relationship> relationships, final Connectable destination) {
        final Component sourceType = determineConnectableType(source);
        final Component destiantionType = determineConnectableType(destination);

        // format the relationship names
        Collection<String> relationshipNames = new HashSet<>(connection.getRelationships().size());
        for (final Relationship relationship : relationships) {
            relationshipNames.add(relationship.getName());
        }
        final String formattedRelationships = relationshipNames.isEmpty() ? StringUtils.EMPTY : StringUtils.join(relationshipNames, ", ");

        // create the connect details
        final FlowChangeConnectDetails connectDetails = new FlowChangeConnectDetails();
        connectDetails.setSourceId(source.getIdentifier());
        connectDetails.setSourceName(source.getName());
        connectDetails.setSourceType(sourceType);
        connectDetails.setRelationship(formattedRelationships);
        connectDetails.setDestinationId(destination.getIdentifier());
        connectDetails.setDestinationName(destination.getName());
        connectDetails.setDestinationType(destiantionType);
        return connectDetails;
    }

    /**
     * Extracts configured settings from the specified connection only if they have also been specified in the connectionDTO.
     *
     * @param connection connection
     * @param connectionDTO dto
     * @return properties
     */
    private Map<String, String> extractConfiguredPropertyValues(Connection connection, ConnectionDTO connectionDTO) {
        Map<String, String> values = new HashMap<>();

        if (connectionDTO.getName() != null) {
            values.put(NAME, connection.getName());
        }
        if (connectionDTO.getFlowFileExpiration() != null) {
            values.put(FLOW_FILE_EXPIRATION, String.valueOf(connection.getFlowFileQueue().getFlowFileExpiration()));
        }
        if (connectionDTO.getBackPressureObjectThreshold() != null) {
            values.put(BACK_PRESSURE_OBJECT_THRESHOLD, String.valueOf(connection.getFlowFileQueue().getBackPressureObjectThreshold()));
        }
        if (connectionDTO.getBackPressureDataSizeThreshold() != null) {
            values.put(BACK_PRESSURE_DATA_SIZE_THRESHOLD, String.valueOf(connection.getFlowFileQueue().getBackPressureDataSizeThreshold()));
        }
        if (connectionDTO.getPrioritizers() != null) {
            List<String> prioritizers = new ArrayList<>();
            for (FlowFilePrioritizer prioritizer : connection.getFlowFileQueue().getPriorities()) {
                prioritizers.add(prioritizer.getClass().getCanonicalName());
            }
            values.put(PRIORITIZERS, StringUtils.join(prioritizers, ", "));
        }

        return values;
    }

    /**
     * Generates the audit records for the specified connection.
     *
     * @param connection connection
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecordForConnection(Connection connection, Operation operation) {
        return generateAuditRecordForConnection(connection, operation, null);
    }

    /**
     * Generates the audit records for the specified connection.
     *
     * @param connection connection
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecordForConnection(Connection connection, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // determine the source details
            final String connectionId = connection.getIdentifier();

            String connectionName = connection.getName();
            if (StringUtils.isBlank(connectionName)) {
                Collection<String> relationshipNames = new HashSet<>(connection.getRelationships().size());
                for (final Relationship relationship : connection.getRelationships()) {
                    relationshipNames.add(relationship.getName());
                }
                connectionName = StringUtils.join(relationshipNames, ", ");
            }

            // go through each relationship added
            Date actionTimestamp = new Date();

            // create a new relationship action
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(actionTimestamp);
            action.setSourceId(connectionId);
            action.setSourceName(connectionName);
            action.setSourceType(Component.Connection);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Determines the type of component the specified connectable is.
     */
    private Component determineConnectableType(Connectable connectable) {
        String sourceId = connectable.getIdentifier();
        Component componentType = Component.Controller;
        if (connectable instanceof ProcessorNode) {
            componentType = Component.Processor;
        } else if (connectable instanceof RemoteGroupPort) {
            final RemoteGroupPort remoteGroupPort = (RemoteGroupPort) connectable;
            if (TransferDirection.RECEIVE.equals(remoteGroupPort.getTransferDirection())) {
                if (remoteGroupPort.getRemoteProcessGroup() == null) {
                    componentType = Component.InputPort;
                } else {
                    componentType = Component.OutputPort;
                }
            } else {
                if (remoteGroupPort.getRemoteProcessGroup() == null) {
                    componentType = Component.OutputPort;
                } else {
                    componentType = Component.InputPort;
                }
            }
        } else if (connectable instanceof Port) {
            ProcessGroup processGroup = connectable.getProcessGroup();
            if (processGroup.getInputPort(sourceId) != null) {
                componentType = Component.InputPort;
            } else if (processGroup.getOutputPort(sourceId) != null) {
                componentType = Component.OutputPort;
            }
        } else if (connectable instanceof Funnel) {
            componentType = Component.Funnel;
        }
        return componentType;
    }

}
