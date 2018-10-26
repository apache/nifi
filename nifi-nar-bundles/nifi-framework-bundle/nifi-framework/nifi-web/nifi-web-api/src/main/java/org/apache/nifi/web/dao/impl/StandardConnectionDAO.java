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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.ListFlowFileStatus;
import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

public class StandardConnectionDAO extends ComponentDAO implements ConnectionDAO {

    private static final Logger logger = LoggerFactory.getLogger(StandardConnectionDAO.class);

    private FlowController flowController;
    private Authorizer authorizer;

    private Connection locateConnection(final String connectionId) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        final Connection connection = rootGroup.findConnection(connectionId);

        if (connection == null) {
            throw new ResourceNotFoundException(String.format("Unable to find connection with id '%s'.", connectionId));
        } else {
            return connection;
        }
    }

    @Override
    public boolean hasConnection(String id) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        return rootGroup.findConnection(id) != null;
    }

    @Override
    public Connection getConnection(final String id) {
        return locateConnection(id);
    }

    @Override
    public Set<Connection> getConnections(final String groupId) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getConnections();
    }

    @Override
    public DropFlowFileStatus getFlowFileDropRequest(String connectionId, String dropRequestId) {
        final Connection connection = locateConnection(connectionId);
        final FlowFileQueue queue = connection.getFlowFileQueue();

        final DropFlowFileStatus dropRequest = queue.getDropFlowFileStatus(dropRequestId);
        if (dropRequest == null) {
            throw new ResourceNotFoundException(String.format("Unable to find drop request with id '%s'.", dropRequestId));
        }

        return dropRequest;
    }

    @Override
    public ListFlowFileStatus getFlowFileListingRequest(String connectionId, String listingRequestId) {
        final Connection connection = locateConnection(connectionId);
        final FlowFileQueue queue = connection.getFlowFileQueue();

        final ListFlowFileStatus listRequest = queue.getListFlowFileStatus(listingRequestId);
        if (listRequest == null) {
            throw new ResourceNotFoundException(String.format("Unable to find listing request with id '%s'.", listingRequestId));
        }

        return listRequest;
    }

    @Override
    public FlowFileRecord getFlowFile(String id, String flowFileUuid) {
        try {
            final Connection connection = locateConnection(id);
            final FlowFileQueue queue = connection.getFlowFileQueue();
            final FlowFileRecord flowFile = queue.getFlowFile(flowFileUuid);

            if (flowFile == null) {
                throw new ResourceNotFoundException(String.format("The FlowFile with UUID %s is no longer in the active queue.", flowFileUuid));
            }

            // get the attributes and ensure appropriate access
            final Map<String, String> attributes = flowFile.getAttributes();
            final Authorizable dataAuthorizable = new DataAuthorizable(connection.getSourceAuthorizable());
            dataAuthorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser(), attributes);

            return flowFile;
        } catch (final IOException ioe) {
            logger.error(String.format("Unable to get the flowfile (%s) at this time.", flowFileUuid), ioe);
            throw new IllegalStateException("Unable to get the FlowFile at this time.");
        }
    }

    /**
     * Configures the specified connection using the specified dto.
     */
    private void configureConnection(Connection connection, ConnectionDTO connectionDTO) {
        // validate flow file comparators/prioritizers
        List<FlowFilePrioritizer> newPrioritizers = null;
        final List<String> prioritizers = connectionDTO.getPrioritizers();
        if (isNotNull(prioritizers)) {
            final List<String> newPrioritizersClasses = new ArrayList<>(prioritizers);
            newPrioritizers = new ArrayList<>();
            for (final String className : newPrioritizersClasses) {
                try {
                    newPrioritizers.add(flowController.getFlowManager().createPrioritizer(className));
                } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    throw new IllegalArgumentException("Unable to set prioritizer " + className + ": " + e);
                }
            }
        }

        // update connection queue
        if (isNotNull(connectionDTO.getFlowFileExpiration())) {
            connection.getFlowFileQueue().setFlowFileExpiration(connectionDTO.getFlowFileExpiration());
        }
        if (isNotNull(connectionDTO.getBackPressureObjectThreshold())) {
            connection.getFlowFileQueue().setBackPressureObjectThreshold(connectionDTO.getBackPressureObjectThreshold());
        }
        if (isNotNull(connectionDTO.getBackPressureDataSizeThreshold())) {
            connection.getFlowFileQueue().setBackPressureDataSizeThreshold(connectionDTO.getBackPressureDataSizeThreshold());
        }
        if (isNotNull(newPrioritizers)) {
            connection.getFlowFileQueue().setPriorities(newPrioritizers);
        }

        final String loadBalanceStrategyName = connectionDTO.getLoadBalanceStrategy();
        final String loadBalancePartitionAttribute = connectionDTO.getLoadBalancePartitionAttribute();
        if (isNotNull(loadBalanceStrategyName)) {
            final LoadBalanceStrategy loadBalanceStrategy = LoadBalanceStrategy.valueOf(loadBalanceStrategyName);
            connection.getFlowFileQueue().setLoadBalanceStrategy(loadBalanceStrategy, loadBalancePartitionAttribute);
        }

        final String loadBalanceCompressionName = connectionDTO.getLoadBalanceCompression();
        if (isNotNull(loadBalanceCompressionName)) {
            connection.getFlowFileQueue().setLoadBalanceCompression(LoadBalanceCompression.valueOf(loadBalanceCompressionName));
        }

        // update the connection state
        if (isNotNull(connectionDTO.getBends())) {
            final List<Position> bendPoints = new ArrayList<>();
            for (final PositionDTO bend : connectionDTO.getBends()) {
                if (bend != null) {
                    bendPoints.add(new Position(bend.getX(), bend.getY()));
                }
            }
            connection.setBendPoints(bendPoints);
        }
        if (isNotNull(connectionDTO.getName())) {
            connection.setName(connectionDTO.getName());
        }
        if (isNotNull(connectionDTO.getLabelIndex())) {
            connection.setLabelIndex(connectionDTO.getLabelIndex());
        }
        if (isNotNull(connectionDTO.getzIndex())) {
            connection.setZIndex(connectionDTO.getzIndex());
        }
    }

    /**
     * Validates the proposed processor configuration.
     */
    private List<String> validateProposedConfiguration(final String groupId, final ConnectionDTO connectionDTO) {
        List<String> validationErrors = new ArrayList<>();

        if (isNotNull(connectionDTO.getBackPressureObjectThreshold()) && connectionDTO.getBackPressureObjectThreshold() < 0) {
            validationErrors.add("Max queue size must be a non-negative integer");
        }
        if (isNotNull(connectionDTO.getFlowFileExpiration())) {
            Matcher expirationMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(connectionDTO.getFlowFileExpiration());
            if (!expirationMatcher.matches()) {
                validationErrors.add("Flow file expiration is not a valid time duration (ie 30 sec, 5 min)");
            }
        }
        if (isNotNull(connectionDTO.getLabelIndex())) {
            if (connectionDTO.getLabelIndex() < 0) {
                validationErrors.add("The label index must be positive.");
            }
        }

        // validation is required when connecting to a remote process group since each node in a
        // cluster may or may not be authorized
        final ConnectableDTO proposedDestination = connectionDTO.getDestination();
        if (proposedDestination != null && ConnectableType.REMOTE_INPUT_PORT.name().equals(proposedDestination.getType())) {
            // the group id must be specified
            if (proposedDestination.getGroupId() == null) {
                validationErrors.add("When the destination is a remote input port its group id is required.");
                return validationErrors;
            }

            // attempt to location the proprosed destination
            final ProcessGroup destinationParentGroup = locateProcessGroup(flowController, groupId);
            final RemoteProcessGroup remoteProcessGroup = destinationParentGroup.getRemoteProcessGroup(proposedDestination.getGroupId());
            if (remoteProcessGroup == null) {
                validationErrors.add("Unable to find the specified remote process group.");
                return validationErrors;
            }

            // ensure the new destination was found
            final RemoteGroupPort remoteInputPort = remoteProcessGroup.getInputPort(proposedDestination.getId());
            if (remoteInputPort == null) {
                validationErrors.add("Unable to find the specified destination.");
                return validationErrors;
            }
        }

        return validationErrors;
    }

    @Override
    public Connection createConnection(final String groupId, final ConnectionDTO connectionDTO) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);

        if (isNotNull(connectionDTO.getParentGroupId()) && !flowController.getFlowManager().areGroupsSame(connectionDTO.getParentGroupId(), groupId)) {
            throw new IllegalStateException("Cannot specify a different Parent Group ID than the Group to which the Connection is being added");
        }

        // get the source and destination connectables
        final ConnectableDTO sourceConnectableDTO = connectionDTO.getSource();
        final ConnectableDTO destinationConnectableDTO = connectionDTO.getDestination();

        // ensure both are specified
        if (sourceConnectableDTO == null || destinationConnectableDTO == null) {
            throw new IllegalArgumentException("Both source and destinations must be specified.");
        }

        // if the source/destination connectable's group id has not been set, its inferred to be the current group
        if (sourceConnectableDTO.getGroupId() == null) {
            sourceConnectableDTO.setGroupId(groupId);
        }
        if (destinationConnectableDTO.getGroupId() == null) {
            destinationConnectableDTO.setGroupId(groupId);
        }

        // validate the proposed configuration
        final List<String> validationErrors = validateProposedConfiguration(groupId, connectionDTO);

        // ensure there was no validation errors
        if (!validationErrors.isEmpty()) {
            throw new ValidationException(validationErrors);
        }

        // find the source
        final Connectable source;
        if (ConnectableType.REMOTE_OUTPUT_PORT.name().equals(sourceConnectableDTO.getType())) {
            final ProcessGroup sourceParentGroup = locateProcessGroup(flowController, groupId);
            final RemoteProcessGroup remoteProcessGroup = sourceParentGroup.getRemoteProcessGroup(sourceConnectableDTO.getGroupId());
            source = remoteProcessGroup.getOutputPort(sourceConnectableDTO.getId());
        } else {
            final ProcessGroup sourceGroup = locateProcessGroup(flowController, sourceConnectableDTO.getGroupId());
            source = sourceGroup.getConnectable(sourceConnectableDTO.getId());
        }

        // find the destination
        final Connectable destination;
        if (ConnectableType.REMOTE_INPUT_PORT.name().equals(destinationConnectableDTO.getType())) {
            final ProcessGroup destinationParentGroup = locateProcessGroup(flowController, groupId);
            final RemoteProcessGroup remoteProcessGroup = destinationParentGroup.getRemoteProcessGroup(destinationConnectableDTO.getGroupId());
            destination = remoteProcessGroup.getInputPort(destinationConnectableDTO.getId());
        } else {
            final ProcessGroup destinationGroup = locateProcessGroup(flowController, destinationConnectableDTO.getGroupId());
            destination = destinationGroup.getConnectable(destinationConnectableDTO.getId());
        }

        // determine the relationships
        final Set<String> relationships = new HashSet<>();
        if (isNotNull(connectionDTO.getSelectedRelationships())) {
            relationships.addAll(connectionDTO.getSelectedRelationships());
        }

        // create the connection
        final Connection connection = flowController.createConnection(connectionDTO.getId(), connectionDTO.getName(), source, destination, relationships);

        // configure the connection
        configureConnection(connection, connectionDTO);

        // add the connection to the group
        group.addConnection(connection);
        return connection;
    }

    @Override
    public DropFlowFileStatus createFlowFileDropRequest(String id, String dropRequestId) {
        final Connection connection = locateConnection(id);
        final FlowFileQueue queue = connection.getFlowFileQueue();

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        return queue.dropFlowFiles(dropRequestId, user.getIdentity());
    }

    @Override
    public ListFlowFileStatus createFlowFileListingRequest(String id, String listingRequestId) {
        final Connection connection = locateConnection(id);
        final FlowFileQueue queue = connection.getFlowFileQueue();

        // ensure we can list
        verifyList(queue);

        return queue.listFlowFiles(listingRequestId, 100);
    }

    @Override
    public void verifyCreate(String groupId, ConnectionDTO connectionDTO) {
        // validate the incoming request
        final List<String> validationErrors = validateProposedConfiguration(groupId, connectionDTO);

        // ensure there was no validation errors
        if (!validationErrors.isEmpty()) {
            throw new ValidationException(validationErrors);
        }

        // Ensure that both the source and the destination for the connection exist.
        // In the case that the source or destination is a port in a Remote Process Group,
        // this is necessary because the ports can change in the background. It may still be
        // possible for a port to disappear between the 'verify' stage and the creation stage,
        // but this prevents the case where some nodes already know about the port while other
        // nodes in the cluster do not. This is a more common case, as users may try to connect
        // to the port as soon as the port is created.
        final ConnectableDTO sourceDto = connectionDTO.getSource();
        if (sourceDto == null || sourceDto.getId() == null) {
            throw new IllegalArgumentException("Cannot create connection without specifying source");
        }

        final ConnectableDTO destinationDto = connectionDTO.getDestination();
        if (destinationDto == null || destinationDto.getId() == null) {
            throw new IllegalArgumentException("Cannot create connection without specifying destination");
        }

        if (ConnectableType.REMOTE_OUTPUT_PORT.name().equals(sourceDto.getType())) {
            final ProcessGroup sourceParentGroup = locateProcessGroup(flowController, groupId);

            final RemoteProcessGroup remoteProcessGroup = sourceParentGroup.getRemoteProcessGroup(sourceDto.getGroupId());
            if (remoteProcessGroup == null) {
                throw new IllegalArgumentException("Unable to find the specified remote process group.");
            }

            final RemoteGroupPort sourceConnectable = remoteProcessGroup.getOutputPort(sourceDto.getId());
            if (sourceConnectable == null) {
                throw new IllegalArgumentException("The specified source for the connection does not exist");
            } else if (!sourceConnectable.getTargetExists()) {
                throw new IllegalArgumentException("The specified remote output port does not exist.");
            }
        } else {
            final ProcessGroup sourceGroup = locateProcessGroup(flowController, sourceDto.getGroupId());
            final Connectable sourceConnectable = sourceGroup.getConnectable(sourceDto.getId());
            if (sourceConnectable == null) {
                throw new IllegalArgumentException("The specified source for the connection does not exist");
            }
        }

        if (ConnectableType.REMOTE_INPUT_PORT.name().equals(destinationDto.getType())) {
            final ProcessGroup destinationParentGroup = locateProcessGroup(flowController, groupId);

            final RemoteProcessGroup remoteProcessGroup = destinationParentGroup.getRemoteProcessGroup(destinationDto.getGroupId());
            if (remoteProcessGroup == null) {
                throw new IllegalArgumentException("Unable to find the specified remote process group.");
            }

            final RemoteGroupPort destinationConnectable = remoteProcessGroup.getInputPort(destinationDto.getId());
            if (destinationConnectable == null) {
                throw new IllegalArgumentException("The specified destination for the connection does not exist");
            } else if (!destinationConnectable.getTargetExists()) {
                throw new IllegalArgumentException("The specified remote input port does not exist.");
            }
        } else {
            final ProcessGroup destinationGroup = locateProcessGroup(flowController, destinationDto.getGroupId());
            final Connectable destinationConnectable = destinationGroup.getConnectable(destinationDto.getId());
            if (destinationConnectable == null) {
                throw new IllegalArgumentException("The specified destination for the connection does not exist");
            }
        }
    }

    private void verifyList(final FlowFileQueue queue) {
        queue.verifyCanList();
    }

    @Override
    public void verifyList(String id) {
        final Connection connection = locateConnection(id);
        final FlowFileQueue queue = connection.getFlowFileQueue();
        verifyList(queue);
    }

    @Override
    public void verifyUpdate(ConnectionDTO connectionDTO) {
        verifyUpdate(locateConnection(connectionDTO.getId()), connectionDTO);
    }

    private void verifyUpdate(final Connection connection, final ConnectionDTO connectionDTO) {
        // determine what the request is attempting
        if (isAnyNotNull(connectionDTO.getBackPressureDataSizeThreshold(),
                connectionDTO.getBackPressureObjectThreshold(),
                connectionDTO.getDestination(),
                connectionDTO.getFlowFileExpiration(),
                connectionDTO.getName(),
                connectionDTO.getPosition(),
                connectionDTO.getPrioritizers(),
                connectionDTO.getSelectedRelationships())) {

            // validate the incoming request
            final List<String> validationErrors = validateProposedConfiguration(connection.getProcessGroup().getIdentifier(), connectionDTO);

            // ensure there was no validation errors
            if (!validationErrors.isEmpty()) {
                throw new ValidationException(validationErrors);
            }

            // If destination is changing, ensure that current destination is not running. This check is done here, rather than
            // in the Connection object itself because the Connection object itself does not know which updates are to occur and
            // we don't want to prevent updating things like the connection name or backpressure just because the destination is running
            final Connectable destination = connection.getDestination();
            if (destination != null && destination.isRunning() && destination.getConnectableType() != ConnectableType.FUNNEL && destination.getConnectableType() != ConnectableType.INPUT_PORT) {
                throw new ValidationException(Collections.singletonList("Cannot change the destination of connection because the current destination is running"));
            }

            // verify that this connection supports modification
            connection.verifyCanUpdate();
        }
    }

    @Override
    public Connection updateConnection(final ConnectionDTO connectionDTO) {
        final Connection connection = locateConnection(connectionDTO.getId());
        final ProcessGroup group = connection.getProcessGroup();

        // ensure we can update
        verifyUpdate(connection, connectionDTO);

        final Collection<Relationship> newProcessorRelationships = new ArrayList<>();
        Connectable newDestination = null;

        // ensure that the source ID is correct, if specified.
        final Connectable existingSource = connection.getSource();
        if (isNotNull(connectionDTO.getSource()) && !existingSource.getIdentifier().equals(connectionDTO.getSource().getId())) {
            throw new IllegalStateException("Connection with ID " + connectionDTO.getId() + " has conflicting Source ID");
        }

        // determine any new relationships
        final Set<String> relationships = connectionDTO.getSelectedRelationships();
        if (isNotNull(relationships)) {
            if (relationships.isEmpty()) {
                throw new IllegalArgumentException("Cannot remove all relationships from Connection with ID " + connection.getIdentifier() + " -- remove the Connection instead");
            }
            if (existingSource == null) {
                throw new IllegalArgumentException("Cannot specify new relationships without including the source.");
            }

            for (final String relationship : relationships) {
                final Relationship processorRelationship = existingSource.getRelationship(relationship);
                if (processorRelationship == null) {
                    throw new IllegalArgumentException("Unable to locate " + relationship + " relationship.");
                }
                newProcessorRelationships.add(processorRelationship);
            }
        }

        // determine if the destination changed
        final ConnectableDTO proposedDestination = connectionDTO.getDestination();
        if (proposedDestination != null) {
            final Connectable currentDestination = connection.getDestination();

            // handle remote input port differently
            if (ConnectableType.REMOTE_INPUT_PORT.name().equals(proposedDestination.getType())) {
                // the group id must be specified
                if (proposedDestination.getGroupId() == null) {
                    throw new IllegalArgumentException("When the destination is a remote input port its group id is required.");
                }

                // if the current destination is a remote input port
                boolean isDifferentRemoteProcessGroup = false;
                if (currentDestination.getConnectableType() == ConnectableType.REMOTE_INPUT_PORT) {
                    RemoteGroupPort remotePort = (RemoteGroupPort) currentDestination;
                    if (!proposedDestination.getGroupId().equals(remotePort.getRemoteProcessGroup().getIdentifier())) {
                        isDifferentRemoteProcessGroup = true;
                    }
                }

                // if the destination is changing or the previous destination was a different remote process group
                if (!proposedDestination.getId().equals(currentDestination.getIdentifier()) || isDifferentRemoteProcessGroup) {
                    final ProcessGroup destinationParentGroup = locateProcessGroup(flowController, group.getIdentifier());
                    final RemoteProcessGroup remoteProcessGroup = destinationParentGroup.getRemoteProcessGroup(proposedDestination.getGroupId());

                    // ensure the remote process group was found
                    if (remoteProcessGroup == null) {
                        throw new IllegalArgumentException("Unable to find the specified remote process group.");
                    }

                    final RemoteGroupPort remoteInputPort = remoteProcessGroup.getInputPort(proposedDestination.getId());

                    // ensure the new destination was found
                    if (remoteInputPort == null) {
                        throw new IllegalArgumentException("Unable to find the specified destination.");
                    }

                    // ensure the remote port actually exists
                    if (!remoteInputPort.getTargetExists()) {
                        throw new IllegalArgumentException("The specified remote input port does not exist.");
                    } else {
                        newDestination = remoteInputPort;
                    }
                }
            } else {
                // if there is a different destination id
                if (!proposedDestination.getId().equals(currentDestination.getIdentifier())) {
                    // if the destination connectable's group id has not been set, its inferred to be the current group
                    if (proposedDestination.getGroupId() == null) {
                        proposedDestination.setGroupId(group.getIdentifier());
                    }

                    final ProcessGroup destinationGroup = locateProcessGroup(flowController, proposedDestination.getGroupId());
                    newDestination = destinationGroup.getConnectable(proposedDestination.getId());

                    // ensure the new destination was found
                    if (newDestination == null) {
                        throw new IllegalArgumentException("Unable to find the specified destination.");
                    }
                }
            }
        }

        // configure the connection
        configureConnection(connection, connectionDTO);
        group.onComponentModified();

        // update the relationships if necessary
        if (!newProcessorRelationships.isEmpty()) {
            connection.setRelationships(newProcessorRelationships);
        }

        // update the destination if necessary
        if (isNotNull(newDestination)) {
            connection.setDestination(newDestination);
        }

        return connection;
    }

    @Override
    public void verifyDelete(String id) {
        final Connection connection = locateConnection(id);
        connection.verifyCanDelete();
    }

    @Override
    public void deleteConnection(final String id) {
        final Connection connection = locateConnection(id);
        connection.getProcessGroup().removeConnection(connection);
    }

    @Override
    public DropFlowFileStatus deleteFlowFileDropRequest(String connectionId, String dropRequestId) {
        final Connection connection = locateConnection(connectionId);
        final FlowFileQueue queue = connection.getFlowFileQueue();

        final DropFlowFileStatus dropFlowFileStatus = queue.cancelDropFlowFileRequest(dropRequestId);
        if (dropFlowFileStatus == null) {
            throw new ResourceNotFoundException(String.format("Unable to find drop request with id '%s'.", dropRequestId));
        }

        return dropFlowFileStatus;
    }

    @Override
    public ListFlowFileStatus deleteFlowFileListingRequest(String connectionId, String listingRequestId) {
        final Connection connection = locateConnection(connectionId);
        final FlowFileQueue queue = connection.getFlowFileQueue();

        final ListFlowFileStatus listFlowFileStatus = queue.cancelListFlowFileRequest(listingRequestId);
        if (listFlowFileStatus == null) {
            throw new ResourceNotFoundException(String.format("Unable to find listing request with id '%s'.", listingRequestId));
        }

        return listFlowFileStatus;
    }

    @Override
    public DownloadableContent getContent(String id, String flowFileUuid, String requestUri) {
        try {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();

            final Connection connection = locateConnection(id);
            final FlowFileQueue queue = connection.getFlowFileQueue();
            final FlowFileRecord flowFile = queue.getFlowFile(flowFileUuid);

            if (flowFile == null) {
                throw new ResourceNotFoundException(String.format("The FlowFile with UUID %s is no longer in the active queue.", flowFileUuid));
            }

            // get the attributes and ensure appropriate access
            final Map<String, String> attributes = flowFile.getAttributes();
            final Authorizable dataAuthorizable = new DataAuthorizable(connection.getSourceAuthorizable());
            dataAuthorizable.authorize(authorizer, RequestAction.READ, user, attributes);

            // get the filename and fall back to the identifier (should never happen)
            String filename = attributes.get(CoreAttributes.FILENAME.key());
            if (filename == null) {
                filename = flowFileUuid;
            }

            // get the mime-type
            final String type = attributes.get(CoreAttributes.MIME_TYPE.key());

            // get the content
            final InputStream content = flowController.getContent(flowFile, user.getIdentity(), requestUri);
            return new DownloadableContent(filename, type, content);
        } catch (final ContentNotFoundException cnfe) {
            throw new ResourceNotFoundException("Unable to find the specified content.");
        } catch (final IOException ioe) {
            logger.error(String.format("Unable to get the content for flowfile (%s) at this time.", flowFileUuid), ioe);
            throw new IllegalStateException("Unable to get the content at this time.");
        }
    }

    /* setters */
    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
