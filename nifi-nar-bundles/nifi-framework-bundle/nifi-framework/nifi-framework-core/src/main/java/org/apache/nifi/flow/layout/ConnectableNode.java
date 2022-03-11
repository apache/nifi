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

package org.apache.nifi.flow.layout;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ConnectableNode {
    public static double PROCESSOR_HEIGHT = 128D;
    public static double PROCESSOR_WIDTH = 352D;
    public static double PORT_HEIGHT = 80D;
    public static double PORT_WIDTH = 240D;
    public static double FUNNEL_HEIGHT = 48D;
    public static double FUNNEL_WIDTH = 48D;
    public static double PROCESS_GROUP_HEIGHT = 176D;
    public static double PROCESS_GROUP_WIDTH = 384D;
    public static double RPG_HEIGHT = 176D;
    public static double RPG_WIDTH = 384D;

    private final Connectable connectable;
    private final Set<Connection> outgoingConnections;
    private final List<Connection> incomingConnections;
    private final Position originalPosition;
    private final double height;
    private final double width;
    private final Positionable positionableComponent;
    private final ProcessGroup layoutGroup;

    private int graphLineIndex;
    private Position updatedPosition;
    private final ConnectableNodeRegistry nodeRegistry;

    // Lazily initialized in order to avoid infinite recursion
    private Set<ConnectableEdge> outgoingEdges;
    private Set<ConnectableEdge> incomingEdges;


    public ConnectableNode(final Connectable connectable, final ProcessGroup layoutGroup, final int graphLineIndex, final ConnectableNodeRegistry nodeRegistry) {
        this.connectable = connectable;
        this.graphLineIndex = graphLineIndex;
        this.nodeRegistry = nodeRegistry;
        this.layoutGroup = layoutGroup;

        final ConnectableType connectableType = connectable.getConnectableType();
        final boolean sameGroup = connectable.getProcessGroup().equals(layoutGroup);

        switch (connectableType) {
            case INPUT_PORT:
                if (sameGroup) {
                    incomingConnections = Collections.emptyList();
                    outgoingConnections = connectable.getConnections();
                    width = PORT_WIDTH;
                    height = PORT_HEIGHT;
                    originalPosition = connectable.getPosition();
                    positionableComponent = connectable;
                } else {
                    // Port is an Input Port of a child group. For the outgoing connections, we consider any connections coming out of the Process Group
                    incomingConnections = getIncomingConnections(connectable.getProcessGroup());
                    outgoingConnections = getOutgoingConnections(connectable.getProcessGroup());
                    width = PROCESS_GROUP_WIDTH;
                    height = PROCESS_GROUP_HEIGHT;
                    originalPosition = connectable.getProcessGroup().getPosition();
                    positionableComponent = connectable.getProcessGroup();
                }
                break;
            case OUTPUT_PORT:
                if (sameGroup) {
                    incomingConnections = connectable.getIncomingConnections();
                    outgoingConnections = Collections.emptySet();
                    width = PORT_WIDTH;
                    height = PORT_HEIGHT;
                    originalPosition = connectable.getPosition();
                    positionableComponent = connectable;
                } else {
                    // Port is an Output Port of a child group. For the incoming connections, we consider any connections coming in to the Process Group
                    incomingConnections = getIncomingConnections(connectable.getProcessGroup());
                    outgoingConnections = getOutgoingConnections(connectable.getProcessGroup());
                    width = PROCESS_GROUP_WIDTH;
                    height = PROCESS_GROUP_HEIGHT;
                    originalPosition = connectable.getProcessGroup().getPosition();
                    positionableComponent = connectable.getProcessGroup();
                }
                break;
            case REMOTE_INPUT_PORT:
            case REMOTE_OUTPUT_PORT: {
                final RemoteGroupPort remoteGroupPort = (RemoteGroupPort) connectable;
                final RemoteProcessGroup rpg = remoteGroupPort.getRemoteProcessGroup();
                incomingConnections = getIncomingConnections(rpg);
                outgoingConnections = getOutgoingConnections(rpg);
                width = RPG_WIDTH;
                height = RPG_HEIGHT;
                originalPosition = rpg.getPosition();
                positionableComponent = rpg;
                break;
            }
            case FUNNEL:
                incomingConnections = connectable.getIncomingConnections();
                outgoingConnections = connectable.getConnections();
                width = FUNNEL_WIDTH;
                height = FUNNEL_HEIGHT;
                originalPosition = connectable.getPosition();
                positionableComponent = connectable;
                break;
            case PROCESSOR:
            default:
                incomingConnections = connectable.getIncomingConnections();
                outgoingConnections = connectable.getConnections();
                width = PROCESSOR_WIDTH;
                height = PROCESSOR_HEIGHT;
                originalPosition = connectable.getPosition();
                positionableComponent = connectable;
                break;
        }

        updatedPosition = originalPosition;
    }

    public static List<Connection> getIncomingConnections(final ProcessGroup group) {
        return group.getInputPorts().stream()
            .flatMap(port -> port.getIncomingConnections().stream())
            .collect(Collectors.toList());
    }

    public static Set<Connection> getOutgoingConnections(final ProcessGroup group) {
        return group.getOutputPorts().stream()
            .flatMap(port -> port.getConnections().stream())
            .collect(Collectors.toSet());
    }

    public static List<Connection> getIncomingConnections(final RemoteProcessGroup rpg) {
        return rpg.getInputPorts().stream()
            .flatMap(port -> port.getIncomingConnections().stream())
            .collect(Collectors.toList());
    }

    public static Set<Connection> getOutgoingConnections(final RemoteProcessGroup rpg) {
        return rpg.getOutputPorts().stream()
            .flatMap(port -> port.getConnections().stream())
            .collect(Collectors.toSet());
    }

    private ConnectableEdge createEdge(final Connection connection, final ProcessGroup layoutGroup) {
        final ConnectableNode source;
        final ConnectableNode destination;

        if (connection.getSource() == connectable) {
            source = this;
        } else {
            source = nodeRegistry.getOrCreate(connection.getSource(), () -> new ConnectableNode(connection.getSource(), layoutGroup, graphLineIndex, nodeRegistry));
        }

        if (connection.getSource() == connection.getDestination()) {
            destination = source;
        } else {
            destination = nodeRegistry.getOrCreate(connection.getDestination(), () -> new ConnectableNode(connection.getDestination(), layoutGroup, graphLineIndex, nodeRegistry));
        }

        final ConnectableEdge edge = nodeRegistry.getOrCreate(connection, () -> new ConnectableEdge(source, destination, connection));
        return edge;
    }

    public Set<ConnectableEdge> getOutgoingEdges() {
        if (outgoingEdges != null) {
            return outgoingEdges;
        }

        outgoingEdges = outgoingConnections.stream()
            .map(con -> createEdge(con, layoutGroup))
            .collect(Collectors.toSet());

        return outgoingEdges;
    }

    public Set<ConnectableEdge> getIncomingEdges() {
        if (incomingEdges != null) {
            return incomingEdges;
        }

        incomingEdges = incomingConnections.stream()
            .map(con -> createEdge(con, layoutGroup))
            .collect(Collectors.toSet());

        return incomingEdges;
    }

    public Position getOriginalPosition() {
        return originalPosition;
    }

    public double getHeight() {
        return height;
    }

    public double getWidth() {
        return width;
    }

    public Position getUpdatedPosition() {
        return updatedPosition;
    }

    public Position getCenter() {
        final double centerX = updatedPosition.getX() + width / 2;
        final double centerY = updatedPosition.getY() + height / 2;
        return new Position(centerX, centerY);
    }

    public Position getRightCenter() {
        final double rightX = updatedPosition.getX() + width;
        final double centerY = updatedPosition.getY() + height / 2;
        return new Position(rightX, centerY);
    }

    public void setPosition(final Position position) {
        updatedPosition = position;
    }

    public void setCenter(final Position position) {
        final double left = position.getX() - width / 2;
        final double top = position.getY() - height / 2;
        setPosition(new Position(left, top));
    }

    public void applyPosition() {
        positionableComponent.setPosition(updatedPosition);

        getOutgoingEdges().forEach(ConnectableEdge::applyUpdates);
        getIncomingEdges().forEach(ConnectableEdge::applyUpdates);
    }

    public Positionable getPositionableComponent() {
        return positionableComponent;
    }

    public void setXCoordinate(final double x) {
        setPosition(new Position(x, updatedPosition.getY()));
    }

    public void setCenterXCoordinate(final double x) {
        final double left = x - width / 2;
        setXCoordinate(left);
    }

    public void setYCoordinate(final double y) {
        updatedPosition = new Position(updatedPosition.getX(), y);
    }

    public void setCenterYCoordinate(final double y) {
        final double top = y - height / 2;
        setYCoordinate(top);
    }

    public int getGraphLineIndex() {
        return graphLineIndex;
    }

    public void setGraphLineIndex(final int index) {
        this.graphLineIndex = index;
    }

    public void resetBendpoints(final ConnectableEdge edge) {
        if (edge.getSource().equals(edge.getDestination())) {
            // Self-looping connection
            final double rightCenterX = updatedPosition.getX() + width;
            final double rightCenterY = updatedPosition.getY() + height / 2;
            final double selfLoopXOffset = width / 2 + 5;
            final double selfLoopYOffset = 25;

            final Position topBendPoint = new Position(rightCenterX + selfLoopXOffset, rightCenterY - selfLoopYOffset);
            final Position bottomBendPoint = new Position(rightCenterX + selfLoopXOffset, rightCenterY + selfLoopYOffset);
            edge.setBendPoints(Arrays.asList(topBendPoint, bottomBendPoint));
        } else {
            // Not self looping so remove bendpoints
            edge.setBendPoints(Collections.emptyList());
        }
    }

    public Connectable getConnectable() {
        return connectable;
    }

    public boolean isTerminalNode() {
        final Set<ConnectableEdge> outgoingEdges = getOutgoingEdges();
        final boolean hasOutgoing = outgoingEdges.stream()
            .anyMatch(edge -> !edge.isSelfLoop());

        return !hasOutgoing;
    }

    public boolean isSourceNode() {
        final Set<ConnectableEdge> incomingEdges = getIncomingEdges();
        final boolean hasIncoming = incomingEdges.stream()
            .anyMatch(edge -> !edge.isSelfLoop());

        return !hasIncoming;
    }

    public Set<ConnectableEdge> getOutgoingEdgesTo(final ConnectableNode node) {
        Set<ConnectableEdge> matchingEdges = null; // initialize to null because most of the time we don't need this so don't bother creating it

        for (final ConnectableEdge edge : outgoingEdges) {
            if (edge.getDestination().equals(node)) {
                if (matchingEdges == null) {
                    matchingEdges = new HashSet<>();
                }

                matchingEdges.add(edge);
            }
        }

        return matchingEdges == null ? Collections.emptySet() : matchingEdges;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ConnectableNode that = (ConnectableNode) o;
        return positionableComponent.equals(that.positionableComponent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(positionableComponent);
    }

    @Override
    public String toString() {
        return "ConnectableNode[" + connectable + ", x=" + updatedPosition.getX() + ", y=" + updatedPosition.getY() + "]";
    }
}
