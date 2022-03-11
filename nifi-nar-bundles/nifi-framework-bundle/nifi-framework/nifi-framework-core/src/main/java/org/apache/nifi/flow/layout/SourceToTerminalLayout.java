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
import org.apache.nifi.connectable.Port;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.RemoteGroupPort;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A FlowLayout that identifies all source components within a Process Group, moves them to the top, and then traverses the graph depth-first
 * from source components to terminal components, laying out the components in a table-like structure with rows and columns, effectively.
 */
public class SourceToTerminalLayout implements FlowLayout {
    private static final int SHALLOW_GRAPH_DEPTH = 3;

    private ProcessGroup group;
    private final ConnectableNodeRegistry nodeRegistry = new ConnectableNodeRegistry();

    public void layout(final ProcessGroup groupToLayout) {
        group = groupToLayout;

        final Set<Connectable> sourceConnectables = locateSourceConnectables();

        final PositionTable positionTable = new PositionTable();
        for (final Connectable sourceConnectable : sourceConnectables) {
            final ConnectableNode node = nodeRegistry.getOrCreate(sourceConnectable, () -> new ConnectableNode(sourceConnectable, group, 0, nodeRegistry));

            positionTable.add(node, 0);
            addDownstreamComponents(sourceConnectable, 0, positionTable);
        }

        positionTable.layout();
    }

    private void addDownstreamComponents(final Connectable component, final int componentGraphLineIndex, final PositionTable positionTable) {
        // Determine how many destinations are expected to be on the same graph line. We use this information
        // to know if we need to push some destinations to the next graph line or not.
        int destinationsOnSameLine = 0;
        for (final Connection connection : getComponentConnections(component)) {
            final int destinationLineIndex = determineDestinationGraphLineIndex(connection, componentGraphLineIndex, 0);
            if (destinationLineIndex == componentGraphLineIndex) {
                destinationsOnSameLine++;
            }
        }

        for (final Connection connection : getComponentConnections(component)) {
            final Connectable destination = connection.getDestination();

            // If Connection doesn't live within the Process Group being laid out, skip it.
            if (connection.getProcessGroup() != group) {
                continue;
            }

            // Determine which graph line the destination component should be on.
            // Generally, it will be on the next graph line. However, there are some circumstances where this is not done.
            // For example, if we have a 'failure' connection to a funnel or a terminal processor, we may just keep the destination
            // on the same line.
            final int destinationLineIndex = determineDestinationGraphLineIndex(connection, componentGraphLineIndex, destinationsOnSameLine);
            final ConnectableNode destinationNode = nodeRegistry.getOrCreate(destination, () -> new ConnectableNode(destination, group, destinationLineIndex, nodeRegistry));

            final boolean added = positionTable.add(destinationNode, destinationLineIndex);

            if (added) {
                addDownstreamComponents(destination, destinationLineIndex, positionTable);
            }
        }
    }

    private int determineDestinationGraphLineIndex(final Connection connection, final int sourceLineIndex, final int destinationsOnSameLine) {
        // If there's more than 1 destination on the same line, don't consider allowing a simple terminal connection to be on the same line.
        if (destinationsOnSameLine > 1) {
            return sourceLineIndex + 1;
        }

        final Collection<Relationship> relationships = connection.getRelationships();

        boolean commonTerminalName = true;
        for (final Relationship relationship : relationships) {
            if (!isCommonTerminalRelationshipName(relationship.getName())) {
                commonTerminalName = false;
                break;
            }
        }

        if (!commonTerminalName) {
            return sourceLineIndex + 1;
        }

        final boolean shallow = isShallowGraphDepth(connection.getDestination());
        if (shallow) {
            return sourceLineIndex;
        }

        return sourceLineIndex + 1;
    }

    private boolean isShallowGraphDepth(final Connectable connectable) {
        final Set<Connectable> visited = new HashSet<>();
        return isShallowGraphDepth(connectable, visited, 1);
    }

    private boolean isShallowGraphDepth(final Connectable connectable, final Set<Connectable> visited, final int startDepth) {
        final boolean added = visited.add(connectable);
        if (!added || connectable.getConnectableType() == ConnectableType.OUTPUT_PORT) {
            return startDepth <= SHALLOW_GRAPH_DEPTH;
        }

        final Set<Connection> connections = connectable.getConnections();
        if (connections.size() > 1) {
            return false;
        }
        if (connectable.getIncomingConnections().size() > 1) {
            return false;
        }
        if (connections.isEmpty()) {
            return startDepth <= SHALLOW_GRAPH_DEPTH;
        }

        final Connection connection = connections.iterator().next();
        return isShallowGraphDepth(connection.getDestination(), visited, startDepth + 1);
    }

    private boolean isCommonTerminalRelationshipName(final String relationshipName) {
        final String lower = relationshipName.toLowerCase();
        return lower.contains("failure") || lower.contains("unmatched") || lower.contains("matched")
            || lower.contains("not found") || lower.contains("timeout") || lower.contains("retry")
            || lower.contains("invalid") || lower.contains("duplicate");
    }

    private Set<Connection> getComponentConnections(final Connectable component) {
        final ConnectableType connectableType = component.getConnectableType();
        final boolean sameGroup = component.getProcessGroup().equals(group);

        switch (connectableType) {
            case INPUT_PORT:
                if (sameGroup) {
                    return component.getConnections();
                } else {
                    return ConnectableNode.getOutgoingConnections(component.getProcessGroup());
                }
            case OUTPUT_PORT:
                if (sameGroup) {
                    return Collections.emptySet();
                } else {
                    return ConnectableNode.getOutgoingConnections(component.getProcessGroup());
                }
            case REMOTE_INPUT_PORT:
            case REMOTE_OUTPUT_PORT:
                final RemoteProcessGroup rpg = ((RemoteGroupPort) component).getRemoteProcessGroup();
                return ConnectableNode.getOutgoingConnections(rpg);
            default:
                return component.getConnections();
        }
    }

    private Set<Connectable> locateSourceConnectables() {
        final Set<Connectable> sourceConnectables = new HashSet<>();
        sourceConnectables.addAll(group.getInputPorts());

        addIfSource(group.getProcessors(), sourceConnectables);
        addIfSource(group.getFunnels(), sourceConnectables);

        // any child group can be a source if it has data coming from its output port
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            boolean groupHasIncomingConnections = false;
            for (final Port inPort : childGroup.getInputPorts()) {
                for (final Connection connection : inPort.getIncomingConnections()) {
                    if (connection.getProcessGroup() == group) {
                        groupHasIncomingConnections = true;
                        break;
                    }
                }
            }

            if (groupHasIncomingConnections) {
                continue;
            }

            for (final Port outPort : childGroup.getOutputPorts()) {
                if (!outPort.getConnections().isEmpty()) {
                    sourceConnectables.add(outPort);
                }
            }
        }

        // any rpg can be a source if it has data coming from its output port
        for (final RemoteProcessGroup remoteGroup : group.getRemoteProcessGroups()) {
            boolean rpgHasIncomingConnections = false;
            for (final Port inPort : remoteGroup.getInputPorts()) {
                for (final Connection connection : inPort.getIncomingConnections()) {
                    if (connection.getProcessGroup() == group) {
                        rpgHasIncomingConnections = true;
                        break;
                    }
                }
            }

            if (rpgHasIncomingConnections) {
                continue;
            }

            for (final Port outPort : remoteGroup.getOutputPorts()) {
                if (!outPort.getConnections().isEmpty()) {
                    sourceConnectables.add(outPort);
                }
            }
        }

        return sourceConnectables;
    }

    private void addIfSource(final Collection<? extends Connectable> connectables, final Set<Connectable> sources) {
        for (final Connectable connectable : connectables) {
            if (isSource(connectable)) {
                sources.add(connectable);
            }
        }
    }

    private boolean isSource(final Connectable connectable) {
        for (final Connection connection : connectable.getIncomingConnections()) {
            if (connection.getSource() == connection.getDestination()) {
                continue;
            }

            return false;
        }

        return true;
    }

}
