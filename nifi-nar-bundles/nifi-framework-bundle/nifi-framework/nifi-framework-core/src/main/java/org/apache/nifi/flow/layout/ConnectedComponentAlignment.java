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
import org.apache.nifi.connectable.Position;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Aligns the X coordinates of components that are connected to one another and where the X
 * coordinates are close to one another but the Y coordinates are not, and also aligns the Y coordinates
 * of components that are connected to one another and where the Y coordinates are close to one another but
 * the X coordinates are not.
 */
public class ConnectedComponentAlignment implements FlowAlignment {
    private static final double ALIGN_X_MAX_X_DIFFERENCE = 50D;
    private static final double ALIGN_X_MIN_Y_DIFFERENCE = 120D;

    private static final double ALIGN_Y_MAX_Y_DIFFERENCE = 50D;
    private static final double ALIGN_Y_MIN_X_DIFFERENCE = 350D;

    private ConnectableNodeRegistry nodeRegistry;

    @Override
    public void alignComponents(final ProcessGroup processGroup) {
        nodeRegistry = new ConnectableNodeRegistry();

        final Set<ConnectableNode> connectableNodes = mapToConnectableNodes(processGroup);
        final Set<ConnectableNode> sourceNodes = findSourceNodes(connectableNodes);

        for (final ConnectableNode sourceNode : sourceNodes) {
            alignDescendents(sourceNode);
        }

        applyChanges(connectableNodes);
    }

    private void applyChanges(final Set<ConnectableNode> connectableNodes) {
        for (final ConnectableNode node : connectableNodes) {
            node.applyPosition();
            node.getOutgoingEdges().forEach(ConnectableEdge::applyUpdates);
        }
    }

    private void alignDescendents(final ConnectableNode node) {
        final Set<ConnectableNode> visited = new HashSet<>();
        alignDescendents(node, visited);
    }

    private void alignDescendents(final ConnectableNode node, final Set<ConnectableNode> visited) {
        final boolean nodeAdded = visited.add(node);
        if (!nodeAdded) {
            return;
        }

        final Position nodeCenter = node.getCenter();
        final double nodeCenterX = nodeCenter.getX();
        final double nodeCenterY = nodeCenter.getY();

        for (final ConnectableEdge edge : node.getOutgoingEdges()) {
            final ConnectableNode destination = edge.getDestination();

            if (isAlignXCoordinate(node, destination)) {
                destination.setCenterXCoordinate(nodeCenterX);
            } else if (isAlignYCoordinate(node, destination)) {
                destination.setCenterYCoordinate(nodeCenterY);
            }

            alignDescendents(destination, visited);
        }
    }

    private boolean isAlignXCoordinate(final ConnectableNode parent, final ConnectableNode child) {
        final Position parentCenter = parent.getCenter();
        final Position childCenter = child.getCenter();

        return Math.abs(parentCenter.getX() - childCenter.getX()) < ALIGN_X_MAX_X_DIFFERENCE
            && Math.abs(parentCenter.getY() - childCenter.getY()) > ALIGN_X_MIN_Y_DIFFERENCE;
    }

    private boolean isAlignYCoordinate(final ConnectableNode parent, final ConnectableNode child) {
        final Position parentCenter = parent.getCenter();
        final Position childCenter = child.getCenter();

        return Math.abs(parentCenter.getY() - childCenter.getY()) < ALIGN_Y_MAX_Y_DIFFERENCE
            && Math.abs(parentCenter.getX() - childCenter.getX()) > ALIGN_Y_MIN_X_DIFFERENCE;
    }

    private Set<ConnectableNode> findSourceNodes(final Set<ConnectableNode> connectableNodes) {
        return connectableNodes.stream()
            .filter(ConnectableNode::isSourceNode)
            .collect(Collectors.toSet());
    }

    private Set<ConnectableNode> mapToConnectableNodes(final ProcessGroup group) {
        final Set<ConnectableNode> connectableNodes = new HashSet<>();

        mapConnectableNodes(group.getProcessors(), connectableNodes);
        mapConnectableNodes(group.getInputPorts(), connectableNodes);
        mapConnectableNodes(group.getOutputPorts(), connectableNodes);
        mapConnectableNodes(group.getFunnels(), connectableNodes);

        for (final RemoteProcessGroup rpg : group.getRemoteProcessGroups()) {
            mapConnectableNodes(rpg.getInputPorts(), connectableNodes);
            mapConnectableNodes(rpg.getOutputPorts(), connectableNodes);
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            mapConnectableNodes(childGroup.getInputPorts(), connectableNodes);
            mapConnectableNodes(childGroup.getOutputPorts(), connectableNodes);
        }

        return connectableNodes;
    }

    private void mapConnectableNodes(final Collection<? extends Connectable> connectables, final Set<ConnectableNode> connectableNodes) {
        connectables.stream()
            .map(connectable -> nodeRegistry.getOrCreate(connectable, () -> createConnectableNode(connectable)))
            .forEach(connectableNodes::add);
    }

    private ConnectableNode createConnectableNode(final Connectable connectable) {
        return new ConnectableNode(connectable, connectable.getProcessGroup(), 0, nodeRegistry);
    }
}
