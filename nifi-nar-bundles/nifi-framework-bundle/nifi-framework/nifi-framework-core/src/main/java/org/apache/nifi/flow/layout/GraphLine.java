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

import org.apache.nifi.connectable.Position;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A GraphLine represents a line of components in a flow. Or, essentially, a row of components, if you were to think of the flow in
 * a more table-like structure.
 */
public class GraphLine {
    private final Set<ConnectableNode> nodes = new HashSet<>();
    private double maxComponentHeight = 0D;
    private int index;

    public GraphLine(final int index) {
        this.index = index;
    }

    public void addNode(final ConnectableNode node) {
        nodes.add(node);
        maxComponentHeight = Math.max(maxComponentHeight, node.getHeight());
        node.setGraphLineIndex(index);
    }

    public void setIndex(final int index) {
        this.index = index;
        nodes.forEach(node -> node.setGraphLineIndex(index));
    }

    public boolean relocateNode(final ConnectableNode node, final GraphLine destination) {
        final boolean removed = nodes.remove(node);
        if (removed) {
            destination.addNode(node);
        }

        return removed;
    }

    public Set<ConnectableNode> getNodes() {
        return Collections.unmodifiableSet(nodes);
    }

    /**
     * @return the nodes in this GraphLine in the order that they are laid out from left to right
     */
    public List<ConnectableNode> orderedNodes() {
        return orderByXCoordinate(nodes);
    }

    public void assignYCoordinates(final double baseline) {
        for (final ConnectableNode component : nodes) {
            final double componentY = baseline + ((maxComponentHeight - component.getHeight()) / 2);
            component.setYCoordinate(componentY);
        }
    }

    public double getMaxComponentHeight() {
        return maxComponentHeight;
    }

    public void assignXCoordinates(final GraphTranslation graphTranslation) {
        final List<ConnectableNode> orderedComponents = initializeXCoordinates();
        expandOverlaps(orderedComponents, graphTranslation);
    }

    private List<ConnectableNode> initializeXCoordinates() {
        // For first line index, spread evenly over the X axis
        if (index == 0) {
            return distributeXCoordinates();
        }

        // For other lines, inherit X coordinates based on parents' positions
        final List<ConnectableNode> orderedComponents = orderByXCoordinate(nodes);

        for (final ConnectableNode component : orderedComponents) {

            // If there's only a single incoming edge (ignoring loops) and that edge is on the same graph line, we set X based on the source's X and width of label
            final List<ConnectableEdge> nonLoopIncomingConnections = component.getIncomingEdges().stream()
                .filter(edge -> !edge.getSource().equals(edge.getDestination()))
                .collect(Collectors.toList());

            if (nonLoopIncomingConnections.size() == 1) {
                final ConnectableEdge incomingEdge = nonLoopIncomingConnections.get(0);
                if (incomingEdge.getSource().getGraphLineIndex() == component.getGraphLineIndex()) {
                    final double componentX = incomingEdge.getSource().getRightCenter().getX() + PositionTable.CONNECTION_LABEL_WIDTH;
                    component.setXCoordinate(componentX);
                    continue;
                }
            }

            // Otherwise, determine X coordinate based on the siblings in the same graph line
            final Set<ConnectableEdge> incomingEdges = new HashSet<>(component.getIncomingEdges());
            incomingEdges.removeIf(edge -> edge.getSource().equals(edge.getDestination()));     // Ignore self-loops.
            incomingEdges.removeIf(edge -> edge.getSource().getGraphLineIndex() >= component.getGraphLineIndex());  // Ignore any incoming edges that are in the same Graph Line or lower.

            double xSum = 0D;
            for (final ConnectableEdge incoming : incomingEdges) {
                final ConnectableNode parent = incoming.getSource();

                double childrenWidth = 0D;
                final Set<ConnectableEdge> parentEdges = parent.getOutgoingEdges();
                final List<ConnectableNode> siblings = new ArrayList<>();
                int relevantParentEdgeCount = 0;
                for (final ConnectableEdge parentEdge : parentEdges) {
                    final ConnectableNode child = parentEdge.getDestination();
                    if (child.getGraphLineIndex() != component.getGraphLineIndex()) {
                        continue;
                    }

                    // If there are multiple connections from same source/destination, don't count the child twice.
                    if (!siblings.contains(child)) {
                        relevantParentEdgeCount++;
                        childrenWidth += child.getWidth();
                        siblings.add(child);
                    }
                }
                childrenWidth += (relevantParentEdgeCount - 1) * PositionTable.X_BETWEEN_COMPONENTS;

                // Determine which child in the array this component is.
                final List<ConnectableNode> orderedSiblings = orderByXCoordinate(siblings);
                int indexInChildrenArray = 0;
                for (int i=0; i < orderedSiblings.size(); i++) {
                    if (orderedSiblings.get(i) == component) {
                        indexInChildrenArray = i;
                        break;
                    }
                }

                // Calculate the x coordinate
                final double averageChildrenWidth = childrenWidth / Math.max(1, relevantParentEdgeCount);
                final double childrenCenter = parent.getCenter().getX();
                final double childrenLeft = childrenCenter - childrenWidth / 2;
                final double componentLeft = childrenLeft + (averageChildrenWidth * indexInChildrenArray);

                // Sum together all of the x Coordinates calculated based on each incoming connection of the component so that we can average them
                xSum += componentLeft;
            }

            final double averageX = xSum / Math.max(1, incomingEdges.size());
            component.setXCoordinate(averageX);
        }

        return orderedComponents;
    }

    /**
     * Initializes the X coordinate of each node in the GraphLine by spreading the X coordinates across the X axis
     * @return the components in the GraphLine in the order that they are laid out from left to right
     */
    private List<ConnectableNode> distributeXCoordinates() {
        // Order components by their current X coordinate so that we try to keep the same ordering
        final List<ConnectableNode> orderedComponents = orderByXCoordinate(nodes);
        final int componentCount = orderedComponents.size();
        final double shallowWidths = componentCount * ConnectableNode.PROCESS_GROUP_WIDTH + ((componentCount - 1) * PositionTable.X_BETWEEN_COMPONENTS);

        double x = -(shallowWidths / 2);
        for (final ConnectableNode component : orderedComponents) {
            final double componentX = x + ((ConnectableNode.PROCESS_GROUP_WIDTH - component.getWidth()) / 2);
            component.setCenterXCoordinate(componentX);

            x += ConnectableNode.PROCESS_GROUP_WIDTH + PositionTable.X_BETWEEN_COMPONENTS;
        }

        return orderedComponents;
    }


    private List<ConnectableNode> orderByXCoordinate(final Collection<ConnectableNode> components) {
        final List<ConnectableNode> orderedComponents = new ArrayList<>(components);
        orderedComponents.sort(Comparator.comparing(node -> node.getOriginalPosition().getX()));
        return orderedComponents;
    }

    /**
     * If any components in the given collection overlap one another, uses the provided GraphTranslation to move the components' X coordinates until they
     * no longer overlap
     * @param components the (potentially overlapping) components
     * @param graphTranslation the GraphTranslation to use for moving the components
     */
    public void expandOverlaps(final List<ConnectableNode> components, final GraphTranslation graphTranslation) {
        if (components.isEmpty()) {
            return;
        }

        final ConnectableNode firstComponent = components.get(0);
        double x = firstComponent.getUpdatedPosition().getX() + firstComponent.getWidth() + PositionTable.X_BETWEEN_COMPONENTS;
        if (hasOutgoingEdgeOnSameGraphLine(firstComponent)) {
            x += PositionTable.CONNECTION_LABEL_WIDTH;
        }

        for (int i=1; i < components.size(); i++) {
            final ConnectableNode component = components.get(i);
            final double componentX = component.getUpdatedPosition().getX();
            if (componentX < x) {
                final double pixelsToShift = x - componentX;

                graphTranslation.shiftHorizontally(Collections.singletonList(component), pixelsToShift);
            }

            x = component.getUpdatedPosition().getX() + component.getWidth() + PositionTable.X_BETWEEN_COMPONENTS;

            if (hasOutgoingEdgeOnSameGraphLine(component)) {
                x += PositionTable.CONNECTION_LABEL_WIDTH;
            }
        }
    }

    private boolean hasOutgoingEdgeOnSameGraphLine(final ConnectableNode component) {
        final Set<ConnectableEdge> outgoingEdges = component.getOutgoingEdges();
        for (final ConnectableEdge outgoingEdge : outgoingEdges) {
            if (outgoingEdge.getDestination().getGraphLineIndex() == component.getGraphLineIndex()) {
                return true;
            }
        }

        return false;
    }

    public ConnectableNode getComponentLeftOf(final ConnectableNode node) {
        final List<ConnectableNode> ordered = orderByXCoordinate(nodes);
        ConnectableNode previous = null;
        for (final ConnectableNode current : ordered) {
            if (current == node) {
                return previous;
            }

            previous = current;
        }

        return null;
    }

    public int getIndex() {
        return index;
    }

    public Set<ConnectableNode> getTerminalNodes() {
        final Set<ConnectableNode> terminalNodes = new HashSet<>();

        for (final ConnectableNode node : nodes) {
            if (node.isTerminalNode()) {
                terminalNodes.add(node);
            }
        }

        return terminalNodes;
    }

    public boolean isComponentOverlap(final double x) {
        for (final ConnectableNode node : nodes) {
            final Position position = node.getUpdatedPosition();
            if (position.getX() < x && (position.getX() + node.getWidth()) > x) {
                return true;
            }
        }

        return false;
    }


    @Override
    public String toString() {
        return "GraphLine[index=" + index + ", nodes=" + nodes.size() + "]";
    }
}
