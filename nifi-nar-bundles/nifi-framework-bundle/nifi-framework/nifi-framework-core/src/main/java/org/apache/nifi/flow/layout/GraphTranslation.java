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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GraphTranslation {
    private final List<GraphLine> graphLines;
    private final Set<ConnectableNode> visited = new HashSet<>();

    public GraphTranslation(final List<GraphLine> graphLines) {
        this.graphLines = graphLines;
    }

    public void shiftHorizontally(final List<ConnectableNode> components, final double pixels) {
        visited.clear();
        shiftXCoordinates(components, pixels);
    }

    private void shiftXCoordinates(final List<ConnectableNode> components, final double pixels) {
        if (components.isEmpty()) {
            return;
        }

        for (final ConnectableNode component : components) {
            final boolean added = visited.add(component);
            if (!added) {
                continue;
            }

            shiftHorizontally(component, pixels);
            shiftChildrenHorizontally(component, pixels);
        }
    }

    private void shiftChildrenHorizontally(final ConnectableNode component, final double pixels) {
        final Set<ConnectableEdge> edges = component.getOutgoingEdges();
        for (final ConnectableEdge edge : edges) {
            if (edge.getSource().equals(edge.getDestination())) {
                continue;
            }

            final ConnectableNode destination = edge.getDestination();

            final boolean added = visited.add(destination);
            if (!added) {
                continue;
            }

            // If the child is not on the same graph line or the one below it (index + 1), don't move it.
            if (destination.getGraphLineIndex() != component.getGraphLineIndex() && destination.getGraphLineIndex() != component.getGraphLineIndex() + 1) {
                continue;
            }

            shiftHorizontally(destination, pixels);
            shiftSiblingsHorizontally(destination, pixels);
        }
    }

    private void shiftHorizontally(final ConnectableNode component, final double pixels) {
        final Position currentPosition = component.getUpdatedPosition();
        component.setXCoordinate(currentPosition.getX() + pixels);

        for (final ConnectableEdge edge : component.getOutgoingEdges()) {
            final List<Position> bendPoints = edge.getBendPoints();
            if (bendPoints.isEmpty()) {
                continue;
            }

            final List<Position> updatedBendPoints = new ArrayList<>();
            for (final Position bendPoint : bendPoints) {
                final double minBendpointX = component.getUpdatedPosition().getX() + component.getWidth() + PositionTable.X_BETWEEN_COMPONENT_AND_LABEL;
                final Position updatedBendPoint = new Position(Math.max(minBendpointX, bendPoint.getX()), bendPoint.getY());
                updatedBendPoints.add(updatedBendPoint);
            }

            edge.setBendPoints(updatedBendPoints);
        }
    }

    private void shiftSiblingsHorizontally(final ConnectableNode component, final double pixels) {
        final int graphLineIndex = component.getGraphLineIndex();
        final GraphLine graphLine = graphLines.get(graphLineIndex);
        if (graphLine == null) {
            return;
        }

        final List<ConnectableNode> orderedComponents = graphLine.orderedNodes();

        for (int i=0; i < orderedComponents.size() - 1; i++) {
            final ConnectableNode node = orderedComponents.get(i);
            if (node == component) {
                // Siblings to include are those to the right, if the we're shifting right, and those to the left if we're shifting left.
                final List<ConnectableNode> siblings = (pixels > 0) ? orderedComponents.subList(i + 1, orderedComponents.size()) : orderedComponents.subList(0, i);
                shiftXCoordinates(siblings, pixels);
                return;
            }
        }
    }

}
