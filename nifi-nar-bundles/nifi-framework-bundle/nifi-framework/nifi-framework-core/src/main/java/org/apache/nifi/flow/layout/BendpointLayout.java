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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

public class BendpointLayout {
    public static double CONNECTION_OVERLAP_STEP_SIZE_X = (PositionTable.CONNECTION_LABEL_WIDTH + PositionTable.X_BETWEEN_COMPONENT_AND_LABEL) / 2;
    public static double CONNECTION_OVERLAP_STEP_SIZE_Y = 15D;

    private final List<GraphLine> graphLines;

    public BendpointLayout(final List<GraphLine> graphLines) {
        this.graphLines = graphLines;
    }

    public void layoutBendpoints() {
        for (final GraphLine graphLine : graphLines) {
            graphLine.getNodes().forEach(this::resetBendpoints);
        }

        // If we skip a level in graph lines, add bendpoints
        addBendpointsForSkippedGraphLines();

        // Add bendpoint when A -> B and B -> A (or for when we have multiple connections A -> B) so that the connections don't overlap
        addBendpointsForOverlappingConnections();
    }

    private void resetBendpoints(final ConnectableNode node) {
        for (final ConnectableEdge edge : node.getOutgoingEdges()) {
            node.resetBendpoints(edge);
        }
        for (final ConnectableEdge edge : node.getIncomingEdges()) {
            node.resetBendpoints(edge);
        }
    }

    private void addBendpointsForOverlappingConnections() {
        for (final GraphLine graphLine : graphLines) {
            final Set<ConnectableNode> nodes = graphLine.getNodes();

            for (final ConnectableNode source : nodes) {
                // Find any destination that also has the source as its destination
                addBendpointsForOverlappingConnections(source);
            }
        }
    }

    private void addBendpointsForOverlappingConnections(final ConnectableNode source) {
        // For Each <Source, Destination> pair, create a Set of all edges between them (regardless of which component is source & which is destination).
        // I.e., the Set will contain edges for A -> B as well as B -> A.
        // Then, if there's more than 1 connection between the two components, add bendpoints.
        for (final ConnectableEdge sourceToDestinationEdge : source.getOutgoingEdges()) {
            final ConnectableNode destination = sourceToDestinationEdge.getDestination();
            final Set<ConnectableEdge> edgesBetweenComponents = new HashSet<>();
            edgesBetweenComponents.addAll(source.getOutgoingEdgesTo(destination));
            edgesBetweenComponents.addAll(destination.getOutgoingEdgesTo(source));
            edgesBetweenComponents.removeIf(e -> !e.getBendPoints().isEmpty()); // Ignore connections that already have bendpoints.

            addBendpointsForOverlappingConnections(edgesBetweenComponents, source, destination);
        }
    }

    private void addBendpointsForOverlappingConnections(final Set<ConnectableEdge> edges, final ConnectableNode nodeA, final ConnectableNode nodeB) {
        if (edges == null || edges.size() < 2) {
            return;
        }

        final boolean sameGraphLine = nodeA.getGraphLineIndex() == nodeB.getGraphLineIndex();
        final double stepSize = sameGraphLine ? CONNECTION_OVERLAP_STEP_SIZE_Y : CONNECTION_OVERLAP_STEP_SIZE_X;

        final List<ConnectableEdge> edgeList = new ArrayList<>(edges);
        double direction = 1;
        for (int i=0; i < edgeList.size(); i++) {
            final ConnectableEdge edge = edgeList.get(i);
            final double step = ((double) (i / 2) + 1) * direction * stepSize;

            final Position edgeCenter = getEdgeCenter(edge);
            final Position bendPoint;
            if (sameGraphLine) {
                bendPoint = new Position(edgeCenter.getX(), edgeCenter.getY() + step);
            } else {
                bendPoint = new Position(edgeCenter.getX() + step, edgeCenter.getY());
            }

            edge.addBendPoint(bendPoint);
            direction *= -1; // switch direction
        }
    }

    private Position getEdgeCenter(final ConnectableEdge edge) {
        final Position sourceCenter = edge.getSource().getCenter();
        final Position destinationCenter = edge.getDestination().getCenter();
        final double edgeCenterX = (sourceCenter.getX() + destinationCenter.getX()) / 2;
        final double edgeCenterY = (sourceCenter.getY() + destinationCenter.getY()) / 2;
        return new Position(edgeCenterX, edgeCenterY);
    }

    /**
     * If we have A connected to B, A -> B, and B is more than 1 level below A on the graph, add a bendpoint
     */
    private void addBendpointsForSkippedGraphLines() {
        final Map<ConnectableNode, GraphLine> nodeToGraphLine = mapComponentsToGraphLines();

        final Map<Double, Set<ConnectableEdge>> edgesBySourceDestDistance = new TreeMap<>(Comparator.comparing(Double::doubleValue).reversed());
        for (final Map.Entry<ConnectableNode, GraphLine> entry : nodeToGraphLine.entrySet()) {
            final ConnectableNode sourceNode = entry.getKey();
            final GraphLine sourceGraphLine = entry.getValue();
            final int sourceLineIndex = sourceGraphLine.getIndex();

            final Set<ConnectableEdge> edges = sourceNode.getOutgoingEdges();
            for (final ConnectableEdge edge : edges) {
                final ConnectableNode destinationNode = edge.getDestination();
                final int destinationLineIndex = destinationNode.getGraphLineIndex();

                // If (Destination's Graph Line Index - Source's Graph Line Index) <= 1 (i.e., the 'row' in the graph differs by 0 or 1), don't add any bendpoint.
                final int indexDiff = Math.abs(destinationLineIndex - sourceLineIndex);
                if (indexDiff <= 1) {
                    continue;
                }

                final double xDifference = sourceNode.getUpdatedPosition().getX() - destinationNode.getUpdatedPosition().getX();
                final double distance = Math.abs(xDifference);
                boolean bendpointAdded = false;
                if (indexDiff < 3 && distance > PositionTable.PROCESSOR_WIDTH / 2) {
                    // There's at least one Graph Line between source and destination and the X component differs by a decent bit. Try adding
                    // a single bendpoint, if it won't lead to overlapping components.
                    bendpointAdded = tryAddingSingleBendpoint(edge, sourceGraphLine);
                }

                if (!bendpointAdded) {
                    addSquareBendpoints(edge, sourceGraphLine);
                }

                // Keep track of which connections were updated and how far away the bendpoint was added. This can then be used to set zIndex so that the furthest away has the
                // lowest zIndex and the one closest has the highest zIndex. We do this so that the connection labels aren't covered by other connections.
                final Set<ConnectableEdge> edgesUpdated = edgesBySourceDestDistance.computeIfAbsent(distance, d -> new HashSet<>());
                edgesUpdated.add(edge);
            }
        }

        // Update zIndex of the edges so that if we have several edges with bendpoints, the ones further away from the source get a lower z-index.
        // This way, the ones closer have labels that are on top, so that all labels are readable.
        int zIndex = 0;
        for (final Set<ConnectableEdge> edgesUpdated : edgesBySourceDestDistance.values()) {
            final int edgeZIndex = zIndex;
            edgesUpdated.forEach(edge -> edge.setZIndex(edgeZIndex));
            zIndex++;
        }
    }

    /**
     * Adds 1 or 2 bendpoints, as necessary, in order to connect source to destination by creating bendpoints to the left or right in a squared off fashion.
     * For example, if we have a flow that's laid out such as
     *
     * <pre>
     * A
     * |
     * v
     * B
     * |
     * v
     * C
     * |
     * v
     * D
     * </pre>
     *
     * And D is connected back to A, we will create bendpoints such that there's a squared off look, such as:
     *
     * <pre>
     * A <--+
     * |    |
     * v    |
     * B    |
     * |    |
     * v    |
     * C    |
     * |    |
     * v    |
     * D ---+
     * </pre>
     *
     * Or, alternatively, if the destination is moved over significantly, we may not have a second bendpoint. For example:
     *
     * <pre>
     * X -> A
     * |    ^
     * v    |
     * B    |
     * |    |
     * v    |
     * C    |
     * |    |
     * v    |
     * D ---+
     * </pre>
     *
     * @param edge the edge to add bendpoints to
     * @param sourceGraphLine the graph line that the source lives in
     */
    private void addSquareBendpoints(final ConnectableEdge edge, final GraphLine sourceGraphLine) {
        final ConnectableNode sourceNode = edge.getSource();
        final ConnectableNode destinationNode = edge.getDestination();

        // In order to determine whether we should add a bendpoint to the left or the right of the source component, we determine
        // how many components on the same graph line are to the left and right of our source. We add a bendpoint to the left if there are
        // fewer components to the left, else we add to the right.
        final List<ConnectableNode> sourceLineNodes = new ArrayList<>(sourceGraphLine.getNodes());
        sourceLineNodes.sort(Comparator.comparing(node -> node.getUpdatedPosition().getX()));
        int index = 0;
        for (final ConnectableNode node : sourceLineNodes) {
            if (node == sourceNode) {
                break;
            }
            index++;
        }

        final int nodesToRight = sourceLineNodes.size() - index - 1;
        final int nodesToLeft = index;
        final boolean addBendpointToRight = nodesToRight <= nodesToLeft;

        // Determine x coordinate for bendpoints
        final double bendpointX;
        if (addBendpointToRight) {
            // Add bendpoint to the right of the source component
            double x = sourceNode.getRightCenter().getX() + PositionTable.X_BETWEEN_COMPONENT_AND_LABEL + PositionTable.CONNECTION_LABEL_WIDTH / 2;
            if (nodesToRight == 0) {
                x = Math.max(x, destinationNode.getCenter().getX());
            }

            final double calculatedX = x;
            final List<ConnectableNode> components = getAllComponents(sourceNode.getGraphLineIndex(), destinationNode.getGraphLineIndex(), node -> node.getRightCenter().getX() > calculatedX);

            bendpointX = findXWithoutConflict(components, x, PositionTable.CONNECTION_LABEL_WIDTH);
        } else {
            // Add bendpoints to the left of the source component
            double x = sourceNode.getUpdatedPosition().getX() - PositionTable.X_BETWEEN_COMPONENT_AND_LABEL - PositionTable.CONNECTION_LABEL_WIDTH / 2;
            if (nodesToLeft == 0) {
                x = Math.min(x, destinationNode.getCenter().getX());
            }

            final double calculatedX = x;
            final List<ConnectableNode> components = getAllComponents(sourceNode.getGraphLineIndex(), destinationNode.getGraphLineIndex(), node -> node.getUpdatedPosition().getX() < calculatedX);

            bendpointX = findXWithoutConflict(components, x, -PositionTable.CONNECTION_LABEL_WIDTH);
        }

        // Add the first bendpoint.
        final Position bendpoint1 = new Position(bendpointX, sourceNode.getCenter().getY());
        edge.addBendPoint(bendpoint1);

        // If the bendpoint overlaps with the destination (i.e., the bendpoint would be within the destination node itself) don't create the second bendpoint. Otherwise, add it.
        final Position bendpoint2 = new Position(bendpointX, destinationNode.getCenter().getY());
        final boolean bendpoint2WithinDestination = destinationNode.getUpdatedPosition().getX() < bendpointX && destinationNode.getRightCenter().getX() > bendpointX;

        if (!bendpoint2WithinDestination) {
            edge.addBendPoint(bendpoint2);
        }
    }

    /**
     * Adds a new bendpoint to the given edge, if able to do so without overlapping existing components
     * @param edge the edge to add the bendpoint to
     * @param sourceGraphLine the GraphLine that the source component lives in
     * @return <code>true</code> if a bendpoint was added, <code>false</code> otherwise
     */
    private boolean tryAddingSingleBendpoint(final ConnectableEdge edge, final GraphLine sourceGraphLine) {
        // Find position for a new bendpoint
        final ConnectableNode sourceNode = edge.getSource();
        final ConnectableNode destinationNode = edge.getDestination();

        boolean addToRight = sourceNode.getUpdatedPosition().getX() < destinationNode.getCenter().getX();
        Position bendpoint = createBendpointPosition(sourceNode, destinationNode, addToRight);

        // Check if the bendpoint would overlap other components on the same graph line
        boolean overlap = isBendpointOverlapOnSameGraphLine(sourceNode, bendpoint, sourceGraphLine);
        if (overlap) {
            // Try adding breakpoint in the other direction.
            bendpoint = createBendpointPosition(sourceNode, destinationNode, !addToRight);
            overlap = isBendpointOverlapOnSameGraphLine(sourceNode, bendpoint, sourceGraphLine);
        }

        // No overlap on same graph line. Check for overlap between the components
        if (!overlap) {
            final List<ConnectableNode> components = getAllComponents(sourceNode.getGraphLineIndex(), destinationNode.getGraphLineIndex(), node -> true);
            overlap = isComponentOverlap(components, bendpoint.getX());
        }

        // If there's no overlap, add the bendpoint. Otherwise, we'll use 2 bendpoints to go around the outside of the components
        if (!overlap) {
            edge.addBendPoint(bendpoint);
            return true;
        }

        return false;
    }

    /**
     * Check is there are any components on the given graph line that overlap the given component/source node and the given bendpoint
     * @param sourceNode the source of the Edge that has the bendpoint
     * @param bendpoint the bendpoint
     * @param sourceGraphLine the graph line that contains the source component
     * @return <code>true</code> if any components in the graphline overlap, <code>false</code> otherwise
     */
    private boolean isBendpointOverlapOnSameGraphLine(final ConnectableNode sourceNode, final Position bendpoint, final GraphLine sourceGraphLine) {
        final Set<ConnectableNode> sourceLineComponents = sourceGraphLine.getNodes();
        final double sourceAndBendpointMinX = Math.min(bendpoint.getX(), sourceNode.getUpdatedPosition().getX());
        final double sourceAndBendpointMaxX = Math.max(bendpoint.getX(), sourceNode.getUpdatedPosition().getX());
        for (final ConnectableNode sourceLineComponent : sourceLineComponents) {
            if (sourceNode.equals(sourceLineComponent)) {
                continue;
            }

            if (sourceLineComponent.getRightCenter().getX() > sourceAndBendpointMinX && sourceLineComponent.getUpdatedPosition().getX() < sourceAndBendpointMaxX) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns all components that fall on graph lines between the given indices (exclusive) that also match the given filter
     * @param firstLineIndex the starting index
     * @param lastLineIndex the ending index
     * @param filter the filter to determine whether or not a component should be included
     * @return the selected components
     */
    private List<ConnectableNode> getAllComponents(final int firstLineIndex, final int lastLineIndex, final Predicate<ConnectableNode> filter) {
        final List<ConnectableNode> selectedNodes = new ArrayList<>();

        final int minIndex = Math.min(firstLineIndex, lastLineIndex);
        final int maxIndex = Math.max(firstLineIndex, lastLineIndex);
        for (int i=minIndex + 1; i < maxIndex; i++) {
            final GraphLine graphLine = graphLines.get(i);
            for (final ConnectableNode node : graphLine.getNodes()) {
                if (filter.test(node)) {
                    selectedNodes.add(node);
                }
            }
        }

        selectedNodes.sort(Comparator.comparing(node -> node.getUpdatedPosition().getX()));
        return selectedNodes;
    }

    /**
     * Determines whether or not any of the given components overlap the given X coordinate. I.e., if a vertical line existed with the given X coordinate, would it
     * overlap any of the given components?
     *
     * @param components the components to check
     * @param x the x coordinate
     * @return <code>true</code> if any component overlaps, <code>false</code> otherwise.
     */
    private boolean isComponentOverlap(final Collection<ConnectableNode> components, final double x) {
        for (final ConnectableNode component : components) {
            final double componentX = component.getUpdatedPosition().getX();
            if (componentX < x && componentX + component.getWidth() > x) {
                return true;
            }
        }

        return false;
    }

    private double findXWithoutConflict(final Collection<ConnectableNode> components, final double x, final double pixelsToShift) {
        double bendpointX = x;
        while (isComponentOverlap(components, bendpointX)) {
            bendpointX += pixelsToShift;
        }

        return bendpointX;
    }


    private Map<ConnectableNode, GraphLine> mapComponentsToGraphLines() {
        final Map<ConnectableNode, GraphLine> nodeToGraphLine = new HashMap<>();
        for (final GraphLine graphLine : graphLines) {
            graphLine.getNodes().forEach(node -> nodeToGraphLine.put(node, graphLine));
        }
        return nodeToGraphLine;
    }

    /**
     * Determines the position that should be used for a bendpoint that connects the given source and destination nodes
     * @param sourceNode source of the connection
     * @param destinationNode destination of the connection
     * @param addToRight if <code>true</code>, creates position to the right of the source node, else creates position to the left
     */
    private Position createBendpointPosition(final ConnectableNode sourceNode, final ConnectableNode destinationNode, final boolean addToRight) {
        final Position sourcePosition = sourceNode.getUpdatedPosition();
        final Position destinationPosition = destinationNode.getUpdatedPosition();
        final double destinationCenterX = destinationPosition.getX() + destinationNode.getWidth() / 2;

        final double bendpointX;
        if (addToRight) {
            // Add bendpoint to the right.
            // We don't want to add a bendpoint that causes the label to overlap with our component. So push it far enough right for that not to happen.
            final double sourceRightX = sourcePosition.getX() + sourceNode.getWidth();
            final double minBendpointLeft = sourceRightX + PositionTable.CONNECTION_LABEL_WIDTH / 2 + PositionTable.X_BETWEEN_COMPONENT_AND_LABEL;
            bendpointX = Math.max(minBendpointLeft, destinationCenterX);
        } else {
            // Add bendpoint to the left
            final double sourceLeftX = sourcePosition.getX();
            final double maxBendpointRight = sourceLeftX - PositionTable.CONNECTION_LABEL_WIDTH / 2 - PositionTable.X_BETWEEN_COMPONENT_AND_LABEL;
            bendpointX = Math.min(maxBendpointRight, destinationCenterX);
        }

        // Create bendpoint position that is in the center of the source in terms of y-coordinate and has X coordinate that is the center of the destination.
        final Position bendpoint = new Position(bendpointX, sourcePosition.getY() + sourceNode.getHeight() / 2);
        return bendpoint;
    }

}
