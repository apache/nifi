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

import org.apache.nifi.groups.ProcessGroup;

import java.util.List;

public class ComponentArrangement {
    public static double Y_BETWEEN_COMPONENTS = 80D;

    private final List<GraphLine> graphLines;

    public ComponentArrangement(final List<GraphLine> graphLines) {
        this.graphLines = graphLines;
    }

    public void arrangeComponents() {
        computeYCoordinates();
        computeXCoordinates();

        final BendpointLayout bendpointLayout = new BendpointLayout(graphLines);
        bendpointLayout.layoutBendpoints();
    }

    private void computeYCoordinates() {
        double y = 0;

        for (final GraphLine graphLine : graphLines) {
            // If the graph line has any Process Groups and they have incoming connections, give it a little extra room because
            // connection labels will be a bit taller.
            final boolean hasProcessGroupWithIncomingConnection = graphLine.getNodes().stream()
                .filter(node -> !node.getIncomingEdges().isEmpty())
                .anyMatch(node -> node.getPositionableComponent() instanceof ProcessGroup);
            if (hasProcessGroupWithIncomingConnection) {
                y += 25D;
            }

            graphLine.assignYCoordinates(y);
            y += (graphLine.getMaxComponentHeight() + Y_BETWEEN_COMPONENTS);
        }
    }

    private void computeXCoordinates() {
        // For each y position, determine the x coordinate of each component at that y coordinate
        final GraphTranslation graphTranslation = new GraphTranslation(graphLines);

        for (final GraphLine graphLine : graphLines) {
            graphLine.assignXCoordinates(graphTranslation);
        }
    }


    public void applyPositions() {
        for (final GraphLine graphLine : graphLines) {
            graphLine.getNodes().forEach(ConnectableNode::applyPosition);
        }
    }

}
