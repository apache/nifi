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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PositionTable {
    public static double X_BETWEEN_COMPONENTS = 110D;
    public static double Y_BETWEEN_COMPONENTS = 80D;
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
    public static double CONNECTION_LABEL_WIDTH = 224D;
    public static double X_BETWEEN_COMPONENT_AND_LABEL = 10D;

    private final Map<Integer, GraphLine> graphLines = new TreeMap<>();
    private final Set<ConnectableNode> componentsAdded = new HashSet<>();


    public boolean add(final ConnectableNode node, final int graphLineIndex) {
        final GraphLine graphLine = graphLines.computeIfAbsent(graphLineIndex, GraphLine::new);
        final boolean added = componentsAdded.add(node);
        if (!added) {
            return false;
        }

        graphLine.addNode(node);
        return true;
    }

    public void layout() {
        final List<GraphLine> graphLines = getGraphLines();

        final ComponentArrangement componentArrangement = new ComponentArrangement(graphLines);
        componentArrangement.arrangeComponents();
        componentArrangement.applyPositions();
    }

    public Set<Position> getAllComponentPositions() {
        final Set<Position> positions = new HashSet<>();

        for (final GraphLine graphLine : graphLines.values()) {
            graphLine.getNodes().stream()
                .map(ConnectableNode::getUpdatedPosition)
                .forEach(positions::add);
        }

        return positions;
    }

    public List<GraphLine> getGraphLines() {
        return new ArrayList<>(graphLines.values());
    }
}
