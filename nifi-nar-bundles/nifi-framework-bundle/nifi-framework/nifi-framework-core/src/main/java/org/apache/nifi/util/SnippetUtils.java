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
package org.apache.nifi.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.PositionDTO;

/**
 * Utility class for moving Snippets.
 */
public final class SnippetUtils {

    /**
     * Moves the content of the specified template around the specified location.
     *
     * @param snippet snippet
     * @param x x location
     * @param y y location
     */
    public static void moveSnippet(FlowSnippetDTO snippet, Double x, Double y) {
        // ensure the point is specified
        if (x != null && y != null) {
            final PositionDTO origin = new PositionDTO(x, y);

            // get the connections
            final Collection<ConnectionDTO> connections = getConnections(snippet);

            // get the components and their positions from the template contents
            final Collection<ComponentDTO> components = getComponents(snippet);

            // only perform the operation if there are components in this snippet
            if (connections.isEmpty() && components.isEmpty()) {
                return;
            }

            // get the component positions from the snippet contents
            final Map<ComponentDTO, PositionDTO> componentPositionLookup = getPositionLookup(components);
            final Map<ConnectionDTO, List<PositionDTO>> connectionPositionLookup = getConnectionPositionLookup(connections);
            final PositionDTO currentOrigin = getOrigin(componentPositionLookup.values(), connectionPositionLookup.values());

            // adjust all component positions
            for (final PositionDTO position : componentPositionLookup.values()) {
                position.setX(origin.getX() + (position.getX() - currentOrigin.getX()));
                position.setY(origin.getY() + (position.getY() - currentOrigin.getY()));
            }

            // adjust all connection positions
            for (final List<PositionDTO> bends : connectionPositionLookup.values()) {
                for (final PositionDTO bend : bends) {
                    bend.setX(origin.getX() + (bend.getX() - currentOrigin.getX()));
                    bend.setY(origin.getY() + (bend.getY() - currentOrigin.getY()));
                }
            }

            // apply the updated positions
            applyUpdatedPositions(componentPositionLookup, connectionPositionLookup);
        }
    }

    /**
     * Gets all connections that are part of the specified template.
     *
     * @param contents snippet content
     * @return connection dtos
     */
    private static Collection<ConnectionDTO> getConnections(FlowSnippetDTO contents) {
        final Collection<ConnectionDTO> connections = new HashSet<>();
        if (contents.getConnections() != null) {
            connections.addAll(contents.getConnections());
        }
        return connections;
    }

    /**
     * Gets all components, but not connections, that are part of the specified template.
     *
     * @param contents snippet
     * @return component dtos
     */
    private static Collection<ComponentDTO> getComponents(FlowSnippetDTO contents) {
        final Collection<ComponentDTO> components = new HashSet<>();

        // add all components
        if (contents.getInputPorts() != null) {
            components.addAll(contents.getInputPorts());
        }
        if (contents.getLabels() != null) {
            components.addAll(contents.getLabels());
        }
        if (contents.getOutputPorts() != null) {
            components.addAll(contents.getOutputPorts());
        }
        if (contents.getProcessGroups() != null) {
            components.addAll(contents.getProcessGroups());
        }
        if (contents.getProcessors() != null) {
            components.addAll(contents.getProcessors());
        }
        if (contents.getFunnels() != null) {
            components.addAll(contents.getFunnels());
        }
        if (contents.getRemoteProcessGroups() != null) {
            components.addAll(contents.getRemoteProcessGroups());
        }

        return components;
    }

    /**
     * Builds a mapping of components to PositionDTO's.
     *
     * @param components components
     * @return component and position map
     */
    private static Map<ComponentDTO, PositionDTO> getPositionLookup(Collection<ComponentDTO> components) {
        final Map<ComponentDTO, PositionDTO> positionLookup = new HashMap<>();

        // determine the position for each component
        for (final ComponentDTO component : components) {
            positionLookup.put(component, new PositionDTO(component.getPosition().getX(), component.getPosition().getY()));
        }

        return positionLookup;
    }

    /**
     * Builds a mapping of components to PositionDTO's.
     *
     * @param connections connections
     * @return position of connections map
     */
    private static Map<ConnectionDTO, List<PositionDTO>> getConnectionPositionLookup(final Collection<ConnectionDTO> connections) {
        final Map<ConnectionDTO, List<PositionDTO>> positionLookup = new HashMap<>();

        for (final ConnectionDTO connection : connections) {
            final List<PositionDTO> bendsCopy;
            if (connection.getBends() == null) {
                bendsCopy = Collections.emptyList();
            } else {
                bendsCopy = new ArrayList<>(connection.getBends().size());
                for (final PositionDTO bend : connection.getBends()) {
                    bendsCopy.add(new PositionDTO(bend.getX(), bend.getY()));
                }
            }

            positionLookup.put(connection, bendsCopy);
        }

        return positionLookup;
    }

    /**
     * Gets the origin of the bounding box of all specified component positions
     *
     * @param componentPositions position list for components
     * @param connectionPositions position list for connections
     * @return position
     */
    private static PositionDTO getOrigin(Collection<PositionDTO> componentPositions, Collection<List<PositionDTO>> connectionPositions) {
        Double x = null;
        Double y = null;

        // ensure valid input
        if (componentPositions.isEmpty() && connectionPositions.isEmpty()) {
            throw new IllegalArgumentException("Unable to compute the origin for an empty snippet.");
        }

        // go through each component position to find the upper left most point
        for (PositionDTO position : componentPositions) {
            if (position != null) {
                if (x == null || position.getX() < x) {
                    x = position.getX();
                }
                if (y == null || position.getY() < y) {
                    y = position.getY();
                }
            }
        }

        // go through each connection position to find the upper left most point
        for (final List<PositionDTO> bendPoints : connectionPositions) {
            for (PositionDTO point : bendPoints) {
                if (x == null || point.getX() < x) {
                    x = point.getX();
                }
                if (y == null || point.getY() < y) {
                    y = point.getY();
                }
            }
        }

        // not null because we don't allow empty snippets...
        return new PositionDTO(x, y);
    }

    /**
     * Applies the updated positions to the corresponding components.
     *
     * @param componentPositionLookup lookup
     * @param connectionPositionLookup lookup
     */
    private static void applyUpdatedPositions(final Map<ComponentDTO, PositionDTO> componentPositionLookup, final Map<ConnectionDTO, List<PositionDTO>> connectionPositionLookup) {
        for (final Map.Entry<ComponentDTO, PositionDTO> entry : componentPositionLookup.entrySet()) {
            final ComponentDTO component = entry.getKey();
            final PositionDTO position = entry.getValue();
            component.setPosition(position);
        }

        for (final Map.Entry<ConnectionDTO, List<PositionDTO>> entry : connectionPositionLookup.entrySet()) {
            final ConnectionDTO connection = entry.getKey();
            final List<PositionDTO> bends = entry.getValue();
            connection.setBends(bends);
        }
    }
}
