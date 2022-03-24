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

import org.apache.nifi.controller.Snippet;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Utility class for moving Snippets.
 */
public final class SnippetUtils {

    /**
     * Moves the content of the specified snippet around the specified location.  Does not scale components in child process groups.
     *
     * @param snippet snippet
     * @param x       x location
     * @param y       y location
     */
    public static void moveSnippet(FlowSnippetDTO snippet, Double x, Double y) {
        moveAndScaleSnippet(snippet, x, y, 1.0, 1.0);
    }

    /**
     * Moves the content of the specified snippet around the specified location
     * and scales the placement of individual components of the template by the
     * given factorX and factorY.  Does not scale components in child process groups.
     *
     * @param snippet snippet
     * @param x       x location
     * @param y       y location
     * @param factorX x location scaling factor
     * @param factorY y location scaling factor
     */
    public static void moveAndScaleSnippet(FlowSnippetDTO snippet, Double x, Double y, double factorX, double factorY) {
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
                position.setX(origin.getX() + ((position.getX() - currentOrigin.getX()) * factorX));
                position.setY(origin.getY() + ((position.getY() - currentOrigin.getY()) * factorY));
            }

            // adjust all connection positions
            for (final List<PositionDTO> bends : connectionPositionLookup.values()) {
                for (final PositionDTO bend : bends) {
                    bend.setX(origin.getX() + ((bend.getX() - currentOrigin.getX()) * factorX));
                    bend.setY(origin.getY() + ((bend.getY() - currentOrigin.getY()) * factorY));
                }
            }

            // apply the updated positions
            applyUpdatedPositions(componentPositionLookup, connectionPositionLookup);
        }
    }

    /**
     * Scales the placement of individual components of the snippet by the
     * given factorX and factorY. Does not scale components in child process groups.
     *
     * @param snippet snippet
     * @param factorX x location scaling factor
     * @param factorY y location scaling factor
     */
    public static void scaleSnippet(FlowSnippetDTO snippet, double factorX, double factorY) {
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

        // adjust all component positions
        for (final PositionDTO position : componentPositionLookup.values()) {
            position.setX(position.getX() * factorX);
            position.setY(position.getY() * factorY);
        }

        // adjust all connection positions
        for (final List<PositionDTO> bends : connectionPositionLookup.values()) {
            for (final PositionDTO bend : bends) {
                bend.setX(bend.getX() * factorX);
                bend.setY(bend.getY() * factorY);
            }
        }

        // apply the updated positions
        applyUpdatedPositions(componentPositionLookup, connectionPositionLookup);
    }

    /**
     * Finds all {@link ProcessGroupDTO}s in the given {@link FlowSnippetDTO}.
     * @param snippet containing the child {@link ProcessGroupDTO}s to be returned
     * @return List of child {@link ProcessGroupDTO}s found in the given {@link FlowSnippetDTO}.
     */
    public static List<ProcessGroupDTO> findAllProcessGroups(FlowSnippetDTO snippet) {
        final List<ProcessGroupDTO> allProcessGroups = new ArrayList<>(snippet.getProcessGroups());
        for (final ProcessGroupDTO childGroup : snippet.getProcessGroups()) {
            allProcessGroups.addAll(findAllProcessGroups(childGroup.getContents()));
        }
        return allProcessGroups;
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
     * @param componentPositions  position list for components
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
     * @param componentPositionLookup  lookup
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

    public static void verifyNoVersionControlConflicts(final Snippet snippet, final ProcessGroup parentGroup, final ProcessGroup destination) {
        if (snippet == null) {
            return;
        }
        if (snippet.getProcessGroups() == null) {
            return;
        }

        final List<VersionControlInformation> vcis = new ArrayList<>();
        for (final String groupId : snippet.getProcessGroups().keySet()) {
            final ProcessGroup group = parentGroup.getProcessGroup(groupId);
            if (group != null) {
                findAllVersionControlInfo(group, vcis);
            }
        }

        verifyNoDuplicateVersionControlInfo(destination, vcis);
    }

    public static void verifyNoVersionControlConflicts(final FlowSnippetDTO snippetContents, final ProcessGroup destination) {
        final List<VersionControlInformationDTO> vcis = new ArrayList<>();
        for (final ProcessGroupDTO childGroup : snippetContents.getProcessGroups()) {
            findAllVersionControlInfo(childGroup, vcis);
        }

        verifyNoDuplicateVersionControlInfoDtos(destination, vcis);
    }

    private static void verifyNoDuplicateVersionControlInfoDtos(final ProcessGroup group, final Collection<VersionControlInformationDTO> snippetVcis) {
        final VersionControlInformation vci = group.getVersionControlInformation();
        if (vci != null) {
            for (final VersionControlInformationDTO snippetVci : snippetVcis) {
                if (vci.getBucketIdentifier().equals(snippetVci.getBucketId()) && vci.getFlowIdentifier().equals(snippetVci.getFlowId())) {
                    throw new IllegalArgumentException("Cannot place the given Process Group into the desired destination because the destination group or one of its ancestor groups is "
                        + "under Version Control and one of the selected Process Groups is also under Version Control with the same Flow. A Process Group that is under Version Control "
                        + "cannot contain a child Process Group that points to the same Versioned Flow.");
                }
            }
        }

        final ProcessGroup parent = group.getParent();
        if (parent != null) {
            verifyNoDuplicateVersionControlInfoDtos(parent, snippetVcis);
        }
    }

    private static void verifyNoDuplicateVersionControlInfo(final ProcessGroup group, final Collection<VersionControlInformation> snippetVcis) {
        final VersionControlInformation vci = group.getVersionControlInformation();
        if (vci != null) {
            for (final VersionControlInformation snippetVci : snippetVcis) {
                if (vci.getBucketIdentifier().equals(snippetVci.getBucketIdentifier()) && vci.getFlowIdentifier().equals(snippetVci.getFlowIdentifier())) {
                    throw new IllegalArgumentException("Cannot place the given Process Group into the desired destination because the destination group or one of its ancestor groups is "
                        + "under Version Control and one of the selected Process Groups is also under Version Control with the same Flow. A Process Group that is under Version Control "
                        + "cannot contain a child Process Group that points to the same Versioned Flow.");
                }
            }
        }

        final ProcessGroup parent = group.getParent();
        if (parent != null) {
            verifyNoDuplicateVersionControlInfo(parent, snippetVcis);
        }
    }


    private static void findAllVersionControlInfo(final ProcessGroupDTO dto, final List<VersionControlInformationDTO> found) {
        final VersionControlInformationDTO vci = dto.getVersionControlInformation();
        if (vci != null) {
            found.add(vci);
        }

        final FlowSnippetDTO contents = dto.getContents();
        if (contents != null) {
            for (final ProcessGroupDTO child : contents.getProcessGroups()) {
                findAllVersionControlInfo(child, found);
            }
        }
    }

    private static void findAllVersionControlInfo(final ProcessGroup group, final List<VersionControlInformation> found) {
        if (group == null) {
            return;
        }

        final VersionControlInformation vci = group.getVersionControlInformation();
        if (vci != null) {
            found.add(vci);
        }

        for (final ProcessGroup childGroup : group.findAllProcessGroups()) {
            findAllVersionControlInfo(childGroup, found);
        }
    }

}
