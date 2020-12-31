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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.ComponentType;
import org.apache.nifi.registry.flow.ScheduledState;
import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedComponent;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedControllerService;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public class FlowDifferenceFilters {

    /**
     * Predicate that returns true if the difference is NOT a name change on a public port (i.e. VersionedPort that allows remote access).
     */
    public static Predicate<FlowDifference> FILTER_PUBLIC_PORT_NAME_CHANGES = (fd) -> !isPublicPortNameChange(fd);

    public static boolean isPublicPortNameChange(final FlowDifference fd) {
        final VersionedComponent versionedComponent = fd.getComponentA();
        if (fd.getDifferenceType() == DifferenceType.NAME_CHANGED && versionedComponent instanceof VersionedPort) {
            final VersionedPort versionedPort = (VersionedPort) versionedComponent;
            if (versionedPort.isAllowRemoteAccess()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Predicate that returns true if the difference is NOT a remote port being added, and false if it is.
     */
    public static Predicate<FlowDifference> FILTER_ADDED_REMOVED_REMOTE_PORTS =  (fd) -> !isAddedOrRemovedRemotePort(fd);

    public static boolean isAddedOrRemovedRemotePort(final FlowDifference fd) {
        if (fd.getDifferenceType() == DifferenceType.COMPONENT_ADDED || fd.getDifferenceType() == DifferenceType.COMPONENT_REMOVED) {
            VersionedComponent component = fd.getComponentA();
            if (component == null || fd.getComponentB() instanceof InstantiatedVersionedComponent) {
                component = fd.getComponentB();
            }

            if (component.getComponentType() == ComponentType.REMOTE_INPUT_PORT
                    || component.getComponentType() == ComponentType.REMOTE_OUTPUT_PORT) {
                return true;
            }
        }

        return false;
    }

    public static Predicate<FlowDifference> FILTER_IGNORABLE_VERSIONED_FLOW_COORDINATE_CHANGES = (fd) -> !isIgnorableVersionedFlowCoordinateChange(fd);

    public static boolean isIgnorableVersionedFlowCoordinateChange(final FlowDifference fd) {
        if (fd.getDifferenceType() == DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED) {
            final VersionedComponent componentA = fd.getComponentA();
            final VersionedComponent componentB = fd.getComponentB();

            if (componentA instanceof VersionedProcessGroup && componentB instanceof VersionedProcessGroup) {
                final VersionedProcessGroup versionedProcessGroupA = (VersionedProcessGroup) componentA;
                final VersionedProcessGroup versionedProcessGroupB = (VersionedProcessGroup) componentB;

                final VersionedFlowCoordinates coordinatesA = versionedProcessGroupA.getVersionedFlowCoordinates();
                final VersionedFlowCoordinates coordinatesB = versionedProcessGroupB.getVersionedFlowCoordinates();

                if (coordinatesA != null && coordinatesB != null) {
                    String registryUrlA = coordinatesA.getRegistryUrl();
                    String registryUrlB = coordinatesB.getRegistryUrl();

                    if (registryUrlA != null && registryUrlB != null && !registryUrlA.equals(registryUrlB)) {
                        if (registryUrlA.endsWith("/")) {
                            registryUrlA = registryUrlA.substring(0, registryUrlA.length() - 1);
                        }

                        if (registryUrlB.endsWith("/")) {
                            registryUrlB = registryUrlB.substring(0, registryUrlB.length() - 1);
                        }

                        if (registryUrlA.equals(registryUrlB)) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }


    public static boolean isNewPropertyWithDefaultValue(final FlowDifference fd, final FlowManager flowManager) {
        if (fd.getDifferenceType() != DifferenceType.PROPERTY_ADDED) {
            return false;
        }

        final VersionedComponent componentB = fd.getComponentB();

        if (componentB instanceof InstantiatedVersionedProcessor) {
            final InstantiatedVersionedProcessor instantiatedProcessor = (InstantiatedVersionedProcessor) componentB;
            final ProcessorNode processorNode = flowManager.getProcessorNode(instantiatedProcessor.getInstanceId());
            return isNewPropertyWithDefaultValue(fd, processorNode);
        } else if (componentB instanceof InstantiatedVersionedControllerService) {
            final InstantiatedVersionedControllerService instantiatedControllerService = (InstantiatedVersionedControllerService) componentB;
            final ControllerServiceNode controllerService = flowManager.getControllerServiceNode(instantiatedControllerService.getInstanceId());
            return isNewPropertyWithDefaultValue(fd, controllerService);
        }

        return false;
    }

    private static boolean isNewPropertyWithDefaultValue(final FlowDifference fd, final ComponentNode componentNode) {
        if (componentNode == null) {
            return false;
        }

        final Optional<String> optionalFieldName = fd.getFieldName();
        if (!optionalFieldName.isPresent()) {
            return false;
        }

        final String fieldName = optionalFieldName.get();
        final PropertyDescriptor propertyDescriptor = componentNode.getPropertyDescriptor(fieldName);
        if (propertyDescriptor == null) {
            return false;
        }

        if (Objects.equals(fd.getValueB(), propertyDescriptor.getDefaultValue())) {
            return true;
        }

        return false;
    }

    public static boolean isScheduledStateNew(final FlowDifference fd) {
        if (fd.getDifferenceType() != DifferenceType.SCHEDULED_STATE_CHANGED) {
            return false;
        }

        // If Scheduled State transitions from null to ENABLED or ENABLED to null, consider it a "new" scheduled state.
        if (fd.getValueA() == null && ScheduledState.ENABLED.equals(fd.getValueB())) {
            return true;
        }
        if (fd.getValueB() == null && "ENABLED".equals(fd.getValueA())) {
            return true;
        }

        return false;
    }

    public static boolean isNewRelationshipAutoTerminatedAndDefaulted(final FlowDifference fd, final VersionedProcessGroup processGroup, final FlowManager flowManager) {
        if (fd.getDifferenceType() != DifferenceType.AUTO_TERMINATED_RELATIONSHIPS_CHANGED) {
            return false;
        }

        if (!(fd.getComponentA() instanceof VersionedProcessor) || !(fd.getComponentB() instanceof InstantiatedVersionedProcessor)) {
            // Should not happen, since only processors have auto-terminated relationships.
            return false;
        }

        final VersionedProcessor processorA = (VersionedProcessor) fd.getComponentA();
        final VersionedProcessor processorB = (VersionedProcessor) fd.getComponentB();

        // Determine if this Flow Difference indicates that Processor B has all of the same Auto-Terminated Relationships as Processor A, plus some.
        // If that is the case, then it may be that a new Relationship was added, defaulting to 'Auto-Terminated' and that Processor B is still auto-terminated.
        // We want to be able to identify that case.
        final Set<String> autoTerminatedA = replaceNull(processorA.getAutoTerminatedRelationships(), Collections.emptySet());
        final Set<String> autoTerminatedB = replaceNull(processorB.getAutoTerminatedRelationships(), Collections.emptySet());

        // If B is smaller than A, then B cannot possibly contain all of A. So use that as a first comparison to avoid the expense of #containsAll
        if (autoTerminatedB.size() < autoTerminatedA.size() || !autoTerminatedB.containsAll(autoTerminatedA)) {
            // If B does not contain all of A, then the FlowDifference is indicative of some other change.
            return false;
        }

        final InstantiatedVersionedProcessor instantiatedVersionedProcessor = (InstantiatedVersionedProcessor) processorB;
        final ProcessorNode processorNode = flowManager.getProcessorNode(instantiatedVersionedProcessor.getInstanceId());
        if (processorNode == null) {
            return false;
        }

        final Set<String> newlyAddedAutoTerminated = new HashSet<>(autoTerminatedB);
        newlyAddedAutoTerminated.removeAll(autoTerminatedA);

        for (final String relationshipName : newlyAddedAutoTerminated) {
            final Relationship relationship = processorNode.getRelationship(relationshipName);
            if (relationship == null) {
                return false;
            }

            final boolean defaultAutoTerminated = relationship.isAutoTerminated();
            if (!defaultAutoTerminated) {
                return false;
            }

            if (hasConnection(processGroup, processorA, relationshipName)) {
                return false;
            }
        }

        return true;
    }

    private static <T> T replaceNull(final T value, final T replacement) {
        return value == null ? replacement : value;
    }

    /**
     * Determines whether or not the given Process Group has a Connection whose source is the given Processor and that contains the given relationship
     *
     * @param processGroup the process group
     * @param processor the source processor
     * @param relationship the relationship
     *
     * @return <code>true</code> if such a connection exists, <code>false</code> otherwise.
     */
    private static boolean hasConnection(final VersionedProcessGroup processGroup, final VersionedProcessor processor, final String relationship) {
        for (final VersionedConnection connection : processGroup.getConnections()) {
            if (connection.getSource().getId().equals(processor.getIdentifier()) && connection.getSelectedRelationships().contains(relationship)) {
                return true;
            }
        }

        return false;
    }
}
