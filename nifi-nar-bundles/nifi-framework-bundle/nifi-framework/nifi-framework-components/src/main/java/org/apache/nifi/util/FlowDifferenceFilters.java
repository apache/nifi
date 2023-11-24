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
import org.apache.nifi.controller.label.StandardLabel;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.Relationship;
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
     * Determines whether or not the Flow Difference depicts an environmental change. I.e., a change that is expected to happen from environment to environment,
     * and which should be considered a "local modification" to a dataflow after a flow has been imported from a flow registry
     * @param difference the Flow Difference to consider
     * @param localGroup a mapping of the local Process Group
     * @param flowManager the Flow Manager
     * @return <code>true</code> if the change is an environment-specific change, <code>false</code> otherwise
     */
    public static boolean isEnvironmentalChange(final FlowDifference difference, final VersionedProcessGroup localGroup, final FlowManager flowManager) {
        return difference.getDifferenceType() == DifferenceType.BUNDLE_CHANGED
            || isSensitivePropertyDueToGhosting(difference, flowManager)
            || isRpgUrlChange(difference)
            || isAddedOrRemovedRemotePort(difference)
            || isPublicPortNameChange(difference)
            || isNewPropertyWithDefaultValue(difference, flowManager)
            || isNewRelationshipAutoTerminatedAndDefaulted(difference, localGroup, flowManager)
            || isScheduledStateNew(difference)
            || isLocalScheduleStateChange(difference)
            || isPropertyMissingFromGhostComponent(difference, flowManager)
            || isNewRetryConfigWithDefaultValue(difference, flowManager)
            || isNewZIndexLabelConfigWithDefaultValue(difference, flowManager)
            || isNewZIndexConnectionConfigWithDefaultValue(difference, flowManager)
            || isRegistryUrlChange(difference)
            || isParameterContextChange(difference)
            || isLogFileSuffixChange(difference);
    }

    private static boolean isSensitivePropertyDueToGhosting(final FlowDifference difference, final FlowManager flowManager) {
        final DifferenceType differenceType = difference.getDifferenceType();
        if (differenceType != DifferenceType.PROPERTY_SENSITIVITY_CHANGED && differenceType != DifferenceType.PROPERTY_ADDED) {
            return false;
        }

        final String componentAId = difference.getComponentA().getInstanceIdentifier();
        if (componentAId != null) {
            final ComponentNode componentNode = getComponent(flowManager, difference.getComponentA().getComponentType(), componentAId);
            if (componentNode != null && componentNode.isExtensionMissing()) {
                return true;
            }
        }

        final String componentBId = difference.getComponentB().getInstanceIdentifier();
        if (componentBId != null) {
            final ComponentNode componentNode = getComponent(flowManager, difference.getComponentA().getComponentType(), componentBId);
            if (componentNode != null && componentNode.isExtensionMissing()) {
                return true;
            }
        }

        return false;
    }

    private static ComponentNode getComponent(final FlowManager flowManager, final ComponentType componentType, final String componentId) {
        switch (componentType) {
            case CONTROLLER_SERVICE:
                return flowManager.getControllerServiceNode(componentId);
            case PROCESSOR:
                return flowManager.getProcessorNode(componentId);
            case REPORTING_TASK:
                return flowManager.getReportingTaskNode(componentId);
        }

        return null;
    }

    // The Registry URL may change if, for instance, a registry is moved to a new host, or is made secure, the port changes, etc.
    // Since this can be handled by the client anyway, there's no need to flag this as a 'local modification'
    private static boolean isRegistryUrlChange(final FlowDifference difference) {
        if (difference.getDifferenceType() != DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED) {
            return false;
        }
        if (!(difference.getValueA() instanceof VersionedFlowCoordinates)) {
            return false;
        }
        if (!(difference.getValueB() instanceof VersionedFlowCoordinates)) {
            return false;
        }

        final VersionedFlowCoordinates coordinatesA = (VersionedFlowCoordinates) difference.getValueA();
        final VersionedFlowCoordinates coordinatesB = (VersionedFlowCoordinates) difference.getValueB();

        if (Objects.equals(coordinatesA.getBucketId(), coordinatesB.getBucketId())
            && Objects.equals(coordinatesA.getFlowId(), coordinatesB.getFlowId())
            && Objects.equals(coordinatesA.getVersion(), coordinatesB.getVersion())) {

            return true;
        }

        return false;
    }

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


    private static boolean isNewZIndexLabelConfigWithDefaultValue(final FlowDifference fd, final FlowManager flowManager) {
        final Object valueA = fd.getValueA();
        if (valueA != null) {
            return false;
        }

        final VersionedComponent componentB = fd.getComponentB();
        if (!(componentB instanceof VersionedLabel)) {
            return false;
        }

        final VersionedLabel versionedLabel = (VersionedLabel) componentB;
        if (fd.getDifferenceType() == DifferenceType.ZINDEX_CHANGED) {
            final Long zIndex = versionedLabel.getzIndex();

            // should not be possible as the default value will serialize as non-null but protecting the comparison below
            if (zIndex == null) {
                return false;
            }

            return zIndex.longValue() == StandardLabel.DEFAULT_Z_INDEX;
        }
        return false;
    }

    private static boolean isNewZIndexConnectionConfigWithDefaultValue(final FlowDifference fd, final FlowManager flowManager) {
        final Object valueA = fd.getValueA();
        if (valueA != null) {
            return false;
        }

        final VersionedComponent componentB = fd.getComponentB();
        if (!(componentB instanceof VersionedConnection)) {
            return false;
        }

        final VersionedConnection versionedConnection = (VersionedConnection) componentB;
        if (fd.getDifferenceType() == DifferenceType.ZINDEX_CHANGED) {
            final Long zIndex = versionedConnection.getzIndex();

            // should not be possible as the default value will serialize as non-null but protecting the comparison below
            if (zIndex == null) {
                return false;
            }

            return zIndex.longValue() == StandardLabel.DEFAULT_Z_INDEX;
        }
        return false;
    }

    private static boolean isNewRetryConfigWithDefaultValue(final FlowDifference fd, final FlowManager flowManager) {
        final Object valueA = fd.getValueA();
        if (valueA != null) {
            return false;
        }

        final VersionedComponent componentB = fd.getComponentB();
        if (!(componentB instanceof InstantiatedVersionedProcessor)) {
            return false;
        }

        final DifferenceType type = fd.getDifferenceType();
        final InstantiatedVersionedProcessor instantiatedProcessor = (InstantiatedVersionedProcessor) componentB;
        final ProcessorNode processorNode = flowManager.getProcessorNode(instantiatedProcessor.getInstanceIdentifier());
        if (processorNode == null) {
            return false;
        }

        switch (type) {
            case RETRIED_RELATIONSHIPS_CHANGED:
                return processorNode.getRetriedRelationships().isEmpty();
            case RETRY_COUNT_CHANGED:
                return processorNode.getRetryCount() == ProcessorNode.DEFAULT_RETRY_COUNT;
            case MAX_BACKOFF_PERIOD_CHANGED:
                return ProcessorNode.DEFAULT_MAX_BACKOFF_PERIOD.equals(processorNode.getMaxBackoffPeriod());
            case BACKOFF_MECHANISM_CHANGED:
                return ProcessorNode.DEFAULT_BACKOFF_MECHANISM == processorNode.getBackoffMechanism();
            default:
                return false;
        }
    }

    public static boolean isNewPropertyWithDefaultValue(final FlowDifference fd, final FlowManager flowManager) {
        if (fd.getDifferenceType() != DifferenceType.PROPERTY_ADDED) {
            return false;
        }

        final VersionedComponent componentB = fd.getComponentB();

        if (componentB instanceof InstantiatedVersionedProcessor) {
            final InstantiatedVersionedProcessor instantiatedProcessor = (InstantiatedVersionedProcessor) componentB;
            final ProcessorNode processorNode = flowManager.getProcessorNode(instantiatedProcessor.getInstanceIdentifier());
            return isNewPropertyWithDefaultValue(fd, processorNode);
        } else if (componentB instanceof InstantiatedVersionedControllerService) {
            final InstantiatedVersionedControllerService instantiatedControllerService = (InstantiatedVersionedControllerService) componentB;
            final ControllerServiceNode controllerService = flowManager.getControllerServiceNode(instantiatedControllerService.getInstanceIdentifier());
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

        // If Scheduled State transitions from null to a real state or
        // from a real state to null, consider it a "new" scheduled state.
        if (fd.getValueA() == null && fd.getValueB() != null) {
            return true;
        }
        if (fd.getValueB() == null && fd.getValueA() != null) {
            return true;
        }

        return false;
    }

    /**
     * @return <code>true</code> if the Flow Difference shows a processor/port transitioning between stopped/running or a controller service transitioning
     * between enabled/disabled. These are a normal part of the flow lifecycle and don't represent changes to the flow itself.
     */
    public static boolean isLocalScheduleStateChange(final FlowDifference fd) {
        if (fd.getDifferenceType() != DifferenceType.SCHEDULED_STATE_CHANGED) {
            return false;
        }

        if (fd.getComponentA().getComponentType() == ComponentType.CONTROLLER_SERVICE) {
            return true;
        }

        final String scheduledStateB = String.valueOf(fd.getValueB());
        final String scheduledStateA = String.valueOf(fd.getValueA());

        // If transitioned from 'STOPPED' or 'ENABLED' to 'RUNNING', this is a 'local' schedule State Change.
        // Because of this, it won't be a considered a difference between the local, running flow, and a versioned flow
        if ("RUNNING".equals(scheduledStateB) && ("STOPPED".equals(scheduledStateA) || "ENABLED".equals(scheduledStateA))) {
            return true;
        }
        if ("RUNNING".equals(scheduledStateA) && ("STOPPED".equals(scheduledStateB) || "ENABLED".equals(scheduledStateB))) {
            return true;
        }

        return false;
    }

    public static boolean isRpgUrlChange(final FlowDifference flowDifference) {
        return flowDifference.getDifferenceType() == DifferenceType.RPG_URL_CHANGED;
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
        final ProcessorNode processorNode = flowManager.getProcessorNode(instantiatedVersionedProcessor.getInstanceIdentifier());
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
     * If a property is removed from a ghosted component, we may want to ignore it. This is because all properties will be considered sensitive for
     * a ghosted component and as a result, the property map may not be populated with its property value, resulting in an indication that the property
     * is missing when it is not.
     */
    public static boolean isPropertyMissingFromGhostComponent(final FlowDifference difference, final FlowManager flowManager) {
        if (difference.getDifferenceType() != DifferenceType.PROPERTY_REMOVED) {
            return false;
        }

        final Optional<String> fieldName = difference.getFieldName();
        if (!fieldName.isPresent()) {
            return false;
        }

        final VersionedComponent componentB = difference.getComponentB();
        if (componentB instanceof InstantiatedVersionedProcessor) {
            final ProcessorNode procNode = flowManager.getProcessorNode(componentB.getInstanceIdentifier());
            return procNode.isExtensionMissing() && isPropertyPresent(procNode, difference);
        }

        if (componentB instanceof InstantiatedVersionedControllerService) {
            final ControllerServiceNode serviceNode = flowManager.getControllerServiceNode(componentB.getInstanceIdentifier());
            return serviceNode.isExtensionMissing() && isPropertyPresent(serviceNode, difference);
        }

        return false;
    }

    private static boolean isPropertyPresent(final ComponentNode componentNode, final FlowDifference difference) {
        if (componentNode == null) {
            return false;
        }

        final Optional<String> fieldNameOptional = difference.getFieldName();
        if (!fieldNameOptional.isPresent()) {
            return false;
        }

        // Check if a value is configured. If any value is configured, then the property is not actually missing.
        final PropertyDescriptor descriptor = componentNode.getPropertyDescriptor(fieldNameOptional.get());
        final String rawPropertyValue = componentNode.getRawPropertyValue(descriptor);
        return rawPropertyValue != null;
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

    private static boolean isParameterContextChange(final FlowDifference flowDifference) {
        return flowDifference.getDifferenceType() == DifferenceType.PARAMETER_CONTEXT_CHANGED;
    }

    private static boolean isLogFileSuffixChange(final FlowDifference flowDifference) {
        return flowDifference.getDifferenceType() == DifferenceType.LOG_FILE_SUFFIX_CHANGED;
    }
}
