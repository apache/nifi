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

import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.StandardLabel;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConfigurableComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedComponent;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedConnection;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedControllerService;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class FlowDifferenceFilters {

    private static final Pattern PARAMETER_REFERENCE_PATTERN = Pattern.compile("#\\{[A-Za-z0-9\\-_. ]+}");

    /**
     * Determines whether or not the Flow Difference depicts an environmental change. I.e., a change that is expected to happen from environment to environment,
     * and which should be considered a "local modification" to a dataflow after a flow has been imported from a flow registry
     * @param difference the Flow Difference to consider
     * @param localGroup a mapping of the local Process Group
     * @param flowManager the Flow Manager
     * @return <code>true</code> if the change is an environment-specific change, <code>false</code> otherwise
     */
    public static boolean isEnvironmentalChange(final FlowDifference difference, final VersionedProcessGroup localGroup, final FlowManager flowManager) {
        return isEnvironmentalChange(difference, localGroup, flowManager, EnvironmentalChangeContext.empty());
    }

    public static boolean isEnvironmentalChange(final FlowDifference difference, final VersionedProcessGroup localGroup, final FlowManager flowManager,
                                                final EnvironmentalChangeContext context) {
        final EnvironmentalChangeContext evaluatedContext = Objects.requireNonNull(context, "EnvironmentalChangeContext required");
        return isBundleChange(difference)
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
            || isLogFileSuffixChange(difference)
            || isStaticPropertyRemoved(difference, flowManager)
            || isControllerServiceCreatedForNewProperty(difference, evaluatedContext)
            || isPropertyParameterizationRename(difference, evaluatedContext)
            || isPropertyRenameWithMatchingValue(difference, evaluatedContext)
            || isSelectedRelationshipChangeForNewRelationship(difference, flowManager);
    }

    public static boolean isBundleChange(final FlowDifference difference) {
        return difference.getDifferenceType() == DifferenceType.BUNDLE_CHANGED;
    }

    private static boolean isSensitivePropertyDueToGhosting(final FlowDifference difference, final FlowManager flowManager) {
        final DifferenceType differenceType = difference.getDifferenceType();
        if (differenceType != DifferenceType.PROPERTY_SENSITIVITY_CHANGED && differenceType != DifferenceType.PROPERTY_ADDED) {
            return false;
        }

        final VersionedComponent componentA = difference.getComponentA();
        if (componentA != null) {
            final String componentAId = componentA.getInstanceIdentifier();
            if (componentAId != null) {
                final ComponentNode componentNode = getComponent(flowManager, componentA.getComponentType(), componentAId);
                if (componentNode != null && componentNode.isExtensionMissing()) {
                    return true;
                }
            }
        }

        final VersionedComponent componentB = difference.getComponentB();
        if (componentB != null) {
            final String componentBId = componentB.getInstanceIdentifier();
            if (componentBId != null) {
                final ComponentNode componentNode = getComponent(flowManager, componentB.getComponentType(), componentBId);
                if (componentNode != null && componentNode.isExtensionMissing()) {
                    return true;
                }
            }
        }

        return false;
    }

    private static ComponentNode getComponent(final FlowManager flowManager, final ComponentType componentType, final String componentId) {
        return switch (componentType) {
            case CONTROLLER_SERVICE -> flowManager.getControllerServiceNode(componentId);
            case PROCESSOR -> flowManager.getProcessorNode(componentId);
            case REPORTING_TASK -> flowManager.getReportingTaskNode(componentId);
            default -> null;
        };

    }

    private static ComponentNode getComponent(final FlowManager flowManager, final VersionedComponent component) {
        if (component instanceof InstantiatedVersionedComponent instantiatedComponent) {
            return getComponent(flowManager, component.getComponentType(), instantiatedComponent.getInstanceIdentifier());
        } else {
            return null;
        }
    }

    private static boolean supportsDynamicProperties(final ConfigurableComponent component) {
        final Class<?> componentClass = component.getClass();
        return componentClass.isAnnotationPresent(DynamicProperty.class) || componentClass.isAnnotationPresent(DynamicProperties.class);
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

        return switch (type) {
            case RETRIED_RELATIONSHIPS_CHANGED -> processorNode.getRetriedRelationships().isEmpty();
            case RETRY_COUNT_CHANGED -> processorNode.getRetryCount() == ProcessorNode.DEFAULT_RETRY_COUNT;
            case MAX_BACKOFF_PERIOD_CHANGED ->
                ProcessorNode.DEFAULT_MAX_BACKOFF_PERIOD.equals(processorNode.getMaxBackoffPeriod());
            case BACKOFF_MECHANISM_CHANGED ->
                ProcessorNode.DEFAULT_BACKOFF_MECHANISM == processorNode.getBackoffMechanism();
            default -> false;
        };
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

        // If there is no local component, this is a change affecting only the proposed flow.
        if (fd.getComponentA() == null) {
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

    private static boolean isSelectedRelationshipChangeForNewRelationship(final FlowDifference difference, final FlowManager flowManager) {
        if (difference.getDifferenceType() != DifferenceType.SELECTED_RELATIONSHIPS_CHANGED) {
            return false;
        }

        if (!(difference.getComponentA() instanceof VersionedConnection connectionA)) {
            return false;
        }

        if (!(difference.getComponentB() instanceof InstantiatedVersionedConnection connectionB)) {
            return false;
        }

        final Set<String> selectedA = new HashSet<>(replaceNull(connectionA.getSelectedRelationships(), Collections.emptySet()));
        final Set<String> selectedB = new HashSet<>(replaceNull(connectionB.getSelectedRelationships(), Collections.emptySet()));

        final Set<String> newlySelected = new HashSet<>(selectedB);
        newlySelected.removeAll(selectedA);
        if (newlySelected.isEmpty()) {
            return false;
        }

        final Set<String> removedRelationships = new HashSet<>(selectedA);
        removedRelationships.removeAll(selectedB);

        if (flowManager == null) {
            return false;
        }

        final String connectionInstanceId = connectionB.getInstanceIdentifier();
        final String connectionGroupId = connectionB.getInstanceGroupId();
        if (connectionInstanceId == null || connectionGroupId == null) {
            return false;
        }

        final ProcessGroup processGroup = flowManager.getGroup(connectionGroupId);
        if (processGroup == null) {
            return false;
        }

        final Connection connection = processGroup.getConnection(connectionInstanceId);
        if (connection == null) {
            return false;
        }

        final Connectable source = connection.getSource();
        if (source == null || source.getConnectableType() != ConnectableType.PROCESSOR) {
            return false;
        }

        final ProcessorNode processorNode = flowManager.getProcessorNode(source.getIdentifier());
        if (processorNode == null) {
            return false;
        }

        final Processor processor = processorNode.getProcessor();
        if (processor != null) {
            final Class<?> processorClass = processor.getClass();
            if (processorClass.isAnnotationPresent(DynamicRelationship.class)) {
                return false;
            }
        }

        for (final String relationshipName : newlySelected) {
            final Relationship relationship = processorNode.getRelationship(relationshipName);
            if (relationship == null) {
                return false;
            }

            if (processorNode.isAutoTerminated(relationship)) {
                return false;
            }

            final Set<Connection> relationshipConnections = replaceNull(processorNode.getConnections(relationship), Collections.emptySet());
            for (final Connection relationshipConnection : relationshipConnections) {
                if (!relationshipConnection.getIdentifier().equals(connection.getIdentifier())) {
                    return false;
                }
            }
        }

        for (final String removedRelationshipName : removedRelationships) {
            final Relationship removedRelationship = processorNode.getRelationship(removedRelationshipName);
            if (removedRelationship != null) {
                return false;
            }
        }

        return true;
    }

    private static <T> T replaceNull(final T value, final T replacement) {
        return value == null ? replacement : value;
    }

    /**
     * Determines whether a property difference is caused by a statically defined property being removed from the component definition.
     * When a processor or controller service drops a property (for example, as part of a version upgrade that invokes {@code removeProperty}
     * during migration), the reconciled component in NiFi should not report a "local change" so long as the component does not support
     * dynamic properties.
     *
     * @param difference the flow difference under evaluation
     * @param flowManager the flow manager used to resolve instantiated components
     * @return {@code true} if the property is no longer exposed by the component definition and the component does not support dynamic
     * properties; {@code false} otherwise
     */
    public static boolean isStaticPropertyRemoved(final FlowDifference difference, final FlowManager flowManager) {
        final DifferenceType differenceType = difference.getDifferenceType();
        if (differenceType != DifferenceType.PROPERTY_REMOVED) {
            return false;
        }

        final Optional<String> fieldName = difference.getFieldName();
        if (!fieldName.isPresent()) {
            return false;
        }

        final VersionedComponent componentB = difference.getComponentB();

        if (componentB instanceof InstantiatedVersionedProcessor) {
            final InstantiatedVersionedProcessor instantiatedProcessor = (InstantiatedVersionedProcessor) componentB;
            final ProcessorNode processorNode = flowManager.getProcessorNode(instantiatedProcessor.getInstanceIdentifier());
            return isStaticPropertyRemoved(fieldName.get(), processorNode);
        } else if (componentB instanceof InstantiatedVersionedControllerService) {
            final InstantiatedVersionedControllerService instantiatedControllerService = (InstantiatedVersionedControllerService) componentB;
            final ControllerServiceNode controllerService = flowManager.getControllerServiceNode(instantiatedControllerService.getInstanceIdentifier());
            return isStaticPropertyRemoved(fieldName.get(), controllerService);
        }

        return false;
    }

    private static boolean isStaticPropertyRemoved(String propertyName, ComponentNode componentNode) {
        if (componentNode == null) {
            return false;
        }

        final ConfigurableComponent configurableComponent = componentNode.getComponent();
        if (configurableComponent == null) {
            return false;
        }

        final boolean staticallyDefined = configurableComponent.getPropertyDescriptors().stream()
                .map(PropertyDescriptor::getName)
                .anyMatch(propertyName::equals);
        if (staticallyDefined) {
            return false;
        }

        if (supportsDynamicProperties(configurableComponent)) {
            return false;
        }

        return true;
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

    public static EnvironmentalChangeContext buildEnvironmentalChangeContext(final Collection<FlowDifference> differences, final FlowManager flowManager) {
        if (differences == null || differences.isEmpty() || flowManager == null) {
            return EnvironmentalChangeContext.empty();
        }

        final Map<String, List<PropertyDiffInfo>> parameterizedAddsByComponent = new HashMap<>();
        final Map<String, List<PropertyDiffInfo>> parameterizationRemovalsByComponent = new HashMap<>();
        final Map<String, List<PropertyDiffInfo>> controllerServicePropertyAddsByValue = new HashMap<>();
        final Map<String, List<PropertyDiffInfo>> propertyAddsByComponent = new HashMap<>();
        final Map<String, List<PropertyDiffInfo>> propertyRemovalsByComponent = new HashMap<>();

        for (final FlowDifference difference : differences) {
            if (difference.getDifferenceType() == DifferenceType.PROPERTY_ADDED) {
                final Optional<String> componentIdOptional = getComponentInstanceIdentifier(difference);
                if (componentIdOptional.isPresent()) {
                    final PropertyDiffInfo diffInfo = new PropertyDiffInfo(getPropertyValue(difference, false), difference);
                    propertyAddsByComponent.computeIfAbsent(componentIdOptional.get(), key -> new ArrayList<>()).add(diffInfo);
                }

                final Optional<String> propertyValue = getPropertyValue(difference, false);
                if (propertyValue.isPresent()
                        && !isParameterReference(propertyValue.get())
                        && isControllerServiceProperty(difference, flowManager)) {
                    controllerServicePropertyAddsByValue.computeIfAbsent(propertyValue.get(), key -> new ArrayList<>())
                            .add(new PropertyDiffInfo(propertyValue, difference));
                }
            }

            if (difference.getDifferenceType() == DifferenceType.PROPERTY_REMOVED) {
                final Optional<String> componentIdOptional = getComponentInstanceIdentifier(difference);
                if (componentIdOptional.isPresent()) {
                    final PropertyDiffInfo diffInfo = new PropertyDiffInfo(getPropertyValue(difference, true), difference);
                    propertyRemovalsByComponent.computeIfAbsent(componentIdOptional.get(), key -> new ArrayList<>()).add(diffInfo);
                }
            } else if (difference.getDifferenceType() == DifferenceType.PROPERTY_PARAMETERIZED
                    || difference.getDifferenceType() == DifferenceType.PROPERTY_PARAMETERIZATION_REMOVED) {

                final Optional<String> propertyNameOptional = difference.getFieldName();
                final Optional<String> componentIdOptional = getComponentInstanceIdentifier(difference);

                if (propertyNameOptional.isEmpty() || componentIdOptional.isEmpty()) {
                    continue;
                }

                final String componentId = componentIdOptional.get();
                final Optional<String> propertyValue = difference.getDifferenceType() == DifferenceType.PROPERTY_PARAMETERIZED
                        ? getParameterReferenceValue(difference, false)
                        : getParameterReferenceValue(difference, true);

                final PropertyDiffInfo diffInfo = new PropertyDiffInfo(propertyValue, difference);

                if (difference.getDifferenceType() == DifferenceType.PROPERTY_PARAMETERIZED) {
                    parameterizedAddsByComponent.computeIfAbsent(componentId, key -> new ArrayList<>()).add(diffInfo);
                } else {
                    parameterizationRemovalsByComponent.computeIfAbsent(componentId, key -> new ArrayList<>()).add(diffInfo);
                }
            }
        }

        final Set<String> serviceIdsWithMatchingAdditions = new HashSet<>();
        for (final FlowDifference difference : differences) {
            if (difference.getDifferenceType() != DifferenceType.COMPONENT_ADDED) {
                continue;
            }

            for (final String candidateId : getControllerServiceIdentifiers(difference)) {
                if (controllerServicePropertyAddsByValue.containsKey(candidateId)) {
                    serviceIdsWithMatchingAdditions.add(candidateId);
                }
            }
        }

        final Set<FlowDifference> parameterizedPropertyRenameDifferences = new HashSet<>();
        for (final Map.Entry<String, List<PropertyDiffInfo>> entry : parameterizationRemovalsByComponent.entrySet()) {
            final String componentId = entry.getKey();
            final List<PropertyDiffInfo> removals = entry.getValue();
            final List<PropertyDiffInfo> additions = new ArrayList<>(parameterizedAddsByComponent.getOrDefault(componentId, Collections.emptyList()));
            if (additions.isEmpty()) {
                continue;
            }

            for (final PropertyDiffInfo removalInfo : removals) {
                final Optional<String> removalValue = removalInfo.propertyValue();
                if (removalValue.isPresent() && !isParameterReference(removalValue.get())) {
                    continue;
                }

                PropertyDiffInfo matchingAddition = null;
                for (final Iterator<PropertyDiffInfo> iterator = additions.iterator(); iterator.hasNext();) {
                    final PropertyDiffInfo additionInfo = iterator.next();
                    final Optional<String> additionValue = additionInfo.propertyValue();
                    if (additionValue.isPresent() && !isParameterReference(additionValue.get())) {
                        continue;
                    }

                    if (valuesMatch(removalValue, additionValue)) {
                        matchingAddition = additionInfo;
                        iterator.remove();
                        break;
                    }
                }

                if (matchingAddition != null) {
                    parameterizedPropertyRenameDifferences.add(removalInfo.difference());
                    parameterizedPropertyRenameDifferences.add(matchingAddition.difference());
                }
            }
        }

        final Set<FlowDifference> propertyRenamesWithMatchingValues = new HashSet<>();
        for (final Map.Entry<String, List<PropertyDiffInfo>> entry : propertyRemovalsByComponent.entrySet()) {
            final String componentId = entry.getKey();
            final List<PropertyDiffInfo> removals = entry.getValue();
            final List<PropertyDiffInfo> additions = new ArrayList<>(propertyAddsByComponent.getOrDefault(componentId, Collections.emptyList()));
            if (additions.isEmpty()) {
                continue;
            }

            for (final PropertyDiffInfo removalInfo : removals) {
                final Optional<String> removalValue = removalInfo.propertyValue();
                if (removalValue.isEmpty()) {
                    continue;
                }

                PropertyDiffInfo matchingAddition = null;
                for (final Iterator<PropertyDiffInfo> iterator = additions.iterator(); iterator.hasNext();) {
                    final PropertyDiffInfo additionInfo = iterator.next();
                    final Optional<String> additionValue = additionInfo.propertyValue();
                    if (additionValue.isEmpty()) {
                        continue;
                    }

                    if (valuesMatch(removalValue, additionValue)) {
                        matchingAddition = additionInfo;
                        iterator.remove();
                        break;
                    }
                }

                if (matchingAddition != null) {
                    final String removalField = removalInfo.difference().getFieldName().orElse(null);
                    final String additionField = matchingAddition.difference().getFieldName().orElse(null);
                    if (!Objects.equals(removalField, additionField)) {
                        propertyRenamesWithMatchingValues.add(removalInfo.difference());
                        propertyRenamesWithMatchingValues.add(matchingAddition.difference());
                    }
                }
            }
        }

        if (serviceIdsWithMatchingAdditions.isEmpty() && parameterizedPropertyRenameDifferences.isEmpty() && propertyRenamesWithMatchingValues.isEmpty()) {
            return EnvironmentalChangeContext.empty();
        }

        return new EnvironmentalChangeContext(serviceIdsWithMatchingAdditions, parameterizedPropertyRenameDifferences, propertyRenamesWithMatchingValues);
    }

    public static boolean isControllerServiceCreatedForNewProperty(final FlowDifference difference, final EnvironmentalChangeContext context) {
        final EnvironmentalChangeContext evaluatedContext = Objects.requireNonNull(context, "EnvironmentalChangeContext required");
        return isControllerServiceCreatedForNewPropertyInternal(difference, evaluatedContext);
    }

    private static boolean isControllerServiceCreatedForNewPropertyInternal(final FlowDifference difference, final EnvironmentalChangeContext context) {
        if (context.serviceIdsCreatedForNewProperties().isEmpty()) {
            return false;
        }

        if (difference.getDifferenceType() == DifferenceType.PROPERTY_ADDED) {
            return getPropertyValue(difference, false)
                    .filter(value -> context.serviceIdsCreatedForNewProperties().contains(value))
                    .isPresent();
        }

        if (difference.getDifferenceType() == DifferenceType.COMPONENT_ADDED) {
            for (final String candidateId : getControllerServiceIdentifiers(difference)) {
                if (context.serviceIdsCreatedForNewProperties().contains(candidateId)) {
                    return true;
                }
            }
            return false;
        }

        return false;
    }

    private static Set<String> getControllerServiceIdentifiers(final FlowDifference difference) {
        final Set<String> identifiers = new HashSet<>();

        addControllerServiceIdentifiers(difference.getComponentB(), identifiers);
        addControllerServiceIdentifiers(difference.getComponentA(), identifiers);

        return identifiers;
    }

    private static void addControllerServiceIdentifiers(final VersionedComponent component, final Set<String> identifiers) {
        if (component == null) {
            return;
        }

        if (component instanceof InstantiatedVersionedControllerService instantiatedControllerService) {
            final String instanceIdentifier = instantiatedControllerService.getInstanceIdentifier();
            if (instanceIdentifier != null) {
                identifiers.add(instanceIdentifier);
            }
        }

        if (component instanceof VersionedControllerService) {
            final String identifier = component.getIdentifier();
            if (identifier != null) {
                identifiers.add(identifier);
            }
        }
    }

    public static boolean isPropertyParameterizationRename(final FlowDifference difference, final EnvironmentalChangeContext context) {
        final EnvironmentalChangeContext evaluatedContext = Objects.requireNonNull(context, "EnvironmentalChangeContext required");
        return evaluatedContext.parameterizedPropertyRenames().isEmpty() ? false : evaluatedContext.parameterizedPropertyRenames().contains(difference);
    }

    public static boolean isPropertyRenameWithMatchingValue(final FlowDifference difference, final EnvironmentalChangeContext context) {
        final EnvironmentalChangeContext evaluatedContext = Objects.requireNonNull(context, "EnvironmentalChangeContext required");
        return evaluatedContext.propertyRenamesWithMatchingValues().isEmpty() ? false : evaluatedContext.propertyRenamesWithMatchingValues().contains(difference);
    }

    private static Optional<String> getComponentInstanceIdentifier(final FlowDifference difference) {
        final Optional<String> identifierB = getComponentInstanceIdentifier(difference.getComponentB());
        if (identifierB.isPresent()) {
            return identifierB;
        }

        return getComponentInstanceIdentifier(difference.getComponentA());
    }

    private static Optional<String> getComponentInstanceIdentifier(final VersionedComponent component) {
        if (component == null) {
            return Optional.empty();
        }

        if (component instanceof InstantiatedVersionedComponent) {
            final String instanceId = ((InstantiatedVersionedComponent) component).getInstanceIdentifier();
            if (instanceId != null) {
                return Optional.of(instanceId);
            }
        }

        return Optional.ofNullable(component.getIdentifier());
    }

    private static Optional<String> getParameterReferenceValue(final FlowDifference difference, final boolean fromComponentA) {
        final VersionedComponent primaryComponent = fromComponentA ? difference.getComponentA() : difference.getComponentB();
        final VersionedComponent secondaryComponent = fromComponentA ? difference.getComponentB() : difference.getComponentA();

        final Map<String, String> primaryProperties = getProperties(primaryComponent);
        final Optional<String> parameterReference;
        if (primaryProperties.isEmpty()) {
            parameterReference = Optional.empty();
        } else {
            final Map<String, String> secondaryProperties = getProperties(secondaryComponent);
            Optional<String> resolvedReference = Optional.empty();

            final Optional<String> fieldNameOptional = difference.getFieldName();
            if (fieldNameOptional.isPresent()) {
                final String fieldName = fieldNameOptional.get();
                final String propertyValue = primaryProperties.get(fieldName);
                if (propertyValue != null) {
                    resolvedReference = Optional.of(propertyValue);
                }
            }

            if (resolvedReference.isEmpty()) {
                for (Map.Entry<String, String> entry : primaryProperties.entrySet()) {
                    final String propertyName = entry.getKey();
                    final String propertyValue = entry.getValue();
                    if (!isParameterReference(propertyValue)) {
                        continue;
                    }

                    if (!secondaryProperties.containsKey(propertyName)) {
                        resolvedReference = Optional.of(propertyValue);
                        break;
                    }
                }
            }

            parameterReference = resolvedReference;
        }

        return parameterReference;
    }

    private static Optional<String> getPropertyValue(final FlowDifference difference, final boolean fromComponentA) {
        final Optional<String> fieldNameOptional = difference.getFieldName();
        if (fieldNameOptional.isEmpty()) {
            return Optional.empty();
        }

        final VersionedComponent component = fromComponentA ? difference.getComponentA() : difference.getComponentB();
        final Map<String, String> properties = getProperties(component);
        final String propertyValue = properties.get(fieldNameOptional.get());
        if (propertyValue != null) {
            return Optional.of(propertyValue);
        }

        final Object differenceValue = fromComponentA ? difference.getValueA() : difference.getValueB();
        if (differenceValue instanceof String stringValue) {
            return Optional.of(stringValue);
        }

        return Optional.empty();
    }

    private static Optional<PropertyDescriptor> getComponentPropertyDescriptor(final FlowManager flowManager, final FlowDifference difference, final String propertyName) {
        final ComponentNode componentNode = Optional.ofNullable(getComponent(flowManager, difference.getComponentB()))
                .orElseGet(() -> getComponent(flowManager, difference.getComponentA()));

        if (componentNode == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(componentNode.getPropertyDescriptor(propertyName));
    }

    private static Optional<VersionedPropertyDescriptor> getVersionedPropertyDescriptor(final FlowDifference difference, final boolean fromComponentA) {
        final VersionedComponent component = fromComponentA ? difference.getComponentA() : difference.getComponentB();
        final Map<String, VersionedPropertyDescriptor> descriptors = getPropertyDescriptors(component);
        if (descriptors == null || descriptors.isEmpty()) {
            return Optional.empty();
        }

        final Optional<String> fieldNameOptional = difference.getFieldName();
        if (fieldNameOptional.isEmpty()) {
            return Optional.empty();
        }

        final String fieldName = fieldNameOptional.get();
        VersionedPropertyDescriptor descriptor = descriptors.get(fieldName);
        if (descriptor == null) {
            descriptor = descriptors.values().stream()
                    .filter(candidate -> fieldName.equals(candidate.getName()) || fieldName.equals(candidate.getDisplayName()))
                    .findFirst()
                    .orElse(null);
        }

        return Optional.ofNullable(descriptor);
    }

    private static Map<String, VersionedPropertyDescriptor> getPropertyDescriptors(final VersionedComponent component) {
        final Map<String, VersionedPropertyDescriptor> descriptors;

        if (component == null) {
            descriptors = Collections.emptyMap();
        } else if (component instanceof VersionedConfigurableComponent configurableComponent) {
            descriptors = configurableComponent.getPropertyDescriptors();
        } else if (component instanceof VersionedProcessor processor) {
            descriptors = processor.getPropertyDescriptors();
        } else if (component instanceof VersionedControllerService controllerService) {
            descriptors = controllerService.getPropertyDescriptors();
        } else {
            descriptors = Collections.emptyMap();
        }

        return descriptors == null ? Collections.emptyMap() : descriptors;
    }

    private static Map<String, String> getProperties(final VersionedComponent component) {
        final Map<String, String> properties;

        if (component == null) {
            properties = Collections.emptyMap();
        } else if (component instanceof VersionedConfigurableComponent configurableComponent) {
            properties = configurableComponent.getProperties();
        } else if (component instanceof VersionedProcessor processor) {
            properties = processor.getProperties();
        } else if (component instanceof VersionedControllerService controllerService) {
            properties = controllerService.getProperties();
        } else {
            properties = Collections.emptyMap();
        }

        return properties == null ? Collections.emptyMap() : properties;
    }

    private static boolean isControllerServiceProperty(final FlowDifference difference, final FlowManager flowManager) {
        final Optional<String> fieldNameOptional = difference.getFieldName();
        if (fieldNameOptional.isEmpty()) {
            return false;
        }

        final String propertyName = fieldNameOptional.get();
        final Optional<PropertyDescriptor> componentDescriptor = getComponentPropertyDescriptor(flowManager, difference, propertyName);
        if (componentDescriptor.isPresent()) {
            final PropertyDescriptor descriptor = componentDescriptor.get();
            return !descriptor.isDynamic() && descriptor.getControllerServiceDefinition() != null;
        }

        final Optional<VersionedPropertyDescriptor> versionedDescriptor = getVersionedPropertyDescriptor(difference, false);
        if (versionedDescriptor.isPresent()) {
            final VersionedPropertyDescriptor descriptor = versionedDescriptor.get();
            return Boolean.TRUE.equals(descriptor.getIdentifiesControllerService());
        }

        return false;
    }

    private static boolean valuesMatch(final Optional<String> first, final Optional<String> second) {
        return first.isPresent() && second.isPresent() && first.get().equals(second.get());
    }

    private static boolean isParameterReference(final String propertyValue) {
        return propertyValue != null && PARAMETER_REFERENCE_PATTERN.matcher(propertyValue).matches();
    }

    private static final class PropertyDiffInfo {
        private final Optional<String> propertyValue;
        private final FlowDifference difference;

        private PropertyDiffInfo(final Optional<String> propertyValue, final FlowDifference difference) {
            this.propertyValue = propertyValue;
            this.difference = difference;
        }

        Optional<String> propertyValue() {
            return propertyValue;
        }

        FlowDifference difference() {
            return difference;
        }
    }

    public static final class EnvironmentalChangeContext {
        private static final EnvironmentalChangeContext EMPTY = new EnvironmentalChangeContext(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

        private final Set<String> serviceIdsCreatedForNewProperties;
        private final Set<FlowDifference> parameterizedPropertyRenames;
        private final Set<FlowDifference> propertyRenamesWithMatchingValues;

        private EnvironmentalChangeContext(final Set<String> serviceIdsCreatedForNewProperties,
                                           final Set<FlowDifference> parameterizedPropertyRenames,
                                           final Set<FlowDifference> propertyRenamesWithMatchingValues) {
            this.serviceIdsCreatedForNewProperties = Collections.unmodifiableSet(new HashSet<>(serviceIdsCreatedForNewProperties));
            this.parameterizedPropertyRenames = Collections.unmodifiableSet(new HashSet<>(parameterizedPropertyRenames));
            this.propertyRenamesWithMatchingValues = Collections.unmodifiableSet(new HashSet<>(propertyRenamesWithMatchingValues));
        }

        static EnvironmentalChangeContext empty() {
            return EMPTY;
        }

        Set<String> serviceIdsCreatedForNewProperties() {
            return serviceIdsCreatedForNewProperties;
        }

        Set<FlowDifference> parameterizedPropertyRenames() {
            return parameterizedPropertyRenames;
        }

        Set<FlowDifference> propertyRenamesWithMatchingValues() {
            return propertyRenamesWithMatchingValues;
        }
    }
}
