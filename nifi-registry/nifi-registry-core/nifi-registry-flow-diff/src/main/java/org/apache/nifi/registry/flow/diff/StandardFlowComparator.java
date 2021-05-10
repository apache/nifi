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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedFunnel;
import org.apache.nifi.registry.flow.VersionedLabel;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedPropertyDescriptor;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StandardFlowComparator implements FlowComparator {
    private static final String DEFAULT_LOAD_BALANCE_STRATEGY = "DO_NOT_LOAD_BALANCE";
    private static final String DEFAULT_PARTITIONING_ATTRIBUTE = "";
    private static final String DEFAULT_LOAD_BALANCE_COMPRESSION = "DO_NOT_COMPRESS";
    private static final String DEFAULT_FLOW_FILE_CONCURRENCY = "UNBOUNDED";
    private static final String DEFAULT_OUTBOUND_FLOW_FILE_POLICY = "STREAM_WHEN_AVAILABLE";

    private static final Pattern PARAMETER_REFERENCE_PATTERN = Pattern.compile("#\\{[A-Za-z0-9\\-_. ]+}");

    private final ComparableDataFlow flowA;
    private final ComparableDataFlow flowB;
    private final Set<String> externallyAccessibleServiceIds;
    private final DifferenceDescriptor differenceDescriptor;

    public StandardFlowComparator(final ComparableDataFlow flowA, final ComparableDataFlow flowB,
        final Set<String> externallyAccessibleServiceIds, final DifferenceDescriptor differenceDescriptor) {
        this.flowA = flowA;
        this.flowB = flowB;
        this.externallyAccessibleServiceIds = externallyAccessibleServiceIds;
        this.differenceDescriptor = differenceDescriptor;
    }

    @Override
    public FlowComparison compare() {
        final VersionedProcessGroup groupA = flowA.getContents();
        final VersionedProcessGroup groupB = flowB.getContents();
        final Set<FlowDifference> differences = compare(groupA, groupB);

        return new StandardFlowComparison(flowA, flowB, differences);
    }

    private Set<FlowDifference> compare(final VersionedProcessGroup groupA, final VersionedProcessGroup groupB) {
        final Set<FlowDifference> differences = new HashSet<>();
        // Note that we do not compare the names, because when we import a Flow into NiFi, we may well give it a new name.
        // Child Process Groups' names will still compare but the main group that is under Version Control will not
        compare(groupA, groupB, differences, false);
        return differences;
    }


    private <T extends VersionedComponent> Set<FlowDifference> compareComponents(final Set<T> componentsA, final Set<T> componentsB, final ComponentComparator<T> comparator) {
        final Map<String, T> componentMapA = byId(componentsA == null ? Collections.emptySet() : componentsA);
        final Map<String, T> componentMapB = byId(componentsB == null ? Collections.emptySet() : componentsB);

        final Set<FlowDifference> differences = new HashSet<>();

        componentMapA.forEach((key, componentA) -> {
            final T componentB = componentMapB.get(key);
            comparator.compare(componentA, componentB, differences);
        });

        componentMapB.forEach((key, componentB) -> {
            final T componentA = componentMapA.get(key);

            // if component A is not null, it has already been compared above. If component A
            // is null, then it is missing from Flow A but present in Flow B, so we will just call
            // compare(), which will handle this for us.
            if (componentA == null) {
                comparator.compare(componentA, componentB, differences);
            }
        });

        return differences;
    }


    private boolean compareComponents(final VersionedComponent componentA, final VersionedComponent componentB, final Set<FlowDifference> differences) {
        return compareComponents(componentA, componentB, differences, true, true, true);
    }

    private boolean compareComponents(final VersionedComponent componentA, final VersionedComponent componentB, final Set<FlowDifference> differences,
        final boolean compareName, final boolean comparePos, final boolean compareComments) {
        if (componentA == null) {
            differences.add(difference(DifferenceType.COMPONENT_ADDED, componentA, componentB, componentA, componentB));
            return true;
        }

        if (componentB == null) {
            differences.add(difference(DifferenceType.COMPONENT_REMOVED, componentA, componentB, componentA, componentB));
            return true;
        }

        if (compareComments) {
            addIfDifferent(differences, DifferenceType.COMMENTS_CHANGED, componentA, componentB, VersionedComponent::getComments, false);
        }

        if (compareName) {
            addIfDifferent(differences, DifferenceType.NAME_CHANGED, componentA, componentB, VersionedComponent::getName);
        }

        if (comparePos) {
            addIfDifferent(differences, DifferenceType.POSITION_CHANGED, componentA, componentB, VersionedComponent::getPosition);
        }

        return false;
    }

    private void compare(final VersionedProcessor processorA, final VersionedProcessor processorB, final Set<FlowDifference> differences) {
        if (compareComponents(processorA, processorB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.ANNOTATION_DATA_CHANGED, processorA, processorB, VersionedProcessor::getAnnotationData);
        addIfDifferent(differences, DifferenceType.AUTO_TERMINATED_RELATIONSHIPS_CHANGED, processorA, processorB, VersionedProcessor::getAutoTerminatedRelationships);
        addIfDifferent(differences, DifferenceType.BULLETIN_LEVEL_CHANGED, processorA, processorB, VersionedProcessor::getBulletinLevel);
        addIfDifferent(differences, DifferenceType.BUNDLE_CHANGED, processorA, processorB, VersionedProcessor::getBundle);
        addIfDifferent(differences, DifferenceType.CONCURRENT_TASKS_CHANGED, processorA, processorB, VersionedProcessor::getConcurrentlySchedulableTaskCount);
        addIfDifferent(differences, DifferenceType.EXECUTION_MODE_CHANGED, processorA, processorB, VersionedProcessor::getExecutionNode);
        addIfDifferent(differences, DifferenceType.PENALTY_DURATION_CHANGED, processorA, processorB, VersionedProcessor::getPenaltyDuration);
        addIfDifferent(differences, DifferenceType.RUN_DURATION_CHANGED, processorA, processorB, VersionedProcessor::getRunDurationMillis);
        addIfDifferent(differences, DifferenceType.RUN_SCHEDULE_CHANGED, processorA, processorB, VersionedProcessor::getSchedulingPeriod);
        addIfDifferent(differences, DifferenceType.SCHEDULING_STRATEGY_CHANGED, processorA, processorB, VersionedProcessor::getSchedulingStrategy);
        addIfDifferent(differences, DifferenceType.SCHEDULED_STATE_CHANGED, processorA, processorB, VersionedProcessor::getScheduledState);
        addIfDifferent(differences, DifferenceType.STYLE_CHANGED, processorA, processorB, VersionedProcessor::getStyle);
        addIfDifferent(differences, DifferenceType.YIELD_DURATION_CHANGED, processorA, processorB, VersionedProcessor::getYieldDuration);
        compareProperties(processorA, processorB, processorA.getProperties(), processorB.getProperties(), processorA.getPropertyDescriptors(), processorB.getPropertyDescriptors(), differences);
    }

    @Override
    public Set<FlowDifference> compareControllerServices(final VersionedControllerService serviceA, final VersionedControllerService serviceB) {
        final Set<FlowDifference> differences = new HashSet<>();
        compare(serviceA, serviceB, differences);
        return differences;
    }

    private void compare(final VersionedControllerService serviceA, final VersionedControllerService serviceB, final Set<FlowDifference> differences) {
        if (compareComponents(serviceA, serviceB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.ANNOTATION_DATA_CHANGED, serviceA, serviceB, VersionedControllerService::getAnnotationData);
        addIfDifferent(differences, DifferenceType.BUNDLE_CHANGED, serviceA, serviceB, VersionedControllerService::getBundle);
        compareProperties(serviceA, serviceB, serviceA.getProperties(), serviceB.getProperties(), serviceA.getPropertyDescriptors(), serviceB.getPropertyDescriptors(), differences);
    }


    private void compareProperties(final VersionedComponent componentA, final VersionedComponent componentB,
        final Map<String, String> propertiesA, final Map<String, String> propertiesB,
        final Map<String, VersionedPropertyDescriptor> descriptorsA, final Map<String, VersionedPropertyDescriptor> descriptorsB,
        final Set<FlowDifference> differences) {

        propertiesA.forEach((key, valueA) -> {
            final String valueB = propertiesB.get(key);

            VersionedPropertyDescriptor descriptor = descriptorsA.get(key);
            if (descriptor == null) {
                descriptor = descriptorsB.get(key);
            }

            final String displayName;
            if (descriptor == null) {
                displayName = key;
            } else {
                displayName = descriptor.getDisplayName() == null ? descriptor.getName() : descriptor.getDisplayName();
            }

            if (valueA == null && valueB != null) {
                if (isParameterReference(valueB)) {
                    differences.add(difference(DifferenceType.PROPERTY_PARAMETERIZED, componentA, componentB, key, displayName, null, null));
                } else {
                    differences.add(difference(DifferenceType.PROPERTY_ADDED, componentA, componentB, key, displayName, valueA, valueB));
                }
            } else if (valueA != null && valueB == null) {
                if (isParameterReference(valueA)) {
                    differences.add(difference(DifferenceType.PROPERTY_PARAMETERIZATION_REMOVED, componentA, componentB, key, displayName, null, null));
                } else {
                    differences.add(difference(DifferenceType.PROPERTY_REMOVED, componentA, componentB, key, displayName, valueA, valueB));
                }
            } else if (valueA != null && !valueA.equals(valueB)) {
                // If the property in Flow A references a Controller Service that is not available in the flow
                // and the property in Flow B references a Controller Service that is available in its environment
                // but not part of the Versioned Flow, then we do not want to consider this to be a Flow Difference.
                // This is typically the case when a flow is versioned in one instance, referencing an external Controller Service,
                // and then imported into another NiFi instance. When imported, the property does not point to any existing Controller
                // Service, and the user must then point the property an existing Controller Service. We don't want to consider the
                // flow as having changed, since it is an environment-specific change (similar to how we handle variables).
                if (descriptor != null && descriptor.getIdentifiesControllerService()) {
                    final boolean accessibleA = externallyAccessibleServiceIds.contains(valueA);
                    final boolean accessibleB = externallyAccessibleServiceIds.contains(valueB);
                    if (!accessibleA && accessibleB) {
                        return;
                    }
                }

                final boolean aParameterized = isParameterReference(valueA);
                final boolean bParameterized = isParameterReference(valueB);
                if (aParameterized && !bParameterized) {
                    differences.add(difference(DifferenceType.PROPERTY_PARAMETERIZATION_REMOVED, componentA, componentB, key, displayName, null, null));
                } else if (!aParameterized && bParameterized) {
                    differences.add(difference(DifferenceType.PROPERTY_PARAMETERIZED, componentA, componentB, key, displayName, null, null));
                } else {
                    differences.add(difference(DifferenceType.PROPERTY_CHANGED, componentA, componentB, key, displayName, valueA, valueB));
                }
            }
        });

        propertiesB.forEach((key, valueB) -> {
            final String valueA = propertiesA.get(key);

            // If there are any properties for component B that do not exist for Component A, add those as differences as well.
            if (valueA == null && valueB != null) {
                final VersionedPropertyDescriptor descriptor = descriptorsB.get(key);

                final String displayName;
                if (descriptor == null) {
                    displayName = key;
                } else {
                    displayName = descriptor.getDisplayName() == null ? descriptor.getName() : descriptor.getDisplayName();
                }

                if (isParameterReference(valueB)) {
                    differences.add(difference(DifferenceType.PROPERTY_PARAMETERIZED, componentA, componentB, key, displayName, null, null));
                } else {
                    differences.add(difference(DifferenceType.PROPERTY_ADDED, componentA, componentB, key, displayName, null, valueB));
                }
            }
        });
    }


    private boolean isParameterReference(final String propertyValue) {
        return PARAMETER_REFERENCE_PATTERN.matcher(propertyValue).matches();
    }

    private void compare(final VersionedFunnel funnelA, final VersionedFunnel funnelB, final Set<FlowDifference> differences) {
        compareComponents(funnelA, funnelB, differences);
    }

    private void compare(final VersionedLabel labelA, final VersionedLabel labelB, final Set<FlowDifference> differences) {
        if (compareComponents(labelA, labelB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.LABEL_VALUE_CHANGED, labelA, labelB, VersionedLabel::getLabel);
        addIfDifferent(differences, DifferenceType.POSITION_CHANGED, labelA, labelB, VersionedLabel::getHeight);
        addIfDifferent(differences, DifferenceType.POSITION_CHANGED, labelA, labelB, VersionedLabel::getWidth);
        addIfDifferent(differences, DifferenceType.STYLE_CHANGED, labelA, labelB, VersionedLabel::getStyle);
    }

    private void compare(final VersionedPort portA, final VersionedPort portB, final Set<FlowDifference> differences) {
        if (compareComponents(portA, portB, differences)) {
            return;
        }

        if (portA != null && portA.isAllowRemoteAccess() && portB != null && portB.isAllowRemoteAccess()) {
            addIfDifferent(differences, DifferenceType.CONCURRENT_TASKS_CHANGED, portA, portB, VersionedPort::getConcurrentlySchedulableTaskCount);
        }
    }

    private void compare(final VersionedRemoteProcessGroup rpgA, final VersionedRemoteProcessGroup rpgB, final Set<FlowDifference> differences) {
        if (compareComponents(rpgA, rpgB, differences, false, true, false)) { // do not compare comments for RPG because they come from remote system, not our local flow
            return;
        }

        addIfDifferent(differences, DifferenceType.RPG_COMMS_TIMEOUT_CHANGED, rpgA, rpgB, VersionedRemoteProcessGroup::getCommunicationsTimeout);
        addIfDifferent(differences, DifferenceType.RPG_NETWORK_INTERFACE_CHANGED, rpgA, rpgB, VersionedRemoteProcessGroup::getLocalNetworkInterface);
        addIfDifferent(differences, DifferenceType.RPG_PROXY_HOST_CHANGED, rpgA, rpgB, VersionedRemoteProcessGroup::getProxyHost);
        addIfDifferent(differences, DifferenceType.RPG_PROXY_PORT_CHANGED, rpgA, rpgB, VersionedRemoteProcessGroup::getProxyPort);
        addIfDifferent(differences, DifferenceType.RPG_PROXY_USER_CHANGED, rpgA, rpgB, VersionedRemoteProcessGroup::getProxyUser);
        addIfDifferent(differences, DifferenceType.RPG_TRANSPORT_PROTOCOL_CHANGED, rpgA, rpgB, VersionedRemoteProcessGroup::getTransportProtocol);
        addIfDifferent(differences, DifferenceType.YIELD_DURATION_CHANGED, rpgA, rpgB, VersionedRemoteProcessGroup::getYieldDuration);

        differences.addAll(compareComponents(rpgA.getInputPorts(), rpgB.getInputPorts(), this::compare));
        differences.addAll(compareComponents(rpgA.getOutputPorts(), rpgB.getOutputPorts(), this::compare));
    }

    private void compare(final VersionedRemoteGroupPort portA, final VersionedRemoteGroupPort portB, final Set<FlowDifference> differences) {
        if (compareComponents(portA, portB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.REMOTE_PORT_BATCH_SIZE_CHANGED, portA, portB, VersionedRemoteGroupPort::getBatchSize);
        addIfDifferent(differences, DifferenceType.REMOTE_PORT_COMPRESSION_CHANGED, portA, portB, VersionedRemoteGroupPort::isUseCompression);
        addIfDifferent(differences, DifferenceType.CONCURRENT_TASKS_CHANGED, portA, portB, VersionedRemoteGroupPort::getConcurrentlySchedulableTaskCount);
    }


    private void compare(final VersionedProcessGroup groupA, final VersionedProcessGroup groupB, final Set<FlowDifference> differences, final boolean compareNamePos) {
        if (compareComponents(groupA, groupB, differences, compareNamePos, compareNamePos, true)) {
            return;
        }

        if (groupA == null) {
            differences.add(difference(DifferenceType.COMPONENT_ADDED, groupA, groupB, groupA, groupB));
            return;
        }

        if (groupB == null) {
            differences.add(difference(DifferenceType.COMPONENT_REMOVED, groupA, groupB, groupA, groupB));
            return;
        }

        addIfDifferent(differences, DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED, groupA, groupB, VersionedProcessGroup::getVersionedFlowCoordinates);
        addIfDifferent(differences, DifferenceType.FLOWFILE_CONCURRENCY_CHANGED, groupA, groupB, VersionedProcessGroup::getFlowFileConcurrency,
            true, DEFAULT_FLOW_FILE_CONCURRENCY);
        addIfDifferent(differences, DifferenceType.FLOWFILE_OUTBOUND_POLICY_CHANGED, groupA, groupB, VersionedProcessGroup::getFlowFileOutboundPolicy,
            true, DEFAULT_OUTBOUND_FLOW_FILE_POLICY);

        final VersionedFlowCoordinates groupACoordinates = groupA.getVersionedFlowCoordinates();
        final VersionedFlowCoordinates groupBCoordinates = groupB.getVersionedFlowCoordinates();

        if ((groupACoordinates == null && groupBCoordinates == null)
                || (groupACoordinates != null && groupBCoordinates != null && !groupACoordinates.equals(groupBCoordinates)) ) {
            differences.addAll(compareComponents(groupA.getConnections(), groupB.getConnections(), this::compare));
            differences.addAll(compareComponents(groupA.getProcessors(), groupB.getProcessors(), this::compare));
            differences.addAll(compareComponents(groupA.getControllerServices(), groupB.getControllerServices(), this::compare));
            differences.addAll(compareComponents(groupA.getFunnels(), groupB.getFunnels(), this::compare));
            differences.addAll(compareComponents(groupA.getInputPorts(), groupB.getInputPorts(), this::compare));
            differences.addAll(compareComponents(groupA.getLabels(), groupB.getLabels(), this::compare));
            differences.addAll(compareComponents(groupA.getOutputPorts(), groupB.getOutputPorts(), this::compare));
            differences.addAll(compareComponents(groupA.getProcessGroups(), groupB.getProcessGroups(), (a, b, diffs) -> compare(a, b, diffs, true)));
            differences.addAll(compareComponents(groupA.getRemoteProcessGroups(), groupB.getRemoteProcessGroups(), this::compare));
        }
    }


    private void compare(final VersionedConnection connectionA, final VersionedConnection connectionB, final Set<FlowDifference> differences) {
        if (compareComponents(connectionA, connectionB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED, connectionA, connectionB, VersionedConnection::getBackPressureDataSizeThreshold);
        addIfDifferent(differences, DifferenceType.BACKPRESSURE_OBJECT_THRESHOLD_CHANGED, connectionA, connectionB, VersionedConnection::getBackPressureObjectThreshold);
        addIfDifferent(differences, DifferenceType.BENDPOINTS_CHANGED, connectionA, connectionB, VersionedConnection::getBends);
        addIfDifferent(differences, DifferenceType.DESTINATION_CHANGED, connectionA, connectionB, VersionedConnection::getDestination);
        addIfDifferent(differences, DifferenceType.FLOWFILE_EXPIRATION_CHANGED, connectionA, connectionB, VersionedConnection::getFlowFileExpiration);
        addIfDifferent(differences, DifferenceType.PRIORITIZERS_CHANGED, connectionA, connectionB, VersionedConnection::getPrioritizers);
        addIfDifferent(differences, DifferenceType.SELECTED_RELATIONSHIPS_CHANGED, connectionA, connectionB, VersionedConnection::getSelectedRelationships);
        addIfDifferent(differences, DifferenceType.SOURCE_CHANGED, connectionA, connectionB, c -> c.getSource().getId());

        addIfDifferent(differences, DifferenceType.LOAD_BALANCE_STRATEGY_CHANGED, connectionA, connectionB,
                conn -> conn.getLoadBalanceStrategy() == null ? DEFAULT_LOAD_BALANCE_STRATEGY : conn.getLoadBalanceStrategy());

        addIfDifferent(differences, DifferenceType.PARTITIONING_ATTRIBUTE_CHANGED, connectionA, connectionB,
                conn -> conn.getPartitioningAttribute() == null ? DEFAULT_PARTITIONING_ATTRIBUTE : conn.getPartitioningAttribute());

        addIfDifferent(differences, DifferenceType.LOAD_BALANCE_COMPRESSION_CHANGED, connectionA, connectionB,
            conn -> conn.getLoadBalanceCompression() == null ? DEFAULT_LOAD_BALANCE_COMPRESSION : conn.getLoadBalanceCompression());
    }


    private <T extends VersionedComponent> Map<String, T> byId(final Set<T> components) {
        return components.stream().collect(Collectors.toMap(VersionedComponent::getIdentifier, Function.identity()));
    }

    private <T extends VersionedComponent> void addIfDifferent(final Set<FlowDifference> differences, final DifferenceType type, final T componentA, final T componentB,
        final Function<T, Object> transform) {

        addIfDifferent(differences, type, componentA, componentB, transform, true);
    }

    private <T extends VersionedComponent> void addIfDifferent(final Set<FlowDifference> differences, final DifferenceType type, final T componentA, final T componentB,
                                                               final Function<T, Object> transform, final boolean differentiateNullAndEmptyString) {
        addIfDifferent(differences, type, componentA, componentB, transform, differentiateNullAndEmptyString, null);
    }

    private <T extends VersionedComponent> void addIfDifferent(final Set<FlowDifference> differences, final DifferenceType type, final T componentA, final T componentB,
        final Function<T, Object> transform, final boolean differentiateNullAndEmptyString, final Object defaultValue) {

        Object valueA = transform.apply(componentA);
        if (valueA == null) {
            valueA = defaultValue;
        }

        Object valueB = transform.apply(componentB);
        if (valueB == null) {
            valueB = defaultValue;
        }

        if (Objects.equals(valueA, valueB)) {
            return;
        }

        // We don't want to disambiguate between an empty collection and null.
        if ((valueA == null || valueA instanceof Collection) && (valueB == null || valueB instanceof Collection) && isEmpty((Collection<?>) valueA) && isEmpty((Collection<?>) valueB)) {
            return;
        }

        if (!differentiateNullAndEmptyString && isEmptyString(valueA) && isEmptyString(valueB)) {
            return;
        }

        differences.add(difference(type, componentA, componentB, valueA, valueB));
    }

    private boolean isEmpty(final Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    private boolean isEmptyString(final Object potentialString) {
        if (potentialString == null) {
            return true;
        }

        if (potentialString instanceof String) {
            final String string = (String) potentialString;
            return string.isEmpty();
        } else {
            return false;
        }
    }

    private FlowDifference difference(final DifferenceType type, final VersionedComponent componentA, final VersionedComponent componentB,
            final Object valueA, final Object valueB) {

        final String description = differenceDescriptor.describeDifference(type, flowA.getName(), flowB.getName(), componentA, componentB, null, valueA, valueB);
        return new StandardFlowDifference(type, componentA, componentB, valueA, valueB, description);
    }

    private FlowDifference difference(final DifferenceType type, final VersionedComponent componentA, final VersionedComponent componentB, final String fieldName, final String prettyPrintFieldName,
                                      final Object valueA, final Object valueB) {

        final String description = differenceDescriptor.describeDifference(type, flowA.getName(), flowB.getName(), componentA, componentB, prettyPrintFieldName, valueA, valueB);
        return new StandardFlowDifference(type, componentA, componentB, fieldName, valueA, valueB, description);
    }


    private static interface ComponentComparator<T extends VersionedComponent> {
        void compare(T componentA, T componentB, Set<FlowDifference> differences);
    }
}
