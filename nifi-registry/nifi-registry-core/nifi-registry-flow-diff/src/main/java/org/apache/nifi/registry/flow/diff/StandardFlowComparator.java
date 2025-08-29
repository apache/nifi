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

import org.apache.nifi.components.PortFunction;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFlowAnalysisRule;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedFlowRegistryClient;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedParameterProvider;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StandardFlowComparator implements FlowComparator {
    private static final Logger logger = LoggerFactory.getLogger(StandardFlowComparator.class);

    private static final String ENCRYPTED_VALUE_PREFIX = "enc{";
    private static final String ENCRYPTED_VALUE_SUFFIX = "}";
    private static final String FLOW_VERSION = "Flow Version";

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
    private final Function<String, String> propertyDecryptor;
    private final Function<VersionedComponent, String> idLookup;
    private final FlowComparatorVersionedStrategy flowComparatorVersionedStrategy;

    public StandardFlowComparator(final ComparableDataFlow flowA, final ComparableDataFlow flowB, final Set<String> externallyAccessibleServiceIds,
                                  final DifferenceDescriptor differenceDescriptor, final Function<String, String> propertyDecryptor,
                                  final Function<VersionedComponent, String> idLookup, final FlowComparatorVersionedStrategy flowComparatorVersionedStrategy) {
        this.flowA = flowA;
        this.flowB = flowB;
        this.externallyAccessibleServiceIds = externallyAccessibleServiceIds;
        this.differenceDescriptor = differenceDescriptor;
        this.propertyDecryptor = propertyDecryptor;
        this.idLookup = idLookup;
        this.flowComparatorVersionedStrategy = flowComparatorVersionedStrategy;
    }

    @Override
    public FlowComparison compare() {
        final VersionedProcessGroup groupA = flowA.getContents();
        final VersionedProcessGroup groupB = flowB.getContents();
        final Set<FlowDifference> differences = compare(groupA, groupB);

        differences.addAll(compareComponents(flowA.getControllerLevelServices(), flowB.getControllerLevelServices(), this::compare));
        differences.addAll(compareComponents(flowA.getReportingTasks(), flowB.getReportingTasks(), this::compare));
        differences.addAll(compareComponents(flowA.getFlowAnalysisRules(), flowB.getFlowAnalysisRules(), this::compare));
        differences.addAll(compareComponents(flowA.getParameterProviders(), flowB.getParameterProviders(), this::compare));
        differences.addAll(compareComponents(flowA.getParameterContexts(), flowB.getParameterContexts(), this::compare));
        differences.addAll(compareComponents(flowA.getFlowRegistryClients(), flowB.getFlowRegistryClients(), this::compare));

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

            if (flowComparatorVersionedStrategy == FlowComparatorVersionedStrategy.DEEP
                    && componentB instanceof VersionedProcessGroup groupB) {
                // we want to also add the differences of the added sub process groups
                extractPGConfigDifferences(null, groupB, differences);
                extractPGComponentsDifferences(null, groupB, differences);
            }

            return true;
        }

        if (componentB == null) {
            differences.add(difference(DifferenceType.COMPONENT_REMOVED, componentA, componentB, componentA, componentB));

            if (flowComparatorVersionedStrategy == FlowComparatorVersionedStrategy.DEEP
                    && componentA instanceof VersionedProcessGroup groupA) {
                // we want to also add the differences of the removed sub process groups
                extractPGComponentsDifferences(groupA, null, differences);
            }

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
        addIfDifferent(differences, DifferenceType.RETRY_COUNT_CHANGED, processorA, processorB, VersionedProcessor::getRetryCount);
        addIfDifferent(differences, DifferenceType.RETRIED_RELATIONSHIPS_CHANGED, processorA, processorB, VersionedProcessor::getRetriedRelationships);
        addIfDifferent(differences, DifferenceType.BACKOFF_MECHANISM_CHANGED, processorA, processorB, VersionedProcessor::getBackoffMechanism);
        addIfDifferent(differences, DifferenceType.MAX_BACKOFF_PERIOD_CHANGED, processorA, processorB, VersionedProcessor::getMaxBackoffPeriod);
        compareProperties(processorA, processorB, processorA.getProperties(), processorB.getProperties(), processorA.getPropertyDescriptors(), processorB.getPropertyDescriptors(), differences);
    }

    private void compare(final VersionedReportingTask taskA, final VersionedReportingTask taskB, final Set<FlowDifference> differences) {
        if (compareComponents(taskA, taskB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.ANNOTATION_DATA_CHANGED, taskA, taskB, VersionedReportingTask::getAnnotationData);
        addIfDifferent(differences, DifferenceType.BUNDLE_CHANGED, taskA, taskB, VersionedReportingTask::getBundle);
        addIfDifferent(differences, DifferenceType.RUN_SCHEDULE_CHANGED, taskA, taskB, VersionedReportingTask::getSchedulingPeriod);
        addIfDifferent(differences, DifferenceType.SCHEDULING_STRATEGY_CHANGED, taskA, taskB, VersionedReportingTask::getSchedulingStrategy);
        addIfDifferent(differences, DifferenceType.SCHEDULED_STATE_CHANGED, taskA, taskB, VersionedReportingTask::getScheduledState);
        compareProperties(taskA, taskB, taskA.getProperties(), taskB.getProperties(), taskA.getPropertyDescriptors(), taskB.getPropertyDescriptors(), differences);
    }

    private void compare(final VersionedFlowAnalysisRule ruleA, final VersionedFlowAnalysisRule ruleB, final Set<FlowDifference> differences) {
        if (compareComponents(ruleA, ruleB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.BUNDLE_CHANGED, ruleA, ruleB, VersionedFlowAnalysisRule::getBundle);
        addIfDifferent(differences, DifferenceType.ENFORCEMENT_POLICY_CHANGED, ruleA, ruleB, VersionedFlowAnalysisRule::getEnforcementPolicy);
        addIfDifferent(differences, DifferenceType.SCHEDULED_STATE_CHANGED, ruleA, ruleB, VersionedFlowAnalysisRule::getScheduledState);
        compareProperties(ruleA, ruleB, ruleA.getProperties(), ruleB.getProperties(), ruleA.getPropertyDescriptors(), ruleB.getPropertyDescriptors(), differences);
    }

    private void compare(final VersionedParameterProvider parameterProviderA, final VersionedParameterProvider parameterProviderB, final Set<FlowDifference> differences) {
        if (compareComponents(parameterProviderA, parameterProviderB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.ANNOTATION_DATA_CHANGED, parameterProviderA, parameterProviderB, VersionedParameterProvider::getAnnotationData);
        addIfDifferent(differences, DifferenceType.BUNDLE_CHANGED, parameterProviderA, parameterProviderB, VersionedParameterProvider::getBundle);
        compareProperties(parameterProviderA, parameterProviderB, parameterProviderA.getProperties(), parameterProviderB.getProperties(),
                parameterProviderA.getPropertyDescriptors(), parameterProviderB.getPropertyDescriptors(), differences);
    }

    void compare(final VersionedParameterContext contextA, final VersionedParameterContext contextB, final Set<FlowDifference> differences) {
        if (compareComponents(contextA, contextB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.DESCRIPTION_CHANGED, contextA, contextB, VersionedParameterContext::getDescription);
        addIfDifferent(differences, DifferenceType.INHERITED_CONTEXTS_CHANGED, contextA, contextB, VersionedParameterContext::getInheritedParameterContexts);
        addIfDifferent(differences, DifferenceType.PARAMETER_GROUP_NAME_CHANGED, contextA, contextB, VersionedParameterContext::getParameterGroupName);
        addIfDifferent(differences, DifferenceType.PARAMETER_PROVIDER_SYNCHRONIZED_CHANGED, contextA, contextB, VersionedParameterContext::isSynchronized);

        final Map<String, VersionedParameter> contextAParameters = parametersByName(contextA.getParameters());
        final Map<String, VersionedParameter> contextBParameters = parametersByName(contextB.getParameters());

        for (final VersionedParameter parameterA : contextA.getParameters() ) {
            final String name = parameterA.getName();

            final VersionedParameter parameterB = contextBParameters.get(parameterA.getName());
            if (parameterB == null) {
                differences.add(difference(DifferenceType.PARAMETER_REMOVED, contextA, contextB, name, name, null, parameterA.isSensitive() ? "<Sensitive Value>" : parameterA.getValue()));
                continue;
            }

            final String decryptedValueA = decryptValue(parameterA);
            final String decryptedValueB = decryptValue(parameterB);
            if (!Objects.equals(decryptedValueA, decryptedValueB)) {
                final String valueA = parameterA.isSensitive() ? "<Sensitive Value A>" : parameterA.getValue();
                final String valueB = parameterB.isSensitive() ? "<Sensitive Value B>" : parameterB.getValue();
                final String description = differenceDescriptor.describeDifference(DifferenceType.PARAMETER_VALUE_CHANGED, flowA.getName(), flowB.getName(), contextA, contextB, name, valueA, valueB);
                differences.add(new StandardFlowDifference(DifferenceType.PARAMETER_VALUE_CHANGED, contextA, contextB, name, valueA, valueB, description));
            }

            final Set<String> assetIdsA = Stream.ofNullable(parameterA.getReferencedAssets())
                    .flatMap(Collection::stream)
                    .map(VersionedAsset::getIdentifier)
                    .collect(Collectors.toSet());
            final Set<String> assetIdsB = Stream.ofNullable(parameterB.getReferencedAssets())
                    .flatMap(Collection::stream)
                    .map(VersionedAsset::getIdentifier)
                    .collect(Collectors.toSet());
            if (!assetIdsA.equals(assetIdsB)) {
                final List<VersionedAsset> valueA = parameterA.getReferencedAssets();
                final List<VersionedAsset> valueB = parameterB.getReferencedAssets();
                final String description = differenceDescriptor.describeDifference(DifferenceType.PARAMETER_ASSET_REFERENCES_CHANGED,
                        flowA.getName(), flowB.getName(), contextA, contextB, name, valueA, valueB);
                differences.add(new StandardFlowDifference(DifferenceType.PARAMETER_ASSET_REFERENCES_CHANGED, contextA, contextB, name, valueA, valueB, description));
            }

            if (!Objects.equals(parameterA.getDescription(), parameterB.getDescription())) {
                final String valueA = parameterA.getDescription();
                final String valueB = parameterB.getDescription();

                final String description = differenceDescriptor.describeDifference(DifferenceType.PARAMETER_DESCRIPTION_CHANGED,
                    flowA.getName(), flowB.getName(), contextA, contextB, name, valueA, valueB);
                differences.add(new StandardFlowDifference(DifferenceType.PARAMETER_DESCRIPTION_CHANGED, contextA, contextB, name, valueA, valueB, description));
            }
        }

        // Check for any parameters that exist in Context B but not Context A.
        for (final VersionedParameter parameter : contextB.getParameters()) {
            final String name = parameter.getName();
            if (contextAParameters.containsKey(name)) {
                continue;
            }

            differences.add(difference(DifferenceType.PARAMETER_ADDED, contextA, contextB, name, name, null, parameter.isSensitive() ? "<Sensitive Value>" : parameter.getValue()));
        }
    }

    void compare(final VersionedFlowRegistryClient clientA, final VersionedFlowRegistryClient clientB, final Set<FlowDifference> differences) {
        if (compareComponents(clientA, clientB, differences)) {
            return;
        }

        addIfDifferent(differences, DifferenceType.DESCRIPTION_CHANGED, clientA, clientB, VersionedFlowRegistryClient::getDescription);
        addIfDifferent(differences, DifferenceType.ANNOTATION_DATA_CHANGED, clientA, clientB, VersionedFlowRegistryClient::getAnnotationData);
        addIfDifferent(differences, DifferenceType.BUNDLE_CHANGED, clientA, clientB, VersionedFlowRegistryClient::getBundle);

        compareProperties(clientA, clientB, nullToEmpty(clientA.getProperties()), nullToEmpty(clientB.getProperties()),
            nullToEmpty(clientA.getPropertyDescriptors()), nullToEmpty(clientB.getPropertyDescriptors()), differences);
    }

    private <K, V> Map<K, V> nullToEmpty(final Map<K, V> map) {
        return map == null ? Collections.emptyMap() : map;
    }

    private Map<String, VersionedParameter> parametersByName(final Collection<VersionedParameter> parameters) {
        return parameters.stream().collect(Collectors.toMap(VersionedParameter::getName, Function.identity()));
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
        addIfDifferent(differences, DifferenceType.SCHEDULED_STATE_CHANGED, serviceA, serviceB, VersionedControllerService::getScheduledState);
        addIfDifferent(differences, DifferenceType.BULLETIN_LEVEL_CHANGED, serviceA, serviceB, VersionedControllerService::getBulletinLevel, true, "WARN");
    }

    private String decrypt(final String value, final VersionedPropertyDescriptor descriptor) {
        if (value == null) {
            return null;
        }

        final boolean sensitive = (descriptor == null || descriptor.isSensitive()) && value.startsWith(ENCRYPTED_VALUE_PREFIX) && value.endsWith(ENCRYPTED_VALUE_SUFFIX);
        if (!sensitive) {
            return value;
        }

        return propertyDecryptor.apply(value.substring(ENCRYPTED_VALUE_PREFIX.length(), value.length() - ENCRYPTED_VALUE_SUFFIX.length()));
    }

    private String decryptValue(final VersionedParameter parameter) {
        final String rawValue = parameter.getValue();
        if (rawValue == null) {
            return null;
        }

        final boolean sensitive = parameter.isSensitive() && rawValue.startsWith(ENCRYPTED_VALUE_PREFIX) && rawValue.endsWith(ENCRYPTED_VALUE_SUFFIX);
        if (!sensitive) {
            logger.debug("Will not decrypt value for parameter {} because it is not encrypted", parameter.getName());
            return rawValue;
        }

        return propertyDecryptor.apply(rawValue.substring(ENCRYPTED_VALUE_PREFIX.length(), rawValue.length() - ENCRYPTED_VALUE_SUFFIX.length()));
    }

    private void compareProperties(final VersionedComponent componentA, final VersionedComponent componentB,
        final Map<String, String> propertiesA, final Map<String, String> propertiesB,
        final Map<String, VersionedPropertyDescriptor> descriptorsA, final Map<String, VersionedPropertyDescriptor> descriptorsB,
        final Set<FlowDifference> differences) {

        propertiesA.forEach((key, rawValueA) -> {
            final String rawValueB = propertiesB.get(key);
            final String valueB = decrypt(rawValueB, descriptorsB.get(key));
            final String valueA = decrypt(rawValueA, descriptorsA.get(key));

            final VersionedPropertyDescriptor descriptorA = descriptorsA.get(key);
            final VersionedPropertyDescriptor descriptorB = descriptorsB.get(key);

            VersionedPropertyDescriptor descriptor = descriptorA;
            if (descriptor == null) {
                descriptor = descriptorB;
            }

            final String displayName;
            if (descriptor == null) {
                displayName = key;
            } else {
                displayName = descriptor.getDisplayName() == null ? descriptor.getName() : descriptor.getDisplayName();
            }

            if (descriptorA != null && descriptorB != null) {
                if (descriptorA.isSensitive() != descriptorB.isSensitive()) {
                    differences.add(difference(DifferenceType.PROPERTY_SENSITIVITY_CHANGED, componentA, componentB, key, displayName, descriptorA.isSensitive(), descriptorB.isSensitive()));
                }
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
        addIfDifferent(differences, DifferenceType.SIZE_CHANGED, labelA, labelB, VersionedLabel::getHeight);
        addIfDifferent(differences, DifferenceType.SIZE_CHANGED, labelA, labelB, VersionedLabel::getWidth);
        addIfDifferent(differences, DifferenceType.STYLE_CHANGED, labelA, labelB, VersionedLabel::getStyle);
        addIfDifferent(differences, DifferenceType.ZINDEX_CHANGED, labelA, labelB, VersionedLabel::getzIndex);
    }

    private void compare(final VersionedPort portA, final VersionedPort portB, final Set<FlowDifference> differences) {
        if (compareComponents(portA, portB, differences)) {
            return;
        }

        if (portA != null && portA.isAllowRemoteAccess() && portB != null && portB.isAllowRemoteAccess()) {
            addIfDifferent(differences, DifferenceType.CONCURRENT_TASKS_CHANGED, portA, portB, VersionedPort::getConcurrentlySchedulableTaskCount);
        }

        addIfDifferent(differences, DifferenceType.SCHEDULED_STATE_CHANGED, portA, portB, VersionedPort::getScheduledState);
        addIfDifferent(differences, DifferenceType.PORT_FUNCTION_CHANGED, portA, portB, VersionedPort::getPortFunction, true, PortFunction.STANDARD);
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
        addIfDifferent(differences, DifferenceType.RPG_URL_CHANGED, rpgA, rpgB, VersionedRemoteProcessGroup::getTargetUris);

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
        addIfDifferent(differences, DifferenceType.SCHEDULED_STATE_CHANGED, portA, portB, VersionedRemoteGroupPort::getScheduledState);
    }


    private void compare(final VersionedProcessGroup groupA, final VersionedProcessGroup groupB, final Set<FlowDifference> differences, final boolean compareNamePos) {
        if (compareComponents(groupA, groupB, differences, compareNamePos, compareNamePos, true)) {
            return;
        }

        // Compare Flow Coordinates for any differences. Because the way in which we store flow coordinates has changed between versions,
        // we have to use a specific method for this instead of just using addIfDifferent. We also store the differences into a different set
        // so that we can later check if there were any differences or not.
        final Set<FlowDifference> flowCoordinateDifferences = new HashSet<>();
        compareFlowCoordinates(groupA, groupB, flowCoordinateDifferences);
        differences.addAll(flowCoordinateDifferences);

        extractPGConfigDifferences(groupA, groupB, differences);

        final VersionedFlowCoordinates groupACoordinates = groupA.getVersionedFlowCoordinates();
        final VersionedFlowCoordinates groupBCoordinates = groupB.getVersionedFlowCoordinates();

        // We will compare group contents if either:
        // - both versions say the group is not under version control
        // OR
        // - both versions say the group IS under version control but disagree about the coordinates
        // OR
        // - explicitly requested comparison for embedded versioned groups
        final boolean shouldCompareVersioned = flowCoordinateDifferences.stream()
            .anyMatch(diff -> !diff.getFieldName().isPresent() || !diff.getFieldName().get().equals(FLOW_VERSION)) || flowComparatorVersionedStrategy == FlowComparatorVersionedStrategy.DEEP;
        final boolean compareGroupContents = (groupACoordinates == null && groupBCoordinates == null)
            || (groupACoordinates != null && groupBCoordinates != null && shouldCompareVersioned);


        if (compareGroupContents) {
            extractPGComponentsDifferences(groupA, groupB, differences);
        }
    }

    private void extractPGConfigDifferences(final VersionedProcessGroup groupA, final VersionedProcessGroup groupB, final Set<FlowDifference> differences) {
        addIfDifferent(differences, DifferenceType.FLOWFILE_CONCURRENCY_CHANGED, groupA, groupB, VersionedProcessGroup::getFlowFileConcurrency,
            true, DEFAULT_FLOW_FILE_CONCURRENCY);
        addIfDifferent(differences, DifferenceType.FLOWFILE_OUTBOUND_POLICY_CHANGED, groupA, groupB, VersionedProcessGroup::getFlowFileOutboundPolicy,
            true, DEFAULT_OUTBOUND_FLOW_FILE_POLICY);

        addIfDifferent(differences, DifferenceType.DEFAULT_BACKPRESSURE_DATA_SIZE_CHANGED, groupA, groupB, VersionedProcessGroup::getDefaultBackPressureDataSizeThreshold, true, "1 GB");
        addIfDifferent(differences, DifferenceType.DEFAULT_BACKPRESSURE_OBJECT_COUNT_CHANGED, groupA, groupB, VersionedProcessGroup::getDefaultBackPressureObjectThreshold, true, 10_000L);
        addIfDifferent(differences, DifferenceType.DEFAULT_FLOWFILE_EXPIRATION_CHANGED, groupA, groupB, VersionedProcessGroup::getDefaultFlowFileExpiration, true, "0 sec");
        addIfDifferent(differences, DifferenceType.PARAMETER_CONTEXT_CHANGED, groupA, groupB, VersionedProcessGroup::getParameterContextName, true, null);
        addIfDifferent(differences, DifferenceType.LOG_FILE_SUFFIX_CHANGED, groupA, groupB, VersionedProcessGroup::getLogFileSuffix, true, null);
        addIfDifferent(differences, DifferenceType.EXECUTION_ENGINE_CHANGED, groupA, groupB, VersionedProcessGroup::getExecutionEngine, true, ExecutionEngine.INHERITED);
        addIfDifferent(differences, DifferenceType.SCHEDULED_STATE_CHANGED, groupA, groupB, VersionedProcessGroup::getScheduledState, true, org.apache.nifi.flow.ScheduledState.ENABLED);
        addIfDifferent(differences, DifferenceType.CONCURRENT_TASKS_CHANGED, groupA, groupB, VersionedProcessGroup::getMaxConcurrentTasks, true, 1);
        addIfDifferent(differences, DifferenceType.TIMEOUT_CHANGED, groupA, groupB, VersionedProcessGroup::getStatelessFlowTimeout, false, "1 min");
    }

    private void extractPGComponentsDifferences(final VersionedProcessGroup groupA, final VersionedProcessGroup groupB, final Set<FlowDifference> differences) {
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getConnections(),
                groupB == null ? Set.of() : groupB.getConnections(),
                this::compare));
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getProcessors(),
                groupB == null ? Set.of() : groupB.getProcessors(),
                this::compare));
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getControllerServices(),
                groupB == null ? Set.of() : groupB.getControllerServices(),
                this::compare));
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getFunnels(),
                groupB == null ? Set.of() : groupB.getFunnels(),
                this::compare));
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getInputPorts(),
                groupB == null ? Set.of() : groupB.getInputPorts(),
                this::compare));
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getLabels(),
                groupB == null ? Set.of() : groupB.getLabels(),
                this::compare));
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getOutputPorts(),
                groupB == null ? Set.of() : groupB.getOutputPorts(),
                this::compare));
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getProcessGroups(),
                groupB == null ? Set.of() : groupB.getProcessGroups(),
                (a, b, diffs) -> compare(a, b, diffs, true)));
        differences.addAll(compareComponents(groupA == null ? Set.of() : groupA.getRemoteProcessGroups(),
                groupB == null ? Set.of() : groupB.getRemoteProcessGroups(),
                this::compare));
    }


    private void compareFlowCoordinates(final VersionedProcessGroup groupA, final VersionedProcessGroup groupB, final Set<FlowDifference> differences) {
        final VersionedFlowCoordinates coordinatesA = groupA.getVersionedFlowCoordinates();
        final VersionedFlowCoordinates coordinatesB = groupB.getVersionedFlowCoordinates();

        if (coordinatesA == null && coordinatesB == null) {
            return;
        }

        // If only one is specified, use the usual method for comparing values.
        if (coordinatesA == null || coordinatesB == null) {
            addIfDifferent(differences, DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED, groupA, groupB, VersionedProcessGroup::getVersionedFlowCoordinates);
            return;
        }

        if (!Objects.equals(coordinatesA.getBucketId(), coordinatesB.getBucketId())) {
            differences.add(difference(DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED, groupA, groupB, coordinatesA, coordinatesB));
            return;
        }
        if (!Objects.equals(coordinatesA.getFlowId(), coordinatesB.getFlowId())) {
            differences.add(difference(DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED, groupA, groupB, coordinatesA, coordinatesB));
            return;
        }

        if (!Objects.equals(coordinatesA.getVersion(), coordinatesB.getVersion())) {
            differences.add(difference(DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED, groupA, groupB, FLOW_VERSION, FLOW_VERSION, coordinatesA, coordinatesB));
            return;
        }

        // If Storage Location is specified for both coordinates, compare them
        final String storageLocationA = coordinatesA.getStorageLocation();
        final String storageLocationB = coordinatesB.getStorageLocation();
        if (storageLocationA != null && storageLocationB != null && !storageLocationA.equals(storageLocationB)) {
            differences.add(difference(DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED, groupA, groupB, coordinatesA, coordinatesB));
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
        addIfDifferent(differences, DifferenceType.ZINDEX_CHANGED, connectionA, connectionB, VersionedConnection::getzIndex);

        addIfDifferent(differences, DifferenceType.LOAD_BALANCE_STRATEGY_CHANGED, connectionA, connectionB,
                conn -> conn.getLoadBalanceStrategy() == null ? DEFAULT_LOAD_BALANCE_STRATEGY : conn.getLoadBalanceStrategy());

        addIfDifferent(differences, DifferenceType.PARTITIONING_ATTRIBUTE_CHANGED, connectionA, connectionB,
                conn -> conn.getPartitioningAttribute() == null ? DEFAULT_PARTITIONING_ATTRIBUTE : conn.getPartitioningAttribute());

        addIfDifferent(differences, DifferenceType.LOAD_BALANCE_COMPRESSION_CHANGED, connectionA, connectionB,
            conn -> conn.getLoadBalanceCompression() == null ? DEFAULT_LOAD_BALANCE_COMPRESSION : conn.getLoadBalanceCompression());
    }


    private <T extends VersionedComponent> Map<String, T> byId(final Set<T> components) {
        return components.stream().collect(Collectors.toMap(idLookup::apply, Function.identity()));
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

        Object valueA = componentA == null ? null : transform.apply(componentA);
        if (valueA == null) {
            valueA = defaultValue;
        }

        Object valueB = componentB == null ? null : transform.apply(componentB);
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


    private interface ComponentComparator<T extends VersionedComponent> {
        void compare(T componentA, T componentB, Set<FlowDifference> differences);
    }
}
