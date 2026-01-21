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

package org.apache.nifi.components.connector.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.connector.ComponentBundleLookup;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

public class VersionedFlowUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static VersionedExternalFlow loadFlowFromResource(final String resourceName) {
        try (final InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceName);
            }

            return OBJECT_MAPPER.readValue(in, VersionedExternalFlow.class);
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to load resource: " + resourceName, e);
        }
    }

    public static Optional<VersionedProcessor> findProcessor(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate) {
        final List<VersionedProcessor> processors = findProcessors(group, predicate);
        if (processors.size() == 1) {
            return Optional.of(processors.getFirst());
        }
        return Optional.empty();
    }

    public static List<VersionedProcessor> findProcessors(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate) {
        final List<VersionedProcessor> processors = new ArrayList<>();
        findProcessors(group, predicate, processors);
        return processors;
    }

    private static void findProcessors(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate, final List<VersionedProcessor> processors) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (predicate.test(processor)) {
                processors.add(processor);
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            findProcessors(childGroup, predicate, processors);
        }
    }

    public static ConnectableComponent createConnectableComponent(final VersionedProcessor processor) {
        final ConnectableComponent component = new ConnectableComponent();
        component.setId(processor.getIdentifier());
        component.setName(processor.getName());
        component.setType(ConnectableComponentType.PROCESSOR);
        component.setGroupId(processor.getGroupIdentifier());
        return component;
    }

    public static ConnectableComponent createConnectableComponent(final VersionedPort port) {
        final ConnectableComponent component = new ConnectableComponent();
        component.setId(port.getIdentifier());
        component.setName(port.getName());
        component.setType(port.getComponentType() == ComponentType.INPUT_PORT ? ConnectableComponentType.INPUT_PORT : ConnectableComponentType.OUTPUT_PORT);
        component.setGroupId(port.getGroupIdentifier());
        return component;
    }

    public static void addConnection(final VersionedProcessGroup group, final ConnectableComponent source, final ConnectableComponent destination, final Set<String> relationships) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setSelectedRelationships(relationships);
        connection.setBends(List.of());
        connection.setLabelIndex(0);
        connection.setzIndex(0L);
        connection.setGroupIdentifier(group.getIdentifier());
        connection.setLoadBalanceStrategy("DO_NOT_LOAD_BALANCE");
        connection.setBackPressureDataSizeThreshold("1 GB");
        connection.setBackPressureObjectThreshold(10000L);
        connection.setFlowFileExpiration("0 sec");
        connection.setPrioritizers(new ArrayList<>());
        connection.setComponentType(ComponentType.CONNECTION);

        Set<VersionedConnection> connections = group.getConnections();
        if (connections == null) {
            connections = new HashSet<>();
            group.setConnections(connections);
        }
        connections.add(connection);

        final String uuid = generateDeterministicUuid(group, ComponentType.CONNECTION);
        connection.setIdentifier(uuid);
    }

    public static List<VersionedConnection> findOutboundConnections(final VersionedProcessGroup group, final VersionedProcessor processor) {
        final VersionedProcessGroup processorGroup = findGroupForProcessor(group, processor);
        if (processorGroup == null) {
            return List.of();
        }

        final List<VersionedConnection> outboundConnections = new ArrayList<>();
        final Set<VersionedConnection> connections = processorGroup.getConnections();
        if (connections == null) {
            return outboundConnections;
        }

        for (final VersionedConnection connection : connections) {
            final ConnectableComponent source = connection.getSource();
            if (Objects.equals(source.getId(), processor.getIdentifier()) && source.getType() == ConnectableComponentType.PROCESSOR) {
                outboundConnections.add(connection);
            }
        }

        return outboundConnections;
    }

    public static VersionedProcessGroup findGroupForProcessor(final VersionedProcessGroup rootGroup, final VersionedProcessor processor) {
        if (rootGroup.getProcessors().contains(processor)) {
            return rootGroup;
        }

        for (final VersionedProcessGroup childGroup : rootGroup.getProcessGroups()) {
            final VersionedProcessGroup foundGroup = findGroupForProcessor(childGroup, processor);
            if (foundGroup != null) {
                return foundGroup;
            }
        }

        return null;
    }

    public static String generateDeterministicUuid(final VersionedProcessGroup group, final ComponentType componentType) {
        final int componentCount = getComponentCount(group, componentType);
        final String uuidSeed = "%s-%s-%d".formatted(group.getIdentifier(), componentType.name(), componentCount);
        return UUID.nameUUIDFromBytes(uuidSeed.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static int getComponentCount(final VersionedProcessGroup group, final ComponentType componentType) {
        final Collection<?> components = switch (componentType) {
            case PROCESSOR -> group.getProcessors();
            case INPUT_PORT -> group.getInputPorts();
            case OUTPUT_PORT -> group.getOutputPorts();
            case CONNECTION -> group.getConnections();
            case FUNNEL -> group.getFunnels();
            case LABEL -> group.getLabels();
            case PROCESS_GROUP -> group.getProcessGroups();
            case CONTROLLER_SERVICE -> group.getControllerServices();
            default -> List.of();
        };

        return components == null ? 0 : components.size();
    }

    public static VersionedProcessor addProcessor(final VersionedProcessGroup group, final String processorType, final Bundle bundle, final String name, final Position position) {
        final VersionedProcessor processor = new VersionedProcessor();

        // Generate deterministic UUID based on group and component type
        processor.setIdentifier(generateDeterministicUuid(group, ComponentType.PROCESSOR));

        processor.setName(name);
        processor.setType(processorType);
        processor.setPosition(position);
        processor.setBundle(bundle);

        // Set default processor configuration
        processor.setProperties(new HashMap<>());
        processor.setPropertyDescriptors(new HashMap<>());
        processor.setStyle(new HashMap<>());
        processor.setSchedulingPeriod("0 sec");
        processor.setSchedulingStrategy("TIMER_DRIVEN");
        processor.setExecutionNode("ALL");
        processor.setPenaltyDuration("30 sec");
        processor.setYieldDuration("1 sec");
        processor.setBulletinLevel("WARN");
        processor.setRunDurationMillis(25L);
        processor.setConcurrentlySchedulableTaskCount(1);
        processor.setAutoTerminatedRelationships(new HashSet<>());
        processor.setScheduledState(ScheduledState.ENABLED);
        processor.setRetryCount(10);
        processor.setRetriedRelationships(new HashSet<>());
        processor.setBackoffMechanism("PENALIZE_FLOWFILE");
        processor.setMaxBackoffPeriod("10 mins");
        processor.setComponentType(ComponentType.PROCESSOR);
        processor.setGroupIdentifier(group.getIdentifier());

        group.getProcessors().add(processor);
        return processor;
    }

    public static VersionedControllerService addControllerService(final VersionedProcessGroup group, final String serviceType, final Bundle bundle, final String name) {
        final VersionedControllerService controllerService = new VersionedControllerService();

        // Generate deterministic UUID based on group and component type
        controllerService.setIdentifier(generateDeterministicUuid(group, ComponentType.CONTROLLER_SERVICE));

        controllerService.setName(name);
        controllerService.setType(serviceType);
        controllerService.setBundle(bundle);
        controllerService.setComponentType(ComponentType.CONTROLLER_SERVICE);

        // Set default controller service configuration
        controllerService.setProperties(new HashMap<>());
        controllerService.setPropertyDescriptors(new HashMap<>());
        controllerService.setControllerServiceApis(new ArrayList<>());
        controllerService.setAnnotationData(null);
        controllerService.setScheduledState(ScheduledState.DISABLED);
        controllerService.setBulletinLevel("WARN");
        controllerService.setComments(null);
        controllerService.setGroupIdentifier(group.getIdentifier());

        // Initialize controller services collection if it doesn't exist
        Set<VersionedControllerService> controllerServices = group.getControllerServices();
        if (controllerServices == null) {
            controllerServices = new HashSet<>();
            group.setControllerServices(controllerServices);
        }
        controllerServices.add(controllerService);

        return controllerService;
    }

    public static Set<VersionedControllerService> getReferencedControllerServices(final VersionedProcessGroup group) {
        final Set<VersionedControllerService> referencedServices = new HashSet<>();
        collectReferencedControllerServices(group, referencedServices);
        return referencedServices;
    }

    private static void collectReferencedControllerServices(final VersionedProcessGroup group, final Set<VersionedControllerService> referencedServices) {
        final Map<String, VersionedControllerService> serviceMap = new HashMap<>();
        for (final VersionedControllerService service : group.getControllerServices()) {
            serviceMap.put(service.getIdentifier(), service);
        }

        for (final VersionedProcessor processor : group.getProcessors()) {
            for (final String propertyValue : processor.getProperties().values()) {
                final VersionedControllerService referencedService = serviceMap.get(propertyValue);
                if (referencedService != null) {
                    referencedServices.add(referencedService);
                }
            }
        }

        while (true) {
            final Set<VersionedControllerService> newlyAddedServices = new HashSet<>();

            for (final VersionedControllerService service : referencedServices) {
                for (final String propertyValue : service.getProperties().values()) {
                    final VersionedControllerService referencedService = serviceMap.get(propertyValue);
                    if (referencedService != null && !referencedServices.contains(referencedService)) {
                        newlyAddedServices.add(referencedService);
                    }
                }
            }

            referencedServices.addAll(newlyAddedServices);
            if (newlyAddedServices.isEmpty()) {
                break;
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            collectReferencedControllerServices(childGroup, referencedServices);
        }
    }

    public static void removeControllerServiceReferences(final VersionedProcessGroup processGroup, final String serviceIdentifier) {
        for (final VersionedProcessor processor : processGroup.getProcessors()) {
            removeValuesFromMap(processor.getProperties(), serviceIdentifier);
        }

        for (final VersionedControllerService service : processGroup.getControllerServices()) {
            removeValuesFromMap(service.getProperties(), serviceIdentifier);
        }

        for (final VersionedProcessGroup childGroup : processGroup.getProcessGroups()) {
            removeControllerServiceReferences(childGroup, serviceIdentifier);
        }
    }

    private static void removeValuesFromMap(final Map<String, String> properties, final String valueToRemove) {
        final List<String> keysToRemove = new ArrayList<>();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            if (Objects.equals(entry.getValue(), valueToRemove)) {
                keysToRemove.add(entry.getKey());
            }
        }

        keysToRemove.forEach(properties::remove);
    }

    public static void setParameterValue(final VersionedExternalFlow externalFlow, final String parameterName, final String parameterValue) {
        for (final VersionedParameterContext context : externalFlow.getParameterContexts().values()) {
            setParameterValue(context, parameterName, parameterValue);
        }
    }

    public static void setParameterValue(final VersionedParameterContext parameterContext, final String parameterName, final String parameterValue) {
        final Set<VersionedParameter> parameters = parameterContext.getParameters();
        for (final VersionedParameter parameter : parameters) {
            if (parameter.getName().equals(parameterName)) {
                parameter.setValue(parameterValue);
            }
        }
    }

    public static void removeUnreferencedControllerServices(final VersionedProcessGroup processGroup) {
        final Set<VersionedControllerService> referencedServices = getReferencedControllerServices(processGroup);
        final Set<String> referencedServiceIds = new HashSet<>();
        for (final VersionedControllerService service : referencedServices) {
            referencedServiceIds.add(service.getIdentifier());
        }

        removeUnreferencedControllerServices(processGroup, referencedServiceIds);
    }

    private static void removeUnreferencedControllerServices(final VersionedProcessGroup processGroup, final Set<String> referencedServiceIds) {
        processGroup.getControllerServices().removeIf(service -> !referencedServiceIds.contains(service.getIdentifier()));

        for (final VersionedProcessGroup childGroup : processGroup.getProcessGroups()) {
            removeUnreferencedControllerServices(childGroup, referencedServiceIds);
        }
    }

    /**
     * Updates all processors and controller services in the given process group (and its child groups)
     * to use the latest available bundle version. See {@link #getLatest(List)} for details on how
     * version comparison is performed.
     *
     * @param processGroup the process group containing components to update
     * @param componentBundleLookup the lookup used to find available bundles for each component type
     */
    public static void updateToLatestBundles(final VersionedProcessGroup processGroup, final ComponentBundleLookup componentBundleLookup) {
        for (final VersionedProcessor processor : processGroup.getProcessors()) {
            updateToLatestBundle(processor, componentBundleLookup);
        }

        for (final VersionedControllerService service : processGroup.getControllerServices()) {
            updateToLatestBundle(service, componentBundleLookup);
        }

        for (final VersionedProcessGroup childGroup : processGroup.getProcessGroups()) {
            updateToLatestBundles(childGroup, componentBundleLookup);
        }
    }

    /**
     * Updates the given processor to use the latest available bundle version.
     * See {@link #getLatest(List)} for details on how version comparison is performed.
     *
     * @param processor the processor to update
     * @param componentBundleLookup the lookup used to find available bundles for the processor type
     */
    public static void updateToLatestBundle(final VersionedProcessor processor, final ComponentBundleLookup componentBundleLookup) {
        final Bundle latest = getLatestBundle(processor, componentBundleLookup);
        processor.setBundle(latest);
    }

    /**
     * Updates the given controller service to use the latest available bundle version.
     * See {@link #getLatest(List)} for details on how version comparison is performed.
     *
     * @param service the controller service to update
     * @param componentBundleLookup the lookup used to find available bundles for the service type
     */
    public static void updateToLatestBundle(final VersionedControllerService service, final ComponentBundleLookup componentBundleLookup) {
        final Bundle latest = getLatestBundle(service, componentBundleLookup);
        service.setBundle(latest);
    }

    private static Bundle getLatestBundle(final VersionedConfigurableExtension versionedComponent, final ComponentBundleLookup componentBundleLookup) {
        final String type = versionedComponent.getType();
        final List<Bundle> bundles = componentBundleLookup.getAvailableBundles(type);
        final List<Bundle> includingExisting = new ArrayList<>(bundles);
        if (versionedComponent.getBundle() != null && !includingExisting.contains(versionedComponent.getBundle())) {
            includingExisting.add(versionedComponent.getBundle());
        }

        return getLatest(includingExisting);
    }

    /**
     * Returns the bundle with the latest version from the given list.
     *
     * <p>Version comparison follows these rules:</p>
     * <ol>
     *   <li>Versions are split by dots (e.g., "2.1.0" becomes ["2", "1", "0"]).
     *       This also supports calendar-date formats (e.g., "2026.01.01").</li>
     *   <li>Each part is compared numerically when possible; numeric parts are considered
     *       newer than non-numeric parts (e.g., "2.0.0" &gt; "2.0.next")</li>
     *   <li>When base versions are equal, qualifiers are compared with the following precedence
     *       (highest to lowest):
     *       <ul>
     *         <li>Release (no qualifier)</li>
     *         <li>RC (Release Candidate) - e.g., "-RC1", "-RC2"</li>
     *         <li>M (Milestone) - e.g., "-M1", "-M2"</li>
     *         <li>Other/unknown qualifiers</li>
     *         <li>SNAPSHOT</li>
     *       </ul>
     *   </li>
     *   <li>Within the same qualifier type, numeric suffixes are compared
     *       (e.g., "2.0.0-RC2" &gt; "2.0.0-RC1", "2.0.0-M4" &gt; "2.0.0-M1")</li>
     * </ol>
     *
     * <p>Examples of version ordering (highest to lowest):</p>
     * <ul>
     *   <li>2.0.0 &gt; 2.0.0-RC2 &gt; 2.0.0-RC1 &gt; 2.0.0-M4 &gt; 2.0.0-M1 &gt; 2.0.0-SNAPSHOT</li>
     *   <li>2.1.0-SNAPSHOT &gt; 2.0.0 (higher base version wins)</li>
     *   <li>2026.01.01 &gt; 2025.12.31 (calendar-date format)</li>
     * </ul>
     *
     * @param bundles the list of bundles to compare
     * @return the bundle with the latest version, or null if the list is empty
     */
    public static Bundle getLatest(final List<Bundle> bundles) {
        Bundle latest = null;
        for (final Bundle bundle : bundles) {
            if (latest == null || compareVersion(bundle.getVersion(), latest.getVersion()) > 0) {
                latest = bundle;
            }
        }

        return latest;
    }

    private static int compareVersion(final String v1, final String v2) {
        final String baseVersion1 = getBaseVersion(v1);
        final String baseVersion2 = getBaseVersion(v2);

        final String[] parts1 = baseVersion1.split("\\.");
        final String[] parts2 = baseVersion2.split("\\.");

        final int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            final String part1Str = i < parts1.length ? parts1[i] : "0";
            final String part2Str = i < parts2.length ? parts2[i] : "0";

            final int comparison = compareVersionPart(part1Str, part2Str);
            if (comparison != 0) {
                return comparison;
            }
        }

        // Base versions are equal; compare qualifiers
        final String qualifier1 = getQualifier(v1);
        final String qualifier2 = getQualifier(v2);
        return compareQualifiers(qualifier1, qualifier2);
    }

    private static int compareQualifiers(final String qualifier1, final String qualifier2) {
        final int rank1 = getQualifierRank(qualifier1);
        final int rank2 = getQualifierRank(qualifier2);

        if (rank1 != rank2) {
            return Integer.compare(rank1, rank2);
        }

        // Same qualifier type; compare numeric suffixes (e.g., RC2 > RC1, M4 > M3)
        final int num1 = getQualifierNumber(qualifier1);
        final int num2 = getQualifierNumber(qualifier2);
        return Integer.compare(num1, num2);
    }

    private static int getQualifierRank(final String qualifier) {
        if (qualifier == null || qualifier.isEmpty()) {
            return 4;
        } else if (qualifier.startsWith("RC")) {
            return 3;
        } else if (qualifier.startsWith("M")) {
            return 2;
        } else if (qualifier.equals("SNAPSHOT")) {
            return 0;
        } else {
            return 1;
        }
    }

    private static int getQualifierNumber(final String qualifier) {
        if (qualifier == null || qualifier.isEmpty()) {
            return 0;
        }

        final StringBuilder digits = new StringBuilder();
        for (int i = 0; i < qualifier.length(); i++) {
            final char c = qualifier.charAt(i);
            if (Character.isDigit(c)) {
                digits.append(c);
            }
        }

        if (digits.isEmpty()) {
            return 0;
        }

        try {
            return Integer.parseInt(digits.toString());
        } catch (final NumberFormatException e) {
            return 0;
        }
    }

    private static String getQualifier(final String version) {
        final int qualifierIndex = version.indexOf('-');
        return qualifierIndex > 0 ? version.substring(qualifierIndex + 1) : null;
    }

    private static int compareVersionPart(final String part1, final String part2) {
        final Integer num1 = parseVersionPart(part1);
        final Integer num2 = parseVersionPart(part2);

        if (num1 != null && num2 != null) {
            return Integer.compare(num1, num2);
        } else if (num1 != null) {
            return 1;
        } else if (num2 != null) {
            return -1;
        } else {
            return part1.compareTo(part2);
        }
    }

    private static Integer parseVersionPart(final String part) {
        try {
            return Integer.parseInt(part);
        } catch (final NumberFormatException e) {
            return null;
        }
    }

    private static String getBaseVersion(final String version) {
        final int qualifierIndex = version.indexOf('-');
        return qualifierIndex > 0 ? version.substring(0, qualifierIndex) : version;
    }
}
