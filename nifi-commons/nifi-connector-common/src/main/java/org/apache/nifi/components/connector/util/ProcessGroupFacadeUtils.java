/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.nifi.components.connector.components.ComponentHierarchyScope;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessorFacade;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Searches a live connector flow through {@link ProcessGroupFacade}. Recursive search matches
 * {@link org.apache.nifi.components.connector.AbstractConnector#findProcessors AbstractConnector.findProcessors}.
 * Group-path search walks immediate children by name and then inspects only the terminal group, so a processor
 * type that exists in more than one group can be selected without a recursive type match.
 */
public final class ProcessGroupFacadeUtils {

    private ProcessGroupFacadeUtils() {
    }

    /**
     * Returns every processor under {@code group}, including child groups, that matches {@code predicate}.
     *
     * @param group the process group to search
     * @param predicate the processor filter
     * @return matching processors in depth-first order, parent group before child groups
     */
    public static List<ProcessorFacade> findProcessors(final ProcessGroupFacade group, final Predicate<ProcessorFacade> predicate) {
        return findProcessors(group, predicate, ComponentHierarchyScope.INCLUDE_CHILD_GROUPS);
    }

    /**
     * Returns every processor under {@code group} that matches {@code predicate}, limited by {@code hierarchyScope}.
     *
     * @param group the process group to search
     * @param predicate the processor filter
     * @param hierarchyScope whether to recurse into child groups
     * @return matching processors in depth-first order, parent group before child groups
     */
    public static List<ProcessorFacade> findProcessors(final ProcessGroupFacade group, final Predicate<ProcessorFacade> predicate,
            final ComponentHierarchyScope hierarchyScope) {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(hierarchyScope, "hierarchyScope");

        final List<ProcessorFacade> processors = new ArrayList<>();
        collectProcessors(group, predicate, hierarchyScope, processors);
        return processors;
    }

    /**
     * Returns the matching processor when exactly one match exists under {@code group} (including child groups).
     * Returns empty when there are zero matches or more than one match.
     */
    public static Optional<ProcessorFacade> findProcessor(final ProcessGroupFacade group, final Predicate<ProcessorFacade> predicate) {
        return unique(findProcessors(group, predicate));
    }

    /**
     * Returns the matching processor when exactly one match exists under {@code group}, limited by {@code hierarchyScope}.
     * Returns empty when there are zero matches or more than one match.
     *
     * @param group the process group to search
     * @param predicate the processor filter
     * @param hierarchyScope whether to recurse into child groups
     * @return the unique matching processor, or empty
     */
    public static Optional<ProcessorFacade> findProcessor(final ProcessGroupFacade group, final Predicate<ProcessorFacade> predicate,
            final ComponentHierarchyScope hierarchyScope) {
        return unique(findProcessors(group, predicate, hierarchyScope));
    }

    /**
     * Walks immediate child groups named {@code groupNamePath} in order, then finds processors in that terminal
     * group only. An empty path searches the given group immediately. Returns an empty list when any path
     * segment is missing or is not unique among sibling groups.
     *
     * @param root the process group at which the path starts
     * @param predicate the processor filter applied to the terminal group
     * @param groupNamePath immediate child group names from {@code root} to the terminal group
     * @return matching processors in the terminal group
     */
    public static List<ProcessorFacade> findProcessorsInGroupPath(final ProcessGroupFacade root, final Predicate<ProcessorFacade> predicate,
            final String... groupNamePath) {
        final Optional<ProcessGroupFacade> group = findProcessGroupByNamePath(root, groupNamePath);
        if (group.isEmpty()) {
            return List.of();
        }

        return findProcessors(group.get(), predicate, ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY);
    }

    /**
     * Returns the matching processor when exactly one match exists in the terminal group of {@code groupNamePath}.
     * Returns empty when the path is missing or not unique, or when the terminal group does not contain exactly one match.
     *
     * @param root the process group at which the path starts
     * @param predicate the processor filter applied to the terminal group
     * @param groupNamePath immediate child group names from {@code root} to the terminal group
     * @return the unique matching processor, or empty
     */
    public static Optional<ProcessorFacade> findProcessorInGroupPath(final ProcessGroupFacade root, final Predicate<ProcessorFacade> predicate,
            final String... groupNamePath) {
        return unique(findProcessorsInGroupPath(root, predicate, groupNamePath));
    }

    /**
     * Returns every controller service under {@code group}, including child groups, that matches {@code predicate}.
     *
     * @param group the process group to search
     * @param predicate the controller service filter
     * @return matching controller services in depth-first order, parent group before child groups
     */
    public static List<ControllerServiceFacade> findControllerServices(final ProcessGroupFacade group, final Predicate<ControllerServiceFacade> predicate) {
        return findControllerServices(group, predicate, ComponentHierarchyScope.INCLUDE_CHILD_GROUPS);
    }

    /**
     * Returns every controller service under {@code group} that matches {@code predicate}, limited by {@code hierarchyScope}.
     *
     * @param group the process group to search
     * @param predicate the controller service filter
     * @param hierarchyScope whether to recurse into child groups
     * @return matching controller services in depth-first order, parent group before child groups
     */
    public static List<ControllerServiceFacade> findControllerServices(final ProcessGroupFacade group, final Predicate<ControllerServiceFacade> predicate,
            final ComponentHierarchyScope hierarchyScope) {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(hierarchyScope, "hierarchyScope");

        final List<ControllerServiceFacade> services = new ArrayList<>();
        collectControllerServices(group, predicate, hierarchyScope, services);
        return services;
    }

    /**
     * Returns the matching controller service when exactly one match exists under {@code group} (including child groups).
     * Returns empty when there are zero matches or more than one match.
     *
     * @param group the process group to search
     * @param predicate the controller service filter
     * @return the unique matching controller service, or empty
     */
    public static Optional<ControllerServiceFacade> findControllerService(final ProcessGroupFacade group, final Predicate<ControllerServiceFacade> predicate) {
        return unique(findControllerServices(group, predicate));
    }

    /**
     * Returns the matching controller service when exactly one match exists under {@code group}, limited by {@code hierarchyScope}.
     * Returns empty when there are zero matches or more than one match.
     *
     * @param group the process group to search
     * @param predicate the controller service filter
     * @param hierarchyScope whether to recurse into child groups
     * @return the unique matching controller service, or empty
     */
    public static Optional<ControllerServiceFacade> findControllerService(final ProcessGroupFacade group, final Predicate<ControllerServiceFacade> predicate,
            final ComponentHierarchyScope hierarchyScope) {
        return unique(findControllerServices(group, predicate, hierarchyScope));
    }

    /**
     * Returns every child process group under {@code group}, including nested groups, that matches {@code predicate}.
     * The given {@code group} itself is not tested.
     *
     * @param group the process group whose descendants are searched
     * @param predicate the process group filter
     * @return matching child process groups in depth-first order
     */
    public static List<ProcessGroupFacade> findProcessGroups(final ProcessGroupFacade group, final Predicate<ProcessGroupFacade> predicate) {
        return findProcessGroups(group, predicate, ComponentHierarchyScope.INCLUDE_CHILD_GROUPS);
    }

    /**
     * Returns every child process group under {@code group} that matches {@code predicate}, limited by {@code hierarchyScope}.
     * The given {@code group} itself is not tested.
     *
     * @param group the process group whose descendants are searched
     * @param predicate the process group filter
     * @param hierarchyScope whether to recurse into nested groups
     * @return matching child process groups in depth-first order
     */
    public static List<ProcessGroupFacade> findProcessGroups(final ProcessGroupFacade group, final Predicate<ProcessGroupFacade> predicate,
            final ComponentHierarchyScope hierarchyScope) {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(hierarchyScope, "hierarchyScope");

        final List<ProcessGroupFacade> processGroups = new ArrayList<>();
        collectProcessGroups(group, predicate, hierarchyScope, processGroups);
        return processGroups;
    }

    /**
     * Returns the matching child process group when exactly one match exists under {@code group} (including nested groups).
     * Returns empty when there are zero matches or more than one match. The given {@code group} itself is not tested.
     *
     * @param group the process group whose descendants are searched
     * @param predicate the process group filter
     * @return the unique matching child process group, or empty
     */
    public static Optional<ProcessGroupFacade> findProcessGroup(final ProcessGroupFacade group, final Predicate<ProcessGroupFacade> predicate) {
        return unique(findProcessGroups(group, predicate));
    }

    /**
     * Walks immediate child groups by name. An empty path returns {@code root}. Returns empty when a segment
     * is missing or is not unique among sibling groups.
     *
     * @param root the process group at which the path starts
     * @param groupNamePath immediate child group names from {@code root} to the terminal group
     * @return the terminal process group, or empty
     */
    public static Optional<ProcessGroupFacade> findProcessGroupByNamePath(final ProcessGroupFacade root, final String... groupNamePath) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(groupNamePath, "groupNamePath");

        ProcessGroupFacade current = root;
        for (final String groupName : groupNamePath) {
            Objects.requireNonNull(groupName, "groupName");
            final Optional<ProcessGroupFacade> child = unique(findImmediateProcessGroupsByName(current, groupName));
            if (child.isEmpty()) {
                return Optional.empty();
            }

            current = child.get();
        }

        return Optional.of(current);
    }

    /**
     * @param typeSuffix a suffix compared against {@link org.apache.nifi.flow.VersionedProcessor#getType()}
     * @return a predicate that matches processors whose type ends with {@code typeSuffix}
     */
    public static Predicate<ProcessorFacade> processorTypeEndsWith(final String typeSuffix) {
        Objects.requireNonNull(typeSuffix, "typeSuffix");
        return processor -> processor.getDefinition().getType().endsWith(typeSuffix);
    }

    /**
     * @param typeSuffix a suffix compared against {@link org.apache.nifi.flow.VersionedControllerService#getType()}
     * @return a predicate that matches controller services whose type ends with {@code typeSuffix}
     */
    public static Predicate<ControllerServiceFacade> controllerServiceTypeEndsWith(final String typeSuffix) {
        Objects.requireNonNull(typeSuffix, "typeSuffix");
        return service -> service.getDefinition().getType().endsWith(typeSuffix);
    }

    /**
     * @param name the process group name
     * @return a predicate that matches process groups with that name
     */
    public static Predicate<ProcessGroupFacade> processGroupNamed(final String name) {
        Objects.requireNonNull(name, "name");
        return processGroup -> name.equals(processGroup.getDefinition().getName());
    }

    private static void collectProcessors(final ProcessGroupFacade group, final Predicate<ProcessorFacade> predicate,
            final ComponentHierarchyScope hierarchyScope, final List<ProcessorFacade> found) {
        for (final ProcessorFacade processor : group.getProcessors()) {
            if (predicate.test(processor)) {
                found.add(processor);
            }
        }

        if (hierarchyScope == ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY) {
            return;
        }

        for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
            collectProcessors(childGroup, predicate, hierarchyScope, found);
        }
    }

    private static void collectControllerServices(final ProcessGroupFacade group, final Predicate<ControllerServiceFacade> predicate,
            final ComponentHierarchyScope hierarchyScope, final List<ControllerServiceFacade> found) {
        for (final ControllerServiceFacade service : group.getControllerServices()) {
            if (predicate.test(service)) {
                found.add(service);
            }
        }

        if (hierarchyScope == ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY) {
            return;
        }

        for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
            collectControllerServices(childGroup, predicate, hierarchyScope, found);
        }
    }

    private static void collectProcessGroups(final ProcessGroupFacade group, final Predicate<ProcessGroupFacade> predicate,
            final ComponentHierarchyScope hierarchyScope, final List<ProcessGroupFacade> found) {
        for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
            if (predicate.test(childGroup)) {
                found.add(childGroup);
            }

            if (hierarchyScope == ComponentHierarchyScope.INCLUDE_CHILD_GROUPS) {
                collectProcessGroups(childGroup, predicate, hierarchyScope, found);
            }
        }
    }

    private static List<ProcessGroupFacade> findImmediateProcessGroupsByName(final ProcessGroupFacade parent, final String name) {
        final List<ProcessGroupFacade> matches = new ArrayList<>();
        for (final ProcessGroupFacade childGroup : parent.getProcessGroups()) {
            if (name.equals(childGroup.getDefinition().getName())) {
                matches.add(childGroup);
            }
        }

        return matches;
    }

    private static <T> Optional<T> unique(final List<T> values) {
        if (values.size() == 1) {
            return Optional.of(values.getFirst());
        }

        return Optional.empty();
    }
}
