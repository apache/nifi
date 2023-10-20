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

package org.apache.nifi.registry.flow;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.BundleDTO;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class FlowRegistryUtils {

    public static Set<ConfigurableComponent> getRestrictedComponents(final VersionedProcessGroup group, final NiFiServiceFacade serviceFacade) {
        final Set<Tuple<String, BundleCoordinate>> componentTypes = new HashSet<>();
        populateComponentTypes(group, componentTypes);
        return getRestrictedComponents(serviceFacade, componentTypes);
    }

    public static Set<ConfigurableComponent> getRestrictedComponents(final VersionedReportingTaskSnapshot reportingTaskSnapshot, final NiFiServiceFacade serviceFacade) {
        final Set<Tuple<String, BundleCoordinate>> componentTypes = new HashSet<>();
        populateComponentTypes(reportingTaskSnapshot, componentTypes);
        return getRestrictedComponents(serviceFacade, componentTypes);

    }

    private static Set<ConfigurableComponent> getRestrictedComponents(NiFiServiceFacade serviceFacade, Set<Tuple<String, BundleCoordinate>> componentTypes) {
        final Set<ConfigurableComponent> restrictedComponents = new HashSet<>();

        for (final Tuple<String, BundleCoordinate> tuple : componentTypes) {
            final ConfigurableComponent component = serviceFacade.getTempComponent(tuple.getKey(), tuple.getValue());
            if (component == null) {
                continue;
            }

            final boolean isRestricted = component.getClass().isAnnotationPresent(Restricted.class);
            if (isRestricted) {
                restrictedComponents.add(component);
            }
        }

        return restrictedComponents;
    }

    private static void populateComponentTypes(final VersionedProcessGroup group, final Set<Tuple<String, BundleCoordinate>> componentTypes) {
        group.getProcessors().stream()
            .map(versionedProc -> new Tuple<>(versionedProc.getType(), createBundleCoordinate(versionedProc.getBundle())))
            .forEach(componentTypes::add);

        group.getControllerServices().stream()
            .map(versionedSvc -> new Tuple<>(versionedSvc.getType(), createBundleCoordinate(versionedSvc.getBundle())))
            .forEach(componentTypes::add);

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            populateComponentTypes(childGroup, componentTypes);
        }
    }

    private static void populateComponentTypes(final VersionedReportingTaskSnapshot reportingTaskSnapshot, final Set<Tuple<String, BundleCoordinate>> componentTypes) {
        Optional.ofNullable(reportingTaskSnapshot.getReportingTasks()).orElse(Collections.emptyList()).stream()
                .map(versionedReportingTask -> new Tuple<>(versionedReportingTask.getType(), createBundleCoordinate(versionedReportingTask.getBundle())))
                .forEach(componentTypes::add);

        Optional.ofNullable(reportingTaskSnapshot.getControllerServices()).orElse(Collections.emptyList()).stream()
                .map(versionedSvc -> new Tuple<>(versionedSvc.getType(), createBundleCoordinate(versionedSvc.getBundle())))
                .forEach(componentTypes::add);
    }


    public static BundleCoordinate createBundleCoordinate(final Bundle bundle) {
        return new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
    }

    public static BundleDTO createBundleDto(final Bundle bundle) {
        return new BundleDTO(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
    }
}
