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

import org.apache.nifi.registry.flow.ComponentType;
import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedComponent;

import java.util.function.Predicate;

public class FlowDifferenceFilters {

    /**
     * Predicate that returns true if the difference is NOT a remote port being added, and false if it is.
     */
    public static Predicate<FlowDifference> FILTER_ADDED_REMOVED_REMOTE_PORTS =  (fd) -> {
        return !isAddedOrRemovedRemotePort(fd);
    };

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

    public static Predicate<FlowDifference> FILTER_IGNORABLE_VERSIONED_FLOW_COORDINATE_CHANGES = (fd) -> {
        return !isIgnorableVersionedFlowCoordinateChange(fd);
    };

    public static boolean isIgnorableVersionedFlowCoordinateChange(final FlowDifference fd) {
        if (fd.getDifferenceType() == DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED) {
            final VersionedComponent componentA = fd.getComponentA();
            final VersionedComponent componentB = fd.getComponentB();

            if (componentA != null && componentB != null
                    && componentA instanceof VersionedProcessGroup
                    && componentB instanceof VersionedProcessGroup) {

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
}
