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

import java.util.Objects;

import org.apache.nifi.registry.flow.ScheduledState;
import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;

/**
 * Describes differences between flows as if Flow A is an 'earlier version' of the same flow than Flow B.
 * This provides verbiage such as "Processor with ID 123 was added to flow."
 */
public class ConciseEvolvingDifferenceDescriptor implements DifferenceDescriptor {

    @Override
    public String describeDifference(final DifferenceType type, final String flowAName, final String flowBName, final VersionedComponent componentA,
        final VersionedComponent componentB, final String fieldName, final Object valueA, final Object valueB) {

        final String description;
        switch (type) {
            case COMPONENT_ADDED:
                description = String.format("%s was added", componentB.getComponentType().getTypeName());
                break;
            case COMPONENT_REMOVED:
                description = String.format("%s was removed", componentA.getComponentType().getTypeName());
                break;
            case SCHEDULED_STATE_CHANGED:
                if (ScheduledState.DISABLED.equals(valueA)) {
                    description = String.format("%s was enabled", componentA.getComponentType().getTypeName());
                } else {
                    description = String.format("%s was disabled", componentA.getComponentType().getTypeName());
                }
                break;
            case PROPERTY_ADDED:
                description = String.format("Property '%s' was added", fieldName);
                break;
            case PROPERTY_REMOVED:
                description = String.format("Property '%s' was removed", fieldName);
                break;
            case PROPERTY_PARAMETERIZED:
                description = String.format("Property '%s' was parameterized", fieldName);
                break;
            case PROPERTY_PARAMETERIZATION_REMOVED:
                description = String.format("Property '%s' is no longer a parameter reference", fieldName);
                break;
            case VARIABLE_ADDED:
                description = String.format("Variable '%s' was added", fieldName);
                break;
            case VARIABLE_REMOVED:
                description = String.format("Variable '%s' was removed", fieldName);
                break;
            case POSITION_CHANGED:
                description = "Position was changed";
                break;
            case BENDPOINTS_CHANGED:
                description = "Connection Bendpoints changed";
                break;
            case VERSIONED_FLOW_COORDINATES_CHANGED:
                if (valueA instanceof VersionedFlowCoordinates && valueB instanceof VersionedFlowCoordinates) {
                    final VersionedFlowCoordinates coordinatesA = (VersionedFlowCoordinates) valueA;
                    final VersionedFlowCoordinates coordinatesB = (VersionedFlowCoordinates) valueB;

                    // If the two vary only by version, then use a more concise message. If anything else is different, then use a fully explanation.
                    if (Objects.equals(coordinatesA.getRegistryUrl(), coordinatesB.getRegistryUrl()) && Objects.equals(coordinatesA.getBucketId(), coordinatesB.getBucketId())
                            && Objects.equals(coordinatesA.getFlowId(), coordinatesB.getFlowId()) && coordinatesA.getVersion() != coordinatesB.getVersion()) {

                        description = String.format("Flow Version changed from %s to %s", coordinatesA.getVersion(), coordinatesB.getVersion());
                        break;
                    }
                }

                description = String.format("From '%s' to '%s'", valueA, valueB);
                break;
            default:
                description = String.format("From '%s' to '%s'", valueA, valueB);
                break;
        }

        return description;
    }

}
