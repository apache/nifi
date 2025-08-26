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

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedFlowCoordinates;

import java.util.Objects;

/**
 * Describes differences between flows as if the flows are two disparate flows that are being
 * compared to one another. This provides verbiage such as "Processor with ID 123 exists in Flow A but not in Flow B."
 */
public class StaticDifferenceDescriptor implements DifferenceDescriptor {

    @Override
    public String describeDifference(final DifferenceType type, final String flowAName, final String flowBName, final VersionedComponent componentA,
        final VersionedComponent componentB, final String fieldName, final Object valueA, final Object valueB) {

        final String description;
        switch (type) {
            case COMPONENT_ADDED:
                description = String.format("%s with ID %s exists in %s but not in %s",
                    componentB.getComponentType().getTypeName(), getId(componentB), flowBName, flowAName);
                break;
            case COMPONENT_REMOVED:
                description = String.format("%s with ID %s exists in %s but not in %s",
                    componentA.getComponentType().getTypeName(), getId(componentA), flowAName, flowBName);
                break;
            case PROPERTY_ADDED:
                description = String.format("Property '%s' exists for %s with ID %s in %s but not in %s",
                    fieldName, componentB.getComponentType().getTypeName(), getId(componentB), flowBName, flowAName);
                break;
            case PROPERTY_REMOVED:
                description = String.format("Property '%s' exists for %s with ID %s in %s but not in %s",
                    fieldName, componentA.getComponentType().getTypeName(), getId(componentA), flowAName, flowBName);
                break;
            case PROPERTY_CHANGED:
                description = String.format("Property '%s' for %s with ID %s is different", fieldName, componentA.getComponentType().getTypeName(), getId(componentA));
                break;
            case PROPERTY_SENSITIVITY_CHANGED:
                description = String.format("Property '%s' for %s with ID %s has a different sensitive status", fieldName, componentA.getComponentType().getTypeName(), componentA.getIdentifier());
                break;
            case PROPERTY_PARAMETERIZED:
                description = String.format("Property '%s' is a parameter reference in %s but not in %s", fieldName, flowAName, flowBName);
                break;
            case PROPERTY_PARAMETERIZATION_REMOVED:
                description = String.format("Property '%s' is a parameter reference in %s but not in %s", fieldName, flowBName, flowAName);
                break;
            case SCHEDULED_STATE_CHANGED:
                description = String.format("%s %s has a Scheduled State of %s in %s but %s in %s",
                        componentA == null ? componentB.getComponentType().getTypeName() : componentA.getComponentType().getTypeName(),
                        componentA == null ? getId(componentB) : getId(componentA), valueA, flowAName, valueB, flowBName);
                break;
            case VERSIONED_FLOW_COORDINATES_CHANGED:
                if (valueA instanceof VersionedFlowCoordinates && valueB instanceof VersionedFlowCoordinates) {
                    final VersionedFlowCoordinates coordinatesA = (VersionedFlowCoordinates) valueA;
                    final VersionedFlowCoordinates coordinatesB = (VersionedFlowCoordinates) valueB;

                    // If the two vary only by version, then use a more concise message. If anything else is different, then use a fully explanation.
                    if (Objects.equals(coordinatesA.getStorageLocation(), coordinatesB.getStorageLocation()) && Objects.equals(coordinatesA.getBucketId(), coordinatesB.getBucketId())
                            && Objects.equals(coordinatesA.getFlowId(), coordinatesB.getFlowId()) && !Objects.equals(coordinatesA.getVersion(), coordinatesB.getVersion())) {

                        description = String.format("Flow Version is %s in %s but %s in %s", coordinatesA.getVersion(), flowAName, coordinatesB.getVersion(), flowBName);
                        break;
                    }
                }

                description = String.format("%s for %s with ID %s; flow '%s' has value %s; flow '%s' has value %s",
                    type.getDescription(), componentA.getComponentType().getTypeName(), getId(componentA),
                    flowAName, valueA, flowBName, valueB);
                break;
            default:
                description = String.format("%s for %s with ID %s; flow '%s' has value %s; flow '%s' has value %s", type.getDescription(),
                        componentA == null ? componentB.getComponentType().getTypeName() : componentA.getComponentType().getTypeName(),
                        componentA == null ? getId(componentB) : getId(componentA), flowAName, valueA, flowBName, valueB);
                break;
        }

        return description;
    }

    private String getId(final VersionedComponent component) {
        if (component == null) {
            return null;
        }

        if (component.getInstanceIdentifier() == null) {
            return component.getIdentifier();
        }

        return component.getInstanceIdentifier();
    }
}
