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

import org.apache.nifi.registry.flow.ScheduledState;
import org.apache.nifi.registry.flow.VersionedComponent;

/**
 * Describes differences between flows as if Flow A is an 'earlier version' of the same flow than Flow B.
 * This provides verbiage such as "Processor with ID 123 was added to flow."
 */
public class EvolvingDifferenceDescriptor implements DifferenceDescriptor {

    @Override
    public String describeDifference(final DifferenceType type, final String flowAName, final String flowBName, final VersionedComponent componentA,
        final VersionedComponent componentB, final String fieldName, final Object valueA, final Object valueB) {

        final String description;
        switch (type) {
            case COMPONENT_ADDED:
                description = String.format("%s with ID %s was added to flow", componentB.getComponentType().getTypeName(), componentB.getIdentifier());
                break;
            case COMPONENT_REMOVED:
                description = String.format("%s with ID %s was removed from flow", componentA.getComponentType().getTypeName(), componentA.getIdentifier());
                break;
            case SCHEDULED_STATE_CHANGED:
                if (ScheduledState.DISABLED.equals(valueA)) {
                    description = String.format("%s was enabled", componentA.getComponentType().getTypeName());
                } else {
                    description = String.format("%s was disabled", componentA.getComponentType().getTypeName());
                }
                break;
            case PROPERTY_ADDED:
                description = String.format("Property '%s' was added to %s with ID %s", fieldName, componentB.getComponentType().getTypeName(), componentB.getIdentifier());
                break;
            case PROPERTY_REMOVED:
                description = String.format("Property '%s' was removed from %s with ID %s", fieldName, componentA.getComponentType().getTypeName(), componentA.getIdentifier());
                break;
            case PROPERTY_PARAMETERIZED:
                description = String.format("Property '%s' was parameterized", fieldName);
                break;
            case PROPERTY_PARAMETERIZATION_REMOVED:
                description = String.format("Property '%s' is no longer a parameter reference", fieldName);
                break;
            case VARIABLE_ADDED:
                description = String.format("Variable '%s' was added to Process Group with ID %s", fieldName, componentB.getIdentifier());
                break;
            case VARIABLE_REMOVED:
                description = String.format("Variable '%s' was removed from Process Group with ID %s", fieldName, componentA.getIdentifier());
                break;
            default:
                description = String.format("%s for %s with ID %s from '%s' to '%s'",
                    type.getDescription(), componentA.getComponentType().getTypeName(), componentA.getIdentifier(), valueA, valueB);
                break;
        }

        return description;
    }

}
