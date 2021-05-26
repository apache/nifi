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
import java.util.Optional;

import org.apache.nifi.registry.flow.VersionedComponent;

public class StandardFlowDifference implements FlowDifference {
    private final DifferenceType type;
    private final VersionedComponent componentA;
    private final VersionedComponent componentB;
    private final Optional<String> fieldName;
    private final Object valueA;
    private final Object valueB;
    private final String description;

    public StandardFlowDifference(final DifferenceType type, final VersionedComponent componentA, final VersionedComponent componentB, final Object valueA, final Object valueB,
            final String description) {
        this(type, componentA, componentB, null, valueA, valueB, description);
    }

    public StandardFlowDifference(final DifferenceType type, final VersionedComponent componentA, final VersionedComponent componentB, final String fieldName,
            final Object valueA, final Object valueB, final String description) {
        this.type = type;
        this.componentA = componentA;
        this.componentB = componentB;
        this.fieldName = Optional.ofNullable(fieldName);
        this.valueA = valueA;
        this.valueB = valueB;
        this.description = description;
    }

    @Override
    public DifferenceType getDifferenceType() {
        return type;
    }

    @Override
    public VersionedComponent getComponentA() {
        return componentA;
    }

    @Override
    public VersionedComponent getComponentB() {
        return componentB;
    }

    @Override
    public Optional<String> getFieldName() {
        return fieldName;
    }

    @Override
    public Object getValueA() {
        return valueA;
    }

    @Override
    public Object getValueB() {
        return valueB;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public int hashCode() {
        return 31 + 17 * (componentA == null ? 0 : componentA.getIdentifier().hashCode()) +
            17 * (componentB == null ? 0 : componentB.getIdentifier().hashCode()) +
            Objects.hash(description, type, valueA, valueB);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof StandardFlowDifference)) {
            return false;
        }
        final StandardFlowDifference other = (StandardFlowDifference) obj;
        final String componentAId = componentA == null ? null : componentA.getIdentifier();
        final String otherComponentAId = other.componentA == null ? null : other.componentA.getIdentifier();

        final String componentBId = componentB == null ? null : componentB.getIdentifier();
        final String otherComponentBId = other.componentB == null ? null : other.componentB.getIdentifier();

        return Objects.equals(componentAId, otherComponentAId) && Objects.equals(componentBId, otherComponentBId)
            && Objects.equals(description, other.description) && Objects.equals(type, other.type)
            && Objects.equals(valueA, other.valueA) && Objects.equals(valueB, other.valueB);
    }
}
