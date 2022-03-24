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

package org.apache.nifi.components;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class PropertyDependency {
    private final String propertyName;
    private final String displayName;
    private final Set<String> dependentValues;

    /**
     * Creates a dependency that is satisfied if any value is set for the property with the given name
     * @param propertyName the name of the property that is depended upon
     * @param propertyDisplayName the display name of the property that is depended upon
     */
    public PropertyDependency(final String propertyName, final String propertyDisplayName) {
        this.propertyName = Objects.requireNonNull(propertyName);
        this.displayName = propertyDisplayName == null ? propertyName : propertyDisplayName;
        this.dependentValues = null;
    }

    /**
     * Creates a dependency that is satisfied only if the property with the given name has a value that is in the given set of dependent values
     * @param propertyName the name of the property that is depended upon
     * @param propertyDisplayName the display name of the property that is depended upon
     * @param dependentValues the values that satisfy the dependency
     */
    public PropertyDependency(final String propertyName, final String propertyDisplayName, final Set<String> dependentValues) {
        this.propertyName = Objects.requireNonNull(propertyName);
        this.displayName = propertyDisplayName == null ? propertyName : propertyDisplayName;
        this.dependentValues = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(dependentValues)));
    }

    /**
     * @return the name of the property that is depended upon
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * @return the display name of the property that is depended upon
     */
    public String getPropertyDisplayName() {
        return displayName;
    }

    /**
     * @return the Set of values that satisfy the dependency
     */
    public Set<String> getDependentValues() {
        return dependentValues;
    }

    @Override
    public String toString() {
        return "PropertyDependency[propertyName=" + propertyName + ", displayName=" + displayName + ", dependentValues=" + dependentValues + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final PropertyDependency that = (PropertyDependency) o;
        return Objects.equals(getPropertyName(), that.getPropertyName())
                && Objects.equals(getDependentValues(), that.getDependentValues());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPropertyName(), getDependentValues());
    }
}
