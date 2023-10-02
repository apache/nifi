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

package org.apache.nifi.migration;

import org.apache.nifi.components.PropertyDescriptor;

import java.util.Map;
import java.util.Optional;

public interface PropertyConfiguration {

    /**
     * Renames an existing property, if it exists. If the property does not exist, this is a no-op
     *
     * @param propertyName the current name of the property
     * @param newName the new name for the property
     * @return <code>true</code> if the property was renamed, <code>false</code> if the property did not exist
     */
    boolean renameProperty(String propertyName, String newName);

    /**
     * Removes the property with the given name, if it exists. If the property does not exist, this is a no-op.
     * @param propertyName the name of the property to remove
     * @return <code>true</code> if the property was removed, <code>false</code> if the property did not exist
     */
    boolean removeProperty(String propertyName);

    /**
     * <p>
     *     Determines whether or not the configuration has an entry for the given property. This method will return <code>true</code>
     *     even if the value for the given property is <code>null</code>. A value of <code>false</code> will be returned only if the
     *     property name is not known to the configuration. This allows for disambiguation between a value that is configured to be
     *     <code>null</code> and a value that has not yet been configured.
     * </p>
     * <p>
     *     An idiom to determine if the property was explicitly set to <code>null</code> is as follows:
     * </p>
     * <pre><code>
     *     final boolean setToNull = configuration.hasProperty("MyProperty") && !configuration.isPropertySet("MyProperty");
     * </code></pre>
     *
     * @param propertyName the name of the property
     * @return <code>true</code> if the property name is known to this configuration, <code>false</code> otherwise.
     */
    boolean hasProperty(String propertyName);

    /**
     * <p>
     *     Determines whether or not the configuration has an entry for the given property. This method will return <code>true</code>
     *     even if the value for the given property is <code>null</code>. A value of <code>false</code> will be returned only if the
     *     property identified by the descriptor is not known to the configuration. This allows for disambiguation between a value that is configured to be
     *     <code>null</code> and a value that has not yet been configured.
     * </p>
     * <p>
     *     An idiom to determine if the property was explicitly set to <code>null</code> is as follows:
     * </p>
     * <pre><code>
     *     final boolean setToNull = configuration.hasProperty(MY_PROPERTY) && !configuration.isSet(MY_PROPERTY);
     * </code></pre>
     *
     * @param descriptor the property descriptor that identifies the property
     * @return <code>true</code> if the property name is known to this configuration, <code>false</code> otherwise.
     */
    default boolean hasProperty(PropertyDescriptor descriptor) {
        return hasProperty(descriptor.getName());
    }

    /**
     * Indicates whether or not the property with the given name has been set to a non-null value
     * @param propertyName the name of the property
     * @return <code>true</code> if the value has been set and is non-null. Returns <code>false</code> if the value is unset or is null
     */
    boolean isPropertySet(String propertyName);

    /**
     * Indicates whether or not the property identified by the given descriptor name has been set to a non-null value
     * @param descriptor the descriptor that identifies the property
     * @return <code>true</code> if the value has been set and is non-null. Returns <code>false</code> if the value is unset or is null
     */
    default boolean isPropertySet(PropertyDescriptor descriptor) {
        return isPropertySet(descriptor.getName());
    }

    /**
     * Sets the value of the property with the given name and value
     * @param propertyName the name of the property
     * @param propertyValue the value to set
     */
    void setProperty(String propertyName, String propertyValue);

    /**
     * Sets the value of the property identified by the given descriptor's name to the given value
     * @param descriptor the property descriptor that identifies the property
     * @param propertyValue the value to set
     */
    default void setProperty(PropertyDescriptor descriptor, String propertyValue) {
        setProperty(descriptor.getName(), propertyValue);
    }

    /**
     * Returns an optional value representing the value of the property with the given name
     * @param propertyName the name of the property
     * @return an empty optional if the value is null or unset, else an Optional representing the configured value
     */
    Optional<String> getPropertyValue(String propertyName);

    /**
     * Returns an optional value representing the value of the property identified by the given descriptor
     * @param descriptor the property descriptor that identifies the property
     * @return an empty optional if the value is null or unset, else an Optional representing the configured value
     */
    default Optional<String> getPropertyValue(PropertyDescriptor descriptor) {
        return getPropertyValue(descriptor.getName());
    }

    /**
     * Returns a map containing all of the configured properties
     * @return a Map containing the names and values of all configured properties
     */
    Map<String, String> getProperties();

}
