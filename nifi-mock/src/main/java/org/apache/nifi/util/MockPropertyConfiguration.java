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

import org.apache.nifi.migration.PropertyConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class MockPropertyConfiguration implements PropertyConfiguration {
    private final Map<String, String> propertyRenames = new HashMap<>();
    private final Set<String> propertiesRemoved = new HashSet<>();
    private final Set<String> propertiesUpdated = new HashSet<>();
    private final Map<String, String> rawProperties;
    private final Set<CreatedControllerService> createdControllerServices = new HashSet<>();


    public MockPropertyConfiguration(final Map<String, String> propertyValues) {
        this.rawProperties = new HashMap<>(propertyValues);
    }

    public PropertyMigrationResult toPropertyMigrationResult() {
        return new PropertyMigrationResult() {

            @Override
            public Set<String> getPropertiesRemoved() {
                return Collections.unmodifiableSet(propertiesRemoved);
            }

            @Override
            public Map<String, String> getPropertiesRenamed() {
                return Collections.unmodifiableMap(propertyRenames);
            }

            @Override
            public Set<CreatedControllerService> getCreatedControllerServices() {
                return Collections.unmodifiableSet(createdControllerServices);
            }

            @Override
            public Set<String> getPropertiesUpdated() {
                return Collections.unmodifiableSet(propertiesUpdated);
            }
        };
    }

    @Override
    public boolean renameProperty(final String propertyName, final String newName) {
        propertyRenames.put(propertyName, newName);

        final boolean hasProperty = hasProperty(propertyName);
        if (!hasProperty) {
            return false;
        }

        final String value = rawProperties.remove(propertyName);
        rawProperties.put(newName, value);
        return true;
    }

    @Override
    public boolean removeProperty(final String propertyName) {
        propertiesRemoved.add(propertyName);

        if (!hasProperty(propertyName)) {
            return false;
        }

        rawProperties.remove(propertyName);
        return true;
    }

    @Override
    public boolean hasProperty(final String propertyName) {
        return rawProperties.containsKey(propertyName);
    }

    @Override
    public boolean isPropertySet(final String propertyName) {
        return rawProperties.get(propertyName) != null;
    }

    @Override
    public void setProperty(final String propertyName, final String propertyValue) {
        propertiesUpdated.add(propertyName);
        rawProperties.put(propertyName, propertyValue);
    }

    @Override
    public Optional<String> getPropertyValue(final String propertyName) {
        return getRawPropertyValue(propertyName);
    }

    @Override
    public Optional<String> getRawPropertyValue(final String propertyName) {
        return Optional.ofNullable(rawProperties.get(propertyName));
    }

    @Override
    public Map<String, String> getProperties() {
        return getRawProperties();
    }

    @Override
    public Map<String, String> getRawProperties() {
        return Collections.unmodifiableMap(rawProperties);
    }

    @Override
    public String createControllerService(final String implementationClassName, final Map<String, String> serviceProperties) {
        final String serviceId = UUID.randomUUID().toString();
        createdControllerServices.add(new CreatedControllerService(serviceId, implementationClassName, serviceProperties));
        return serviceId;
    }

    public record CreatedControllerService(String id, String implementationClassName, Map<String, String> serviceProperties) {
    }
}
