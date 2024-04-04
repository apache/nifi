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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class StandardPropertyConfiguration implements PropertyConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(StandardPropertyConfiguration.class);

    private final Map<String, String> effectiveProperties;
    private final Map<String, String> rawProperties;
    private final Function<String, String> effectiveValueResolver;
    private final String componentDescription;
    private final ControllerServiceFactory controllerServiceFactory;
    private final List<ControllerServiceCreationDetails> createdServices = new ArrayList<>();
    private boolean modified = false;

    public StandardPropertyConfiguration(final Map<String, String> effectivePropertyValues, final Map<String, String> rawPropertyValues,
                                         final Function<String, String> effectiveValueResolver, final String componentDescription, final ControllerServiceFactory controllerServiceFactory) {
        // Create a copy of the configuration so that it can be manipulated. Use LinkedHashMap to preserve order
        this.effectiveProperties = new LinkedHashMap<>(effectivePropertyValues);
        this.rawProperties = new LinkedHashMap<>(rawPropertyValues);
        this.effectiveValueResolver = effectiveValueResolver;
        this.componentDescription = componentDescription;
        this.controllerServiceFactory = controllerServiceFactory;
    }

    @Override
    public boolean renameProperty(final String propertyName, final String newName) {
        if (!effectiveProperties.containsKey(propertyName)) {
            logger.debug("Will not rename property [{}] for [{}] because the property is not known", propertyName, componentDescription);
            return false;
        }

        if (Objects.equals(propertyName, newName)) {
            logger.debug("Will not update property [{}] for [{}] because the new name and the current name are the same", propertyName, componentDescription);
            return false;
        }

        final String effectivePropertyValue = effectiveProperties.remove(propertyName);
        effectiveProperties.put(newName, effectivePropertyValue);

        final String rawPropertyValue = rawProperties.remove(propertyName);
        rawProperties.put(newName, rawPropertyValue);

        modified = true;
        logger.info("Renamed property [{}] to [{}] for [{}]", propertyName, newName, componentDescription);

        return true;
    }

    @Override
    public boolean removeProperty(final String propertyName) {
        if (!effectiveProperties.containsKey(propertyName)) {
            logger.debug("Will not remove property [{}] from [{}] because the property is not known", propertyName, componentDescription);
            return false;
        }

        effectiveProperties.remove(propertyName);
        rawProperties.remove(propertyName);
        modified = true;
        logger.info("Removed property [{}] from [{}]", propertyName, componentDescription);

        return true;
    }

    @Override
    public boolean hasProperty(final String propertyName) {
        return effectiveProperties.containsKey(propertyName);
    }

    @Override
    public boolean isPropertySet(final String propertyName) {
        // Use Effective Properties here because the value may be set to #{MY_PARAM} but if parameter MY_PARAM is not set, the property should be considered unset.
        return effectiveProperties.get(propertyName) != null;
    }

    @Override
    public void setProperty(final String propertyName, final String propertyValue) {
        final String previousValue = rawProperties.put(propertyName, propertyValue);
        if (Objects.equals(previousValue, propertyValue)) {
            logger.debug("Will not update property [{}] for [{}] because the proposed value and the current value are the same", propertyName, componentDescription);
            return;
        }

        final String effectiveValue = effectiveValueResolver.apply(propertyValue);
        effectiveProperties.put(propertyName, effectiveValue);

        modified = true;
        if (previousValue == null) {
            logger.info("Updated property [{}] for [{}], which was previously unset", propertyName, componentDescription);
        } else {
            logger.info("Updated property [{}] for [{}], overwriting previous value", propertyName, componentDescription);
        }
    }

    @Override
    public Optional<String> getPropertyValue(final String propertyName) {
        return Optional.ofNullable(effectiveProperties.get(propertyName));
    }

    @Override
    public Optional<String> getRawPropertyValue(final String propertyName) {
        return Optional.ofNullable(rawProperties.get(propertyName));
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(effectiveProperties);
    }

    @Override
    public Map<String, String> getRawProperties() {
        return Collections.unmodifiableMap(rawProperties);
    }

    @Override
    public String createControllerService(final String implementationClassName, final Map<String, String> serviceProperties) {
        final ControllerServiceCreationDetails creationDetails = controllerServiceFactory.getCreationDetails(implementationClassName, serviceProperties);
        if (creationDetails.creationState() == ControllerServiceCreationDetails.CreationState.SERVICE_TO_BE_CREATED) {
            modified = true;
            createdServices.add(creationDetails);
        }

        return creationDetails.serviceIdentifier();
    }

    public boolean isModified() {
        return modified;
    }

    public List<ControllerServiceCreationDetails> getCreatedServices() {
        return createdServices;
    }
}
