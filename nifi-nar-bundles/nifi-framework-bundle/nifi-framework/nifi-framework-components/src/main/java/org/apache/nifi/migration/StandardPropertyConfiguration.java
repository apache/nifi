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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class StandardPropertyConfiguration implements PropertyConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(StandardPropertyConfiguration.class);

    private final Map<String, String> properties;
    private final String componentDescription;
    private boolean modified = false;

    public StandardPropertyConfiguration(final Map<String, String> configuration, final String componentDescription) {
        // Create a copy of the configuration so that it can be manipulated. Use LinkedHashMap to preserve order
        this.properties = new LinkedHashMap<>(configuration);
        this.componentDescription = componentDescription;
    }


    @Override
    public boolean renameProperty(final String propertyName, final String newName) {
        if (!properties.containsKey(propertyName)) {
            logger.debug("Will not rename property [{}] for [{}] because the property is not known", propertyName, componentDescription);
            return false;
        }

        final String propertyValue = properties.remove(propertyName);
        properties.put(newName, propertyValue);
        modified = true;
        logger.info("Renamed property [{}] to [{}] for [{}]", propertyName, newName, componentDescription);

        return true;
    }

    @Override
    public boolean removeProperty(final String propertyName) {
        if (!properties.containsKey(propertyName)) {
            logger.debug("Will not remove property [{}] from [{}] because the property is not known", propertyName, componentDescription);
            return false;
        }

        properties.remove(propertyName);
        modified = true;
        logger.info("Removed property [{}] from [{}]", propertyName, componentDescription);

        return true;
    }

    @Override
    public boolean hasProperty(final String propertyName) {
        return properties.containsKey(propertyName);
    }

    @Override
    public boolean isPropertySet(final String propertyName) {
        return properties.get(propertyName) != null;
    }

    @Override
    public void setProperty(final String propertyName, final String propertyValue) {
        final String previousValue = properties.put(propertyName, propertyValue);
        if (Objects.equals(previousValue, propertyValue)) {
            logger.debug("Will not update property [{}] for [{}] because the proposed value and the current value are the same", propertyName, componentDescription);
            return;
        }

        modified = true;
        if (previousValue == null) {
            logger.info("Updated property [{}] for [{}], which was previously unset", propertyName, componentDescription);
        } else {
            logger.info("Updated property [{}] for [{}], overwriting previous value", propertyName, componentDescription);
        }
    }

    @Override
    public Optional<String> getPropertyValue(final String propertyName) {
        return Optional.ofNullable(properties.get(propertyName));
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public boolean isModified() {
        return modified;
    }
}
