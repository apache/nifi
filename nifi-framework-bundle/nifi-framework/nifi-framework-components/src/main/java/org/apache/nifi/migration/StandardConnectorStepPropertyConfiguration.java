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

import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Framework implementation of {@link ConnectorStepPropertyConfiguration}. Instances are created and managed by
 * {@link StandardConnectorPropertyConfiguration}; they mutate the parent's per-step map on write and lazily register
 * a new step in the parent's step map the first time a property is written into it.
 */
public class StandardConnectorStepPropertyConfiguration implements ConnectorStepPropertyConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorStepPropertyConfiguration.class);

    private final String stepName;
    private final StandardConnectorPropertyConfiguration parent;
    private final String componentDescription;

    StandardConnectorStepPropertyConfiguration(final String stepName, final StandardConnectorPropertyConfiguration parent, final String componentDescription) {
        this.stepName = stepName;
        this.parent = parent;
        this.componentDescription = componentDescription;
    }

    @Override
    public String getStepName() {
        return stepName;
    }

    @Override
    public boolean renameProperty(final String propertyName, final String newName) {
        final Map<String, ConnectorValueReference> properties = parent.getStepMap(stepName);
        if (properties == null || !properties.containsKey(propertyName)) {
            logger.debug("Will not rename property [{}] in step [{}] for [{}] because the property is not known", propertyName, stepName, componentDescription);
            return false;
        }

        if (Objects.equals(propertyName, newName)) {
            logger.debug("Will not rename property [{}] in step [{}] for [{}] because the new name matches the current name", propertyName, stepName, componentDescription);
            return false;
        }

        final ConnectorValueReference existing = properties.remove(propertyName);
        properties.put(newName, existing);
        parent.markModified(stepName);
        logger.info("Renamed property [{}] to [{}] in step [{}] for [{}]", propertyName, newName, stepName, componentDescription);
        return true;
    }

    @Override
    public boolean removeProperty(final String propertyName) {
        final Map<String, ConnectorValueReference> properties = parent.getStepMap(stepName);
        if (properties == null || !properties.containsKey(propertyName)) {
            logger.debug("Will not remove property [{}] from step [{}] for [{}] because the property is not known", propertyName, stepName, componentDescription);
            return false;
        }

        properties.remove(propertyName);
        parent.markModified(stepName);
        logger.info("Removed property [{}] from step [{}] for [{}]", propertyName, stepName, componentDescription);
        return true;
    }

    @Override
    public boolean hasProperty(final String propertyName) {
        final Map<String, ConnectorValueReference> properties = parent.getStepMap(stepName);
        return properties != null && properties.containsKey(propertyName);
    }

    @Override
    public boolean isPropertySet(final String propertyName) {
        final Map<String, ConnectorValueReference> properties = parent.getStepMap(stepName);
        if (properties == null) {
            return false;
        }
        final ConnectorValueReference reference = properties.get(propertyName);
        if (reference == null) {
            return false;
        }
        if (reference instanceof StringLiteralValue literal) {
            return literal.getValue() != null;
        }
        return true;
    }

    @Override
    public void setProperty(final String propertyName, final String propertyValue) {
        setValueReference(propertyName, new StringLiteralValue(propertyValue));
    }

    @Override
    public void setValueReference(final String propertyName, final ConnectorValueReference valueReference) {
        if (valueReference == null) {
            removeProperty(propertyName);
            return;
        }

        final Map<String, ConnectorValueReference> properties = parent.getOrCreateStepMap(stepName);
        final ConnectorValueReference previous = properties.put(propertyName, valueReference);
        if (Objects.equals(previous, valueReference)) {
            logger.debug("Will not update property [{}] in step [{}] for [{}] because the proposed value matches the current value", propertyName, stepName, componentDescription);
            return;
        }

        parent.markModified(stepName);
        if (previous == null) {
            logger.info("Updated property [{}] in step [{}] for [{}], which was previously unset", propertyName, stepName, componentDescription);
        } else {
            logger.info("Updated property [{}] in step [{}] for [{}], overwriting previous value", propertyName, stepName, componentDescription);
        }
    }

    @Override
    public Optional<String> getPropertyValue(final String propertyName) {
        final Map<String, ConnectorValueReference> properties = parent.getStepMap(stepName);
        if (properties == null) {
            return Optional.empty();
        }
        final ConnectorValueReference reference = properties.get(propertyName);
        if (reference instanceof StringLiteralValue literal) {
            return Optional.ofNullable(literal.getValue());
        }
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorValueReference> getValueReference(final String propertyName) {
        final Map<String, ConnectorValueReference> properties = parent.getStepMap(stepName);
        if (properties == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(properties.get(propertyName));
    }

    @Override
    public Map<String, String> getProperties() {
        final Map<String, ConnectorValueReference> properties = parent.getStepMap(stepName);
        if (properties == null || properties.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, String> literals = new HashMap<>();
        for (final Map.Entry<String, ConnectorValueReference> entry : properties.entrySet()) {
            if (entry.getValue() instanceof StringLiteralValue literal) {
                literals.put(entry.getKey(), literal.getValue());
            }
        }
        return Collections.unmodifiableMap(literals);
    }

    @Override
    public Map<String, ConnectorValueReference> getValueReferences() {
        final Map<String, ConnectorValueReference> properties = parent.getStepMap(stepName);
        if (properties == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(properties);
    }
}
