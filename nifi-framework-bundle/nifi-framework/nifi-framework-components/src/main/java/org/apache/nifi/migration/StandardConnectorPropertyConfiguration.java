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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Framework implementation of {@link ConnectorPropertyConfiguration}. Holds the per-step property map for a single
 * {@link org.apache.nifi.components.connector.Connector Connector} configuration snapshot (active or working) and
 * tracks whether any modification has been made so callers can conditionally persist the migrated state.
 *
 * <p>
 *     Instances are not thread-safe; a Connector's {@code migrateProperties} invocation is expected to be
 *     single-threaded.
 * </p>
 */
public class StandardConnectorPropertyConfiguration implements ConnectorPropertyConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorPropertyConfiguration.class);

    private final Map<String, Map<String, ConnectorValueReference>> stepProperties;
    private final Set<String> modifiedStepNames = new HashSet<>();
    private final String componentDescription;
    private boolean modified = false;

    public StandardConnectorPropertyConfiguration(final Map<String, Map<String, ConnectorValueReference>> initialProperties, final String componentDescription) {
        this.componentDescription = componentDescription;
        // Preserve step insertion order because downstream framework code iterates the migrated step map to fire
        // per-step configuration callbacks; property order within a step is not observed and uses HashMap.
        this.stepProperties = new LinkedHashMap<>();
        if (initialProperties != null) {
            for (final Map.Entry<String, Map<String, ConnectorValueReference>> entry : initialProperties.entrySet()) {
                final Map<String, ConnectorValueReference> copy = new HashMap<>();
                if (entry.getValue() != null) {
                    copy.putAll(entry.getValue());
                }
                stepProperties.put(entry.getKey(), copy);
            }
        }
    }

    @Override
    public Set<String> getStepNames() {
        return Collections.unmodifiableSet(stepProperties.keySet());
    }

    @Override
    public boolean hasStep(final String stepName) {
        return stepProperties.containsKey(stepName);
    }

    @Override
    public boolean renameStep(final String oldStepName, final String newStepName) {
        if (!stepProperties.containsKey(oldStepName)) {
            logger.debug("Will not rename step [{}] for [{}] because the step is not known", oldStepName, componentDescription);
            return false;
        }
        if (Objects.equals(oldStepName, newStepName)) {
            logger.debug("Will not rename step [{}] for [{}] because the new name matches the current name", oldStepName, componentDescription);
            return false;
        }
        if (stepProperties.containsKey(newStepName)) {
            throw new IllegalStateException("Cannot rename configuration step [" + oldStepName + "] to [" + newStepName
                + "] for [" + componentDescription + "] because a step with the new name already exists");
        }

        final Map<String, ConnectorValueReference> existing = stepProperties.remove(oldStepName);
        stepProperties.put(newStepName, existing);
        markModified(oldStepName);
        markModified(newStepName);
        logger.info("Renamed configuration step [{}] to [{}] for [{}]", oldStepName, newStepName, componentDescription);
        return true;
    }

    @Override
    public boolean removeStep(final String stepName) {
        if (!stepProperties.containsKey(stepName)) {
            logger.debug("Will not remove step [{}] for [{}] because the step is not known", stepName, componentDescription);
            return false;
        }

        stepProperties.remove(stepName);
        markModified(stepName);
        logger.info("Removed configuration step [{}] for [{}]", stepName, componentDescription);
        return true;
    }

    @Override
    public ConnectorStepPropertyConfiguration forStep(final String stepName) {
        return new StandardConnectorStepPropertyConfiguration(stepName, this, componentDescription);
    }

    /**
     * @return <code>true</code> if any step or property modification has been made through this configuration
     */
    public boolean isModified() {
        return modified;
    }

    /**
     * @return the names of steps that were mutated by this configuration (including steps that were removed)
     */
    public Set<String> getModifiedStepNames() {
        return Collections.unmodifiableSet(modifiedStepNames);
    }

    /**
     * @return the authoritative post-migration per-step property map for this configuration snapshot
     */
    public Map<String, Map<String, ConnectorValueReference>> getMutatedProperties() {
        final Map<String, Map<String, ConnectorValueReference>> snapshot = new LinkedHashMap<>();
        for (final Map.Entry<String, Map<String, ConnectorValueReference>> entry : stepProperties.entrySet()) {
            snapshot.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }
        return snapshot;
    }

    /**
     * Returns the mutable per-step property map for the given step, or <code>null</code> if the step is not currently
     * registered. Used by {@link StandardConnectorStepPropertyConfiguration} for read-only lookups.
     */
    Map<String, ConnectorValueReference> getStepMap(final String stepName) {
        return stepProperties.get(stepName);
    }

    /**
     * Returns the mutable per-step property map for the given step, allocating and registering a new empty map when
     * the step is not currently registered. Used by {@link StandardConnectorStepPropertyConfiguration} for the first
     * write into a lazily created step.
     */
    Map<String, ConnectorValueReference> getOrCreateStepMap(final String stepName) {
        return stepProperties.computeIfAbsent(stepName, key -> {
            logger.info("Registered new configuration step [{}] for [{}]", stepName, componentDescription);
            return new HashMap<>();
        });
    }

    /**
     * Flags the configuration and the given step as modified. Invoked by
     * {@link StandardConnectorStepPropertyConfiguration} on every mutating operation and by the top-level step
     * operations on this class.
     */
    void markModified(final String stepName) {
        modified = true;
        modifiedStepNames.add(stepName);
    }
}
