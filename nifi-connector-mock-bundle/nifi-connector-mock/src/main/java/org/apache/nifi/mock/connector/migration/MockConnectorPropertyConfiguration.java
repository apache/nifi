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

package org.apache.nifi.mock.connector.migration;

import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.migration.ConnectorPropertyConfiguration;
import org.apache.nifi.migration.ConnectorStepPropertyConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Test-only {@link ConnectorPropertyConfiguration} implementation for exercising a Connector's
 * {@link org.apache.nifi.components.connector.Connector#migrateProperties(ConnectorPropertyConfiguration) migrateProperties}
 * method in unit tests. Tracks per-step property renames, removals, and additions, plus step-level renames,
 * removals, and additions. Callers can drive the mock with either a plain string-literal shorthand map or a
 * fully typed {@link ConnectorValueReference} map and then inspect the outcome via
 * {@link #toMigrationResult()}, {@link #getStepNames()}, and {@link #forStep(String)}.
 */
public class MockConnectorPropertyConfiguration implements ConnectorPropertyConfiguration {

    private final Map<String, Map<String, ConnectorValueReference>> stepProperties = new HashMap<>();
    private final Set<String> initialStepNames;

    private final Map<String, Map<String, String>> propertiesRenamed = new HashMap<>();
    private final Map<String, Set<String>> propertiesRemoved = new HashMap<>();
    private final Map<String, Set<String>> propertiesUpdated = new HashMap<>();

    private final Map<String, String> renamedSteps = new HashMap<>();
    private final Set<String> removedSteps = new HashSet<>();
    private final Set<String> addedSteps = new HashSet<>();

    private MockConnectorPropertyConfiguration(final Map<String, ? extends Map<String, ConnectorValueReference>> initialProperties) {
        if (initialProperties != null) {
            for (final Map.Entry<String, ? extends Map<String, ConnectorValueReference>> entry : initialProperties.entrySet()) {
                final Map<String, ConnectorValueReference> copy = new HashMap<>();
                if (entry.getValue() != null) {
                    copy.putAll(entry.getValue());
                }
                stepProperties.put(entry.getKey(), copy);
            }
        }
        this.initialStepNames = Collections.unmodifiableSet(new HashSet<>(stepProperties.keySet()));
    }

    /**
     * Creates a {@link MockConnectorPropertyConfiguration} pre-populated with typed {@link ConnectorValueReference}
     * values for each step.
     *
     * @param initialProperties per-step property maps of {@link ConnectorValueReference} values
     * @return a new configuration
     */
    public static MockConnectorPropertyConfiguration fromValueReferences(final Map<String, ? extends Map<String, ConnectorValueReference>> initialProperties) {
        return new MockConnectorPropertyConfiguration(initialProperties);
    }

    /**
     * Creates a {@link MockConnectorPropertyConfiguration} pre-populated with plain string-literal values for each
     * step. Each supplied string is wrapped in a {@link StringLiteralValue}.
     *
     * @param initialProperties per-step property maps of raw string values
     * @return a new configuration
     */
    public static MockConnectorPropertyConfiguration fromStringLiterals(final Map<String, ? extends Map<String, String>> initialProperties) {
        final Map<String, Map<String, ConnectorValueReference>> converted = new HashMap<>();
        if (initialProperties != null) {
            for (final Map.Entry<String, ? extends Map<String, String>> entry : initialProperties.entrySet()) {
                final Map<String, ConnectorValueReference> stepMap = new HashMap<>();
                if (entry.getValue() != null) {
                    for (final Map.Entry<String, String> propertyEntry : entry.getValue().entrySet()) {
                        stepMap.put(propertyEntry.getKey(), new StringLiteralValue(propertyEntry.getValue()));
                    }
                }
                converted.put(entry.getKey(), stepMap);
            }
        }
        return new MockConnectorPropertyConfiguration(converted);
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
            return false;
        }
        if (Objects.equals(oldStepName, newStepName)) {
            return false;
        }
        if (stepProperties.containsKey(newStepName)) {
            throw new IllegalStateException("Cannot rename step [" + oldStepName + "] to [" + newStepName
                + "] because a step with the new name already exists");
        }

        final Map<String, ConnectorValueReference> existing = stepProperties.remove(oldStepName);
        stepProperties.put(newStepName, existing);
        renamedSteps.put(oldStepName, newStepName);

        final Map<String, String> renames = propertiesRenamed.remove(oldStepName);
        if (renames != null) {
            propertiesRenamed.put(newStepName, renames);
        }
        final Set<String> removed = propertiesRemoved.remove(oldStepName);
        if (removed != null) {
            propertiesRemoved.put(newStepName, removed);
        }
        final Set<String> updated = propertiesUpdated.remove(oldStepName);
        if (updated != null) {
            propertiesUpdated.put(newStepName, updated);
        }
        return true;
    }

    @Override
    public boolean removeStep(final String stepName) {
        if (!stepProperties.containsKey(stepName)) {
            return false;
        }
        stepProperties.remove(stepName);
        removedSteps.add(stepName);
        propertiesRenamed.remove(stepName);
        propertiesRemoved.remove(stepName);
        propertiesUpdated.remove(stepName);
        return true;
    }

    @Override
    public ConnectorStepPropertyConfiguration forStep(final String stepName) {
        return new MockStepPropertyConfiguration(stepName);
    }

    /**
     * @return a summary of every mutation observed during migration, suitable for assertions
     */
    public MigrationResult toMigrationResult() {
        return new MigrationResult(
            Collections.unmodifiableMap(new HashMap<>(renamedSteps)),
            Collections.unmodifiableSet(new HashSet<>(removedSteps)),
            Collections.unmodifiableSet(new HashSet<>(addedSteps)),
            deepCopy(propertiesRenamed),
            deepCopySets(propertiesRemoved),
            deepCopySets(propertiesUpdated)
        );
    }

    private static Map<String, Map<String, String>> deepCopy(final Map<String, Map<String, String>> source) {
        final Map<String, Map<String, String>> copy = new HashMap<>();
        for (final Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
            copy.put(entry.getKey(), Collections.unmodifiableMap(new HashMap<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static Map<String, Set<String>> deepCopySets(final Map<String, Set<String>> source) {
        final Map<String, Set<String>> copy = new HashMap<>();
        for (final Map.Entry<String, Set<String>> entry : source.entrySet()) {
            copy.put(entry.getKey(), Collections.unmodifiableSet(new HashSet<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(copy);
    }

    private Map<String, ConnectorValueReference> getOrCreateStepMap(final String stepName) {
        return stepProperties.computeIfAbsent(stepName, key -> {
            if (!initialStepNames.contains(key)) {
                addedSteps.add(key);
            }
            return new HashMap<>();
        });
    }

    private void trackRenamed(final String stepName, final String oldName, final String newName) {
        propertiesRenamed.computeIfAbsent(stepName, key -> new HashMap<>()).put(oldName, newName);
    }

    private void trackRemoved(final String stepName, final String propertyName) {
        propertiesRemoved.computeIfAbsent(stepName, key -> new HashSet<>()).add(propertyName);
    }

    private void trackUpdated(final String stepName, final String propertyName) {
        propertiesUpdated.computeIfAbsent(stepName, key -> new HashSet<>()).add(propertyName);
    }

    /**
     * A summary of every step and property mutation observed during a call to
     * {@link org.apache.nifi.components.connector.Connector#migrateProperties(ConnectorPropertyConfiguration)}.
     *
     * @param renamedSteps       old step name to new step name for every renamed step
     * @param removedSteps       step names removed during migration
     * @param addedSteps         step names introduced during migration
     * @param propertiesRenamed  per-step map from old property name to new property name
     * @param propertiesRemoved  per-step set of property names removed
     * @param propertiesUpdated  per-step set of property names added or overwritten (including via setValueReference)
     */
    public record MigrationResult(
        Map<String, String> renamedSteps,
        Set<String> removedSteps,
        Set<String> addedSteps,
        Map<String, Map<String, String>> propertiesRenamed,
        Map<String, Set<String>> propertiesRemoved,
        Map<String, Set<String>> propertiesUpdated
    ) {
    }

    private final class MockStepPropertyConfiguration implements ConnectorStepPropertyConfiguration {
        private final String stepName;

        private MockStepPropertyConfiguration(final String stepName) {
            this.stepName = stepName;
        }

        @Override
        public String getStepName() {
            return stepName;
        }

        @Override
        public boolean renameProperty(final String propertyName, final String newName) {
            trackRenamed(stepName, propertyName, newName);
            final Map<String, ConnectorValueReference> properties = stepProperties.get(stepName);
            if (properties == null || !properties.containsKey(propertyName)) {
                return false;
            }
            if (Objects.equals(propertyName, newName)) {
                return false;
            }
            final ConnectorValueReference existing = properties.remove(propertyName);
            properties.put(newName, existing);
            return true;
        }

        @Override
        public boolean removeProperty(final String propertyName) {
            trackRemoved(stepName, propertyName);
            final Map<String, ConnectorValueReference> properties = stepProperties.get(stepName);
            if (properties == null || !properties.containsKey(propertyName)) {
                return false;
            }
            properties.remove(propertyName);
            return true;
        }

        @Override
        public boolean hasProperty(final String propertyName) {
            final Map<String, ConnectorValueReference> properties = stepProperties.get(stepName);
            return properties != null && properties.containsKey(propertyName);
        }

        @Override
        public boolean isPropertySet(final String propertyName) {
            final Map<String, ConnectorValueReference> properties = stepProperties.get(stepName);
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
            trackUpdated(stepName, propertyName);
            getOrCreateStepMap(stepName).put(propertyName, valueReference);
        }

        @Override
        public Optional<String> getPropertyValue(final String propertyName) {
            final Map<String, ConnectorValueReference> properties = stepProperties.get(stepName);
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
            final Map<String, ConnectorValueReference> properties = stepProperties.get(stepName);
            if (properties == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(properties.get(propertyName));
        }

        @Override
        public Map<String, String> getProperties() {
            final Map<String, ConnectorValueReference> properties = stepProperties.get(stepName);
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
            final Map<String, ConnectorValueReference> properties = stepProperties.get(stepName);
            if (properties == null) {
                return Collections.emptyMap();
            }
            return Collections.unmodifiableMap(new HashMap<>(properties));
        }
    }
}
