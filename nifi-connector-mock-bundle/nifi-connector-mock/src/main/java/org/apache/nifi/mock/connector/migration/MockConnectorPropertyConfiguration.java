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
import org.apache.nifi.components.connector.StepConfiguration;
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

    private final Map<String, StepConfiguration> stepProperties = new HashMap<>();
    private final Set<String> initialStepNames;

    private final Map<String, StepPropertyRenames> propertiesRenamed = new HashMap<>();
    private final Map<String, Set<String>> propertiesRemoved = new HashMap<>();
    private final Map<String, Set<String>> propertiesUpdated = new HashMap<>();

    private final Map<String, String> renamedSteps = new HashMap<>();
    private final Set<String> removedSteps = new HashSet<>();
    private final Set<String> addedSteps = new HashSet<>();

    private MockConnectorPropertyConfiguration(final Map<String, StepConfiguration> initialProperties) {
        if (initialProperties != null) {
            for (final Map.Entry<String, StepConfiguration> entry : initialProperties.entrySet()) {
                final Map<String, ConnectorValueReference> copy = new HashMap<>();
                final StepConfiguration stepConfig = entry.getValue();
                if (stepConfig != null && stepConfig.getPropertyValues() != null) {
                    copy.putAll(stepConfig.getPropertyValues());
                }
                stepProperties.put(entry.getKey(), new StepConfiguration(copy));
            }
        }
        this.initialStepNames = Collections.unmodifiableSet(new HashSet<>(stepProperties.keySet()));
    }

    /**
     * Creates a {@link MockConnectorPropertyConfiguration} pre-populated with typed {@link ConnectorValueReference}
     * values for each step.
     *
     * @param initialProperties per-step {@link StepConfiguration} snapshots of {@link ConnectorValueReference} values
     * @return a new configuration
     */
    public static MockConnectorPropertyConfiguration fromValueReferences(final Map<String, StepConfiguration> initialProperties) {
        return new MockConnectorPropertyConfiguration(initialProperties);
    }

    /**
     * Creates a {@link MockConnectorPropertyConfiguration} pre-populated with plain string-literal values for each
     * step. Each supplied string is wrapped in a {@link StringLiteralValue}.
     *
     * @param initialProperties per-step {@link StepLiteralProperties} of raw string values
     * @return a new configuration
     */
    public static MockConnectorPropertyConfiguration fromStringLiterals(final Map<String, StepLiteralProperties> initialProperties) {
        final Map<String, StepConfiguration> converted = new HashMap<>();
        if (initialProperties != null) {
            for (final Map.Entry<String, StepLiteralProperties> entry : initialProperties.entrySet()) {
                final Map<String, ConnectorValueReference> stepMap = new HashMap<>();
                final StepLiteralProperties stepLiterals = entry.getValue();
                if (stepLiterals != null) {
                    for (final String propertyName : stepLiterals.getPropertyNames()) {
                        stepMap.put(propertyName, new StringLiteralValue(stepLiterals.getValue(propertyName)));
                    }
                }
                converted.put(entry.getKey(), new StepConfiguration(stepMap));
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

        final StepConfiguration existing = stepProperties.remove(oldStepName);
        stepProperties.put(newStepName, existing);
        renamedSteps.put(oldStepName, newStepName);

        final StepPropertyRenames renames = propertiesRenamed.remove(oldStepName);
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

    private static Map<String, StepPropertyRenames> deepCopy(final Map<String, StepPropertyRenames> source) {
        final Map<String, StepPropertyRenames> copy = new HashMap<>();
        for (final Map.Entry<String, StepPropertyRenames> entry : source.entrySet()) {
            final StepPropertyRenames snapshot = new StepPropertyRenames();
            for (final String oldName : entry.getValue().getOldNames()) {
                snapshot.add(oldName, entry.getValue().getNewName(oldName));
            }
            copy.put(entry.getKey(), snapshot);
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
            return new StepConfiguration(new HashMap<>());
        }).getPropertyValues();
    }

    private Map<String, ConnectorValueReference> getStepMap(final String stepName) {
        final StepConfiguration stepConfig = stepProperties.get(stepName);
        return stepConfig == null ? null : stepConfig.getPropertyValues();
    }

    private void trackRenamed(final String stepName, final String oldName, final String newName) {
        propertiesRenamed.computeIfAbsent(stepName, key -> new StepPropertyRenames()).add(oldName, newName);
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
     * @param propertiesRenamed  per-step {@link StepPropertyRenames} of old property name to new property name
     * @param propertiesRemoved  per-step set of property names removed
     * @param propertiesUpdated  per-step set of property names added or overwritten (including via setValueReference)
     */
    public record MigrationResult(
        Map<String, String> renamedSteps,
        Set<String> removedSteps,
        Set<String> addedSteps,
        Map<String, StepPropertyRenames> propertiesRenamed,
        Map<String, Set<String>> propertiesRemoved,
        Map<String, Set<String>> propertiesUpdated
    ) {
    }

    /**
     * Per-step property renames observed during migration. Callers accumulate rename pairs via
     * {@link #add(String, String)} and read the outcome via {@link #getNewName(String)} and
     * {@link #getOldNames()}. Instances compare equal when they contain the same set of rename pairs, so
     * a builder-style expected value can be compared against the mock's recorded value with
     * {@code assertEquals}.
     */
    public static final class StepPropertyRenames {

        private final Map<String, String> renames = new HashMap<>();

        /**
         * Records a rename of {@code oldName} to {@code newName}.
         *
         * @return this instance for chaining
         */
        public StepPropertyRenames add(final String oldName, final String newName) {
            renames.put(oldName, newName);
            return this;
        }

        /**
         * @return the new name recorded for {@code oldName}, or {@code null} if no rename was recorded
         */
        public String getNewName(final String oldName) {
            return renames.get(oldName);
        }

        /**
         * @return an unmodifiable view of the old property names that have been renamed
         */
        public Set<String> getOldNames() {
            return Collections.unmodifiableSet(renames.keySet());
        }

        public boolean isEmpty() {
            return renames.isEmpty();
        }

        public int size() {
            return renames.size();
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof final StepPropertyRenames that)) {
                return false;
            }
            return renames.equals(that.renames);
        }

        @Override
        public int hashCode() {
            return renames.hashCode();
        }

        @Override
        public String toString() {
            return "StepPropertyRenames" + renames;
        }
    }

    /**
     * Per-step literal-only property values, used to seed a {@link MockConnectorPropertyConfiguration} through
     * {@link #fromStringLiterals(Map)}. Callers accumulate name/value pairs via {@link #add(String, String)}
     * and read them back via {@link #getValue(String)} and {@link #getPropertyNames()}.
     */
    public static final class StepLiteralProperties {

        private final Map<String, String> properties = new HashMap<>();

        /**
         * Records the literal {@code value} for the property named {@code propertyName}.
         *
         * @return this instance for chaining
         */
        public StepLiteralProperties add(final String propertyName, final String value) {
            properties.put(propertyName, value);
            return this;
        }

        /**
         * @return the literal value recorded for {@code propertyName}, or {@code null} if none was recorded
         */
        public String getValue(final String propertyName) {
            return properties.get(propertyName);
        }

        /**
         * @return an unmodifiable view of the property names that have been recorded
         */
        public Set<String> getPropertyNames() {
            return Collections.unmodifiableSet(properties.keySet());
        }

        public boolean isEmpty() {
            return properties.isEmpty();
        }

        public int size() {
            return properties.size();
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof final StepLiteralProperties that)) {
                return false;
            }
            return properties.equals(that.properties);
        }

        @Override
        public int hashCode() {
            return properties.hashCode();
        }

        @Override
        public String toString() {
            return "StepLiteralProperties" + properties;
        }
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
            final Map<String, ConnectorValueReference> properties = getStepMap(stepName);
            if (properties == null || !properties.containsKey(propertyName)) {
                return false;
            }
            if (Objects.equals(propertyName, newName)) {
                return false;
            }
            final ConnectorValueReference existing = properties.remove(propertyName);
            properties.put(newName, existing);
            trackRenamed(stepName, propertyName, newName);
            return true;
        }

        @Override
        public boolean removeProperty(final String propertyName) {
            final Map<String, ConnectorValueReference> properties = getStepMap(stepName);
            if (properties == null || !properties.containsKey(propertyName)) {
                return false;
            }
            properties.remove(propertyName);
            trackRemoved(stepName, propertyName);
            return true;
        }

        @Override
        public boolean hasProperty(final String propertyName) {
            final Map<String, ConnectorValueReference> properties = getStepMap(stepName);
            return properties != null && properties.containsKey(propertyName);
        }

        @Override
        public boolean isPropertySet(final String propertyName) {
            final Map<String, ConnectorValueReference> properties = getStepMap(stepName);
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
            final ConnectorValueReference previous = getOrCreateStepMap(stepName).put(propertyName, valueReference);
            if (Objects.equals(previous, valueReference)) {
                return;
            }
            trackUpdated(stepName, propertyName);
        }

        @Override
        public Optional<String> getPropertyValue(final String propertyName) {
            final Map<String, ConnectorValueReference> properties = getStepMap(stepName);
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
            final Map<String, ConnectorValueReference> properties = getStepMap(stepName);
            if (properties == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(properties.get(propertyName));
        }

        @Override
        public Map<String, String> getProperties() {
            final Map<String, ConnectorValueReference> properties = getStepMap(stepName);
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
            final Map<String, ConnectorValueReference> properties = getStepMap(stepName);
            if (properties == null) {
                return Collections.emptyMap();
            }
            return Collections.unmodifiableMap(new HashMap<>(properties));
        }
    }
}
