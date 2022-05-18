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

package org.apache.nifi.groups;

import org.apache.nifi.flow.VersionedComponent;

import java.util.function.Function;

public class GroupSynchronizationOptions {
    private final ComponentIdGenerator componentIdGenerator;
    private final Function<VersionedComponent, String> componentComparisonIdLookup;
    private final ComponentScheduler componentScheduler;
    private final PropertyDecryptor propertyDecryptor;
    private final boolean ignoreLocalModifications;
    private final boolean updateSettings;
    private final boolean updateDescendantVersionedFlows;
    private final boolean updateGroupVersionControlSnapshot;
    private final boolean updateExistingVariables;
    private final boolean updateRpgUrls;

    private GroupSynchronizationOptions(final Builder builder) {
        this.componentIdGenerator = builder.componentIdGenerator;
        this.componentComparisonIdLookup = builder.componentComparisonIdLookup;
        this.componentScheduler = builder.componentScheduler;
        this.propertyDecryptor = builder.propertyDecryptor;
        this.ignoreLocalModifications = builder.ignoreLocalModifications;
        this.updateSettings = builder.updateSettings;
        this.updateDescendantVersionedFlows = builder.updateDescendantVersionedFlows;
        this.updateGroupVersionControlSnapshot = builder.updateGroupVersionControlSnapshot;
        this.updateExistingVariables = builder.updateExistingVariables;
        this.updateRpgUrls = builder.updateRpgUrls;
    }

    public ComponentIdGenerator getComponentIdGenerator() {
        return componentIdGenerator;
    }

    public Function<VersionedComponent, String> getComponentComparisonIdLookup() {
        return componentComparisonIdLookup;
    }

    public ComponentScheduler getComponentScheduler() {
        return componentScheduler;
    }

    public boolean isIgnoreLocalModifications() {
        return ignoreLocalModifications;
    }

    public boolean isUpdateSettings() {
        return updateSettings;
    }

    public boolean isUpdateDescendantVersionedFlows() {
        return updateDescendantVersionedFlows;
    }

    public boolean isUpdateGroupVersionControlSnapshot() {
        return updateGroupVersionControlSnapshot;
    }

    public boolean isUpdateExistingVariables() {
        return updateExistingVariables;
    }

    public boolean isUpdateRpgUrls() {
        return updateRpgUrls;
    }

    public PropertyDecryptor getPropertyDecryptor() {
        return propertyDecryptor;
    }


    public static class Builder {
        private ComponentIdGenerator componentIdGenerator;
        private Function<VersionedComponent, String> componentComparisonIdLookup;
        private ComponentScheduler componentScheduler;
        private boolean ignoreLocalModifications = false;
        private boolean updateSettings = true;
        private boolean updateDescendantVersionedFlows = true;
        private boolean updateGroupVersionControlSnapshot = true;
        private boolean updateExistingVariables = false;
        private boolean updateRpgUrls = false;
        private PropertyDecryptor propertyDecryptor = value -> value;

        /**
         * Specifies the Component ID Generator to use for generating UUID's of components that are to be added to a ProcessGroup
         * @param componentIdGenerator the ComponentIdGenerator to use
         * @return the builder
         */
        public Builder componentIdGenerator(final ComponentIdGenerator componentIdGenerator) {
            this.componentIdGenerator = componentIdGenerator;
            return this;
        }

        /**
         * When comparing two flows, the components in those two flows must be matched up by their ID's. This specifies how to determine the ID for a given
         * Versioned Component
         * @param idLookup the lookup that indicates the ID to use for components
         * @return the builder
         */
        public Builder componentComparisonIdLookup(final Function<VersionedComponent, String> idLookup) {
            this.componentComparisonIdLookup = idLookup;
            return this;
        }

        /**
         * Specifies the ComponentScheduler to use for starting connectable components
         * @param componentScheduler the ComponentScheduler to use
         * @return the builder
         */
        public Builder componentScheduler(final ComponentScheduler componentScheduler) {
            this.componentScheduler = componentScheduler;
            return this;
        }

        /**
         * Specifies whether local modifications to a dataflow should prevent the flow from being updated
         *
         * @param ignore if <code>true</code>, the Process Group should be synchronized with the proposed VersionedProcessGroup even if it has local modifications.
         * If <code>false</code>, an attempt to synchronize a Process Group with a proposed flow should fail
         * @return the builder
         */
        public Builder ignoreLocalModifications(final boolean ignore) {
            this.ignoreLocalModifications = ignore;
            return this;
        }

        /**
         * Specifies whether or not a Process Group's settings (e.g., name, position) should be updated
         * @param updateSettings whether or not to update the Process Group's settings
         * @return the builder
         */
        public Builder updateGroupSettings(final boolean updateSettings) {
            this.updateSettings = updateSettings;
            return this;
        }

        /**
         * If a child Process Group is under version control, specifies whether or not the child should have its contents synchronized
         * @param updateDescendantVersionedFlows <code>true</code> to synchronize child groups, <code>false</code> otherwise
         * @return the builder
         */
        public Builder updateDescendantVersionedFlows(final boolean updateDescendantVersionedFlows) {
            this.updateDescendantVersionedFlows = updateDescendantVersionedFlows;
            return this;
        }

        /**
         * When a Process Group is version controlled, it tracks whether or not there are any local modifications by comparing the current dataflow
         * to a snapshot of what the Versioned Flow looks like. If this value is set to <code>true</code>, when the Process Group is synchronized
         * with a VersionedProcessGroup, that VersionedProcessGroup will become the snapshot of what the Versioned Flow looks like. If <code>false</code>,
         * the snapshot is not updated.
         *
         * @param updateGroupVersionControlSnapshot <code>true</code> to update the snapshot, <code>false</code> otherwise
         * @return the builder
         */
        public Builder updateGroupVersionControlSnapshot(final boolean updateGroupVersionControlSnapshot) {
            this.updateGroupVersionControlSnapshot = updateGroupVersionControlSnapshot;
            return this;
        }

        /**
         * Specifies whether or not existing variables in the Process Group's Variable Registry should be updated. If <code>false</code>, any Variable
         * that exists in a Versioned Process Group will be added to the group's variable registry but existing variables will not be modified. If <code>true</code>,
         * existing variables will also be updated
         *
         * @param updateExistingVariables whether or not to update existing variables
         * @return the builder
         */
        public Builder updateExistingVariables(final boolean updateExistingVariables) {
            this.updateExistingVariables = updateExistingVariables;
            return this;
        }

        /**
         * Specifies whether or not the URLs / "Target URIs" of a Remote Process Group that exists in both the proposed flow and the current flow
         * should be updated to match that of the proposed flow
         *
         * @param updateRpgUrls whether or not to update the RPG URLs
         * @return the builder
         */
        public Builder updateRpgUrls(final boolean updateRpgUrls) {
            this.updateRpgUrls = updateRpgUrls;
            return this;
        }

        /**
         * Specifies the decryptor to use for sensitive properties
         *
         * @param decryptor the decryptor to use
         * @return the builder
         */
        public Builder propertyDecryptor(final PropertyDecryptor decryptor) {
            this.propertyDecryptor = decryptor;
            return this;
        }


        public GroupSynchronizationOptions build() {
            if (componentIdGenerator == null) {
                throw new IllegalStateException("Must set Component ID Generator");
            }
            if (componentComparisonIdLookup == null) {
                throw new IllegalStateException("Must set the Component Comparison ID Lookup");
            }
            if (componentScheduler == null) {
                throw new IllegalStateException("Must set Component Scheduler");
            }

            return new GroupSynchronizationOptions(this);
        }

        public static Builder from(final GroupSynchronizationOptions options) {
            final Builder builder = new Builder();
            builder.componentIdGenerator = options.getComponentIdGenerator();
            builder.componentComparisonIdLookup = options.getComponentComparisonIdLookup();
            builder.componentScheduler = options.getComponentScheduler();
            builder.ignoreLocalModifications = options.isIgnoreLocalModifications();
            builder.updateSettings = options.isUpdateSettings();
            builder.updateDescendantVersionedFlows = options.isUpdateDescendantVersionedFlows();
            builder.updateGroupVersionControlSnapshot = options.isUpdateGroupVersionControlSnapshot();
            builder.updateExistingVariables = options.isUpdateExistingVariables();
            builder.updateRpgUrls = options.isUpdateRpgUrls();
            builder.propertyDecryptor = options.getPropertyDecryptor();

            return builder;
        }
    }
}
