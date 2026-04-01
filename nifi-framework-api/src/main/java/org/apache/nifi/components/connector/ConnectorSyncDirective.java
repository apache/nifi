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

package org.apache.nifi.components.connector;

import org.apache.nifi.flow.ScheduledState;

/**
 * Directive returned by {@link ConnectorConfigurationProvider#verifySyncable(String, ScheduledState)}
 * indicating how the connector repository should handle synchronization for a connector during
 * flow inheritance.
 */
public class ConnectorSyncDirective {

    /**
     * The action the connector repository should take for this connector during flow sync.
     */
    public enum Action {
        /**
         * Proceed with synchronization. The directive may optionally include a
         * {@link ScheduledState} override and/or a {@link ConnectorWorkingConfiguration}
         * containing the provider's working config and name.
         */
        ALLOW,

        /**
         * Do not synchronize this connector. The connector should be created locally
         * (if not already present) and marked invalid so that a background repair
         * process can attempt synchronization later when conditions improve.
         */
        REJECT,

        /**
         * This connector should not exist on this node. If it exists locally, remove
         * it from the repository. If it does not exist, do not create it. This is used
         * when the external system indicates the connector is being deleted or has been deleted.
         */
        REMOVE
    }

    private static final ConnectorSyncDirective ALLOW_DEFAULT = new ConnectorSyncDirective(Action.ALLOW, null, null);
    private static final ConnectorSyncDirective REJECT_DIRECTIVE = new ConnectorSyncDirective(Action.REJECT, null, null);
    private static final ConnectorSyncDirective REMOVE_DIRECTIVE = new ConnectorSyncDirective(Action.REMOVE, null, null);

    private final Action action;
    private final ScheduledState scheduledStateOverride;
    private final ConnectorWorkingConfiguration workingConfiguration;

    private ConnectorSyncDirective(final Action action, final ScheduledState scheduledStateOverride,
                                   final ConnectorWorkingConfiguration workingConfiguration) {
        this.action = action;
        this.scheduledStateOverride = scheduledStateOverride;
        this.workingConfiguration = workingConfiguration;
    }

    /**
     * Returns an ALLOW directive with no overrides. The connector repository will use the
     * versioned flow's name, working config, and ScheduledState as-is. This is the default
     * behavior when no {@link ConnectorConfigurationProvider} is configured (Apache NiFi).
     */
    public static ConnectorSyncDirective allow() {
        return ALLOW_DEFAULT;
    }

    /**
     * Returns an ALLOW directive with the provider's working configuration (name + working
     * config steps) and no ScheduledState override.
     *
     * @param workingConfiguration the provider's working configuration including name
     */
    public static ConnectorSyncDirective allow(final ConnectorWorkingConfiguration workingConfiguration) {
        return new ConnectorSyncDirective(Action.ALLOW, null, workingConfiguration);
    }

    /**
     * Returns an ALLOW directive with the provider's working configuration and a
     * ScheduledState override. The override replaces the versioned flow's ScheduledState,
     * which may be stale due to in-flight DPS tasks.
     *
     * @param workingConfiguration the provider's working configuration including name
     * @param scheduledStateOverride the ScheduledState to use instead of the versioned flow's value
     */
    public static ConnectorSyncDirective allow(final ConnectorWorkingConfiguration workingConfiguration,
                                               final ScheduledState scheduledStateOverride) {
        return new ConnectorSyncDirective(Action.ALLOW, scheduledStateOverride, workingConfiguration);
    }

    /**
     * Returns a REJECT directive. The connector will be created locally (if not already
     * present) and marked invalid for background repair.
     */
    public static ConnectorSyncDirective reject() {
        return REJECT_DIRECTIVE;
    }

    /**
     * Returns a REMOVE directive. The connector should not exist on this node.
     */
    public static ConnectorSyncDirective remove() {
        return REMOVE_DIRECTIVE;
    }

    public Action getAction() {
        return action;
    }

    /**
     * Returns the ScheduledState override, or {@code null} if the versioned flow's
     * ScheduledState should be used.
     */
    public ScheduledState getScheduledStateOverride() {
        return scheduledStateOverride;
    }

    /**
     * Returns the provider's working configuration (name + working config steps),
     * or {@code null} if the versioned flow's values should be used.
     */
    public ConnectorWorkingConfiguration getWorkingConfiguration() {
        return workingConfiguration;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConnectorSyncDirective[action=").append(action);
        if (scheduledStateOverride != null) {
            sb.append(", scheduledStateOverride=").append(scheduledStateOverride);
        }
        if (workingConfiguration != null) {
            sb.append(", hasWorkingConfig=true");
        }
        sb.append(']');
        return sb.toString();
    }
}
