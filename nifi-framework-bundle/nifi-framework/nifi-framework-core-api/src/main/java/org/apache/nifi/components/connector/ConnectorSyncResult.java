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
 * Result of a connector synchronization attempt during flow inheritance.
 * Contains the outcome, the connector node (if applicable), and the effective
 * ScheduledState that the caller should apply.
 */
public final class ConnectorSyncResult {

    /**
     * The outcome of the sync attempt.
     */
    public enum Outcome {
        SYNCED,
        SYNCED_CONFIG_UNCHANGED,
        REJECTED,
        FAILED,
        REMOVED
    }

    private final Outcome outcome;
    private final ConnectorNode connectorNode;
    private final ScheduledState effectiveScheduledState;

    private ConnectorSyncResult(final Outcome outcome, final ConnectorNode connectorNode, final ScheduledState effectiveScheduledState) {
        this.outcome = outcome;
        this.connectorNode = connectorNode;
        this.effectiveScheduledState = effectiveScheduledState;
    }

    /**
     * Configuration was applied and the connector's run state should be updated
     * to match the effective ScheduledState.
     */
    public static ConnectorSyncResult synced(final ConnectorNode connectorNode, final ScheduledState effectiveScheduledState) {
        return new ConnectorSyncResult(Outcome.SYNCED, connectorNode, effectiveScheduledState);
    }

    /**
     * Connector configuration was unchanged relative to the proposed versioned configuration;
     * no configuration update was applied. The connector's run state should still be updated
     * if it differs from the effective ScheduledState.
     */
    public static ConnectorSyncResult syncedConfigUnchanged(final ConnectorNode connectorNode, final ScheduledState effectiveScheduledState) {
        return new ConnectorSyncResult(Outcome.SYNCED_CONFIG_UNCHANGED, connectorNode, effectiveScheduledState);
    }

    /**
     * The connector was in a state that prevents synchronization. The connector
     * has been marked invalid.
     */
    public static ConnectorSyncResult rejected(final ConnectorNode connectorNode) {
        return new ConnectorSyncResult(Outcome.REJECTED, connectorNode, null);
    }

    /**
     * Synchronization was attempted but failed. The connector has been marked invalid.
     */
    public static ConnectorSyncResult failed(final ConnectorNode connectorNode) {
        return new ConnectorSyncResult(Outcome.FAILED, connectorNode, null);
    }

    /**
     * The connector was removed from this node because the external provider
     * indicated it should not exist.
     */
    public static ConnectorSyncResult removed() {
        return new ConnectorSyncResult(Outcome.REMOVED, null, null);
    }

    public Outcome getOutcome() {
        return outcome;
    }

    /**
     * Returns the connector node, or {@code null} if the connector was removed.
     */
    public ConnectorNode getConnectorNode() {
        return connectorNode;
    }

    /**
     * Returns the effective ScheduledState that should be applied to the connector,
     * or {@code null} if the connector was rejected, failed, or removed.
     * The caller is responsible for applying this state using the appropriate
     * mechanism (e.g., {@code FlowController.startConnector()} which respects
     * deferred-start semantics).
     */
    public ScheduledState getEffectiveScheduledState() {
        return effectiveScheduledState;
    }

    @Override
    public String toString() {
        return "ConnectorSyncResult[outcome=" + outcome
                + (connectorNode != null ? ", connector=" + connectorNode.getIdentifier() : "")
                + (effectiveScheduledState != null ? ", effectiveState=" + effectiveScheduledState : "")
                + "]";
    }
}
