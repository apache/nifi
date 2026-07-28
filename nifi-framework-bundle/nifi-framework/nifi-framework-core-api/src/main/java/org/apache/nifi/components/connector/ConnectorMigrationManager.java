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

import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Framework service that drives the eligibility-listing path used by the migration-sources REST endpoint and the
 * actual migration path used by the migration-request REST endpoint. Connector translation logic is owned by the
 * extension; framework concerns (eligibility gating, source disable/rename, rollback, cluster-topology validation,
 * and atomic flag persistence) live here.
 */
public interface ConnectorMigrationManager {

    /**
     * Lists the Versioned Process Groups on the canvas that the given Connector can use as a migration source.
     *
     * @param connectorId the identifier of the target Connector
     * @return the eligible source Process Groups, after framework prerequisites and the Connector's own
     *         {@code isMigrationSupported(...)} filter have been applied
     */
    List<ConnectorMigrationSource> listMigrationSources(String connectorId);

    /**
     * Verifies that the given Process Group satisfies every framework prerequisite required for a local-source
     * migration into the given Connector. Throws an {@link IllegalStateException} with a state-specific
     * diagnostic when any prerequisite is unmet.
     *
     * @param connectorId the identifier of the target Connector
     * @param processGroupId the identifier of the source Process Group
     */
    void verifyEligibility(String connectorId, String processGroupId);

    /**
     * Verifies that the target Connector is itself ready to receive a migration, independent of any particular source.
     * This asserts that the Connector is stopped and that its active flow has not been modified from its initial flow.
     * It is invoked synchronously when a migration request is submitted so the caller sees an unready Connector
     * reported immediately rather than only after the asynchronous migration task runs. Throws an
     * {@link IllegalStateException} when the Connector is not in a state that can receive a migration.
     *
     * @param connectorId the identifier of the target Connector
     */
    void verifyConnectorReadyForMigration(String connectorId);

    /**
     * Migrates the target Connector by updating the Connector's own flow to mirror the configuration, parameters, and
     * component state captured in the given source flow. When {@code processGroupId} is non-null the migration is
     * treated as a local-source migration (and the source Process Group is disabled and renamed on success); when
     * {@code processGroupId} is null the migration is treated as an uploaded-payload migration. On any failure during
     * the Connector's {@code migrate(...)} call or the framework's post-migrate work, the Connector is rolled back to
     * its initial-flow state and the source Process Group is left untouched.
     *
     * @param connectorId the identifier of the target Connector
     * @param processGroupId the identifier of the source Process Group for local-source migration, or {@code null}
     *                       for an uploaded-payload migration
     * @param sourceFlow the source flow whose configuration, parameters, and component state the Connector should
     *                   mirror into its own managed flow
     * @throws FlowUpdateException when the migration cannot be completed
     */
    default void migrateFromVersionedFlow(final String connectorId, final String processGroupId, final VersionedExternalFlow sourceFlow) throws FlowUpdateException {
        migrateFromVersionedFlow(connectorId, processGroupId, sourceFlow, () -> false);
    }

    /**
     * Equivalent to {@link #migrateFromVersionedFlow(String, String, VersionedExternalFlow)} but additionally polls the
     * given supplier at framework-controlled checkpoints so a caller-driven cancellation is observed promptly. The
     * Connector's own {@code migrate(...)} call is not interrupted; cancellation only takes effect at the next
     * framework checkpoint, after which the migration is rolled back as if it had failed.
     *
     * @param connectorId the identifier of the target Connector
     * @param processGroupId the identifier of the source Process Group for local-source migration, or {@code null}
     *                       for an uploaded-payload migration
     * @param sourceFlow the source flow whose configuration, parameters, and component state the Connector should
     *                   mirror into its own managed flow
     * @param cancellationCheck a supplier that returns {@code true} when the caller has requested cancellation; the
     *                          framework polls it at the natural boundaries between the Connector's {@code migrate(...)}
     *                          call and the subsequent framework post-migrate work
     * @throws FlowUpdateException when the migration cannot be completed
     */
    void migrateFromVersionedFlow(String connectorId, String processGroupId, VersionedExternalFlow sourceFlow,
            BooleanSupplier cancellationCheck) throws FlowUpdateException;
}
