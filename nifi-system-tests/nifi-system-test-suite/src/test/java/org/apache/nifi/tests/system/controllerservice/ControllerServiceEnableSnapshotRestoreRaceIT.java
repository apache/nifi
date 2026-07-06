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
package org.apache.nifi.tests.system.controllerservice;

import org.apache.nifi.tests.system.NiFiClientUtil;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces hypothesis <b>H1</b> from the SNOW-3697891 investigation: the <em>snapshot-then-restore race on
 * transient state</em>.
 *
 * <p>During a connector state-reset, the orchestration re-enables controller services by first taking a snapshot of
 * the services that are <em>currently ENABLED</em>, then disabling everything, then re-enabling only the services in
 * that snapshot. Controller service startup is <b>not instantaneous</b>: a service whose {@code @OnEnabled} does real
 * work (e.g. the "Snowflake Connection Pool" opening a network connection) spends a non-trivial window in the
 * {@code ENABLING} state, and any service that depends on it (e.g. the "Table State Store") cannot finish enabling
 * until the base service is up. If the snapshot is captured during that window, the slow services are still
 * transitioning and are therefore omitted from the restore list, so they are left permanently {@code DISABLED} while
 * the fast-enabling "PostgreSQL Connection Pool" (a local socket) is captured and correctly restored.
 *
 * <p>The test drives a real NiFi with the same shape as the customer runtime and shows the race deterministically:
 * <ul>
 *   <li>{@code StandardCountService} models the fast "PostgreSQL Connection Pool" (enables instantly).</li>
 *   <li>{@code StandardSleepService} with a long {@code @OnEnabled Sleep Time} models the slow
 *       "Snowflake Connection Pool".</li>
 *   <li>A second {@code StandardSleepService} whose {@code Dependent Service} is the slow pool models the
 *       "Table State Store" that requires the Snowflake pool.</li>
 *   <li>A stopped {@code Sleep} processor referencing the slow pool models the "Create Journal Table" processors and
 *       exposes the customer-visible symptom ("Controller Service ... is disabled") once the race leaves the pool
 *       disabled.</li>
 * </ul>
 */
public class ControllerServiceEnableSnapshotRestoreRaceIT extends NiFiSystemIT {

    private static final String ENABLED = "ENABLED";
    private static final String DISABLED = "DISABLED";

    private static final String COUNT_SERVICE_TYPE = "StandardCountService";
    private static final String SLEEP_SERVICE_TYPE = "StandardSleepService";
    private static final String SLEEP_PROCESSOR_TYPE = "Sleep";

    private static final String ON_ENABLED_SLEEP_TIME = "@OnEnabled Sleep Time";
    private static final String DEPENDENT_SERVICE = "Dependent Service";
    private static final String SLEEP_SERVICE_PROPERTY = "Sleep Service";

    // The slow service stays in ENABLING for this long. It must comfortably exceed the time it takes the fast service
    // to reach ENABLED (a fraction of a second) so the snapshot is captured while the slow services are transitioning,
    // while remaining under the 30s the framework waits for a dependency to enable.
    private static final String SLOW_ENABLE_TIME = "15 sec";

    @Test
    public void testSlowEnablingServicesAreMissedBySnapshotRestore()
            throws NiFiClientException, IOException, InterruptedException {

        final NiFiClientUtil util = getClientUtil();

        // STANDARD parent group modelling the affected connector ("Wareneingang").
        final ProcessGroupEntity parent = util.createProcessGroup("PostgreSQL", "root");

        // Fast service: "PostgreSQL Connection Pool" (local socket -> @OnEnabled returns immediately).
        final ControllerServiceEntity postgresPool = util.createControllerService(COUNT_SERVICE_TYPE, parent.getId());

        // Slow service: "Snowflake Connection Pool". @OnEnabled sleeps, so it lingers in ENABLING.
        final ControllerServiceEntity snowflakePool = util.createControllerService(SLEEP_SERVICE_TYPE, parent.getId());
        util.updateControllerServiceProperties(snowflakePool, Map.of(ON_ENABLED_SLEEP_TIME, SLOW_ENABLE_TIME));

        // Dependent slow service: "Table State Store" requires the Snowflake Connection Pool, so it cannot finish
        // enabling until the slow pool is up.
        final ControllerServiceEntity tableStateStore = util.createControllerService(SLEEP_SERVICE_TYPE, parent.getId());
        util.updateControllerServiceProperties(tableStateStore,
                Map.of(DEPENDENT_SERVICE, snowflakePool.getComponent().getId()));

        // Stopped processor referencing the Snowflake pool, modelling the "Create Journal Table" processors. Left
        // stopped throughout; used only to surface the customer-visible validation error at the end.
        final ProcessorEntity journalProcessor = util.createProcessor(SLEEP_PROCESSOR_TYPE, parent.getId());
        util.updateProcessorProperties(journalProcessor,
                Map.of(SLEEP_SERVICE_PROPERTY, snowflakePool.getComponent().getId()));
        util.setAutoTerminatedRelationships(journalProcessor, "success");

        final String postgresPoolId = postgresPool.getComponent().getId();
        final String snowflakePoolId = snowflakePool.getComponent().getId();
        final String tableStateStoreId = tableStateStore.getComponent().getId();
        final List<String> allServiceIds = List.of(postgresPoolId, snowflakePoolId, tableStateStoreId);

        // Step 1: bring up the fast PostgreSQL pool first and wait for it. Enabling it independently guarantees it is
        // ENABLED regardless of the (non-deterministic) order in which the framework processes the bulk enable -- the
        // framework enables services sequentially in HashSet order, so a slow service enabled first would otherwise
        // block the fast one and collapse the transient window this test depends on.
        util.enableControllerService(postgresPool);
        util.waitForControllerServicesEnabled(parent.getId(), postgresPoolId);

        // Step 2: now kick off the bulk "enable all" sweep (asynchronous, exactly like the state-reset re-enable pass).
        // This starts the slow Snowflake pool (which sits in @OnEnabled) and the Table State Store that waits on it,
        // while the already-ENABLED PostgreSQL pool is a no-op.
        util.enableControllerServices(parent.getId(), false);

        // Step 3: capture the "currently ENABLED" snapshot during that window. Because the Snowflake pool is still
        // sleeping in @OnEnabled (and the Table State Store is blocked waiting on it), only the PostgreSQL pool is
        // ENABLED at this moment -- this is the transient state that causes the miss.
        final Set<String> restoreSnapshot = captureEnabledServices(allServiceIds);

        assertTrue(restoreSnapshot.contains(postgresPoolId),
                "PostgreSQL pool enables instantly and must be captured in the snapshot");
        assertFalse(restoreSnapshot.contains(snowflakePoolId),
                "Snowflake pool is still ENABLING and must be missed by the snapshot (the H1 race)");
        assertFalse(restoreSnapshot.contains(tableStateStoreId),
                "Table State Store is still ENABLING (waiting on the Snowflake pool) and must be missed by the snapshot");

        // Step 4: let enabling finish so that, as in the incident (all services were ENABLED by 09:47), the subsequent
        // bulk-disable operates on fully-ENABLED services.
        util.waitForControllerServicesEnabled(parent.getId(), allServiceIds);

        // Step 5: the Phase-2 bulk "disable all" sweep.
        util.disableControllerServices(parent.getId(), true);
        util.waitForControllerServicesDisabled(parent.getId(), allServiceIds.toArray(new String[0]));

        // Step 6: the buggy "restore" -- re-enable ONLY the services captured in the snapshot.
        for (final String serviceId : restoreSnapshot) {
            final ControllerServiceEntity fresh =
                    getNifiClient().getControllerServicesClient().getControllerService(serviceId);
            util.enableControllerService(fresh);
        }
        util.waitForControllerServicesEnabled(parent.getId(), new ArrayList<>(restoreSnapshot));

        // The two Snowflake-side services were never in the restore list, so they are left permanently DISABLED --
        // the silent data-loss condition -- while the PostgreSQL pool is correctly restored.
        assertEquals(ENABLED, currentState(postgresPoolId),
                "PostgreSQL pool was in the snapshot and must be re-enabled");
        assertEquals(DISABLED, currentState(snowflakePoolId),
                "Snowflake pool was missed by the snapshot and is left permanently disabled");
        assertEquals(DISABLED, currentState(tableStateStoreId),
                "Table State Store was missed by the snapshot and is left permanently disabled");

        // Customer-visible symptom: the journal processor that references the now-disabled Snowflake pool is INVALID
        // with the "Controller Service ... is disabled" validation error seen in the incident logs.
        util.waitForInvalidProcessor(journalProcessor.getId());
        final Collection<String> validationErrors = getNifiClient().getProcessorClient()
                .getProcessor(journalProcessor.getId()).getComponent().getValidationErrors();
        assertNotNull(validationErrors, "Expected validation errors while the referenced Controller Service is disabled");
        assertTrue(validationErrors.stream()
                        .anyMatch(error -> error.toLowerCase().contains("disabled") && error.contains(snowflakePoolId)),
                "Expected a 'Controller Service " + snowflakePoolId + " is disabled' validation error but found: " + validationErrors);
    }

    /**
     * Mimics the orchestration building its re-enable worklist: iterate the known services and record only those that
     * are ENABLED right now. Services still in ENABLING (or DISABLED) at this instant are omitted.
     */
    private Set<String> captureEnabledServices(final List<String> serviceIds) throws NiFiClientException, IOException {
        final Set<String> enabled = new LinkedHashSet<>();
        for (final String serviceId : serviceIds) {
            if (ENABLED.equals(currentState(serviceId))) {
                enabled.add(serviceId);
            }
        }
        return enabled;
    }

    private String currentState(final String serviceId) throws NiFiClientException, IOException {
        return getNifiClient().getControllerServicesClient().getControllerService(serviceId).getComponent().getState();
    }
}
