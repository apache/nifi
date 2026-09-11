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

package org.apache.nifi.tests.system.migration;

import org.apache.nifi.migration.StandardControllerServiceFactory;
import org.apache.nifi.tests.system.AbstractNarSwapMigrationIT;
import org.apache.nifi.tests.system.ExceptionalBooleanSupplier;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that a Controller Service created by property migration survives the operations a deployed flow goes
 * through: a runtime upgrade that introduces the migration, a plain runtime restart, and version changes of the
 * enclosing versioned Process Group.
 *
 * Two independent flow lineages are pre-seeded in the registry. In the first, a later version declares the store
 * Controller Service itself, as a published flow would once the vendor adds it. In the second, no version ever
 * declares the service, so the flow relies entirely on property migration to create it, and the later version only
 * adds an unrelated processor.
 */
public class MigrationCreatedControllerServiceVersioningIT extends AbstractNarSwapMigrationIT {
    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String SERVICE_DECLARED_FLOW_ID = "11111111-2222-3333-4444-555555555555";
    private static final String SERVICE_ABSENT_FLOW_ID = "22222222-3333-4444-5555-666666666666";
    private static final String DECLARED_SERVICE_VERSIONED_ID = "99999999-8888-7777-6666-555555555555";
    private static final String STORE_SERVICE_PROPERTY = "Store Service";
    private static final String STORE_SERVICE_TYPE = "org.apache.nifi.cs.tests.system.StateBackedStoreService";
    private static final String PROCESSOR_TYPE = "org.apache.nifi.processors.tests.system.MigrateToControllerService";
    private static final String MIGRATING_PROCESSOR_NAME = "MigrateToControllerService";
    private static final String ADDED_PROCESSOR_NAME = "Added Processor";
    private static final String VERSIONED_FLOWS_DIRECTORY = "src/test/resources/versioned-flows";
    private static final String CREATED_STATE_KEY = "created";
    private static final String ROW_COUNT_STATE_KEY = "rowCount";
    private static final Duration CONDITION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration CONDITION_POLL = Duration.ofMillis(100);

    /**
     * After the runtime is upgraded, the Controller Service that property migration creates must be present,
     * enabled and referenced, and the flow that was running before the upgrade must be running again, with no manual action.
     */
    @Test
    public void testRuntimeUpgradeCreatesEnabledServiceAndKeepsFlowRunning() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_DECLARED_FLOW_ID);
        final ControllerServiceEntity service = waitForSingleStoreService(flow.groupId());
        final String serviceId = service.getComponent().getId();

        assertEquals(StandardControllerServiceFactory.MIGRATION_CREATED_COMMENT, service.getComponent().getComments());
        assertBelongsToLocalFlowOnly(service);
        getClientUtil().waitForControllerServiceRunStatus(serviceId, "ENABLED");
        getClientUtil().waitForRunningProcessor(flow.processorId());
        assertEquals(serviceId, getStoreServiceId(flow.processorId()));

        final Collection<String> validationErrors = getNifiClient().getProcessorClient().getProcessor(flow.processorId()).getComponent().getValidationErrors();
        final boolean processorValid = validationErrors == null || validationErrors.isEmpty();
        assertTrue(processorValid, "Processor must be valid after the runtime upgrade");

        waitForCondition(() -> countRows(serviceId) > 0, "store row count > 0");

        final String versionedFlowState = getClientUtil().getVersionedFlowState(flow.groupId(), "root");
        assertNotEquals("LOCALLY_MODIFIED", versionedFlowState, "The migration-created Controller Service must not make the flow dirty");
        assertNotEquals("LOCALLY_MODIFIED_AND_STALE", versionedFlowState, "The migration-created Controller Service must not make the flow dirty");

        final boolean serviceReportedAsLocalModification = getNifiClient().getProcessGroupClient().getLocalModifications(flow.groupId())
                .getComponentDifferences().stream()
                .anyMatch(diff -> serviceId.equals(diff.getComponentId()));
        assertFalse(serviceReportedAsLocalModification);
    }

    /**
     * Upgrading to a flow version that declares the store Controller Service must keep using the service that
     * property migration already created, rather than removing it and substituting the one the published version declares.
     */
    @Test
    public void testFlowUpgradePreservesMigrationCreatedControllerService() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_DECLARED_FLOW_ID);
        final MigratedStore store = awaitPopulatedStoreService(flow);
        assertBelongsToLocalFlowOnly(waitForSingleStoreService(flow.groupId()));

        final VersionedFlowUpdateRequestEntity upgradeRequest = getClientUtil().changeFlowVersion(flow.groupId(), "2", false);

        assertStorePreserved(flow, store, "flow upgrade");
        getClientUtil().assertFlowUpToDate(flow.groupId());

        final ControllerServiceEntity serviceAfterUpgrade = waitForSingleStoreService(flow.groupId());
        assertEquals(DECLARED_SERVICE_VERSIONED_ID, serviceAfterUpgrade.getComponent().getVersionedComponentId(),
                "The service must track the Controller Service that version 2 declares, instead of belonging to the local flow only");
        assertEquals("", serviceAfterUpgrade.getComponent().getComments(),
                "Comments must come from version 2 once the service tracks the declared Controller Service");
        assertNull(upgradeRequest.getRequest().getFailureReason(),
                "The migration-created Controller Service must not prevent the versioned flow from upgrading");
    }

    /**
     * A flow whose definition never declares the store Controller Service relies entirely on property migration to
     * create it. Upgrading such a flow to a version that only adds an unrelated processor, leaving the migrating
     * processor untouched, must not disturb the service: the version change has nothing to say about it.
     */
    @Test
    public void testFlowUpgradeAddingUnrelatedProcessorPreservesMigrationCreatedControllerService() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_ABSENT_FLOW_ID);
        final MigratedStore store = awaitPopulatedStoreService(flow);

        final VersionedFlowUpdateRequestEntity upgradeRequest = getClientUtil().changeFlowVersion(flow.groupId(), "2", false);

        assertTrue(hasProcessorNamed(flow.groupId(), ADDED_PROCESSOR_NAME), "The upgrade must have added the unrelated processor");

        assertStorePreserved(flow, store, "flow upgrade");
        getClientUtil().assertFlowUpToDate(flow.groupId());

        final ControllerServiceEntity serviceAfterUpgrade = waitForSingleStoreService(flow.groupId());
        assertBelongsToLocalFlowOnly(serviceAfterUpgrade);
        assertEquals(StandardControllerServiceFactory.MIGRATION_CREATED_COMMENT, serviceAfterUpgrade.getComponent().getComments());
        assertNull(upgradeRequest.getRequest().getFailureReason(),
                "The migration-created Controller Service must not prevent the versioned flow from upgrading");
    }

    /**
     * Restarting the runtime with no NAR change must reuse the Controller Service that property migration
     * created previously rather than creating a second one.
     */
    @Test
    public void testRuntimeRestartDoesNotRecreateMigrationCreatedControllerService() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_DECLARED_FLOW_ID);
        final MigratedStore store = awaitPopulatedStoreService(flow);

        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        assertStorePreserved(flow, store, "runtime restart");
        assertBelongsToLocalFlowOnly(waitForSingleStoreService(flow.groupId()));
    }

    /**
     * Asserts that the given Controller Service has no counterpart in the flow definition. Such a service either
     * carries no versioned component id at all, or carries the placeholder that the framework derives from its own
     * instance id while mapping the group. A service that tracks a Controller Service declared by the flow definition
     * carries the identifier from the definition instead.
     */
    private void assertBelongsToLocalFlowOnly(final ControllerServiceEntity service) {
        final String versionedComponentId = service.getComponent().getVersionedComponentId();
        if (versionedComponentId == null) {
            return;
        }

        final String derivedFromInstanceId = UUID.nameUUIDFromBytes(service.getComponent().getId().getBytes(StandardCharsets.UTF_8)).toString();
        assertEquals(derivedFromInstanceId, versionedComponentId,
                "The service tracks a Controller Service declared by the flow definition, so it no longer belongs to the local flow only");
    }

    /**
     * Imports version 1 of the given pre-seeded flow, waits for its processor to be running, and then simulates a
     * runtime upgrade by stopping NiFi, swapping in the alternate-config extensions and starting NiFi again. Nothing
     * in the flow is stopped or started by hand, so the assertions that follow observe what the upgrade does on its own.
     */
    private MigratedFlow importAndUpgradeRuntime(final String flowId) throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(new File(VERSIONED_FLOWS_DIRECTORY));
        final ProcessGroupEntity group = getClientUtil().importFlowFromRegistry("root", registryClient.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity processor = findProcessor(group.getId(), MIGRATING_PROCESSOR_NAME);
        getClientUtil().waitForRunningProcessor(processor.getId());

        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);

        return new MigratedFlow(group.getId(), processor.getId());
    }

    /**
     * Waits until the migration-created Controller Service is enabled and its store has accumulated at least one row,
     * so that the flow is known to be working before the operation under test runs, and returns its identifier.
     */
    private MigratedStore awaitPopulatedStoreService(final MigratedFlow flow) throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity service = waitForSingleStoreService(flow.groupId());
        final String serviceId = service.getComponent().getId();
        getClientUtil().waitForControllerServiceRunStatus(serviceId, "ENABLED");
        waitForCondition(() -> countRows(serviceId) > 0, "store row count > 0");

        return new MigratedStore(serviceId, readCreatedTimestamp(flow, serviceId));
    }

    /**
     * Asserts that the given operation left the migration-created Controller Service in place and the flow working.
     *
     * Two independent properties are checked. An unchanged creation timestamp proves the service was never torn down,
     * since removing a Controller Service clears its state and a replacement records a new timestamp. A row count that
     * keeps climbing afterwards proves the flow is doing work again, which a RUNNING state alone does not show: a
     * processor whose Controller Service reference is broken still reports RUNNING while writing nothing.
     *
     * The store is read through the service the processor references now rather than the one recorded earlier, so
     * that a service substituted by the operation is detected rather than silently passing.
     */
    private void assertStorePreserved(final MigratedFlow flow, final MigratedStore store, final String operation)
            throws NiFiClientException, IOException, InterruptedException {

        waitForCondition(() -> !findStoreServices(flow.groupId()).isEmpty(), "store Controller Service found");

        final List<ControllerServiceEntity> servicesAfter = findControllerServicesInGroup(flow.groupId());
        assertEquals(1, servicesAfter.size(),
                "The " + operation + " left " + servicesAfter.size() + " Controller Services in the group, so one was created alongside the migration-created one");

        final ControllerServiceEntity serviceAfter = servicesAfter.getFirst();
        assertEquals(store.serviceId(), serviceAfter.getComponent().getId(),
                "The " + operation + " replaced the migration-created Controller Service with a different one");
        assertEquals(store.serviceId(), getStoreServiceId(flow.processorId()));

        getClientUtil().waitForControllerServiceRunStatus(store.serviceId(), "ENABLED");
        getClientUtil().waitForRunningProcessor(flow.processorId());

        assertEquals(store.created(), readCreatedTimestamp(flow, store.serviceId()),
                "The migration-created Controller Service was removed during the " + operation + ": its store was created again, so the state it held is gone");

        final long rowsAfterOperation = countRows(getStoreServiceId(flow.processorId()));
        waitForCondition(() -> countRows(getStoreServiceId(flow.processorId())) > rowsAfterOperation, "store row count increase");
    }


    private ProcessorEntity findProcessor(final String groupId, final String name) throws NiFiClientException, IOException {
        final List<ProcessorEntity> matching = getNifiClient().getFlowClient().getProcessGroup(groupId).getProcessGroupFlow().getFlow().getProcessors().stream()
                .filter(processor -> PROCESSOR_TYPE.equals(processor.getComponent().getType()))
                .filter(processor -> name.equals(processor.getComponent().getName()))
                .toList();

        if (matching.size() != 1) {
            throw new AssertionError("Expected exactly one processor named " + name + " in group " + groupId + " but found " + matching.size());
        }

        return matching.getFirst();
    }

    private boolean hasProcessorNamed(final String groupId, final String name) throws NiFiClientException, IOException {
        return getNifiClient().getFlowClient().getProcessGroup(groupId).getProcessGroupFlow().getFlow().getProcessors().stream()
                .anyMatch(processor -> name.equals(processor.getComponent().getName()));
    }

    private List<ControllerServiceEntity> findControllerServicesInGroup(final String groupId) throws NiFiClientException, IOException {
        return getNifiClient().getFlowClient().getControllerServices(groupId).getControllerServices().stream()
                .filter(service -> groupId.equals(service.getComponent().getParentGroupId()))
                .toList();
    }

    private List<ControllerServiceEntity> findStoreServices(final String groupId) throws NiFiClientException, IOException {
        return findControllerServicesInGroup(groupId).stream()
                .filter(service -> STORE_SERVICE_TYPE.equals(service.getComponent().getType()))
                .toList();
    }

    private ControllerServiceEntity waitForSingleStoreService(final String groupId) throws NiFiClientException, IOException, InterruptedException {
        waitForCondition(() -> findStoreServices(groupId).size() == 1, "exactly one store Controller Service");
        return findStoreServices(groupId).getFirst();
    }

    private String getStoreServiceId(final String processorId) throws NiFiClientException, IOException {
        final Map<String, String> properties = getNifiClient().getProcessorClient().getProcessor(processorId).getComponent().getConfig().getProperties();
        return properties.get(STORE_SERVICE_PROPERTY);
    }

    /**
     * Reads the store's local component state. Removing a Controller Service clears its state, so an empty map means
     * either that the service has not been enabled yet or that it was torn down.
     */
    private Map<String, String> readStoreState(final String serviceId) throws NiFiClientException, IOException {
        final ComponentStateEntity stateEntity = getNifiClient().getControllerServicesClient().getControllerServiceState(serviceId);
        final ComponentStateDTO componentState = stateEntity.getComponentState();
        if (componentState == null || componentState.getLocalState() == null || componentState.getLocalState().getState() == null) {
            return Map.of();
        }

        final Map<String, String> state = new HashMap<>();
        for (final StateEntryDTO entry : componentState.getLocalState().getState()) {
            state.put(entry.getKey(), entry.getValue());
        }

        return state;
    }

    /**
     * Returns the timestamp recorded when the store was established, or null if the service is no longer in the group.
     * A service that has been removed outright has no state to read, and reporting that as a missing timestamp lets the
     * caller fail on the comparison rather than on a lookup error.
     */
    private String readCreatedTimestamp(final MigratedFlow flow, final String serviceId) throws NiFiClientException, IOException {
        final boolean stillPresent = findStoreServices(flow.groupId()).stream()
                .anyMatch(service -> serviceId.equals(service.getComponent().getId()));
        if (!stillPresent) {
            return null;
        }

        return readStoreState(serviceId).get(CREATED_STATE_KEY);
    }

    private long countRows(final String serviceId) throws NiFiClientException, IOException {
        final String rowCount = readStoreState(serviceId).get(ROW_COUNT_STATE_KEY);
        return rowCount == null ? 0 : Long.parseLong(rowCount);
    }

    /**
     * Polls the given condition until it holds, failing the test once the timeout elapses. Conditions query a NiFi that
     * may still be starting up or replacing components, so a failing query is treated as the condition not holding yet.
     */
    private void waitForCondition(final ExceptionalBooleanSupplier condition, final String conditionDescription) throws InterruptedException {
        final long deadline = System.nanoTime() + CONDITION_TIMEOUT.toNanos();

        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (final InterruptedException ie) {
                throw ie;
            } catch (final Exception ignored) {
            }

            Thread.sleep(CONDITION_POLL);
        }

        fail("Timed out after " + CONDITION_TIMEOUT + " waiting for " + conditionDescription);
    }

    private record MigratedFlow(String groupId, String processorId) {
    }

    private record MigratedStore(String serviceId, String created) {
    }

}
