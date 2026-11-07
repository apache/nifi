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

import org.apache.nifi.tests.system.AbstractNarSwapMigrationIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that startup and import migration create matching Controller Service identities and that
 * flow version changes preserve the service instance, references, and state.
 */
class MigrationCreatedControllerServiceVersioningIT extends AbstractNarSwapMigrationIT {

    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String FLOW_ID = "0831859c-f4a1-4ff6-99b3-58aeb35cb17f";
    private static final String FLOW_DEFINITION_FILENAME = "snapshot.json";
    private static final String VERSION_1 = "1";
    private static final String VERSION_2 = "2";
    private static final String VERSION_3 = "3";
    private static final Path FLOW_SNAPSHOT = Path.of("src/test/resources/versioned-flows", TEST_FLOWS_BUCKET, FLOW_ID, VERSION_1, FLOW_DEFINITION_FILENAME);

    private static final String STORE_SERVICE_PROPERTY = "Store Service";
    private static final String STORE_SERVICE_TYPE = TEST_CS_PACKAGE + ".StateBackedStoreService";

    private static final String MIGRATING_PROCESSOR_NAME = "MigrateToControllerService";
    private static final String PROCESSOR_TYPE = TEST_PROCESSORS_PACKAGE + "." + MIGRATING_PROCESSOR_NAME;
    private static final String ADDED_PROCESSOR_NAME = "Added Processor";

    private static final String ROOT_PG = "root";

    private static final String CREATED_STATE_KEY = "created";
    private static final String ROW_COUNT_STATE_KEY = "rowCount";
    private static final long CONDITION_POLL_MILLIS = 100L;

    @TempDir
    Path registryDirectory;

    @Test
    void testStartupAndImportMigratedServicesSurviveFlowVersionUpdates() throws NiFiClientException, IOException, InterruptedException {
        // Import V1 as the author flow before property migration is introduced.
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final MigratedFlow authorFlow = flowIn(importInitialFlow(registryClient));
        getClientUtil().waitForRunningProcessor(authorFlow.processorId());

        // Publish V2 with an unrelated Processor, without declaring a Controller Service.
        final ProcessorEntity addedProcessor = getClientUtil().createProcessor(MIGRATING_PROCESSOR_NAME, authorFlow.groupId());
        addedProcessor.getComponent().setName(ADDED_PROCESSOR_NAME);
        getNifiClient().getProcessorClient().updateProcessor(addedProcessor);
        saveFlowVersion(authorFlow.groupId(), registryClient);

        // Import consumer V1 before the runtime upgrade to verify startup migration.
        final MigratedFlow startupConsumerFlow = importFlow(registryClient, FLOW_ID, VERSION_1);
        getClientUtil().startProcessGroupComponents(startupConsumerFlow.groupId());
        getClientUtil().waitForRunningProcessor(startupConsumerFlow.processorId());

        upgradeRuntime();

        // Startup migration creates an enabled, referenced Controller Service with a versioned ID in the author flow.
        final MigratedStore authorStore = awaitPopulatedStoreService(authorFlow);
        final ControllerServiceEntity authorService = waitForSingleStoreService(authorFlow.groupId());
        final String versionedId = authorService.getComponent().getVersionedComponentId();
        assertNotNull(versionedId);
        assertEquals(authorStore.serviceId(), getStoreServiceId(authorFlow.processorId()));
        getClientUtil().waitForRunningProcessor(authorFlow.processorId());
        assertEquals(ProcessorDTO.VALID, getNifiClient().getProcessorClient().getProcessor(authorFlow.processorId()).getComponent().getValidationStatus());
        getClientUtil().assertFlowUpToDate(authorFlow.groupId());

        // The startup consumer has a distinct Controller Service instance with the same versioned ID as the author's.
        final MigratedStore startupStore = awaitPopulatedStoreService(startupConsumerFlow);
        assertNotEquals(authorStore.serviceId(), startupStore.serviceId());
        assertEquals(startupStore.serviceId(), getStoreServiceId(startupConsumerFlow.processorId()));
        assertEquals(versionedId, waitForSingleStoreService(startupConsumerFlow.groupId()).getComponent().getVersionedComponentId());
        getClientUtil().assertFlowStaleAndUnmodified(startupConsumerFlow.groupId());

        // Import V2 after the runtime upgrade to verify import migration produces the same Controller Service versioned ID.
        final MigratedFlow importConsumerFlow = importFlow(registryClient, FLOW_ID, VERSION_2);
        startMigratedFlow(importConsumerFlow);
        final MigratedStore importStore = awaitPopulatedStoreService(importConsumerFlow);
        assertNotEquals(authorStore.serviceId(), importStore.serviceId());
        assertNotEquals(startupStore.serviceId(), importStore.serviceId());
        assertEquals(importStore.serviceId(), getStoreServiceId(importConsumerFlow.processorId()));
        assertEquals(versionedId, waitForSingleStoreService(importConsumerFlow.groupId()).getComponent().getVersionedComponentId());
        getClientUtil().assertFlowUpToDate(importConsumerFlow.groupId());

        // Updating to V2 preserves the startup consumer's Controller Service and state, although V2 does not declare the Controller Service.
        final VersionedFlowUpdateRequestEntity unrelatedUpdate = getClientUtil().changeFlowVersion(startupConsumerFlow.groupId(), VERSION_2, false);
        assertNull(unrelatedUpdate.getRequest().getFailureReason());
        assertTrue(hasProcessorNamed(startupConsumerFlow.groupId(), ADDED_PROCESSOR_NAME));
        assertStorePreserved(startupConsumerFlow, startupStore, "unrelated flow update");
        assertEquals(versionedId, waitForSingleStoreService(startupConsumerFlow.groupId()).getComponent().getVersionedComponentId());
        getClientUtil().assertFlowUpToDate(startupConsumerFlow.groupId());

        // Publish V3 with the migration-created Controller Service and a renamed Processor.
        final String renamedProcessorName = "Renamed Migrating Processor";
        renameProcessor(authorFlow.processorId(), renamedProcessorName);
        saveFlowVersion(authorFlow.groupId(), registryClient);
        getClientUtil().assertFlowUpToDate(authorFlow.groupId());

        // Updating to V3 adopts the published Controller Service without replacing its instance or state.
        final VersionedFlowUpdateRequestEntity startupUpdate = getClientUtil().changeFlowVersion(startupConsumerFlow.groupId(), VERSION_3, false);
        assertNull(startupUpdate.getRequest().getFailureReason());
        assertStorePreserved(startupConsumerFlow, startupStore, "saved flow update after startup migration");
        assertEquals(versionedId, waitForSingleStoreService(startupConsumerFlow.groupId()).getComponent().getVersionedComponentId());
        assertEquals(renamedProcessorName, getNifiClient().getProcessorClient().getProcessor(startupConsumerFlow.processorId()).getComponent().getName());
        getClientUtil().assertFlowUpToDate(startupConsumerFlow.groupId());

        // The import consumer also adopts V3 while retaining its Controller Service instance and state.
        final VersionedFlowUpdateRequestEntity importUpdate = getClientUtil().changeFlowVersion(importConsumerFlow.groupId(), VERSION_3, false);
        assertNull(importUpdate.getRequest().getFailureReason());
        assertStorePreserved(importConsumerFlow, importStore, "saved flow update after import migration");
        assertEquals(versionedId, waitForSingleStoreService(importConsumerFlow.groupId()).getComponent().getVersionedComponentId());
        assertEquals(renamedProcessorName, getNifiClient().getProcessorClient().getProcessor(importConsumerFlow.processorId()).getComponent().getName());
        getClientUtil().assertFlowUpToDate(importConsumerFlow.groupId());
    }

    private void upgradeRuntime() throws IOException {
        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);
    }

    private MigratedFlow flowIn(final ProcessGroupEntity group) throws NiFiClientException, IOException {
        return new MigratedFlow(group.getId(), findProcessor(group.getId(), MIGRATING_PROCESSOR_NAME).getId());
    }

    private MigratedFlow importFlow(final FlowRegistryClientEntity registryClient, final String flowId, final String version)
            throws NiFiClientException, IOException {
        return flowIn(getClientUtil().importFlowFromRegistry(ROOT_PG, registryClient.getId(), TEST_FLOWS_BUCKET, flowId, version));
    }

    private void startMigratedFlow(final MigratedFlow flow) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().enableControllerServices(flow.groupId(), true);
        getClientUtil().startProcessor(getNifiClient().getProcessorClient().getProcessor(flow.processorId()));
        getClientUtil().waitForRunningProcessor(flow.processorId());
    }

    private void saveFlowVersion(final String groupId, final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException {
        getClientUtil().saveFlowVersion(getNifiClient().getProcessGroupClient().getProcessGroup(groupId), registryClient,
                getNifiClient().getVersionsClient().getVersionControlInfo(groupId));
    }

    private void renameProcessor(final String processorId, final String name) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().stopProcessor(getNifiClient().getProcessorClient().getProcessor(processorId));
        final ProcessorEntity processor = getNifiClient().getProcessorClient().getProcessor(processorId);
        processor.getComponent().setName(name);
        getNifiClient().getProcessorClient().updateProcessor(processor);
        getClientUtil().startProcessor(getNifiClient().getProcessorClient().getProcessor(processorId));
        getClientUtil().waitForRunningProcessor(processorId);
    }

    private ProcessGroupEntity importInitialFlow(final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException, InterruptedException {
        final Path versionDirectory = registryDirectory.resolve(TEST_FLOWS_BUCKET).resolve(FLOW_ID).resolve(VERSION_1);
        Files.createDirectories(versionDirectory);
        Files.copy(FLOW_SNAPSHOT, versionDirectory.resolve(FLOW_DEFINITION_FILENAME));
        final ProcessGroupEntity group = getClientUtil().importFlowFromRegistry(ROOT_PG, registryClient.getId(), TEST_FLOWS_BUCKET, FLOW_ID, VERSION_1);
        getClientUtil().startProcessGroupComponents(group.getId());
        return group;
    }

    /**
     * Waits until the migration-created Controller Service is enabled and its store has accumulated at least one row,
     * so that the flow is known to be working before the operation under test runs, and returns its identifier.
     */
    private MigratedStore awaitPopulatedStoreService(final MigratedFlow flow) throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity service = waitForSingleStoreService(flow.groupId());
        final String serviceId = service.getComponent().getId();
        getClientUtil().waitForControllerServiceRunStatus(serviceId, "ENABLED");
        waitFor(() -> countRows(serviceId) > 0, CONDITION_POLL_MILLIS, "store row count > 0");

        final String createdTimestamp = readCreatedTimestamp(flow, serviceId);
        assertNotNull(createdTimestamp);
        return new MigratedStore(serviceId, createdTimestamp);
    }

    private void assertStorePreserved(final MigratedFlow flow, final MigratedStore store, final String operation)
            throws NiFiClientException, IOException, InterruptedException {

        waitFor(() -> !findStoreServices(flow.groupId()).isEmpty(), CONDITION_POLL_MILLIS, "store Controller Service found");

        final List<ControllerServiceEntity> servicesAfter = findControllerServicesInGroup(flow.groupId());
        assertEquals(1, servicesAfter.size(), operation);

        final ControllerServiceEntity serviceAfter = servicesAfter.getFirst();
        final String remainingServiceId = serviceAfter.getComponent().getId();
        assertEquals(store.serviceId(), remainingServiceId, operation + " replaced the Controller Service");

        final String referencedServiceId = getStoreServiceId(flow.processorId());
        assertEquals(store.serviceId(), referencedServiceId);

        getClientUtil().waitForControllerServiceRunStatus(store.serviceId(), "ENABLED");
        getClientUtil().waitForRunningProcessor(flow.processorId());

        final String createdTimestamp = readCreatedTimestamp(flow, store.serviceId());
        assertEquals(store.created(), createdTimestamp, "store was recreated during " + operation);

        final long rowsAfterOperation = countRows(getStoreServiceId(flow.processorId()));
        waitFor(() -> countRows(getStoreServiceId(flow.processorId())) > rowsAfterOperation, CONDITION_POLL_MILLIS, "store row count increase");
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
        waitFor(() -> findStoreServices(groupId).size() == 1, CONDITION_POLL_MILLIS, "exactly one store Controller Service");
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

    private record MigratedFlow(String groupId, String processorId) {
    }

    private record MigratedStore(String serviceId, String created) {
    }
}
