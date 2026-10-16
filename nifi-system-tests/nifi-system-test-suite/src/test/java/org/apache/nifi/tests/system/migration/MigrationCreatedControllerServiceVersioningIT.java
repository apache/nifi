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
import org.apache.nifi.registry.flow.mapping.VersionedComponentFlowMapper;
import org.apache.nifi.tests.system.AbstractNarSwapMigrationIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.CopyRequestEntity;
import org.apache.nifi.web.api.entity.CopyResponseEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.PasteRequestEntity;
import org.apache.nifi.web.api.entity.PasteResponseEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a Controller Service created by property migration survives the operations a deployed flow goes
 * through: a runtime upgrade that introduces the migration, a plain runtime restart, and version changes of the
 * enclosing versioned Process Group.
 */
class MigrationCreatedControllerServiceVersioningIT extends AbstractNarSwapMigrationIT {
    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String LEGACY_FLOW_ID = "0831859c-f4a1-4ff6-99b3-58aeb35cb17f";
    private static final Path LEGACY_FLOW_SNAPSHOT = Path.of("src/test/resources/versioned-flows", TEST_FLOWS_BUCKET, LEGACY_FLOW_ID, "1", "snapshot.json");
    private static final String STORE_SERVICE_PROPERTY = "Store Service";
    private static final String STORE_SERVICE_TYPE = "org.apache.nifi.cs.tests.system.StateBackedStoreService";
    private static final String PROCESSOR_TYPE = "org.apache.nifi.processors.tests.system.MigrateToControllerService";
    private static final String MIGRATING_PROCESSOR_NAME = "MigrateToControllerService";
    private static final String ADDED_PROCESSOR_NAME = "Added Processor";
    private static final String CREATED_STATE_KEY = "created";
    private static final String ROW_COUNT_STATE_KEY = "rowCount";
    private static final long CONDITION_POLL_MILLIS = 100L;

    @TempDir
    Path registryDirectory;

    @Test
    void testRuntimeUpgradeCreatesEnabledReferencedService() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final MigratedFlow flow = flowIn(importLegacyFlow(registryClient));
        getClientUtil().waitForRunningProcessor(flow.processorId());

        upgradeRuntime();

        final MigratedStore store = awaitPopulatedStoreService(flow);
        final ControllerServiceEntity service = waitForSingleStoreService(flow.groupId());
        assertEquals(StandardControllerServiceFactory.MIGRATION_CREATED_COMMENT, service.getComponent().getComments());
        assertNotNull(service.getComponent().getVersionedComponentId());
        assertEquals(store.serviceId(), getStoreServiceId(flow.processorId()));
        getClientUtil().waitForRunningProcessor(flow.processorId());
        assertEquals(ProcessorDTO.VALID, getNifiClient().getProcessorClient().getProcessor(flow.processorId()).getComponent().getValidationStatus());
        getClientUtil().assertFlowUpToDate(flow.groupId());
    }

    @Test
    void testRestartPreservesStartupMigratedService() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final MigratedFlow flow = flowIn(importLegacyFlow(registryClient));
        getClientUtil().waitForRunningProcessor(flow.processorId());
        upgradeRuntime();
        final MigratedStore store = awaitPopulatedStoreService(flow);
        final String versionedId = waitForSingleStoreService(flow.groupId()).getComponent().getVersionedComponentId();
        assertNotNull(versionedId);

        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        assertStorePreserved(flow, store, "runtime restart");
        assertEquals(versionedId, waitForSingleStoreService(flow.groupId()).getComponent().getVersionedComponentId());
    }

    @Test
    void testUnrelatedFlowUpdatePreservesUndeclaredMigrationService() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity author = importLegacyFlow(registryClient);
        final MigratedFlow consumer = importFlow(registryClient, LEGACY_FLOW_ID, "1");
        getClientUtil().startProcessGroupComponents(consumer.groupId());
        getClientUtil().waitForRunningProcessor(consumer.processorId());

        final ProcessorEntity addedProcessor = getClientUtil().createProcessor(MIGRATING_PROCESSOR_NAME, author.getId());
        addedProcessor.getComponent().setName(ADDED_PROCESSOR_NAME);
        getNifiClient().getProcessorClient().updateProcessor(addedProcessor);
        saveFlowVersion(author.getId(), registryClient);

        upgradeRuntime();

        final MigratedStore store = awaitPopulatedStoreService(consumer);
        final String versionedId = waitForSingleStoreService(consumer.groupId()).getComponent().getVersionedComponentId();
        getClientUtil().assertFlowStaleAndUnmodified(consumer.groupId());

        final VersionedFlowUpdateRequestEntity update = getClientUtil().changeFlowVersion(consumer.groupId(), "2", false);

        assertNull(update.getRequest().getFailureReason());
        assertTrue(hasProcessorNamed(consumer.groupId(), ADDED_PROCESSOR_NAME));
        assertStorePreserved(consumer, store, "unrelated flow update");
        final ControllerServiceEntity service = waitForSingleStoreService(consumer.groupId());
        assertEquals(versionedId, service.getComponent().getVersionedComponentId());
        assertEquals(StandardControllerServiceFactory.MIGRATION_CREATED_COMMENT, service.getComponent().getComments());
        getClientUtil().assertFlowUpToDate(consumer.groupId());
    }

    @Test
    void testSavedFlowUpdatePreservesStartupMigratedService() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity author = importLegacyFlow(registryClient);
        final MigratedFlow consumer = importFlow(registryClient, LEGACY_FLOW_ID, "1");
        getClientUtil().startProcessGroupComponents(consumer.groupId());
        getClientUtil().waitForRunningProcessor(consumer.processorId());

        upgradeRuntime();

        final MigratedStore store = awaitPopulatedStoreService(consumer);
        saveMigratedFlow(author.getId(), registryClient);
        final ControllerServiceEntity authorService = waitForSingleStoreService(author.getId());
        assertNotEquals(authorService.getId(), store.serviceId());

        final VersionedFlowUpdateRequestEntity update = getClientUtil().changeFlowVersion(consumer.groupId(), "2", false);

        assertNull(update.getRequest().getFailureReason());
        assertStorePreserved(consumer, store, "saved flow update after startup migration");
        final ControllerServiceEntity service = waitForSingleStoreService(consumer.groupId());
        assertEquals(authorService.getComponent().getVersionedComponentId(), service.getComponent().getVersionedComponentId());
        assertEquals("", service.getComponent().getComments());
        getClientUtil().assertFlowUpToDate(consumer.groupId());
    }

    @Test
    void testSavedFlowUpdatePreservesImportMigratedService() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity author = importLegacyFlow(registryClient);

        upgradeRuntime();

        final MigratedFlow consumer = importFlow(registryClient, LEGACY_FLOW_ID, "1");
        startMigratedFlow(consumer);
        final MigratedStore store = awaitPopulatedStoreService(consumer);
        saveMigratedFlow(author.getId(), registryClient);
        final ControllerServiceEntity authorService = waitForSingleStoreService(author.getId());
        assertNotEquals(authorService.getId(), store.serviceId());

        final VersionedFlowUpdateRequestEntity update = getClientUtil().changeFlowVersion(consumer.groupId(), "2", false);

        assertNull(update.getRequest().getFailureReason());
        assertStorePreserved(consumer, store, "saved flow update after import migration");
        final ControllerServiceEntity service = waitForSingleStoreService(consumer.groupId());
        assertEquals(authorService.getComponent().getVersionedComponentId(), service.getComponent().getVersionedComponentId());
        assertEquals("", service.getComponent().getComments());
        getClientUtil().assertFlowUpToDate(consumer.groupId());
    }

    @Test
    void testFlowUpdateMigrationUsesSameServiceIdentityAsStartup() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity author = getClientUtil().createProcessGroup("Store Flow", "root");
        final VersionControlInformationEntity versionControl = getClientUtil().startVersionControl(author, registryClient, TEST_FLOWS_BUCKET, "Store Flow");
        final String flowId = versionControl.getVersionControlInformation().getFlowId();
        createLegacyProcessor(author.getId());
        saveFlowVersion(author.getId(), registryClient);
        final ProcessGroupEntity consumer = getClientUtil().importFlowFromRegistry("root", registryClient.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        assertTrue(findControllerServicesInGroup(consumer.getId()).isEmpty());

        upgradeRuntime();

        final ControllerServiceEntity startupService = waitForSingleStoreService(author.getId());
        final String startupVersionedId = startupService.getComponent().getVersionedComponentId();
        assertNotNull(startupVersionedId);

        final VersionedFlowUpdateRequestEntity update = getClientUtil().changeFlowVersion(consumer.getId(), "2", false);

        assertNull(update.getRequest().getFailureReason());
        final ControllerServiceEntity service = waitForSingleStoreService(consumer.getId());
        assertEquals(startupVersionedId, service.getComponent().getVersionedComponentId());
        assertNotEquals(startupService.getId(), service.getId());
        assertEquals(service.getId(), getStoreServiceId(findProcessor(consumer.getId(), MIGRATING_PROCESSOR_NAME).getId()));
        assertEquals(StandardControllerServiceFactory.MIGRATION_CREATED_COMMENT, service.getComponent().getComments());
        assertEquals(flowId, getNifiClient().getVersionsClient().getVersionControlInfo(consumer.getId()).getVersionControlInformation().getFlowId());
        assertEquals("2", getNifiClient().getVersionsClient().getVersionControlInfo(consumer.getId()).getVersionControlInformation().getVersion());
    }

    @Test
    void testNestedVersionedFlowMigrationPreservesServiceAcrossParentUpdate() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity childSource = importLegacyFlow(registryClient);
        final String flowId = getNifiClient().getVersionsClient().getVersionControlInfo(childSource.getId()).getVersionControlInformation().getFlowId();
        final ProcessGroupEntity authorParent = getClientUtil().createProcessGroup("Versioned Parent", "root");
        final ProcessGroupEntity authorChild = getClientUtil().importFlowFromRegistry(authorParent.getId(), registryClient.getId(),
                TEST_FLOWS_BUCKET, flowId, "1");
        final VersionControlInformationEntity parentVersionControl = getClientUtil().startVersionControl(authorParent, registryClient, TEST_FLOWS_BUCKET, "Parent Flow");
        final String parentFlowId = parentVersionControl.getVersionControlInformation().getFlowId();
        assertNotEquals(flowId, parentFlowId);

        final ProcessGroupEntity consumerParent = getClientUtil().importFlowFromRegistry("root", registryClient.getId(), TEST_FLOWS_BUCKET, parentFlowId, "1");
        final List<ProcessGroupEntity> consumerChildren = getNifiClient().getFlowClient().getProcessGroup(consumerParent.getId())
                .getProcessGroupFlow().getFlow().getProcessGroups().stream().toList();
        assertEquals(1, consumerChildren.size());
        final String consumerChildId = consumerChildren.getFirst().getId();
        final MigratedFlow consumerFlow = new MigratedFlow(consumerChildId, findProcessor(consumerChildId, MIGRATING_PROCESSOR_NAME).getId());
        assertEquals(parentFlowId, getNifiClient().getVersionsClient().getVersionControlInfo(consumerParent.getId()).getVersionControlInformation().getFlowId());
        assertEquals(flowId, getNifiClient().getVersionsClient().getVersionControlInfo(consumerChildId).getVersionControlInformation().getFlowId());
        final String childWrapperId = getNifiClient().getProcessGroupClient().getProcessGroup(consumerChildId).getComponent().getVersionedComponentId();
        assertNotNull(childWrapperId);
        assertNotEquals(flowId, childWrapperId);
        getClientUtil().startProcessGroupComponents(authorParent.getId());
        getClientUtil().startProcessGroupComponents(consumerParent.getId());
        getClientUtil().waitForRunningProcessor(consumerFlow.processorId());

        upgradeRuntime();

        final MigratedStore consumerStore = awaitPopulatedStoreService(consumerFlow);
        final ControllerServiceEntity nestedService = waitForSingleStoreService(consumerChildId);
        final String nestedVersionedId = nestedService.getComponent().getVersionedComponentId();
        assertNotNull(nestedVersionedId);
        final ProcessGroupEntity standaloneChild = getClientUtil().importFlowFromRegistry("root", registryClient.getId(),
                TEST_FLOWS_BUCKET, flowId, "1");
        final ControllerServiceEntity standaloneService = waitForSingleStoreService(standaloneChild.getId());
        assertEquals(standaloneService.getComponent().getVersionedComponentId(), nestedVersionedId,
                "The same child flow must have the same migration service identity standalone and nested in a versioned parent");
        assertNotEquals(standaloneService.getId(), nestedService.getId());

        final ControllerServiceEntity authorService = waitForSingleStoreService(authorChild.getId());
        assertEquals(nestedVersionedId, authorService.getComponent().getVersionedComponentId());
        saveMigratedFlow(authorChild.getId(), registryClient);
        saveFlowVersion(authorParent.getId(), registryClient);

        final VersionedFlowUpdateRequestEntity update = getClientUtil().changeFlowVersion(consumerParent.getId(), "2", false);
        assertNull(update.getRequest().getFailureReason());
        assertEquals("2", getNifiClient().getVersionsClient().getVersionControlInfo(consumerParent.getId()).getVersionControlInformation().getVersion());
        assertEquals("2", getNifiClient().getVersionsClient().getVersionControlInfo(consumerChildId).getVersionControlInformation().getVersion());
        assertStorePreserved(consumerFlow, consumerStore, "parent flow update containing an independently versioned child");
        assertEquals(nestedVersionedId, waitForSingleStoreService(consumerChildId).getComponent().getVersionedComponentId());
        assertEquals("", waitForSingleStoreService(consumerChildId).getComponent().getComments());
    }

    @Test
    void testPasteIntoSameVersionedFlowUsesSameServiceIdentity() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity source = importLegacyFlow(registryClient);
        final CopyResponseEntity copied = copyLegacyProcessor(source.getId());
        final ProcessGroupEntity author = getClientUtil().createProcessGroup("Paste Target", "root");
        final VersionControlInformationEntity versionControl = getClientUtil().startVersionControl(author, registryClient, TEST_FLOWS_BUCKET, "Paste Target");
        final String flowId = versionControl.getVersionControlInformation().getFlowId();
        final ProcessGroupEntity consumer = getClientUtil().importFlowFromRegistry("root", registryClient.getId(), TEST_FLOWS_BUCKET, flowId, "1");

        upgradeRuntime();

        final MigratedFlow authorFlow = pasteProcessor(author.getId(), copied);
        final MigratedFlow consumerFlow = pasteProcessor(consumer.getId(), copied);

        final ControllerServiceEntity authorService = waitForSingleStoreService(authorFlow.groupId());
        final ControllerServiceEntity consumerService = waitForSingleStoreService(consumerFlow.groupId());
        final String versionedId = authorService.getComponent().getVersionedComponentId();
        assertNotNull(versionedId);
        assertEquals(versionedId, consumerService.getComponent().getVersionedComponentId());
        assertNotEquals(authorService.getId(), consumerService.getId());
        assertNotEquals(waitForSingleStoreService(source.getId()).getComponent().getVersionedComponentId(), versionedId);
        assertEquals(authorService.getId(), getStoreServiceId(authorFlow.processorId()));
        assertEquals(consumerService.getId(), getStoreServiceId(consumerFlow.processorId()));
    }

    @Test
    void testPasteIntoCanvasCreatesServiceWithoutVersionedIdentity() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity source = importLegacyFlow(registryClient);
        final CopyResponseEntity copied = copyLegacyProcessor(source.getId());
        final String canvasId = getNifiClient().getProcessGroupClient().getProcessGroup("root").getId();

        upgradeRuntime();

        final MigratedFlow pasted = pasteProcessor(canvasId, copied);

        final ControllerServiceEntity service = waitForSingleStoreService(canvasId);
        assertNull(service.getComponent().getVersionedComponentId());
        assertEquals(service.getId(), getStoreServiceId(pasted.processorId()));
        startMigratedFlow(pasted);
        awaitPopulatedStoreService(pasted);
    }

    @Test
    void testRestartPreservesServiceCreatedByPasteIntoVersionedFlow() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity source = importLegacyFlow(registryClient);
        final CopyResponseEntity copied = copyLegacyProcessor(source.getId());
        final ProcessGroupEntity destination = getClientUtil().createProcessGroup("Paste Target", "root");
        getClientUtil().startVersionControl(destination, registryClient, TEST_FLOWS_BUCKET, "Paste Target");

        upgradeRuntime();

        final MigratedFlow pasted = pasteProcessor(destination.getId(), copied);
        startMigratedFlow(pasted);
        final MigratedStore store = awaitPopulatedStoreService(pasted);
        final String versionedId = waitForSingleStoreService(pasted.groupId()).getComponent().getVersionedComponentId();
        assertNotNull(versionedId);

        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        assertStorePreserved(pasted, store, "restart after paste into versioned flow");
        assertEquals(versionedId, waitForSingleStoreService(pasted.groupId()).getComponent().getVersionedComponentId());
    }

    @Test
    void testRestartPreservesCanvasServiceCreatedByPaste() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ProcessGroupEntity source = importLegacyFlow(registryClient);
        final CopyResponseEntity copied = copyLegacyProcessor(source.getId());
        final String canvasId = getNifiClient().getProcessGroupClient().getProcessGroup("root").getId();

        upgradeRuntime();

        final MigratedFlow pasted = pasteProcessor(canvasId, copied);
        startMigratedFlow(pasted);
        final MigratedStore store = awaitPopulatedStoreService(pasted);
        assertNull(waitForSingleStoreService(canvasId).getComponent().getVersionedComponentId());

        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        assertStorePreserved(pasted, store, "restart after paste into canvas");
        assertEquals(VersionedComponentFlowMapper.generateVersionedComponentId(store.serviceId()),
                waitForSingleStoreService(canvasId).getComponent().getVersionedComponentId());
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
            throws NiFiClientException, IOException, InterruptedException {
        return flowIn(getClientUtil().importFlowFromRegistry("root", registryClient.getId(), TEST_FLOWS_BUCKET, flowId, version));
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

    private void saveMigratedFlow(final String groupId, final FlowRegistryClientEntity registryClient)
            throws NiFiClientException, IOException, InterruptedException {
        clearMigrationComment(waitForSingleStoreService(groupId), findProcessor(groupId, MIGRATING_PROCESSOR_NAME));
        saveFlowVersion(groupId, registryClient);
    }

    private CopyResponseEntity copyLegacyProcessor(final String groupId) throws NiFiClientException, IOException {
        final CopyRequestEntity request = new CopyRequestEntity();
        request.setProcessors(Set.of(findProcessor(groupId, MIGRATING_PROCESSOR_NAME).getId()));
        final CopyResponseEntity copied = getNifiClient().getProcessGroupClient().copy(groupId, request);
        assertTrue(copied.getExternalControllerServiceReferences().isEmpty());
        return copied;
    }

    private MigratedFlow pasteProcessor(final String groupId, final CopyResponseEntity copied) throws NiFiClientException, IOException {
        final ProcessGroupEntity target = getNifiClient().getProcessGroupClient().getProcessGroup(groupId);
        final PasteRequestEntity request = new PasteRequestEntity();
        request.setRevision(target.getRevision());
        request.setCopyResponse(copied);
        final PasteResponseEntity pasted = getNifiClient().getProcessGroupClient().paste(groupId, request);
        assertEquals(1, pasted.getFlow().getProcessors().size());
        return new MigratedFlow(groupId, pasted.getFlow().getProcessors().iterator().next().getId());
    }

    private void clearMigrationComment(final ControllerServiceEntity service, final ProcessorEntity processor)
            throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().stopProcessor(processor);
        getClientUtil().disableControllerService(service);
        getClientUtil().waitForControllerServiceRunStatus(service.getId(), "DISABLED");
        final ControllerServiceEntity disabledService = getNifiClient().getControllerServicesClient().getControllerService(service.getId());
        disabledService.getComponent().setComments("");
        final ControllerServiceEntity updatedService = getNifiClient().getControllerServicesClient().updateControllerService(disabledService);
        getClientUtil().enableControllerService(updatedService);
        getClientUtil().waitForControllerServiceRunStatus(service.getId(), "ENABLED");
        getClientUtil().startProcessor(getNifiClient().getProcessorClient().getProcessor(processor.getId()));
        getClientUtil().waitForRunningProcessor(processor.getId());
    }

    private ProcessGroupEntity importLegacyFlow(final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException, InterruptedException {
        final Path versionDirectory = registryDirectory.resolve(TEST_FLOWS_BUCKET).resolve(LEGACY_FLOW_ID).resolve("1");
        Files.createDirectories(versionDirectory);
        Files.copy(LEGACY_FLOW_SNAPSHOT, versionDirectory.resolve("snapshot.json"));
        final ProcessGroupEntity group = getClientUtil().importFlowFromRegistry("root", registryClient.getId(), TEST_FLOWS_BUCKET, LEGACY_FLOW_ID, "1");
        getClientUtil().startProcessGroupComponents(group.getId());
        return group;
    }

    private ProcessorEntity createLegacyProcessor(final String groupId) throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity processor = getClientUtil().createProcessor(MIGRATING_PROCESSOR_NAME, groupId);
        processor = getClientUtil().updateProcessorProperties(processor, Map.of("Store Name", "store"));
        processor = getClientUtil().updateProcessorSchedulingPeriod(processor, "100 ms");
        processor = getClientUtil().setAutoTerminatedRelationships(processor, "success");
        getClientUtil().startProcessor(processor);
        getClientUtil().waitForRunningProcessor(processor.getId());
        return getNifiClient().getProcessorClient().getProcessor(processor.getId());
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
