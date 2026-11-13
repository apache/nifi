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
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
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
 * Verifies that Controller Services created by property migration get versioned IDs that match between the flow author
 * and its consumers, so that flow version changes keep each consumer's service instance, state, and secret, and never
 * point a processor at another processor's service.
 *
 * All flows are set up on the old extensions and checked after a single runtime upgrade.
 */
class MigrationCreatedControllerServiceVersioningIT extends AbstractNarSwapMigrationIT {

    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String FLOW_ID = "0831859c-f4a1-4ff6-99b3-58aeb35cb17f";
    private static final String FLOW_DEFINITION_FILENAME = "snapshot.json";
    private static final String VERSION_1 = "1";
    private static final String VERSION_2 = "2";
    private static final String VERSION_3 = "3";
    private static final Path FLOW_SNAPSHOT = Path.of("src/test/resources/versioned-flows", TEST_FLOWS_BUCKET, FLOW_ID, VERSION_1, FLOW_DEFINITION_FILENAME);

    private static final String STORE_NAME_PROPERTY = "Store Name";
    private static final String STORE_PASSWORD_PROPERTY = "Store Password";
    private static final String STORE_SERVICE_PROPERTY = "Store Service";
    private static final String STORE_SERVICE_TYPE = TEST_CS_PACKAGE + ".StateBackedStoreService";
    private static final String STORE_NAME = "store";

    private static final String MIGRATING_PROCESSOR_NAME = "MigrateToControllerService";
    private static final String PROCESSOR_TYPE = TEST_PROCESSORS_PACKAGE + "." + MIGRATING_PROCESSOR_NAME;
    private static final String ADDED_PROCESSOR_NAME = "Added Processor";
    private static final String RENAMED_PROCESSOR_NAME = "Renamed Migrating Processor";
    private static final String FIRST_PROCESSOR = "First Processor";
    private static final String SECOND_PROCESSOR = "Second Processor";

    private static final String SECRETS_CONTEXT = "Store Secrets";
    private static final String FIRST_SECRET = "first-secret";
    private static final String SECOND_SECRET = "second-secret";
    private static final String FIRST_SECRET_REFERENCE = "#{first-password}";
    private static final String SECOND_SECRET_REFERENCE = "#{second-password}";

    private static final String ROOT_PG = "root";

    private static final String CREATED_STATE_KEY = "created";
    private static final String PASSWORD_STATE_KEY = "password";
    private static final long CONDITION_POLL_MILLIS = 100L;

    @TempDir
    Path registryDirectory;

    @Test
    void testMigrationCreatedServicesSurviveFlowVersionUpdates() throws NiFiClientException, IOException, InterruptedException {
        Files.createDirectories(registryDirectory.resolve(TEST_FLOWS_BUCKET));
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());
        final ParameterContextEntity secrets = createSecretsParameterContext();

        // Before the runtime upgrade, every flow is created on the old extensions, which have no Controller Service yet.
        // The author and a consumer import a legacy flow; after the upgrade, one consumer migrates at startup and another at import.
        final MigrationPathFlows migrationPaths = setUpMigrationPathFlows(registryClient);

        // The author's two processors request identical configuration, so they will share one service.
        final LaggingConsumerFlows laggingWithSharedService = setUpLaggingConsumerFlows(registryClient, secrets, "Shared Service", FIRST_SECRET_REFERENCE, FIRST_SECRET_REFERENCE);

        // The author's two processors use different secrets, so they will get separate services.
        final LaggingConsumerFlows laggingWithSeparateServices = setUpLaggingConsumerFlows(registryClient, secrets, "Separate Services", FIRST_SECRET_REFERENCE, SECOND_SECRET_REFERENCE);

        // The author's processors share one inline secret, while each consumer processor has its own inline secret.
        final InlineSecretFlows inlineSecrets = setUpInlineSecretFlows(registryClient);

        upgradeRuntime();

        final MigrationPathConsumers migrationPathConsumers = verifyStartupAndImportMigrationsMatchAuthor(registryClient, migrationPaths);
        verifyAuthorSharesUnversionedServiceWithInlineSecret(inlineSecrets);

        // Each author saves a version that declares the services created by migration.
        renameProcessor(migrationPaths.author().processorId(), RENAMED_PROCESSOR_NAME);
        saveFlowVersion(migrationPaths.author().groupId(), registryClient);
        saveFlowVersion(laggingWithSharedService.authorGroupId(), registryClient);
        saveFlowVersion(laggingWithSeparateServices.authorGroupId(), registryClient);
        saveFlowVersion(inlineSecrets.authorGroupId(), registryClient);

        verifyMigrationPathConsumersAdoptVersionWithService(migrationPathConsumers);
        verifyLaggingConsumerMatchesSharedService(laggingWithSharedService);
        verifyLaggingConsumerMatchesSeparateServices(laggingWithSeparateServices);
        verifyServiceWithInlineSecretIsNotReusedByOtherProcessors(inlineSecrets);
    }

    /**
     * The author imports a legacy V1 and saves V2 with an unrelated processor; the startup consumer imports V1.
     */
    private MigrationPathFlows setUpMigrationPathFlows(final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow authorFlow = flowIn(importInitialFlow(registryClient));
        getClientUtil().waitForRunningProcessor(authorFlow.processorId());

        final ProcessorEntity addedProcessor = getClientUtil().createProcessor(MIGRATING_PROCESSOR_NAME, authorFlow.groupId());
        addedProcessor.getComponent().setName(ADDED_PROCESSOR_NAME);
        getNifiClient().getProcessorClient().updateProcessor(addedProcessor);
        saveFlowVersion(authorFlow.groupId(), registryClient);

        final MigratedFlow startupConsumerFlow = importFlow(registryClient, VERSION_1);
        getClientUtil().startProcessGroupComponents(startupConsumerFlow.groupId());
        getClientUtil().waitForRunningProcessor(startupConsumerFlow.processorId());

        return new MigrationPathFlows(authorFlow, startupConsumerFlow);
    }

    /**
     * The author saves V1 with the first processor and V2 with both processors. The consumer imports V1 and stays on it,
     * so after the upgrade it migrates only the first processor and then skips V2.
     */
    private LaggingConsumerFlows setUpLaggingConsumerFlows(final FlowRegistryClientEntity registryClient, final ParameterContextEntity secrets, final String flowName,
                                                           final String firstPassword, final String secondPassword) throws NiFiClientException, IOException {
        final ProcessGroupEntity authorGroup = getClientUtil().createProcessGroup(flowName, ROOT_PG);
        getClientUtil().setParameterContext(authorGroup.getId(), secrets);
        createMigratingProcessor(authorGroup.getId(), FIRST_PROCESSOR, firstPassword);
        final String flowId = startVersionControl(authorGroup.getId(), registryClient, flowName);

        createMigratingProcessor(authorGroup.getId(), SECOND_PROCESSOR, secondPassword);
        saveFlowVersion(authorGroup.getId(), registryClient);

        final ProcessGroupEntity consumerGroup = getClientUtil().importFlowFromRegistry(ROOT_PG, registryClient.getId(), TEST_FLOWS_BUCKET, flowId, VERSION_1);
        return new LaggingConsumerFlows(authorGroup.getId(), consumerGroup.getId());
    }

    /**
     * Both processors have the same inline password on the author, so they will share one service there once migrated.
     * On the consumer, each processor is given its own inline password, so migration will give them separate services.
     */
    private InlineSecretFlows setUpInlineSecretFlows(final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException {
        final String flowName = "Inline Secrets";
        final ProcessGroupEntity authorGroup = getClientUtil().createProcessGroup(flowName, ROOT_PG);
        createMigratingProcessor(authorGroup.getId(), FIRST_PROCESSOR, "author-secret");
        createMigratingProcessor(authorGroup.getId(), SECOND_PROCESSOR, "author-secret");
        final String flowId = startVersionControl(authorGroup.getId(), registryClient, flowName);

        final ProcessGroupEntity consumerGroup = getClientUtil().importFlowFromRegistry(ROOT_PG, registryClient.getId(), TEST_FLOWS_BUCKET, flowId, VERSION_1);
        setStorePassword(consumerGroup.getId(), FIRST_PROCESSOR, FIRST_SECRET);
        setStorePassword(consumerGroup.getId(), SECOND_PROCESSOR, SECOND_SECRET);
        return new InlineSecretFlows(authorGroup.getId(), consumerGroup.getId());
    }

    /**
     * Migration at startup and at import produces the author's versioned ID, and a version change to a version that does not declare
     * the service keeps the consumer's service instance and state. The import consumer imports V2 only after the upgrade.
     */
    private MigrationPathConsumers verifyStartupAndImportMigrationsMatchAuthor(final FlowRegistryClientEntity registryClient, final MigrationPathFlows flows)
            throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow authorFlow = flows.author();
        final MigratedFlow startupConsumerFlow = flows.startupConsumer();

        final Store authorStore = awaitEnabledStore(authorFlow);
        final String versionedId = authorStore.versionedId();
        assertNotNull(versionedId);
        getClientUtil().waitForRunningProcessor(authorFlow.processorId());
        assertEquals(ProcessorDTO.VALID, getNifiClient().getProcessorClient().getProcessor(authorFlow.processorId()).getComponent().getValidationStatus());
        getClientUtil().assertFlowUpToDate(authorFlow.groupId());

        final Store startupStore = awaitEnabledStore(startupConsumerFlow);
        assertNotEquals(authorStore.serviceId(), startupStore.serviceId());
        assertEquals(versionedId, startupStore.versionedId());
        getClientUtil().assertFlowStaleAndUnmodified(startupConsumerFlow.groupId());

        final MigratedFlow importConsumerFlow = importFlow(registryClient, VERSION_2);
        startMigratedFlow(importConsumerFlow);
        final Store importStore = awaitEnabledStore(importConsumerFlow);
        assertNotEquals(authorStore.serviceId(), importStore.serviceId());
        assertNotEquals(startupStore.serviceId(), importStore.serviceId());
        assertEquals(versionedId, importStore.versionedId());
        getClientUtil().assertFlowUpToDate(importConsumerFlow.groupId());

        // V2 does not declare the service, but the processor still references it, so it is kept.
        getClientUtil().changeFlowVersion(startupConsumerFlow.groupId(), VERSION_2);
        assertTrue(hasProcessorNamed(startupConsumerFlow.groupId(), ADDED_PROCESSOR_NAME));
        assertSameServiceAndState(startupStore, awaitEnabledStore(startupConsumerFlow), "update to a version without the service");
        getClientUtil().assertFlowUpToDate(startupConsumerFlow.groupId());

        return new MigrationPathConsumers(startupConsumerFlow, startupStore, importConsumerFlow, importStore);
    }

    /**
     * Both consumers adopt the version that declares the service without replacing their service instance or state.
     */
    private void verifyMigrationPathConsumersAdoptVersionWithService(final MigrationPathConsumers consumers) throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow startupConsumerFlow = consumers.startupConsumer();
        getClientUtil().changeFlowVersion(startupConsumerFlow.groupId(), VERSION_3);
        assertSameServiceAndState(consumers.startupStore(), awaitEnabledStore(startupConsumerFlow), "update to a version with the service");
        assertEquals(RENAMED_PROCESSOR_NAME, getNifiClient().getProcessorClient().getProcessor(startupConsumerFlow.processorId()).getComponent().getName());
        getClientUtil().assertFlowUpToDate(startupConsumerFlow.groupId());

        final MigratedFlow importConsumerFlow = consumers.importConsumer();
        getClientUtil().changeFlowVersion(importConsumerFlow.groupId(), VERSION_3);
        assertSameServiceAndState(consumers.importStore(), awaitEnabledStore(importConsumerFlow), "update to a version with the service");
        getClientUtil().assertFlowUpToDate(importConsumerFlow.groupId());
    }

    /**
     * A consumer that skipped the flow version adding the second processor still matches the author's shared service:
     * it keeps its own service instance, state, and secret.
     */
    private void verifyLaggingConsumerMatchesSharedService(final LaggingConsumerFlows flows)
            throws NiFiClientException, IOException, InterruptedException {
        final Store authorFirst = readStore(flows.authorGroupId(), FIRST_PROCESSOR);
        final Store authorSecond = readStore(flows.authorGroupId(), SECOND_PROCESSOR);
        assertEquals(authorFirst.serviceId(), authorSecond.serviceId(), "the author's processors share one service");

        final Store consumerBefore = readStore(flows.consumerGroupId(), FIRST_PROCESSOR);
        getClientUtil().changeFlowVersion(flows.consumerGroupId(), VERSION_3);

        final Store consumerAfter = readStore(flows.consumerGroupId(), FIRST_PROCESSOR);
        assertSameServiceAndState(consumerBefore, consumerAfter, "update after skipping a version");
        assertNotNull(authorFirst.versionedId());
        assertEquals(authorFirst.versionedId(), consumerAfter.versionedId());
        assertEquals(FIRST_SECRET, consumerAfter.password());
    }

    /**
     * A consumer that skipped the flow version adding the second processor matches the author's service for the same
     * processor. Each processor keeps its own service and secret.
     */
    private void verifyLaggingConsumerMatchesSeparateServices(final LaggingConsumerFlows flows)
            throws NiFiClientException, IOException, InterruptedException {
        final Store authorFirst = readStore(flows.authorGroupId(), FIRST_PROCESSOR);
        final Store authorSecond = readStore(flows.authorGroupId(), SECOND_PROCESSOR);
        assertNotNull(authorFirst.versionedId());
        assertNotNull(authorSecond.versionedId());
        assertNotEquals(authorFirst.versionedId(), authorSecond.versionedId(), "services with different secrets have different versioned IDs");

        final Store consumerBefore = readStore(flows.consumerGroupId(), FIRST_PROCESSOR);
        getClientUtil().changeFlowVersion(flows.consumerGroupId(), VERSION_3);

        final Store consumerFirst = readStore(flows.consumerGroupId(), FIRST_PROCESSOR);
        assertSameServiceAndState(consumerBefore, consumerFirst, "update after skipping a version");
        assertEquals(authorFirst.versionedId(), consumerFirst.versionedId());
        assertEquals(FIRST_SECRET, consumerFirst.password());

        // The second processor and its service arrive with V3 and are matched to the author's second service.
        final Store consumerSecond = readStoreWithoutWaiting(flows.consumerGroupId(), SECOND_PROCESSOR);
        assertNotEquals(consumerFirst.serviceId(), consumerSecond.serviceId());
        assertEquals(authorSecond.versionedId(), consumerSecond.versionedId());
    }

    /**
     * A service with an inline secret gets no versioned ID, because the secret is not saved to the registry and cannot tell services apart.
     * Both of the author's processors use the same secret, so they share one service.
     */
    private void verifyAuthorSharesUnversionedServiceWithInlineSecret(final InlineSecretFlows flows) throws NiFiClientException, IOException, InterruptedException {
        final Store authorFirst = readStore(flows.authorGroupId(), FIRST_PROCESSOR);
        assertEquals(authorFirst.serviceId(), readStore(flows.authorGroupId(), SECOND_PROCESSOR).serviceId(), "the author's processors share one service");
        assertNull(authorFirst.versionedId(), "a service with an inline secret is left unversioned");
    }

    /**
     * Each consumer processor has its own service and secret. After a version change, a consumer processor may lose its secret, as it would
     * without versioned IDs for services created by migration, but never uses the other processor's secret.
     */
    private void verifyServiceWithInlineSecretIsNotReusedByOtherProcessors(final InlineSecretFlows flows)
            throws NiFiClientException, IOException, InterruptedException {
        assertEquals(FIRST_SECRET, readStore(flows.consumerGroupId(), FIRST_PROCESSOR).password());
        assertEquals(SECOND_SECRET, readStore(flows.consumerGroupId(), SECOND_PROCESSOR).password());

        getClientUtil().changeFlowVersion(flows.consumerGroupId(), VERSION_2);

        assertOwnSecretOrNone(readStoreWithoutWaiting(flows.consumerGroupId(), FIRST_PROCESSOR), FIRST_SECRET);
        assertOwnSecretOrNone(readStoreWithoutWaiting(flows.consumerGroupId(), SECOND_PROCESSOR), SECOND_SECRET);
    }

    private void assertOwnSecretOrNone(final Store store, final String ownSecret) {
        assertTrue(store.password() == null || ownSecret.equals(store.password()), "store holds another processor's secret: " + store.password());
    }

    private void upgradeRuntime() throws IOException {
        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);
    }

    private ParameterContextEntity createSecretsParameterContext() throws NiFiClientException, IOException {
        final Set<ParameterEntity> parameters = Set.of(
                getClientUtil().createParameterEntity("password", null, true, FIRST_SECRET),
                getClientUtil().createParameterEntity("first-password", null, true, FIRST_SECRET),
                getClientUtil().createParameterEntity("second-password", null, true, SECOND_SECRET));
        return getNifiClient().getParamContextClient().createParamContext(getClientUtil().createParameterContextEntity(SECRETS_CONTEXT, null, parameters));
    }

    private void createMigratingProcessor(final String groupId, final String name, final String password) throws NiFiClientException, IOException {
        final ProcessorEntity processor = getClientUtil().createProcessor(MIGRATING_PROCESSOR_NAME, groupId);
        processor.getComponent().setName(name);
        final ProcessorEntity renamed = getNifiClient().getProcessorClient().updateProcessor(processor);
        final ProcessorEntity configured = getClientUtil().updateProcessorProperties(renamed, Map.of(STORE_NAME_PROPERTY, STORE_NAME, STORE_PASSWORD_PROPERTY, password));
        getClientUtil().setAutoTerminatedRelationships(configured, "success");
    }

    private void setStorePassword(final String groupId, final String processorName, final String password) throws NiFiClientException, IOException {
        getClientUtil().updateProcessorProperties(findProcessor(groupId, processorName), Map.of(STORE_PASSWORD_PROPERTY, password));
    }

    private String startVersionControl(final String groupId, final FlowRegistryClientEntity registryClient, final String flowName) throws NiFiClientException, IOException {
        final ProcessGroupEntity group = getNifiClient().getProcessGroupClient().getProcessGroup(groupId);
        return getClientUtil().startVersionControl(group, registryClient, TEST_FLOWS_BUCKET, flowName).getVersionControlInformation().getFlowId();
    }

    private void saveFlowVersion(final String groupId, final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException {
        getClientUtil().saveFlowVersion(getNifiClient().getProcessGroupClient().getProcessGroup(groupId), registryClient,
                getNifiClient().getVersionsClient().getVersionControlInfo(groupId));
    }

    private MigratedFlow flowIn(final ProcessGroupEntity group) throws NiFiClientException, IOException {
        return new MigratedFlow(group.getId(), findProcessor(group.getId(), MIGRATING_PROCESSOR_NAME).getId());
    }

    private MigratedFlow importFlow(final FlowRegistryClientEntity registryClient, final String version)
            throws NiFiClientException, IOException {
        return flowIn(getClientUtil().importFlowFromRegistry(ROOT_PG, registryClient.getId(), TEST_FLOWS_BUCKET, FLOW_ID, version));
    }

    private ProcessGroupEntity importInitialFlow(final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException, InterruptedException {
        final Path versionDirectory = registryDirectory.resolve(TEST_FLOWS_BUCKET).resolve(FLOW_ID).resolve(VERSION_1);
        Files.createDirectories(versionDirectory);
        Files.copy(FLOW_SNAPSHOT, versionDirectory.resolve(FLOW_DEFINITION_FILENAME));
        final ProcessGroupEntity group = getClientUtil().importFlowFromRegistry(ROOT_PG, registryClient.getId(), TEST_FLOWS_BUCKET, FLOW_ID, VERSION_1);
        getClientUtil().startProcessGroupComponents(group.getId());
        return group;
    }

    private void startMigratedFlow(final MigratedFlow flow) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().enableControllerServices(flow.groupId(), true);
        getClientUtil().startProcessor(getNifiClient().getProcessorClient().getProcessor(flow.processorId()));
        getClientUtil().waitForRunningProcessor(flow.processorId());
    }

    private void renameProcessor(final String processorId, final String name) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().stopProcessor(getNifiClient().getProcessorClient().getProcessor(processorId));
        final ProcessorEntity processor = getNifiClient().getProcessorClient().getProcessor(processorId);
        processor.getComponent().setName(name);
        getNifiClient().getProcessorClient().updateProcessor(processor);
        getClientUtil().startProcessor(getNifiClient().getProcessorClient().getProcessor(processorId));
        getClientUtil().waitForRunningProcessor(processorId);
    }

    /**
     * Waits until the flow's only store service is enabled and referenced by the migrated processor, and reads it.
     */
    private Store awaitEnabledStore(final MigratedFlow flow) throws NiFiClientException, IOException, InterruptedException {
        waitFor(() -> findStoreServices(flow.groupId()).size() == 1, CONDITION_POLL_MILLIS, "exactly one store Controller Service");
        final String serviceId = findStoreServices(flow.groupId()).getFirst().getComponent().getId();
        getClientUtil().waitForControllerServiceRunStatus(serviceId, "ENABLED");
        assertEquals(serviceId, getStoreServiceId(flow.processorId()));
        return readStore(serviceId);
    }

    /**
     * Reads the store service that the named processor references, once that service has been enabled.
     */
    private Store readStore(final String groupId, final String processorName) throws NiFiClientException, IOException, InterruptedException {
        final String serviceId = getStoreServiceId(findProcessor(groupId, processorName).getId());
        assertNotNull(serviceId, processorName + " references a store service");
        waitFor(() -> readStoreState(serviceId).containsKey(CREATED_STATE_KEY), CONDITION_POLL_MILLIS, "store service enabled");
        return readStore(serviceId);
    }

    /**
     * Reads the store service that the named processor references without waiting for it to be enabled. A service that
     * a version change adds, or one that lacks the consumer's inline secret, may not be enabled.
     */
    private Store readStoreWithoutWaiting(final String groupId, final String processorName) throws NiFiClientException, IOException {
        final String serviceId = getStoreServiceId(findProcessor(groupId, processorName).getId());
        assertNotNull(serviceId, processorName + " references a store service");
        return readStore(serviceId);
    }

    private Store readStore(final String serviceId) throws NiFiClientException, IOException {
        final ControllerServiceEntity service = getNifiClient().getControllerServicesClient().getControllerService(serviceId);
        final Map<String, String> state = readStoreState(serviceId);
        return new Store(serviceId, service.getComponent().getVersionedComponentId(), state.get(CREATED_STATE_KEY), state.get(PASSWORD_STATE_KEY));
    }

    /**
     * A version change must keep the same service instance and its state. A replaced service has a different identifier
     * or, if recreated with the same identifier, a later creation timestamp.
     */
    private void assertSameServiceAndState(final Store before, final Store after, final String operation) {
        assertEquals(before.serviceId(), after.serviceId(), operation + " replaced the service");
        assertEquals(before.created(), after.created(), operation + " recreated the service's state");
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

    private List<ControllerServiceEntity> findStoreServices(final String groupId) throws NiFiClientException, IOException {
        return getNifiClient().getFlowClient().getControllerServices(groupId).getControllerServices().stream()
                .filter(service -> groupId.equals(service.getComponent().getParentGroupId()))
                .filter(service -> STORE_SERVICE_TYPE.equals(service.getComponent().getType()))
                .toList();
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

    private record MigratedFlow(String groupId, String processorId) {
    }

    private record MigrationPathFlows(MigratedFlow author, MigratedFlow startupConsumer) {
    }

    private record MigrationPathConsumers(MigratedFlow startupConsumer, Store startupStore, MigratedFlow importConsumer, Store importStore) {
    }

    private record LaggingConsumerFlows(String authorGroupId, String consumerGroupId) {
    }

    private record InlineSecretFlows(String authorGroupId, String consumerGroupId) {
    }

    /**
     * A store service as seen at one point in the test: its instance and versioned IDs, its creation timestamp, and the
     * password it was last enabled with.
     */
    private record Store(String serviceId, String versionedId, String created, String password) {
    }
}
