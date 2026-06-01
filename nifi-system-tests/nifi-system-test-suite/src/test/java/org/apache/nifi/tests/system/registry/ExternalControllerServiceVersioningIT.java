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

package org.apache.nifi.tests.system.registry;

import org.apache.nifi.tests.system.NiFiClientUtil;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DifferenceDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowComparisonEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System tests verifying correct version-control behavior when a versioned Process Group
 * references an external Controller Service (one defined in an ancestor group).
 *
 * Several tests simulate a cross-instance (dev to prod) scenario by committing a flow
 * that references an external service, then deleting the original PG and service, creating
 * a new service with the same name (but a different ID), and importing the flow from the
 * registry. This reproduces the ID mismatch that occurs when a flow versioned on one NiFi
 * instance is imported into another instance that has a same-named external service.
 */
public class ExternalControllerServiceVersioningIT extends NiFiSystemIT {
    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String COUNT_SERVICE_TYPE = "StandardCountService";

    /**
     * Simulates a cross-instance import: a flow is committed referencing an external service,
     * then the original PG and service are removed and a new service with the same name (but
     * different ID) is created before re-importing from the registry.
     *
     * After import, the flow should be UP_TO_DATE because the external service was resolved
     * by name during import and the cached snapshot should also be resolved.
     * Both the state badge and the "Show Local Changes" dialog should agree.
     */
    @Test
    public void testCrossInstanceImportWithExternalServiceShowsUpToDate() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ControllerServiceEntity devService = util.createControllerService(COUNT_SERVICE_TYPE, "root");
        util.enableControllerService(devService);

        final ProcessGroupEntity child = util.createProcessGroup("Child", "root");
        final ProcessorEntity counter = util.createProcessor("CountFlowFiles", child.getId());
        util.updateProcessorProperties(counter, Collections.singletonMap("Count Service", devService.getComponent().getId()));
        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", child.getId());
        util.createConnection(counter, terminate, "success");

        final VersionControlInformationEntity vci = util.startVersionControl(child, registryClient, TEST_FLOWS_BUCKET, "cross-instance-flow");
        util.assertFlowUpToDate(child.getId());
        final VersionControlInformationDTO vciDto = vci.getVersionControlInformation();

        getNifiClient().getVersionsClient().stopVersionControl(
                getNifiClient().getProcessGroupClient().getProcessGroup(child.getId()));
        deleteProcessGroupContents(child.getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(
                getNifiClient().getProcessGroupClient().getProcessGroup(child.getId()));

        deleteControllerService(devService);

        final ControllerServiceEntity prodService = util.createControllerService(COUNT_SERVICE_TYPE, "root");
        assertNotEquals(devService.getComponent().getId(), prodService.getComponent().getId(),
                "Prod service should have a different ID than dev service");
        util.enableControllerService(prodService);

        final ProcessGroupEntity imported = util.importFlowFromRegistry("root", vciDto.getRegistryId(),
                vciDto.getBucketId(), vciDto.getFlowId(), vciDto.getVersion());

        waitForVersionedFlowState(imported.getId(), "root", "UP_TO_DATE");

        final FlowComparisonEntity localMods = getNifiClient().getProcessGroupClient().getLocalModifications(imported.getId());
        assertTrue(localMods.getComponentDifferences().isEmpty(),
                "After cross-instance import, Show Local Changes should report no differences");
    }

    /**
     * Simulates a cross-instance upgrade where v1 and v2 both reference the same external
     * service but differ in a non-service property (scheduling period). The original PG and
     * service are deleted, a new service with the same name (different ID) is created, then
     * the flow is re-imported at v1 and upgraded to v2.
     *
     * NiFi preserves existing external service references during upgrades, so the external
     * service reference stays the same in both versions. After upgrade, the flow should be
     * UP_TO_DATE because the only change (scheduling period) is applied by the synchronizer
     * and the external service reference matches in both the local flow and the VCI snapshot.
     */
    @Test
    public void testCrossInstanceUpgradeWithExternalServiceShowsUpToDate() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ControllerServiceEntity devService = util.createControllerService(COUNT_SERVICE_TYPE, "root");
        util.enableControllerService(devService);

        final ProcessGroupEntity child = util.createProcessGroup("Child", "root");
        ProcessorEntity counter = util.createProcessor("CountFlowFiles", child.getId());
        util.updateProcessorProperties(counter, Collections.singletonMap("Count Service", devService.getComponent().getId()));
        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", child.getId());
        util.createConnection(counter, terminate, "success");

        final VersionControlInformationEntity vci = util.startVersionControl(child, registryClient, TEST_FLOWS_BUCKET, "cross-instance-upgrade");
        util.assertFlowUpToDate(child.getId());

        counter = util.updateProcessorSchedulingPeriod(counter, "10 sec");
        util.saveFlowVersion(child, registryClient, vci);
        util.assertFlowUpToDate(child.getId());
        final VersionControlInformationDTO vciDto = vci.getVersionControlInformation();

        getNifiClient().getVersionsClient().stopVersionControl(
                getNifiClient().getProcessGroupClient().getProcessGroup(child.getId()));
        deleteProcessGroupContents(child.getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(
                getNifiClient().getProcessGroupClient().getProcessGroup(child.getId()));

        deleteControllerService(devService);

        final ControllerServiceEntity prodService = util.createControllerService(COUNT_SERVICE_TYPE, "root");
        assertNotEquals(devService.getComponent().getId(), prodService.getComponent().getId(),
                "Prod service should have a different ID than dev service");
        util.enableControllerService(prodService);

        final ProcessGroupEntity imported = util.importFlowFromRegistry("root", vciDto.getRegistryId(),
                vciDto.getBucketId(), vciDto.getFlowId(), "1");

        waitForVersionedFlowState(imported.getId(), "root", "STALE");

        util.changeFlowVersion(imported.getId(), "2");

        waitForVersionedFlowState(imported.getId(), "root", "UP_TO_DATE");

        final FlowComparisonEntity localMods = getNifiClient().getProcessGroupClient().getLocalModifications(imported.getId());
        assertTrue(localMods.getComponentDifferences().isEmpty(),
                "After cross-instance upgrade, Show Local Changes should report no differences");
    }

    /**
     * Reproduces the NIFI-15697 scenario on a single instance:
     *
     * 1. Create an external service "StandardCountService" and a child PG referencing it, commit.
     * 2. Create a second external service "AlternateCountService", switch the processor to it.
     * 3. Delete the original service.
     * 4. Verify that both the state badge (LOCALLY_MODIFIED) and the dialog (shows the change) agree.
     *
     * The two services have different names so the name-based resolver cannot falsely reconcile them.
     */
    @Test
    public void testSwitchExternalServiceAndDeleteOriginalShowsLocalModification() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ControllerServiceEntity serviceA = util.createControllerService(COUNT_SERVICE_TYPE, "root");
        util.enableControllerService(serviceA);

        final ProcessGroupEntity child = util.createProcessGroup("Child", "root");
        final ProcessorEntity counter = util.createProcessor("CountFlowFiles", child.getId());
        util.updateProcessorProperties(counter, Collections.singletonMap("Count Service", serviceA.getComponent().getId()));
        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", child.getId());
        util.createConnection(counter, terminate, "success");

        util.startVersionControl(child, registryClient, TEST_FLOWS_BUCKET, "nifi-15697-flow");
        util.assertFlowUpToDate(child.getId());

        ControllerServiceEntity serviceB = util.createControllerService(COUNT_SERVICE_TYPE, "root");
        serviceB = renameControllerService(serviceB, "AlternateCountService");
        util.enableControllerService(serviceB);

        util.updateProcessorProperties(counter, Collections.singletonMap("Count Service", serviceB.getComponent().getId()));

        String state = util.getVersionedFlowState(child.getId(), "root");
        assertEquals("LOCALLY_MODIFIED", state, "After switching external service reference, PG should be LOCALLY_MODIFIED");

        deleteControllerService(serviceA);

        state = util.getVersionedFlowState(child.getId(), "root");
        assertEquals("LOCALLY_MODIFIED", state, "After deleting original service, PG should still be LOCALLY_MODIFIED");

        final FlowComparisonEntity localMods = getNifiClient().getProcessGroupClient().getLocalModifications(child.getId());
        assertFalse(localMods.getComponentDifferences().isEmpty(), "Show Local Changes should report differences");

        final boolean hasPropertyValueChange = localMods.getComponentDifferences().stream()
                .flatMap(dto -> dto.getDifferences().stream())
                .map(DifferenceDTO::getDifferenceType)
                .anyMatch(type -> type.contains("Property Value Changed"));
        assertTrue(hasPropertyValueChange, "Differences should include a Property Value Changed difference");
    }

    /**
     * Verifies that when a versioned PG references an external service and the user has NOT
     * modified anything, both the state badge and the dialog report UP_TO_DATE / no changes.
     */
    @Test
    public void testExternalServiceReferenceWithoutModificationShowsUpToDate() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity registryClient = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ControllerServiceEntity service = util.createControllerService(COUNT_SERVICE_TYPE, "root");
        util.enableControllerService(service);

        final ProcessGroupEntity child = util.createProcessGroup("Child", "root");
        final ProcessorEntity counter = util.createProcessor("CountFlowFiles", child.getId());
        util.updateProcessorProperties(counter, Collections.singletonMap("Count Service", service.getComponent().getId()));
        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", child.getId());
        util.createConnection(counter, terminate, "success");

        util.startVersionControl(child, registryClient, TEST_FLOWS_BUCKET, "ext-svc-unchanged");
        util.assertFlowUpToDate(child.getId());

        final FlowComparisonEntity localMods = getNifiClient().getProcessGroupClient().getLocalModifications(child.getId());
        assertTrue(localMods.getComponentDifferences().isEmpty(),
                "Show Local Changes should report no differences for an unmodified flow");
    }

    /**
     * Deletes only the connections and processors within a Process Group, without touching
     * Controller Services (which may be inherited from ancestor groups).
     */
    private void deleteProcessGroupContents(final String groupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getFlowClient().getProcessGroup(groupId);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();

        for (final ConnectionEntity connection : flowDto.getConnections()) {
            connection.setDisconnectedNodeAcknowledged(true);
            getNifiClient().getConnectionClient().deleteConnection(connection);
        }

        for (final ProcessorEntity processor : flowDto.getProcessors()) {
            processor.setDisconnectedNodeAcknowledged(true);
            getNifiClient().getProcessorClient().deleteProcessor(processor);
        }
    }

    private ControllerServiceEntity renameControllerService(final ControllerServiceEntity service, final String newName)
            throws NiFiClientException, IOException {
        final ControllerServiceDTO dto = new ControllerServiceDTO();
        dto.setId(service.getId());
        dto.setName(newName);

        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setId(service.getId());
        entity.setComponent(dto);
        entity.setRevision(service.getRevision());

        return getNifiClient().getControllerServicesClient().updateControllerService(entity);
    }

    private void deleteControllerService(final ControllerServiceEntity service) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().disableControllerService(service);
        waitForControllerServiceState(service.getId(), "DISABLED");
        final ControllerServiceEntity refreshed = getNifiClient().getControllerServicesClient().getControllerService(service.getId());
        getNifiClient().getControllerServicesClient().deleteControllerService(refreshed);
    }

    private void waitForControllerServiceState(final String serviceId, final String expectedState) throws NiFiClientException, IOException, InterruptedException {
        final long maxWait = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < maxWait) {
            final ControllerServiceEntity entity = getNifiClient().getControllerServicesClient().getControllerService(serviceId);
            if (expectedState.equalsIgnoreCase(entity.getComponent().getState())) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("Controller Service " + serviceId + " did not reach " + expectedState + " state within 30 seconds");
    }

    private void waitForVersionedFlowState(final String groupId, final String parentGroupId, final String expectedState)
            throws NiFiClientException, IOException, InterruptedException {

        final long maxWait = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < maxWait) {
            final String state = getClientUtil().getVersionedFlowState(groupId, parentGroupId);
            if (expectedState.equalsIgnoreCase(state)) {
                return;
            }
            Thread.sleep(500L);
        }

        final String finalState = getClientUtil().getVersionedFlowState(groupId, parentGroupId);
        if (expectedState.equalsIgnoreCase(finalState)) {
            return;
        }

        if ("LOCALLY_MODIFIED".equalsIgnoreCase(finalState) || "LOCALLY_MODIFIED_AND_STALE".equalsIgnoreCase(finalState)) {
            final FlowComparisonEntity localMods = getNifiClient().getProcessGroupClient().getLocalModifications(groupId);
            final StringBuilder sb = new StringBuilder();
            localMods.getComponentDifferences().stream()
                    .flatMap(dto -> dto.getDifferences().stream())
                    .map(DifferenceDTO::getDifference)
                    .forEach(diff -> sb.append("\n  - ").append(diff));
            throw new AssertionError("Expected versioned flow state " + expectedState + " but was " + finalState
                    + " with modifications:" + sb);
        }

        throw new AssertionError("Expected versioned flow state " + expectedState + " but was " + finalState);
    }
}
