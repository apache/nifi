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
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces the controller-service lifecycle problem observed in SNOW-3697891 (PostgreSQL connector 0.53.0 upgrade),
 * where the "Create Journal Table" processors failed with "Controller Service with ID ... is disabled".
 *
 * <p>Topology under test (mirrors the customer flow):
 * <ul>
 *   <li>A STANDARD parent Process Group ("PostgreSQL") that <b>owns</b> a shared Controller Service
 *       ("Snowflake Connection Pool", modelled here with {@code StandardCountService}).</li>
 *   <li>A nested STATELESS child Process Group ("Create Journal Table") whose processor references the parent's
 *       Controller Service by ID (modelled here with {@code CountFlowFiles}, which requires a {@code CountService}).</li>
 * </ul>
 *
 * <p>The key framework behavior being exercised is in
 * {@code AbstractComponentNode.validateControllerServices()}: a processor inside a STATELESS group is normally exempt
 * from the "referenced Controller Service is disabled" validation, but that exemption is voided when the referenced
 * service lives in a non-stateless (STANDARD) ancestor group. In that case, if the service is not ENABLED, the
 * processor is INVALID with a "Controller Service ... is disabled" validation error. During an upgrade the shared
 * service is transiently DISABLED, so a stateless child running against it in that window fails.
 */
public class StatelessExternalControllerServiceUpgradeIT extends NiFiSystemIT {

    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String COUNT_SERVICE_TYPE = "StandardCountService";
    private static final String COUNT_PROCESSOR_TYPE = "CountFlowFiles";
    private static final String COUNT_SERVICE_PROPERTY = "Count Service";

    /**
     * Deterministically reproduces the exact failure condition: a processor inside a STATELESS group that references a
     * Controller Service defined in its STANDARD parent group becomes INVALID with a "... is disabled" validation error
     * the moment that parent service is disabled. This is the precise validation branch that the connector upgrade race
     * exposed for the customer.
     */
    @Test
    public void testStatelessChildInvalidWhenExternalStandardParentServiceDisabled()
            throws NiFiClientException, IOException, InterruptedException {

        final NiFiClientUtil util = getClientUtil();
        final Topology topology = buildTopology(util);

        // Enable the shared service that lives in the STANDARD parent group. The stateless child's processor,
        // which references that service, should then validate successfully.
        util.enableControllerService(topology.sharedService);
        util.waitForControllerServicesEnabled(topology.parent.getId(), topology.sharedService.getComponent().getId());
        util.waitForValidProcessor(topology.journalProcessor.getId());

        // Now disable the shared service, exactly as the upgrade orchestration does transiently while it reconfigures
        // and re-enables affected controller services.
        util.disableControllerService(topology.sharedService);
        util.waitForControllerServicesDisabled(topology.parent.getId(), topology.sharedService.getComponent().getId());

        // Because the processor lives in a STATELESS group but the referenced service lives in a STANDARD ancestor
        // group, the stateless validation exemption does NOT apply: the processor must become INVALID.
        util.waitForInvalidProcessor(topology.journalProcessor.getId());

        final ProcessorEntity invalid = getNifiClient().getProcessorClient().getProcessor(topology.journalProcessor.getId());
        final Collection<String> validationErrors = invalid.getComponent().getValidationErrors();
        assertNotNull(validationErrors, "Expected validation errors when the referenced Controller Service is disabled");

        final String serviceId = topology.sharedService.getComponent().getId();
        final boolean hasDisabledError = validationErrors.stream()
                .anyMatch(error -> error.toLowerCase().contains("disabled") && error.contains(serviceId));
        assertTrue(hasDisabledError,
                "Expected a 'Controller Service with ID " + serviceId + " is disabled' validation error but found: " + validationErrors);
    }

    /**
     * Exercises the actual version-control upgrade path end-to-end for the same topology. During the change-version
     * upgrade, the shared service is modified (so it enters the "affected" set), which forces the framework to disable
     * it, reconfigure it, and re-enable it while the stateless child is stopped and restarted.
     *
     * <p>On the change-version REST path the re-enable step blocks until the service reaches ENABLED before the
     * stateless child is restarted, so the flow should recover: the service ends ENABLED and the stateless child's
     * processor ends VALID and keeps processing. (The customer failure came from the connector best-effort re-enable
     * path, where the stateless group can restart before the service finishes enabling.)
     */
    @Test
    public void testVersionUpgradeReEnablesExternalServiceForStatelessChild()
            throws NiFiClientException, IOException, InterruptedException {

        final FlowRegistryClientEntity registryClient = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final Topology topology = buildTopology(util);

        // A standalone heartbeat processor that is unchanged across versions and stays running throughout the upgrade,
        // guaranteeing the parent group is "active" so the synchronizer auto-restarts affected components.
        final ProcessorEntity heartbeat = util.createProcessor(GENERATE_FLOWFILE, topology.parent.getId());
        util.setAutoTerminatedRelationships(heartbeat, SUCCESS);

        // Keep produced volume modest since the terminate processor is intentionally left stopped so queues accumulate.
        util.updateProcessorSchedulingPeriod(topology.generate, "500 ms");

        // Save the topology as version 1.
        final VersionControlInformationEntity vci =
                util.startVersionControl(topology.parent, registryClient, TEST_FLOWS_BUCKET, "postgres-connector-flow");

        // Build version 2: change the shared service configuration so it is part of the "affected" set during upgrade
        // (this mirrors the descriptor/bundle change that put "Snowflake Connection Pool" into the affected set).
        final ControllerServiceEntity dependentService = util.createControllerService(COUNT_SERVICE_TYPE, topology.parent.getId());
        util.updateControllerServiceProperties(topology.sharedService,
                Collections.singletonMap("Dependent Service", dependentService.getComponent().getId()));
        util.saveFlowVersion(topology.parent, registryClient, vci);

        // Revert to version 1 and start the flow running.
        util.changeFlowVersion(topology.parent.getId(), "1");
        util.assertFlowStaleAndUnmodified(topology.parent.getId());

        final ControllerServiceEntity sharedServiceV1 =
                getNifiClient().getControllerServicesClient().getControllerService(topology.sharedService.getComponent().getId());
        util.enableControllerService(sharedServiceV1);
        util.waitForControllerServicesEnabled(topology.parent.getId(), topology.sharedService.getComponent().getId());

        util.waitForValidProcessor(topology.journalProcessor.getId());
        util.startProcessGroupComponents(topology.statelessChild.getId());
        util.startProcessor(topology.generate);
        util.startProcessor(heartbeat);

        // Confirm the stateless flow processes FlowFiles before the upgrade (journal tables being created).
        waitForQueueCount(topology.outputToTerminate.getId(), getNumberOfNodes());

        // Quiesce the stateless child (and its external feed) before the upgrade so that the shared service is no longer
        // referenced by a running component and can be disabled/reconfigured/re-enabled cleanly. The heartbeat processor
        // stays running so the parent group remains "active".
        //
        // NOTE: This mirrors the safe upgrade sequence. If the stateless child is left RUNNING during the change-version
        // upgrade, the REST update path (FlowUpdateResource) stops the child's inner processor but leaves the enclosing
        // StandardStatelessGroupNode running, so the shared service cannot be disabled and the change-version request
        // blocks indefinitely -- the deadlock behind SNOW-3697891's transient "Controller Service is disabled" window.
        util.stopProcessor(topology.generate);
        util.waitForStoppedProcessor(topology.generate.getId());
        util.stopProcessGroupComponents(topology.statelessChild.getId());

        // Upgrade to version 2. This disables/reconfigures/re-enables the shared service.
        util.changeFlowVersion(topology.parent.getId(), "2");
        util.assertFlowUpToDate(topology.parent.getId());

        // The shared service must end up ENABLED and the stateless child's processor must end up VALID again.
        util.waitForControllerServicesEnabled(topology.parent.getId(), topology.sharedService.getComponent().getId());
        util.waitForValidProcessor(topology.journalProcessor.getId());

        final ControllerServiceEntity afterUpgrade =
                getNifiClient().getControllerServicesClient().getControllerService(topology.sharedService.getComponent().getId());
        assertEquals("ENABLED", afterUpgrade.getComponent().getState(),
                "Shared Controller Service should be ENABLED after the version upgrade");

        // Restart the stateless child and its feed, and confirm processing resumes after the upgrade (the queue keeps
        // growing because the terminate processor is intentionally left stopped).
        util.startProcessGroupComponents(topology.statelessChild.getId());
        final int postUpgradeTarget = getConnectionQueueSize(topology.outputToTerminate.getId()) + getNumberOfNodes();
        util.startProcessor(topology.generate);
        waitForQueueCount(topology.outputToTerminate.getId(), postUpgradeTarget);
    }

    /**
     * Builds the shared topology: a STANDARD parent group owning a Controller Service, and a nested STATELESS child
     * group whose processor references that service by ID, fed by an external GenerateFlowFile and drained to an
     * external (unstarted) TerminateFlowFile.
     */
    private Topology buildTopology(final NiFiClientUtil util) throws NiFiClientException, IOException {
        // STANDARD parent group (mirrors "PostgreSQL").
        final ProcessGroupEntity parent = util.createProcessGroup("PostgreSQL", "root");

        // Shared Controller Service owned by the STANDARD parent (mirrors "Snowflake Connection Pool").
        final ControllerServiceEntity sharedService = util.createControllerService(COUNT_SERVICE_TYPE, parent.getId());

        // External source and sink in the parent. Terminate is intentionally left stopped so queues accumulate.
        final ProcessorEntity generate = util.createProcessor(GENERATE_FLOWFILE, parent.getId());
        final ProcessorEntity terminate = util.createProcessor(TERMINATE_FLOWFILE, parent.getId());

        // Nested STATELESS child group (mirrors "Create Journal Table").
        final ProcessGroupEntity statelessChild = util.createProcessGroup("Create Journal Table", parent.getId());
        util.markStateless(statelessChild, "1 min");

        final PortEntity inputPort = util.createInputPort("In", statelessChild.getId());
        final PortEntity outputPort = util.createOutputPort("Out", statelessChild.getId());

        // Processor inside the STATELESS child references the parent's Controller Service by ID
        // (mirrors UpdateSnowflakeSchema/Table/Stream referencing the parent's Snowflake Connection Pool).
        final ProcessorEntity journalProcessor = util.createProcessor(COUNT_PROCESSOR_TYPE, statelessChild.getId());
        util.updateProcessorProperties(journalProcessor,
                Collections.singletonMap(COUNT_SERVICE_PROPERTY, sharedService.getComponent().getId()));

        util.createConnection(inputPort, journalProcessor, statelessChild.getId());
        util.createConnection(journalProcessor, outputPort, SUCCESS, statelessChild.getId());

        final ConnectionEntity generateToInput = util.createConnection(generate, inputPort, SUCCESS);
        final ConnectionEntity outputToTerminate = util.createConnection(outputPort, terminate);

        return new Topology(parent, sharedService, statelessChild, journalProcessor, generate, terminate,
                inputPort, outputPort, generateToInput, outputToTerminate);
    }

    /**
     * Simple holder for the components created by {@link #buildTopology(NiFiClientUtil)}.
     */
    private record Topology(ProcessGroupEntity parent,
                            ControllerServiceEntity sharedService,
                            ProcessGroupEntity statelessChild,
                            ProcessorEntity journalProcessor,
                            ProcessorEntity generate,
                            ProcessorEntity terminate,
                            PortEntity inputPort,
                            PortEntity outputPort,
                            ConnectionEntity generateToInput,
                            ConnectionEntity outputToTerminate) {
    }
}
