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

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System tests for verifying that disabling a Controller Service properly stops
 * referencing components (processors, reporting tasks, etc.) before disabling.
 *
 * This addresses the scenario where:
 * 1. A Controller Service is enabled with referencing processors running
 * 2. User attempts to disable the Controller Service
 * 3. The system should automatically stop referencing processors first
 * 4. Then disable the Controller Service
 */
public class ControllerServiceDisableWithReferencesIT extends NiFiSystemIT {

    private static final String DISABLED = "DISABLED";
    private static final String ENABLING = "ENABLING";
    private static final String RUNNING = "RUNNING";
    private static final String STOPPED = "STOPPED";

    /**
     * Test that disabling a Controller Service automatically stops referencing processors.
     * This verifies the fix for the issue where disabling a Controller Service would fail
     * if there were running processors that referenced it.
     */
    @Test
    public void testDisableControllerServiceStopsReferencingProcessors() throws NiFiClientException, IOException, InterruptedException {
        // Create a simple controller service (StandardSleepService doesn't require external resources)
        final ControllerServiceEntity sleepService = getClientUtil().createControllerService("StandardSleepService");

        // Create a processor that references this controller service
        final ProcessorEntity processor = getClientUtil().createProcessor("Sleep");
        getClientUtil().updateProcessorProperties(processor, Map.of("Sleep Service", sleepService.getId()));
        getClientUtil().setAutoTerminatedRelationships(processor, "success");

        // Enable the controller service
        getClientUtil().enableControllerService(sleepService);
        getClientUtil().waitForControllerServicesEnabled("root");

        // Start the processor
        getClientUtil().waitForValidProcessor(processor.getId());
        getClientUtil().startProcessor(processor);
        getClientUtil().waitForProcessorState(processor.getId(), RUNNING);

        // Now disable the controller service - this should automatically stop the processor first
        final ControllerServiceEntity serviceToDisable = getNifiClient().getControllerServicesClient().getControllerService(sleepService.getId());
        getClientUtil().disableControllerService(serviceToDisable);

        // Wait for the controller service to be disabled
        getClientUtil().waitForControllerServiceRunStatus(sleepService.getId(), DISABLED);

        // Verify the processor was stopped
        final ProcessorEntity updatedProcessor = getNifiClient().getProcessorClient().getProcessor(processor.getId());
        final ProcessorDTO processorDto = updatedProcessor.getComponent();
        assertEquals(STOPPED, processorDto.getState(),
            "Processor should be stopped after disabling the referenced controller service");

        // Verify the controller service is disabled
        final ControllerServiceEntity updatedService = getNifiClient().getControllerServicesClient().getControllerService(sleepService.getId());
        assertEquals(DISABLED, updatedService.getComponent().getState(),
            "Controller Service should be disabled");
    }

    /**
     * Test that disabling a Controller Service works when the controller service
     * is stuck in ENABLING state (e.g., due to validation failures).
     * This verifies the fix for the issue where disabling took too long when the
     * service was stuck in ENABLING state.
     */
    @Test
    public void testDisableControllerServiceInEnablingState() throws NiFiClientException, IOException, InterruptedException {
        // Create a LifecycleFailureService configured to fail many times (effectively stuck in ENABLING)
        final ControllerServiceEntity failureService = getClientUtil().createControllerService("LifecycleFailureService");
        getClientUtil().updateControllerServiceProperties(failureService, Collections.singletonMap("Enable Failure Count", "10000"));

        // Try to enable the service - it will be stuck in ENABLING state
        getClientUtil().enableControllerService(failureService);

        // Wait a bit for it to be in ENABLING state
        Thread.sleep(1000);

        // Verify it's in ENABLING state
        ControllerServiceEntity currentService = getNifiClient().getControllerServicesClient().getControllerService(failureService.getId());
        assertEquals(ENABLING, currentService.getComponent().getState(),
            "Controller Service should be in ENABLING state");

        // Now disable the service - this should complete quickly (not wait for retry delay)
        final long startTime = System.currentTimeMillis();
        getClientUtil().disableControllerService(currentService);

        // Wait for the controller service to be disabled
        getClientUtil().waitForControllerServiceRunStatus(failureService.getId(), DISABLED);
        final long endTime = System.currentTimeMillis();

        // Verify it completed in a reasonable time (less than 30 seconds)
        // Previously this could take up to 10 minutes due to retry delays
        final long durationSeconds = (endTime - startTime) / 1000;
        assertTrue(durationSeconds < 30,
            "Disabling controller service from ENABLING state should complete quickly, but took " + durationSeconds + " seconds");

        // Verify the controller service is disabled
        final ControllerServiceEntity updatedService = getNifiClient().getControllerServicesClient().getControllerService(failureService.getId());
        assertEquals(DISABLED, updatedService.getComponent().getState(),
            "Controller Service should be disabled");
    }
}

