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
package org.apache.nifi.tests.system.scheduling;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Exercises the virtual-thread scheduling agent's start/stop behavior across many cycles. Configures a
 * {@code GenerateFlowFile} processor to emit exactly one FlowFile per scheduling session and then starts and stops
 * the processor {@value #START_STOP_CYCLES} times, verifying the queue size after each cycle. This catches regressions
 * in the start/stop lifecycle that a single long-running soak test would not:
 *
 * <ul>
 *   <li>A leaked scheduling loop would cause the queue size to advance by more than one per cycle.</li>
 *   <li>A scheduling loop that fails to start would cause the queue size to not advance at all.</li>
 *   <li>Subtle bugs in the {@link org.apache.nifi.controller.scheduling.VirtualThreadSchedulingAgent} lifecycle (for
 *       example, never calling {@code setScheduled(false)} on unschedule and therefore never exiting the loop) would
 *       either produce extra FlowFiles on restart or hang the test.</li>
 * </ul>
 *
 * Once all cycles have completed and the expected number of FlowFiles is queued, {@code TerminateFlowFile} is started
 * and the test waits for the queue to drain back to zero, confirming that the final FlowFile count is correct and the
 * flow is in a clean state.
 */
public class VirtualThreadStartStopCycleIT extends NiFiSystemIT {

    private static final int START_STOP_CYCLES = 25;

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testRepeatedStartStopProducesExpectedQueueCount() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity generateToTerminate = getClientUtil().createConnection(generate, terminate, "success");

        // Raise back-pressure well above the expected queue size so that the generator is never blocked from producing
        // its one FlowFile per cycle, regardless of load on the test agent.
        getClientUtil().updateConnectionBackpressure(generateToTerminate, 10_000L, 10_000_000L);

        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("File Size", "0 B");
        generateProperties.put("Batch Size", "1");
        generateProperties.put("Max FlowFiles", "1");
        generateProperties.put("State Scope", "LOCAL");
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        final ProcessorConfigDTO generateConfig = new ProcessorConfigDTO();
        generateConfig.setSchedulingPeriod("0 sec");
        generateConfig.setConcurrentlySchedulableTaskCount(1);
        final ProcessorEntity configuredGenerate = getClientUtil().updateProcessorConfig(generate, generateConfig);

        final ProcessorConfigDTO terminateConfig = new ProcessorConfigDTO();
        terminateConfig.setSchedulingPeriod("0 sec");
        terminateConfig.setConcurrentlySchedulableTaskCount(1);
        final ProcessorEntity configuredTerminate = getClientUtil().updateProcessorConfig(terminate, terminateConfig);

        for (int cycle = 1; cycle <= START_STOP_CYCLES; cycle++) {
            getClientUtil().startProcessor(configuredGenerate);
            waitForQueueCount(generateToTerminate, cycle);
            getClientUtil().stopProcessor(configuredGenerate);

            assertEquals(cycle, getConnectionQueueSize(generateToTerminate.getId()),
                    "After start/stop cycle " + cycle + " the queue should contain exactly " + cycle + " FlowFiles, "
                            + "which would not be the case if the prior scheduling loop leaked (too many) or if the new "
                            + "scheduling loop failed to run (too few).");
        }

        assertEquals(START_STOP_CYCLES, getConnectionQueueSize(generateToTerminate.getId()),
                "Expected exactly " + START_STOP_CYCLES + " FlowFiles queued after " + START_STOP_CYCLES + " start/stop cycles");

        getClientUtil().startProcessor(configuredTerminate);
        try {
            waitForQueueCount(generateToTerminate, 0);
        } finally {
            getClientUtil().stopProcessor(configuredTerminate);
        }

        assertEquals(0, getConnectionQueueSize(generateToTerminate.getId()),
                "After TerminateFlowFile drained the queue it should be empty");
    }
}
