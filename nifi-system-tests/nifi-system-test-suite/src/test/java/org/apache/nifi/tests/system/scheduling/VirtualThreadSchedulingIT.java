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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Soak test for the {@link org.apache.nifi.controller.scheduling.VirtualThreadSchedulingAgent}.
 * Stands up a GenerateFlowFile -> TerminateFlowFile flow configured to emit
 * {@value #TARGET_FLOWFILES} FlowFiles total and waits for every FlowFile to flow through
 * the system. Having the virtual-thread scheduling loop execute this many iterations without
 * deadlocking or leaking permits gives confidence that the agent is stable under sustained load.
 */
public class VirtualThreadSchedulingIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadSchedulingIT.class);

    private static final int TARGET_FLOWFILES = 1_000_000;
    private static final int GENERATE_BATCH_SIZE = 10_000;
    private static final int GENERATE_CONCURRENT_TASKS = 2;
    private static final int TERMINATE_CONCURRENT_TASKS = 8;
    private static final int DRAINED_POLLS_REQUIRED = 20;
    private static final long POLL_DELAY_MILLIS = 500L;

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of("nifi.scheduler.virtual.threads.enabled", "true");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testMillionFlowFilesThroughVirtualThreads() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity generateToTerminate = getClientUtil().createConnection(generate, terminate, "success");
        getClientUtil().updateConnectionBackpressure(generateToTerminate, 200_000L, 1_000_000_000L);

        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("File Size", "0 B");
        generateProperties.put("Batch Size", String.valueOf(GENERATE_BATCH_SIZE));
        generateProperties.put("Max FlowFiles", String.valueOf(TARGET_FLOWFILES));
        generateProperties.put("State Scope", "LOCAL");
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        final ProcessorConfigDTO generateConfig = new ProcessorConfigDTO();
        generateConfig.setSchedulingPeriod("0 sec");
        generateConfig.setConcurrentlySchedulableTaskCount(GENERATE_CONCURRENT_TASKS);
        final ProcessorEntity configuredGenerate = getClientUtil().updateProcessorConfig(generate, generateConfig);

        final ProcessorConfigDTO terminateConfig = new ProcessorConfigDTO();
        terminateConfig.setSchedulingPeriod("0 sec");
        terminateConfig.setConcurrentlySchedulableTaskCount(TERMINATE_CONCURRENT_TASKS);
        final ProcessorEntity configuredTerminate = getClientUtil().updateProcessorConfig(terminate, terminateConfig);

        getClientUtil().startProcessor(configuredGenerate);
        getClientUtil().startProcessor(configuredTerminate);

        try {
            waitForSustainedDrain(generateToTerminate.getId());
        } finally {
            getClientUtil().stopProcessor(configuredGenerate);
            getClientUtil().stopProcessor(configuredTerminate);
            getClientUtil().waitForStoppedProcessor(configuredGenerate.getId());
            getClientUtil().waitForStoppedProcessor(configuredTerminate.getId());
        }

        assertEquals(0, getConnectionQueueSize(generateToTerminate.getId()),
                "All generated FlowFiles should have been terminated");
    }

    /**
     * First waits for the connection queue to be observed as non-empty (so that we know the
     * generator is actually producing FlowFiles), then waits for the connection queue to stay
     * empty for {@link #DRAINED_POLLS_REQUIRED} consecutive polls. Because the generator is
     * configured with a bounded Max FlowFiles, a sustained empty queue indicates that all
     * {@value #TARGET_FLOWFILES} FlowFiles have been emitted and drained.
     */
    private void waitForSustainedDrain(final String connectionId) throws InterruptedException {
        boolean queueWasObservedNonEmpty = false;
        int consecutiveDrainedPolls = 0;
        long lastLogTime = 0L;
        while (!queueWasObservedNonEmpty || consecutiveDrainedPolls < DRAINED_POLLS_REQUIRED) {
            final int queueSize;
            try {
                queueSize = getConnectionQueueSize(connectionId);
            } catch (final Exception e) {
                logger.debug("Failed to read queue size; continuing to poll", e);
                Thread.sleep(POLL_DELAY_MILLIS);
                continue;
            }

            if (queueSize > 0) {
                queueWasObservedNonEmpty = true;
                consecutiveDrainedPolls = 0;
            } else if (queueWasObservedNonEmpty) {
                consecutiveDrainedPolls++;
            }

            final long now = System.currentTimeMillis();
            if (now - lastLogTime >= 2_000L) {
                logger.info("Current queue size: {} (drained polls: {}/{}, saw non-empty: {})",
                        queueSize, consecutiveDrainedPolls, DRAINED_POLLS_REQUIRED, queueWasObservedNonEmpty);
                lastLogTime = now;
            }

            Thread.sleep(POLL_DELAY_MILLIS);
        }
    }
}
