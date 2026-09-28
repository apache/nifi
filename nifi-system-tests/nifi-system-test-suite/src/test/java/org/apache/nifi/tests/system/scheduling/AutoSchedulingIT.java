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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AutoSchedulingIT extends NiFiSystemIT {
    private static final int BACKPRESSURE_COUNT = 100;

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of("nifi.scheduling.strategy", "VIRTUAL");
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testSourceAndQueueConsumerWithAutomaticScheduling() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");
        getClientUtil().updateConnectionBackpressure(connection, BACKPRESSURE_COUNT, 10_000_000L);

        final ProcessorEntity automaticGenerate = getClientUtil().updateProcessorSchedulingStrategy(generate, "AUTO");
        final ProcessorEntity automaticTerminate = getClientUtil().updateProcessorSchedulingStrategy(terminate, "AUTO");

        assertCanonicalAutoConfiguration(automaticGenerate);
        assertCanonicalAutoConfiguration(automaticTerminate);

        getClientUtil().startProcessor(automaticGenerate);
        try {
            waitForQueueCount(connection, BACKPRESSURE_COUNT);
        } finally {
            getClientUtil().stopProcessor(automaticGenerate);
        }

        getClientUtil().startProcessor(automaticTerminate);
        try {
            waitForQueueCount(connection, 0);
        } finally {
            getClientUtil().stopProcessor(automaticTerminate);
        }

        assertEquals(0, getConnectionQueueSize(connection.getId()));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testContinuousFlowAdaptsToBlockingWork() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity input = getClientUtil().createConnection(generate, sleep, "success");
        final ConnectionEntity output = getClientUtil().createConnection(sleep, terminate, "success");
        getClientUtil().updateConnectionBackpressure(input, BACKPRESSURE_COUNT, 10_000_000L);
        getClientUtil().updateConnectionBackpressure(output, BACKPRESSURE_COUNT, 10_000_000L);
        getClientUtil().updateProcessorProperties(sleep, Map.of("onTrigger Sleep Time", "100 ms"));
        getClientUtil().updateProcessorSchedulingStrategy(generate, "AUTO");
        getClientUtil().updateProcessorSchedulingStrategy(sleep, "AUTO");
        getClientUtil().updateProcessorSchedulingStrategy(terminate, "AUTO");

        getClientUtil().startProcessor(terminate);
        getClientUtil().startProcessor(sleep);
        getClientUtil().startProcessor(generate);
        try {
            final boolean adaptive = "VIRTUAL".equals(getNifiPropertiesOverrides().get("nifi.scheduling.strategy"));
            final int expectedActiveTasks = adaptive ? 4 : 1;
            waitFor(() -> getNifiClient().getProcessorClient().getProcessor(sleep.getId()).getStatus().getAggregateSnapshot().getActiveThreadCount() >= expectedActiveTasks);
            waitFor(() -> getNifiClient().getProcessorClient().getProcessor(terminate.getId()).getStatus().getAggregateSnapshot().getFlowFilesIn() > 0);
            assertCanonicalAutoConfiguration(getNifiClient().getProcessorClient().getProcessor(sleep.getId()));
            assertTrue(getConnectionQueueSize(input.getId()) > 0);

            getClientUtil().stopProcessor(generate);
            waitForQueueCount(input, 0);
            waitForQueueCount(output, 0);
        } finally {
            getClientUtil().stopProcessor(generate);
            getClientUtil().stopProcessor(sleep);
            getClientUtil().stopProcessor(terminate);
        }
    }

    private void assertCanonicalAutoConfiguration(final ProcessorEntity processorEntity) {
        final ProcessorConfigDTO config = processorEntity.getComponent().getConfig();
        assertEquals("AUTO", config.getSchedulingStrategy());
        assertEquals(1, config.getConcurrentlySchedulableTaskCount());
        assertEquals("0 sec", config.getSchedulingPeriod());
        assertEquals(0L, config.getRunDurationMillis());
    }
}
