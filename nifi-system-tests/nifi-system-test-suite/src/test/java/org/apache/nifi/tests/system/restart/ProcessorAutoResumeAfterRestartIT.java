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

package org.apache.nifi.tests.system.restart;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessorAutoResumeAfterRestartIT extends NiFiSystemIT {

    @Test
    public void testRunningProcessorsResumeAfterRestart() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE);
        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);
        getClientUtil().createConnection(generate, terminate, SUCCESS);

        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 sec");
        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(terminate);
        getClientUtil().waitForRunningProcessor(generate.getId());
        getClientUtil().waitForRunningProcessor(terminate.getId());

        final Set<String> runningProcessorIds = Set.of(generate.getId(), terminate.getId());

        // Wait for the flow to be saved at least once with RUNNING states.
        // The SaveReportingTask runs every 500ms and the default write delay is 500ms,
        // so 3 seconds is sufficient for at least one successful save.
        Thread.sleep(3000);

        // Trigger a new save request by making a flow modification, then immediately stop NiFi.
        // Each REST API modification calls saveFlowChanges() which sets a saveHolder with a 500ms delay.
        // By stopping NiFi immediately after the modification, the pending save has not yet been processed.
        // During NiFi's shutdown sequence, the pending save would execute after all processors have been
        // stopped, persisting ENABLED states instead of RUNNING. The fix in StandardFlowService.stop()
        // flushes any pending save before stopping the controller, preserving RUNNING states.
        final ProcessorEntity addedBeforeShutdown = getClientUtil().createProcessor(TERMINATE_FLOWFILE);

        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();

        // After shutdown, verify that flow.json.gz still has RUNNING states for the started processors.
        // Without the fix, the shutdown race condition would cause processors to be saved with ENABLED
        // states instead of RUNNING, preventing auto-resume on the next startup.
        final File confDir = new File(nifiInstance.getInstanceDirectory(), "conf");
        final File flowJsonGz = new File(confDir, "flow.json.gz");

        final Map<String, String> processorStates = getProcessorScheduledStates(flowJsonGz);
        assertFalse(processorStates.isEmpty(), "Expected processors in flow.json.gz");
        for (final String processorId : runningProcessorIds) {
            final String state = processorStates.get(processorId);
            assertEquals("RUNNING", state);
        }

        assertTrue(processorStates.containsKey(addedBeforeShutdown.getId()));

        nifiInstance.start(true);
        getClientUtil().waitForRunningProcessor(generate.getId());
        getClientUtil().waitForRunningProcessor(terminate.getId());

        final ProcessorEntity generateAfterRestart = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        assertEquals("RUNNING", generateAfterRestart.getComponent().getState());

        final ProcessorEntity terminateAfterRestart = getNifiClient().getProcessorClient().getProcessor(terminate.getId());
        assertEquals("RUNNING", terminateAfterRestart.getComponent().getState());

        final ProcessorEntity addedAfterRestart = getNifiClient().getProcessorClient().getProcessor(addedBeforeShutdown.getId());
        assertNotNull(addedAfterRestart);
        assertEquals("STOPPED", addedAfterRestart.getComponent().getState());
    }

    private Map<String, String> getProcessorScheduledStates(final File flowJsonGz) throws IOException {
        final byte[] decompressed = decompress(flowJsonGz);
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode root = mapper.readTree(decompressed);
        final Map<String, String> states = new HashMap<>();
        final JsonNode rootGroup = root.path("rootGroup");
        collectProcessorStates(rootGroup, states);
        return states;
    }

    private void collectProcessorStates(final JsonNode group, final Map<String, String> states) {
        final JsonNode processors = group.path("processors");
        if (processors.isArray()) {
            for (final JsonNode processor : processors) {
                final JsonNode instanceId = processor.path("instanceIdentifier");
                final JsonNode scheduledState = processor.path("scheduledState");
                if (!instanceId.isMissingNode() && !scheduledState.isMissingNode()) {
                    states.put(instanceId.asText(), scheduledState.asText());
                }
            }
        }
        final JsonNode childGroups = group.path("processGroups");
        if (childGroups.isArray()) {
            for (final JsonNode childGroup : childGroups) {
                collectProcessorStates(childGroup, states);
            }
        }
    }

    private byte[] decompress(final File gzipFile) throws IOException {
        try (final InputStream fis = Files.newInputStream(gzipFile.toPath());
             final GZIPInputStream gzis = new GZIPInputStream(fis);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final byte[] buffer = new byte[8192];
            int len;
            while ((len = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        }
    }
}
