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

package org.apache.nifi.tests.system.stateless;

import jakarta.ws.rs.WebApplicationException;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class StatelessInMemoryContentIT extends NiFiSystemIT {

    private static final String HELLO_WORLD = "Hello World";
    private static final String EXCLAMATIONS = "!!!";

    // Result of reversing "Hello World" to "dlroW olleH", appending "!!!", then reversing "dlroW olleH!!!" back.
    private static final String TRANSFORMED = EXCLAMATIONS + HELLO_WORLD;

    private static final String LARGE_BUDGET = "1 MB";
    private static final String TINY_BUDGET = "10 B";
    private static final String GATE_YIELD_DURATION = "10 millis";

    @Test
    public void testContentUnderBudgetStaysInMemory() throws NiFiClientException, IOException, InterruptedException {
        final SelfContainedFlow flow = createSelfContainedTransformFlow(LARGE_BUDGET);
        final Map<Path, Long> contentFileSizesBeforeStart = getContentFileSizes();

        getClientUtil().startProcessGroupComponents(flow.groupId());

        waitFor(() -> Files.exists(flow.markerFile()));
        assertFalse(hasContentRepositoryGrown(contentFileSizesBeforeStart));
        Files.createFile(flow.gateFile());
        waitFor(() -> getProcessorFlowFilesIn(flow.matchedTerminateId()) >= 1);
        getClientUtil().stopProcessGroupComponents(flow.groupId());

        assertEquals(0, getProcessorFlowFilesIn(flow.unmatchedTerminateId()));
    }

    @Test
    public void testContentOverBudgetSpills() throws NiFiClientException, IOException, InterruptedException {
        final SelfContainedFlow flow = createSelfContainedTransformFlow(TINY_BUDGET);
        final Map<Path, Long> contentFileSizesBeforeStart = getContentFileSizes();

        getClientUtil().startProcessGroupComponents(flow.groupId());

        waitFor(() -> Files.exists(flow.markerFile()));
        waitFor(() -> hasContentRepositoryGrown(contentFileSizesBeforeStart));
        Files.createFile(flow.gateFile());
        waitFor(() -> getProcessorFlowFilesIn(flow.matchedTerminateId()) >= 1);
        getClientUtil().stopProcessGroupComponents(flow.groupId());

        assertEquals(0, getProcessorFlowFilesIn(flow.unmatchedTerminateId()));
    }

    @Test
    public void testInputOutputPortsUnderBudget() throws NiFiClientException, IOException, InterruptedException {
        verifyInputOutputPortsReverseContent(LARGE_BUDGET, HELLO_WORLD);
    }

    @Test
    public void testInputOutputPortsSpillOver() throws NiFiClientException, IOException, InterruptedException {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            builder.append("ABCDEFGHIJ");
        }

        verifyInputOutputPortsReverseContent(TINY_BUDGET, builder.toString());
    }

    @Test
    public void testCannotChangeInMemoryMaxWhileRunning() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(statelessGroup, "1 min", LARGE_BUDGET);
        final String groupId = statelessGroup.getId();

        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, groupId);
        getClientUtil().updateProcessorProperties(generate, Map.of("Text", HELLO_WORLD));
        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, groupId);
        getClientUtil().createConnection(generate, terminate, SUCCESS, groupId);

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().startProcessGroupComponents(groupId);

        waitFor(() -> getProcessorFlowFilesIn(terminate.getId()) >= 1);

        // The in-memory budget is applied when the Stateless flow starts, so it cannot be changed while the group is running.
        final NiFiClientException exception = assertThrows(
            NiFiClientException.class, () -> getClientUtil().setStatelessFlowFileContentInMemoryMax(statelessGroup, TINY_BUDGET));
        final WebApplicationException cause = assertInstanceOf(WebApplicationException.class, exception.getCause());
        assertEquals(409, cause.getResponse().getStatus());

        getClientUtil().stopProcessGroupComponents(groupId);

        final ProcessGroupEntity stoppedGroup = getClientUtil().setStatelessFlowFileContentInMemoryMax(statelessGroup, TINY_BUDGET);
        assertEquals(TINY_BUDGET, stoppedGroup.getComponent().getStatelessFlowFileContentInMemoryMax());

        final Map<Path, Long> contentFileSizesBeforeRestart = getContentFileSizes();
        getClientUtil().startProcessGroupComponents(groupId);
        waitFor(() -> getProcessorFlowFilesIn(terminate.getId()) >= 2);
        waitFor(() -> hasContentRepositoryGrown(contentFileSizesBeforeRestart));
        getClientUtil().stopProcessGroupComponents(groupId);
    }

    /**
     * Builds a flow in which a FlowFile is generated outside the Stateless group, sent into it through an Input Port, has its content rewritten inside the group by
     * ReverseContents, and leaves through an Output Port into a connection whose destination is never started. The FlowFile therefore remains queued outside the
     * group, which keeps the content that left the group referenced in the Content Repository. The transformed content is verified byte-for-byte. For the spill case,
     * a file-controlled processor keeps the transformed FlowFile inside the Stateless group until its claim is confirmed in the Content Repository.
     */
    private void verifyInputOutputPortsReverseContent(final String budget, final String content) throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE);
        getClientUtil().updateProcessorProperties(generate, Map.of("Text", content));
        final boolean verifySpill = TINY_BUDGET.equals(budget);

        final ProcessGroupEntity statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(statelessGroup, "1 min", budget);
        final String groupId = statelessGroup.getId();

        final PortEntity inputPort = getClientUtil().createInputPort("In", groupId);
        final PortEntity outputPort = getClientUtil().createOutputPort("Out", groupId);

        final ProcessorEntity reverse = getClientUtil().createProcessor(REVERSE_CONTENTS, groupId);
        getClientUtil().createConnection(inputPort, reverse, groupId);
        final Path gateFile;
        if (verifySpill) {
            gateFile = prepareGateFile("input-output-" + groupId);
            final ProcessorEntity passThrough = getClientUtil().createProcessor("PassThrough", groupId);
            getClientUtil().updateProcessorProperties(passThrough, Map.of("Gate File", gateFile.toString()));
            getClientUtil().updateProcessorYieldDuration(passThrough, GATE_YIELD_DURATION);
            getClientUtil().createConnection(reverse, passThrough, SUCCESS, groupId);
            getClientUtil().createConnection(passThrough, outputPort, SUCCESS);
            getClientUtil().waitForValidProcessor(passThrough.getId());
        } else {
            gateFile = null;
            getClientUtil().createConnection(reverse, outputPort, SUCCESS);
        }

        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);
        final ConnectionEntity inputToStateless = getClientUtil().createConnection(generate, inputPort, SUCCESS);
        final ConnectionEntity outputToTerminate = getClientUtil().createConnection(outputPort, terminate);

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(reverse.getId());

        getClientUtil().runProcessorOnce(generate);
        waitForQueueCount(inputToStateless.getId(), 1);
        final Map<Path, Long> contentFileSizesBeforeStateless = getContentFileSizes();
        getClientUtil().startProcessGroupComponents(groupId);

        if (verifySpill) {
            waitFor(() -> hasContentRepositoryGrown(contentFileSizesBeforeStateless));
            assertEquals(0, getConnectionQueueSize(outputToTerminate.getId()));
            Files.createFile(gateFile);
        }

        waitForQueueCount(outputToTerminate.getId(), 1);

        final String expected = new StringBuilder(content).reverse().toString();
        final String outputContent = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 0);
        assertEquals(expected, outputContent);
        assertEquals(content.length(), getClientUtil().getQueueFlowFile(outputToTerminate.getId(), 0).getFlowFile().getSize());

        getClientUtil().stopProcessGroupComponents(groupId);
    }

    private SelfContainedFlow createSelfContainedTransformFlow(final String budget) throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(statelessGroup, "1 min", budget);
        final String groupId = statelessGroup.getId();

        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, groupId);
        getClientUtil().updateProcessorProperties(generate, Map.of("Text", HELLO_WORLD));

        final ProcessorEntity reverseFirst = getClientUtil().createProcessor(REVERSE_CONTENTS, groupId);
        final ProcessorEntity append = getClientUtil().createProcessor("UpdateContent", groupId);
        getClientUtil().updateProcessorProperties(append, Map.of("Content", EXCLAMATIONS, "Update Strategy", "Append"));
        final ProcessorEntity reverseSecond = getClientUtil().createProcessor(REVERSE_CONTENTS, groupId);

        final ProcessorEntity verify = getClientUtil().createProcessor("VerifyContents", groupId);
        getClientUtil().updateProcessorProperties(verify, Map.of("matched", TRANSFORMED));
        final Path markerFile = new File(getNiFiInstance().getInstanceDirectory(), "target/stateless-content-marker-" + groupId).getAbsoluteFile().toPath();
        Files.deleteIfExists(markerFile);
        final ProcessorEntity writeMarker = getClientUtil().createProcessor("WriteToFile", groupId);
        getClientUtil().updateProcessorProperties(writeMarker, Map.of("Filename", markerFile.toString()));
        getClientUtil().setAutoTerminatedRelationships(writeMarker, "failure");
        final Path gateFile = prepareGateFile("self-contained-" + groupId);
        final ProcessorEntity passThrough = getClientUtil().createProcessor("PassThrough", groupId);
        getClientUtil().updateProcessorProperties(passThrough, Map.of("Gate File", gateFile.toString()));
        getClientUtil().updateProcessorYieldDuration(passThrough, GATE_YIELD_DURATION);
        final ProcessorEntity matchedTerminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, groupId);
        final ProcessorEntity unmatchedTerminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, groupId);

        getClientUtil().createConnection(generate, reverseFirst, SUCCESS, groupId);
        getClientUtil().createConnection(reverseFirst, append, SUCCESS, groupId);
        getClientUtil().createConnection(append, reverseSecond, SUCCESS, groupId);
        getClientUtil().createConnection(reverseSecond, verify, SUCCESS, groupId);
        getClientUtil().createConnection(verify, writeMarker, "matched", groupId);
        getClientUtil().createConnection(writeMarker, passThrough, SUCCESS, groupId);
        getClientUtil().createConnection(passThrough, matchedTerminate, SUCCESS, groupId);
        getClientUtil().createConnection(verify, unmatchedTerminate, "unmatched", groupId);

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(reverseFirst.getId());
        getClientUtil().waitForValidProcessor(append.getId());
        getClientUtil().waitForValidProcessor(reverseSecond.getId());
        getClientUtil().waitForValidProcessor(verify.getId());
        getClientUtil().waitForValidProcessor(writeMarker.getId());
        getClientUtil().waitForValidProcessor(passThrough.getId());

        return new SelfContainedFlow(groupId, matchedTerminate.getId(), unmatchedTerminate.getId(), markerFile, gateFile);
    }

    private Path prepareGateFile(final String name) throws IOException {
        final Path gateFile = new File(getNiFiInstance().getInstanceDirectory(), "target/stateless-content-gate-" + name).getAbsoluteFile().toPath();
        Files.createDirectories(gateFile.getParent());
        Files.deleteIfExists(gateFile);
        return gateFile;
    }

    private int getProcessorFlowFilesIn(final String processorId) throws NiFiClientException, IOException {
        return getNifiClient().getProcessorClient().getProcessor(processorId).getStatus().getAggregateSnapshot().getFlowFilesIn();
    }

    private Map<Path, Long> getContentFileSizes() throws IOException {
        final Map<Path, Long> contentFileSizes = new HashMap<>();
        final File contentRepository = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");
        if (!contentRepository.exists()) {
            return contentFileSizes;
        }

        try (final Stream<Path> paths = Files.walk(contentRepository.toPath())) {
            paths.filter(Files::isRegularFile).forEach(path -> contentFileSizes.put(path, path.toFile().length()));
        }

        return contentFileSizes;
    }

    private boolean hasContentRepositoryGrown(final Map<Path, Long> originalFileSizes) throws IOException {
        final Map<Path, Long> currentFileSizes = getContentFileSizes();
        for (final Map.Entry<Path, Long> entry : currentFileSizes.entrySet()) {
            if (entry.getValue() > originalFileSizes.getOrDefault(entry.getKey(), 0L)) {
                return true;
            }
        }

        return false;
    }

    private record SelfContainedFlow(String groupId, String matchedTerminateId, String unmatchedTerminateId, Path markerFile, Path gateFile) {
    }
}
