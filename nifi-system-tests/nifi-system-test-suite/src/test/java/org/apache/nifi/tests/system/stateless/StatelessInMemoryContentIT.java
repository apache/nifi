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

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatelessInMemoryContentIT extends NiFiSystemIT {

    private static final String HELLO_WORLD = "Hello World";
    private static final String EXCLAMATIONS = "!!!";

    // Result of reversing "Hello World" to "dlroW olleH", appending "!!!", then reversing "dlroW olleH!!!" back.
    private static final String TRANSFORMED = EXCLAMATIONS + HELLO_WORLD;

    private static final String LARGE_BUDGET = "1 MB";
    private static final String TINY_BUDGET = "10 B";

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    public void testContentUnderBudgetStaysInMemory() throws NiFiClientException, IOException, InterruptedException {
        final SelfContainedFlow flow = createSelfContainedTransformFlow(LARGE_BUDGET);
        final long contentBytesBeforeStart = contentBytesOnDisk();

        getClientUtil().startProcessGroupComponents(flow.groupId());

        waitFor(() -> Files.exists(flow.markerFile()));
        assertEquals(contentBytesBeforeStart, contentBytesOnDisk());
        waitFor(() -> getProcessorFlowFilesIn(flow.matchedTerminateId()) >= 1);
        getClientUtil().stopProcessGroupComponents(flow.groupId());

        assertEquals(0, getProcessorFlowFilesIn(flow.unmatchedTerminateId()));
    }

    @Test
    public void testContentOverBudgetSpills() throws NiFiClientException, IOException, InterruptedException {
        final SelfContainedFlow flow = createSelfContainedTransformFlow(TINY_BUDGET);
        final long contentBytesBeforeStart = contentBytesOnDisk();

        getClientUtil().startProcessGroupComponents(flow.groupId());

        waitFor(() -> Files.exists(flow.markerFile()));
        assertTrue(contentBytesOnDisk() > contentBytesBeforeStart);
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
        assertThrows(NiFiClientException.class, () -> getClientUtil().setStatelessFlowFileContentInMemoryMax(statelessGroup, TINY_BUDGET));

        getClientUtil().stopProcessGroupComponents(groupId);

        final ProcessGroupEntity stoppedGroup = getClientUtil().setStatelessFlowFileContentInMemoryMax(statelessGroup, TINY_BUDGET);
        assertEquals(TINY_BUDGET, stoppedGroup.getComponent().getStatelessFlowFileContentInMemoryMax());

        final long contentBytesBeforeRestart = contentBytesOnDisk();
        getClientUtil().startProcessGroupComponents(groupId);
        waitFor(() -> getProcessorFlowFilesIn(terminate.getId()) >= 2);
        assertTrue(contentBytesOnDisk() > contentBytesBeforeRestart);
        getClientUtil().stopProcessGroupComponents(groupId);
    }

    /**
     * Builds a flow in which a FlowFile is generated outside the Stateless group, sent into it through an Input Port, has its content rewritten inside the group by
     * ReverseContents, and leaves through an Output Port into a connection whose destination is never started. The FlowFile therefore remains queued outside the
     * group, which keeps the content that left the group referenced on disk. The transformed content is verified byte-for-byte. For the spill case, a Sleep
     * processor keeps the transformed FlowFile inside the Stateless group long enough to confirm that its claim is written to disk before it reaches the Output
     * Port boundary.
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
        if (verifySpill) {
            final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", groupId);
            getClientUtil().updateProcessorProperties(sleep, Map.of("onTrigger Sleep Time", "10 sec"));
            getClientUtil().createConnection(reverse, sleep, SUCCESS, groupId);
            getClientUtil().createConnection(sleep, outputPort, SUCCESS);
            getClientUtil().waitForValidProcessor(sleep.getId());
        } else {
            getClientUtil().createConnection(reverse, outputPort, SUCCESS);
        }

        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);
        final ConnectionEntity inputToStateless = getClientUtil().createConnection(generate, inputPort, SUCCESS);
        final ConnectionEntity outputToTerminate = getClientUtil().createConnection(outputPort, terminate);

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(reverse.getId());

        getClientUtil().runProcessorOnce(generate);
        waitForQueueCount(inputToStateless.getId(), 1);
        final long contentBytesBeforeStateless = contentBytesOnDisk();
        getClientUtil().startProcessGroupComponents(groupId);

        if (verifySpill) {
            waitFor(() -> contentBytesOnDisk() > contentBytesBeforeStateless);
            assertEquals(0, getConnectionQueueSize(outputToTerminate.getId()));
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
        statelessGroup.getComponent().setMaxConcurrentTasks(4);
        getClientUtil().markStateless(statelessGroup, "1 min", budget);
        final String groupId = statelessGroup.getId();

        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, groupId);
        getClientUtil().updateProcessorProperties(generate, Map.of("Text", HELLO_WORLD));

        final ProcessorEntity reverseFirst = getClientUtil().createProcessor(REVERSE_CONTENTS, groupId);
        final ProcessorEntity append = getClientUtil().createProcessor("UpdateContent", groupId);
        getClientUtil().updateProcessorProperties(append, Map.of("Content", EXCLAMATIONS, "Update Strategy", "Append"));
        final ProcessorEntity reverseSecond = getClientUtil().createProcessor(REVERSE_CONTENTS, groupId);
        getClientUtil().updateProcessorRunDuration(reverseFirst, 25);
        getClientUtil().updateProcessorRunDuration(append, 25);
        getClientUtil().updateProcessorRunDuration(reverseSecond, 25);

        final ProcessorEntity verify = getClientUtil().createProcessor("VerifyContents", groupId);
        getClientUtil().updateProcessorProperties(verify, Map.of("matched", TRANSFORMED));
        final Path markerFile = new File(getNiFiInstance().getInstanceDirectory(), "target/stateless-content-marker-" + groupId).getAbsoluteFile().toPath();
        Files.deleteIfExists(markerFile);
        final ProcessorEntity writeMarker = getClientUtil().createProcessor("WriteToFile", groupId);
        getClientUtil().updateProcessorProperties(writeMarker, Map.of("Filename", markerFile.toString()));
        getClientUtil().setAutoTerminatedRelationships(writeMarker, "failure");
        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", groupId);
        getClientUtil().updateProcessorProperties(sleep, Map.of("onTrigger Sleep Time", "10 sec"));
        final ProcessorEntity matchedTerminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, groupId);
        final ProcessorEntity unmatchedTerminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, groupId);

        getClientUtil().createConnection(generate, reverseFirst, SUCCESS, groupId);
        getClientUtil().createConnection(reverseFirst, append, SUCCESS, groupId);
        getClientUtil().createConnection(append, reverseSecond, SUCCESS, groupId);
        getClientUtil().createConnection(reverseSecond, verify, SUCCESS, groupId);
        getClientUtil().createConnection(verify, writeMarker, "matched", groupId);
        getClientUtil().createConnection(writeMarker, sleep, SUCCESS, groupId);
        getClientUtil().createConnection(sleep, matchedTerminate, SUCCESS, groupId);
        getClientUtil().createConnection(verify, unmatchedTerminate, "unmatched", groupId);

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(reverseFirst.getId());
        getClientUtil().waitForValidProcessor(append.getId());
        getClientUtil().waitForValidProcessor(reverseSecond.getId());
        getClientUtil().waitForValidProcessor(verify.getId());
        getClientUtil().waitForValidProcessor(writeMarker.getId());
        getClientUtil().waitForValidProcessor(sleep.getId());

        return new SelfContainedFlow(groupId, matchedTerminate.getId(), unmatchedTerminate.getId(), markerFile);
    }

    private int getProcessorFlowFilesIn(final String processorId) throws NiFiClientException, IOException {
        return getNifiClient().getProcessorClient().getProcessor(processorId).getStatus().getAggregateSnapshot().getFlowFilesIn();
    }

    private long contentBytesOnDisk() throws IOException {
        final File contentRepository = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");
        if (!contentRepository.exists()) {
            return 0L;
        }

        try (final Stream<Path> paths = Files.walk(contentRepository.toPath())) {
            return paths.filter(Files::isRegularFile)
                .mapToLong(path -> path.toFile().length())
                .sum();
        }
    }

    private record SelfContainedFlow(String groupId, String matchedTerminateId, String unmatchedTerminateId, Path markerFile) {
    }
}
