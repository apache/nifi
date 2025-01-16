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

import org.apache.nifi.components.PortFunction;
import org.apache.nifi.diagnostics.DiagnosticLevel;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.FlowFileSummaryDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsSnapshotDTO.ResourceClaimDetailsDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.provenance.AttributeDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceSearchValueDTO;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ListingRequestEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.apache.nifi.web.api.entity.SystemDiagnosticsEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatelessBasicsIT extends NiFiSystemIT {

    private static final String HELLO_WORLD = "Hello World";
    private static final String FIVE_LINES = (HELLO_WORLD + "\n").repeat(5);
    private static final String HELLO_WORLD_REVERSED = new StringBuilder(HELLO_WORLD).reverse().toString();
    private static final String HELLO = "Hello";

    private ProcessorEntity generate;
    private ProcessorEntity terminate;
    private PortEntity inputPort;
    private PortEntity outputPort;
    private ConnectionEntity outputToTerminate;
    private ConnectionEntity generateToInput;
    private ProcessGroupEntity statelessGroup;

    private int startClaimantCount;

    @BeforeEach
    public void captureStartClaimantCount() throws NiFiClientException, IOException {
        startClaimantCount = getClaimantCounts();
    }

    @Test
    public void testOrderingIntraSession() throws NiFiClientException, IOException, InterruptedException {
        final int batchSize = 100;

        statelessGroup = getClientUtil().createProcessGroup("testOrderingIntraSession", "root");
        getClientUtil().markStateless(statelessGroup, "1 min");

        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, statelessGroup.getId());
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", HELLO_WORLD);
        generateProperties.put("Batch Size", String.valueOf(batchSize));
        generateProperties.put("Counter", "${nextInt()}");
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        final ProcessorEntity router = getClientUtil().createProcessor("ReOrderFlowFiles", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(router, Map.of("First Group Selection Criteria", "${Counter:mod(2):equals(0)}"));

        // Verify that FlowFiles are ordered correctly within stateless flow.
        final ProcessorEntity verifyProcessor = getClientUtil().createProcessor("VerifyEvenThenOdd", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(verifyProcessor, Map.of("Attribute Name", "Counter"));

        final PortEntity outputPort = getClientUtil().createOutputPort("Out", statelessGroup.getId());

        getClientUtil().createConnection(generate, router, "success");
        getClientUtil().createConnection(router, verifyProcessor, "success");
        getClientUtil().createConnection(verifyProcessor, outputPort, "success");
        getClientUtil().setAutoTerminatedRelationships(verifyProcessor, "failure");

        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);
        final ConnectionEntity outputToTerminate = getClientUtil().createConnection(outputPort, terminate);
        getClientUtil().updateConnectionPrioritizer(outputToTerminate, "FirstInFirstOutPrioritizer");

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(router.getId());
        getClientUtil().waitForValidProcessor(verifyProcessor.getId());
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        waitForQueueCount(outputToTerminate.getId(), batchSize);
        getClientUtil().stopProcessGroupComponents(statelessGroup.getId());

        final List<String> actualCounterValues = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            final FlowFileEntity flowFileEntity = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), i);
            actualCounterValues.add(flowFileEntity.getFlowFile().getAttributes().get("Counter"));
        }

        int expectedCounter = 0;
        for (int i = 0; i < batchSize; i++) {
            assertEquals(String.valueOf(expectedCounter), actualCounterValues.get(i));
            expectedCounter += 2;
            if (expectedCounter >= batchSize) {
                expectedCounter = 1;
            }
        }
    }

    @Test
    public void testOneInOneOut() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();
        getClientUtil().createConnection(inputPort, outputPort);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(outputToTerminate.getId(), 1);

        final String outputContent = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 0);
        assertEquals(HELLO_WORLD, outputContent);

        final FlowFileEntity outputFlowFile = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), 0);
        assertEquals(HELLO, outputFlowFile.getFlowFile().getAttributes().get("Greeting"));
        assertEquals(HELLO_WORLD.length(), outputFlowFile.getFlowFile().getSize());

        assertEquals(0, getConnectionQueueSize(generateToInput.getId()));

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }


    private void createFlowShell() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell("1 min");
    }

    private void createFlowShell(final String timeout) throws NiFiClientException, IOException, InterruptedException {
        generate = getClientUtil().createProcessor(GENERATE_FLOWFILE);
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", HELLO_WORLD);
        generateProperties.put("Greeting", HELLO);
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(statelessGroup, timeout);

        inputPort = getClientUtil().createInputPort("In", statelessGroup.getId());
        outputPort = getClientUtil().createOutputPort("Out", statelessGroup.getId());

        terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);

        generateToInput = getClientUtil().createConnection(generate, inputPort, SUCCESS);
        outputToTerminate = getClientUtil().createConnection(outputPort, terminate);

        getClientUtil().waitForValidProcessor(generate.getId());
    }

    @Test
    public void testOneInClonedOut() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();
        getClientUtil().createConnection(inputPort, outputPort);

        final ConnectionEntity outputToTerminate2 = getClientUtil().createConnection(outputPort, terminate);
        final ConnectionEntity outputToTerminate3 = getClientUtil().createConnection(outputPort, terminate);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        getClientUtil().startProcessor(generate);

        final ConnectionEntity[] outputConnections = new ConnectionEntity[] {outputToTerminate, outputToTerminate2, outputToTerminate3};
        for (final ConnectionEntity connection : outputConnections) {
            waitForQueueCount(connection.getId(), 1);

            final String outputContent = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 0);
            assertEquals(HELLO_WORLD, outputContent);

            final FlowFileEntity outputFlowFile = getClientUtil().getQueueFlowFile(connection.getId(), 0);
            assertEquals(HELLO, outputFlowFile.getFlowFile().getAttributes().get("Greeting"));
            assertEquals(HELLO_WORLD.length(), outputFlowFile.getFlowFile().getSize());
        }

        assertEquals(0, getConnectionQueueSize(generateToInput.getId()));

        assertEquals(startClaimantCount + 3, getClaimantCounts());
    }

    private int getClaimantCounts() throws NiFiClientException, IOException {
        final SystemDiagnosticsEntity systemDiagnostics = getNifiClient().getSystemsDiagnosticsClient().getSystemDiagnostics(false, DiagnosticLevel.VERBOSE);
        final List<ResourceClaimDetailsDTO> resourceClaimDetails = systemDiagnostics.getSystemDiagnostics().getAggregateSnapshot().getResourceClaimDetails();
        return resourceClaimDetails.stream()
            .mapToInt(ResourceClaimDetailsDTO::getClaimantCount)
            .sum();
    }


    @Test
    public void testOneInCloneWaitsForAll() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(sleep, Collections.singletonMap("onTrigger Sleep Time", "3 sec"));

        getClientUtil().createConnection(inputPort, sleep, statelessGroup.getId());
        getClientUtil().createConnection(inputPort, outputPort, statelessGroup.getId());
        getClientUtil().createConnection(sleep, outputPort, "success", statelessGroup.getId());

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        getClientUtil().startProcessor(generate);

        final long fiveSecsFromNow = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() <= fiveSecsFromNow) {
            final int queueCount = getConnectionQueueSize(outputToTerminate.getId());

            if (queueCount == 2) {
                break;
            }
        }

        waitForQueueCount(outputToTerminate.getId(), 2);
    }

    @Test
    public void testContentModification() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final ProcessorEntity reverseContents = getClientUtil().createProcessor(REVERSE_CONTENTS, statelessGroup.getId());
        getClientUtil().createConnection(inputPort, reverseContents, statelessGroup.getId());
        getClientUtil().createConnection(reverseContents, outputPort, SUCCESS);

        getClientUtil().waitForValidProcessor(reverseContents.getId());

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        getClientUtil().startProcessor(generate);

        waitForQueueCount(outputToTerminate.getId(), 1);

        final String outputContent = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 0);
        assertEquals(HELLO_WORLD_REVERSED, outputContent);

        final FlowFileEntity outputFlowFile = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), 0);
        assertEquals(HELLO, outputFlowFile.getFlowFile().getAttributes().get("Greeting"));
        assertEquals(HELLO_WORLD.length(), outputFlowFile.getFlowFile().getSize());

        assertEquals(0, getConnectionQueueSize(generateToInput.getId()));

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }

    @Test
    public void testMultipleInputPorts() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();
        getClientUtil().createConnection(inputPort, outputPort);

        final ProcessorEntity generate2 = getClientUtil().createProcessor(GENERATE_FLOWFILE);
        final PortEntity inputPort2 = getClientUtil().createInputPort("Input2", statelessGroup.getId());
        final ConnectionEntity generateToInput2 = getClientUtil().createConnection(generate2, inputPort2, SUCCESS);
        getClientUtil().createConnection(inputPort2, outputPort, statelessGroup.getId());

        getClientUtil().waitForValidProcessor(generate2.getId());

        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(generate2);
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        waitForQueueCount(outputToTerminate.getId(), 2);
        assertEquals(0, getConnectionQueueSize(generateToInput.getId()));
        assertEquals(0, getConnectionQueueSize(generateToInput2.getId()));

        // Claimant count will be 1, not 2, because generate2 does not generate any content.
        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }

    @Test
    public void testFlowFileDroppedInFlow() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        getNifiClient().getConnectionClient().deleteConnection(outputToTerminate);
        getNifiClient().getProcessorClient().deleteProcessor(this.terminate);
        getNifiClient().getOutputPortClient().deleteOutputPort(outputPort);

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", statelessGroup.getId());
        getClientUtil().createConnection(inputPort, terminate, statelessGroup.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        waitForQueueCount(generateToInput.getId(), 0);

        assertEquals(startClaimantCount, getClaimantCounts());
    }

    @Test
    public void testSplitInFlow() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final ProcessorEntity splitByWrite = getClientUtil().createProcessor("SplitByLine", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(splitByWrite, Collections.singletonMap("Use Clone", "false"));

        final ProcessorEntity splitByClone = getClientUtil().createProcessor("SplitByLine", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(splitByClone, Collections.singletonMap("Use Clone", "true"));

        getClientUtil().createConnection(inputPort, splitByWrite, statelessGroup.getId());
        getClientUtil().createConnection(inputPort, splitByClone, statelessGroup.getId());

        getClientUtil().createConnection(splitByWrite, outputPort, SUCCESS, statelessGroup.getId());
        getClientUtil().createConnection(splitByClone, outputPort, SUCCESS, statelessGroup.getId());

        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("Text", "abc\nxyz\n123"));
        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(splitByWrite.getId());
        getClientUtil().waitForValidProcessor(splitByClone.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        waitForQueueCount(outputToTerminate.getId(), 6);

        assertEquals(0, getConnectionQueueSize(generateToInput.getId()));

        final Map<String, Integer> countsPerContents = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            final String outputContent = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), i);
            countsPerContents.merge(outputContent, 1, (a, b) -> a + b);

            final FlowFileEntity outputFlowFile = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), i);
            assertEquals(HELLO, outputFlowFile.getFlowFile().getAttributes().get("Greeting"));
        }

        assertEquals(2, countsPerContents.get("abc"));
        assertEquals(2, countsPerContents.get("xyz"));
        assertEquals(2, countsPerContents.get("123"));

        assertEquals(startClaimantCount + 6, getClaimantCounts());
    }

    @Test
    public void testMergeMultiFlowFiles() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final ProcessorEntity merge = getClientUtil().createProcessor("ConcatenateFlowFiles", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(merge, Collections.singletonMap("FlowFile Count", "5"));

        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Batch Size", "5");
        generateProperties.put("Text", HELLO_WORLD);
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        getClientUtil().createConnection(inputPort, merge, statelessGroup.getId());
        getClientUtil().createConnection(merge, outputPort, "merged", statelessGroup.getId());
        getClientUtil().setAutoTerminatedRelationships(merge, Collections.singleton("original"));
        getClientUtil().waitForValidProcessor(merge.getId());

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 5);
        assertEquals(startClaimantCount + 5, getClaimantCounts());

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        waitForQueueCount(generateToInput.getId(), 0);
        waitForQueueCount(outputToTerminate.getId(), 1);

        final String outputContent = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 0);
        assertEquals(HELLO_WORLD.repeat(5), outputContent);

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }

    @Test
    public void testWaitForMinToMerge() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        getClientUtil().updateProcessorSchedulingPeriod(generate, "1 sec");

        final ProcessorEntity merge = getClientUtil().createProcessor("ConcatenateFlowFiles", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(merge, Collections.singletonMap("FlowFile Count", "3"));

        getClientUtil().createConnection(inputPort, merge, statelessGroup.getId());
        getClientUtil().createConnection(merge, outputPort, "merged", statelessGroup.getId());
        getClientUtil().setAutoTerminatedRelationships(merge, Collections.singleton("original"));
        getClientUtil().waitForValidProcessor(merge.getId());

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().startProcessor(generate);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        waitForQueueCount(generateToInput.getId(), 0);
        waitForQueueCount(outputToTerminate.getId(), 1);

        final String outputContent = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 0);
        assertEquals(HELLO_WORLD.repeat(3), outputContent);

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }

    @Test
    public void testSplitAndMergeInFlow() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final ProcessorEntity split = getClientUtil().createProcessor("SplitByLine", statelessGroup.getId());

        final ProcessorEntity merge = getClientUtil().createProcessor("ConcatenateFlowFiles", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(merge, Collections.singletonMap("FlowFile Count", "5"));

        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", FIVE_LINES);
        getClientUtil().updateProcessorProperties(generate, generateProperties);
        getClientUtil().waitForValidProcessor(generate.getId());

        getClientUtil().createConnection(inputPort, split, statelessGroup.getId());
        getClientUtil().createConnection(split, merge, SUCCESS, statelessGroup.getId());
        getClientUtil().createConnection(merge, outputPort, "merged", statelessGroup.getId());
        getClientUtil().setAutoTerminatedRelationships(merge, Collections.singleton("original"));
        getClientUtil().waitForValidProcessor(split.getId());
        getClientUtil().waitForValidProcessor(merge.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        waitForQueueCount(generateToInput.getId(), 0);
        waitForQueueCount(outputToTerminate.getId(), 1);

        final String outputContent = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 0);
        assertEquals(HELLO_WORLD.repeat(5), outputContent);

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }

    @Test
    public void testTimeoutWithOneFlowFile() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell("1 sec");

        // Add a sleep for 1 min
        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(sleep, Collections.singletonMap("onTrigger Sleep Time", "1 min"));

        getClientUtil().createConnection(inputPort, sleep, statelessGroup.getId());
        getClientUtil().createConnection(sleep, outputPort, SUCCESS, statelessGroup.getId());
        getClientUtil().waitForValidProcessor(sleep.getId());

        // Start the flow
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        getClientUtil().startProcessor(generate);

        // Wait 2 seconds, stop the group, and ensure that the FlowFile is still available.
        Thread.sleep(2000L);

        waitForBulletin();

        getClientUtil().stopProcessGroupComponents(statelessGroup.getId());
        assertEquals(1, getConnectionQueueSize(generateToInput.getId()));

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }


    @Test
    public void testTimeoutWithSplitFlowFile() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell("2 sec");

        final ProcessorEntity split = getClientUtil().createProcessor("SplitByLine", statelessGroup.getId());

        // Add a sleep for 1 min
        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(sleep, Collections.singletonMap("onTrigger Sleep Time", "1 min"));

        getClientUtil().createConnection(inputPort, split, statelessGroup.getId());
        getClientUtil().createConnection(split, sleep, SUCCESS, statelessGroup.getId());
        getClientUtil().createConnection(sleep, outputPort, SUCCESS, statelessGroup.getId());
        getClientUtil().waitForValidProcessor(sleep.getId());
        getClientUtil().waitForValidProcessor(split.getId());

        final Map<String, String> generateProperties = Collections.singletonMap("Text", FIVE_LINES);
        getClientUtil().updateProcessorProperties(generate, generateProperties);
        getClientUtil().waitForValidProcessor(generate.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        // Wait 2 seconds, stop the group, and ensure that the FlowFile is still available.
        Thread.sleep(2000L);

        waitForBulletin();

        getClientUtil().stopProcessGroupComponents(statelessGroup.getId());
        assertEquals(1, getConnectionQueueSize(generateToInput.getId()));

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }

    private void waitForBulletin() throws InterruptedException {
        waitFor(() -> {
            // We should see bulletins when the flow times out
            final List<BulletinEntity> bulletins = getNifiClient().getProcessGroupClient().getProcessGroup(statelessGroup.getId()).getBulletins();
            return !bulletins.isEmpty();
        });
    }

    @Test
    public void testStopGroupMakesFlowFileAvailable() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        // Add a sleep for 1 min
        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(sleep, Collections.singletonMap("onTrigger Sleep Time", "1 min"));

        getClientUtil().createConnection(inputPort, sleep, statelessGroup.getId());
        getClientUtil().createConnection(sleep, outputPort, SUCCESS, statelessGroup.getId());
        getClientUtil().waitForValidProcessor(sleep.getId());

        // Start the flow
        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        // Wait for the FlowFile not to be available
        waitFor(() -> {
            final ListingRequestEntity listing = getClientUtil().performQueueListing(generateToInput.getId());
            final List<FlowFileSummaryDTO> flowFileSummaries = listing.getListingRequest().getFlowFileSummaries();
            return flowFileSummaries.isEmpty();
        });

        // Should still have a size of 1, but the FlowFile is unavailable.
        assertEquals(1, getConnectionQueueSize(generateToInput.getId()));

        // Stop the group and wait for FlowFile to become available again
        getClientUtil().stopProcessGroupComponents(statelessGroup.getId());

        // Wait for the FlowFile not to be available
        waitFor(() -> {
            final ListingRequestEntity listing = getClientUtil().performQueueListing(generateToInput.getId());
            final List<FlowFileSummaryDTO> flowFileSummaries = listing.getListingRequest().getFlowFileSummaries();
            return flowFileSummaries.size() == 1;
        });

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }


    @Test
    public void testOneInRouteToFailurePort() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final ProcessorEntity reverseContents = getClientUtil().createProcessor(REVERSE_CONTENTS, statelessGroup.getId());
        getClientUtil().createConnection(inputPort, reverseContents, statelessGroup.getId());
        getClientUtil().createConnection(reverseContents, outputPort, SUCCESS, statelessGroup.getId());

        outputPort.getComponent().setPortFunction(PortFunction.FAILURE.name());
        getNifiClient().getOutputPortClient().updateOutputPort(outputPort);

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        waitForBulletin();
        waitForQueueCount(generateToInput.getId(), 0);
        waitForQueueCount(outputToTerminate.getId(), 1);

        final String outputContents = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 0);
        assertEquals(HELLO_WORLD, outputContents);

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }

    @Test
    public void testSplitRouteToFailurePort() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final PortEntity failurePort = getClientUtil().createOutputPort("Failure", statelessGroup.getId());
        failurePort.getComponent().setPortFunction(PortFunction.FAILURE.name());
        getNifiClient().getOutputPortClient().updateOutputPort(failurePort);

        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("Text", FIVE_LINES));

        final ProcessorEntity split = getClientUtil().createProcessor("SplitByLine", statelessGroup.getId());
        final ProcessorEntity roundRobin = getClientUtil().createProcessor("RoundRobinFlowFiles", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(roundRobin, Collections.singletonMap("Number of Relationships", "5"));

        getClientUtil().createConnection(inputPort, split, statelessGroup.getId());
        getClientUtil().createConnection(split, roundRobin, SUCCESS, statelessGroup.getId());
        getClientUtil().createConnection(roundRobin, outputPort, new HashSet<>(Arrays.asList("1", "2", "4", "5")), statelessGroup.getId());
        getClientUtil().createConnection(roundRobin, failurePort, "3", statelessGroup.getId());
        final ConnectionEntity failureToTerminate = getClientUtil().createConnection(failurePort, terminate);

        getClientUtil().waitForValidProcessor(split.getId());
        getClientUtil().waitForValidProcessor(roundRobin.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        waitForBulletin();
        waitForQueueCount(generateToInput.getId(), 0);
        waitForQueueCount(failureToTerminate.getId(), 1);
        waitForQueueCount(outputToTerminate.getId(), 0);

        final String outputContents = getClientUtil().getFlowFileContentAsUtf8(failureToTerminate.getId(), 0);
        assertEquals(FIVE_LINES, outputContents);

        assertEquals(startClaimantCount + 1, getClaimantCounts());
    }

    @Test
    public void testMultipleInvocationsSomeFailure() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final PortEntity failurePort = getClientUtil().createOutputPort("Failure", statelessGroup.getId());
        failurePort.getComponent().setPortFunction(PortFunction.FAILURE.name());
        getNifiClient().getOutputPortClient().updateOutputPort(failurePort);

        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("Batch Size", "5"));

        final ProcessorEntity reverseContents = getClientUtil().createProcessor(REVERSE_CONTENTS, statelessGroup.getId());
        final ProcessorEntity roundRobin = getClientUtil().createProcessor("RoundRobinFlowFiles", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(roundRobin, Collections.singletonMap("Number of Relationships", "5"));

        getClientUtil().createConnection(inputPort, reverseContents, statelessGroup.getId());
        getClientUtil().createConnection(reverseContents, roundRobin, SUCCESS, statelessGroup.getId());
        getClientUtil().createConnection(roundRobin, outputPort, new HashSet<>(Arrays.asList("1", "2", "4", "5")), statelessGroup.getId());
        getClientUtil().createConnection(roundRobin, failurePort, "3", statelessGroup.getId());
        final ConnectionEntity failureToTerminate = getClientUtil().createConnection(failurePort, terminate);

        getClientUtil().waitForValidProcessor(reverseContents.getId());
        getClientUtil().waitForValidProcessor(roundRobin.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 5);
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        waitForBulletin();
        waitForQueueCount(generateToInput.getId(), 0);
        waitForQueueCount(failureToTerminate.getId(), 1);
        waitForQueueCount(outputToTerminate.getId(), 4);

        final String failureOutputContents = getClientUtil().getFlowFileContentAsUtf8(failureToTerminate.getId(), 0);
        assertEquals(HELLO_WORLD, failureOutputContents);

        for (int i = 0; i < 4; i++) {
            final String successOutputContents = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), i);
            assertEquals(HELLO_WORLD_REVERSED, successOutputContents);
        }

        assertEquals(startClaimantCount + 5, getClaimantCounts());
    }

    @Test
    public void testOutboundConnectionRequirementsForOutputPort() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(group, "1 min");

        final PortEntity outputPort = getClientUtil().createOutputPort("Output", group.getId());
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile", group.getId());
        getClientUtil().createConnection(generate, outputPort, SUCCESS, group.getId());

        // Should be invalid because no outbound connection.
        getClientUtil().waitForOutputPortInvalid(outputPort.getId());

        // Make port a failure port. This should make the port valid
        outputPort.getComponent().setPortFunction(PortFunction.FAILURE.name());
        getNifiClient().getOutputPortClient().updateOutputPort(outputPort);
        getClientUtil().waitForOutputPortValid(outputPort.getId());

        // Add an Input Port. This should make the Output Port invalid again.
        getClientUtil().createInputPort("Input", group.getId());
        getClientUtil().waitForOutputPortInvalid(outputPort.getId());

        // Create an Outbound connection. This should make the Output Port valid again.
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", "root");
        getClientUtil().createConnection(outputPort, terminate);
        getClientUtil().waitForOutputPortValid(outputPort.getId());
    }

    @Test
    public void testProvenanceEventsPersisted() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final ProcessorEntity reverseContents = getClientUtil().createProcessor(REVERSE_CONTENTS, statelessGroup.getId());
        final ProcessorEntity setAttribute = getClientUtil().createProcessor("SetAttribute", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(setAttribute, Collections.singletonMap("greeting", HELLO_WORLD));

        getClientUtil().createConnection(inputPort, reverseContents, statelessGroup.getId());
        getClientUtil().createConnection(reverseContents, setAttribute, SUCCESS, statelessGroup.getId());
        getClientUtil().createConnection(setAttribute, outputPort, SUCCESS, statelessGroup.getId());

        final String generatedFilename = UUID.randomUUID().toString();
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("filename", generatedFilename));
        getClientUtil().waitForValidProcessor(generate.getId());

        getClientUtil().waitForValidProcessor(reverseContents.getId());
        getClientUtil().waitForValidProcessor(setAttribute.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        waitForQueueCount(outputToTerminate, 1);
        getClientUtil().startProcessor(terminate);
        waitForQueueCount(outputToTerminate, 0);

        final Map<SearchableField, ProvenanceSearchValueDTO> provQuery = new HashMap<>();
        provQuery.put(SearchableFields.Filename, createSearchValueDto(generatedFilename));
        final ProvenanceEntity provEntity = getClientUtil().queryProvenance(provQuery, null, null);
        final ProvenanceDTO provDto = provEntity.getProvenance();
        final List<ProvenanceEventDTO> eventDtos = provDto.getResults().getProvenanceEvents();
        assertEquals(4, eventDtos.size());

        final Map<String, List<ProvenanceEventDTO>> eventsByType = eventDtos.stream().collect(Collectors.groupingBy(ProvenanceEventDTO::getEventType));
        assertEquals(1, eventsByType.get(ProvenanceEventType.CREATE.name()).size());
        assertEquals(1, eventsByType.get(ProvenanceEventType.CONTENT_MODIFIED.name()).size());
        assertEquals(1, eventsByType.get(ProvenanceEventType.ATTRIBUTES_MODIFIED.name()).size());
        assertEquals(1, eventsByType.get(ProvenanceEventType.DROP.name()).size());

        final ProvenanceEventDTO createEvent = eventsByType.get(ProvenanceEventType.CREATE.name()).get(0);
        assertEquals(generate.getId(), createEvent.getComponentId());
        final Map<String, String> createAttributes = toMap(createEvent.getAttributes());
        assertEquals(generatedFilename, createAttributes.get("filename"));
        assertFalse(createAttributes.containsKey("greeting"));

        final ProvenanceEventDTO contentModifiedEvent = eventsByType.get(ProvenanceEventType.CONTENT_MODIFIED.name()).get(0);
        assertEquals(reverseContents.getId(), contentModifiedEvent.getComponentId());
        final Map<String, String> contentModifiedAttributes = toMap(contentModifiedEvent.getAttributes());
        assertEquals(generatedFilename, contentModifiedAttributes.get("filename"));
        assertFalse(contentModifiedAttributes.containsKey("greeting"));

        final ProvenanceEventDTO attributesModifiedEvent = eventsByType.get(ProvenanceEventType.ATTRIBUTES_MODIFIED.name()).get(0);
        assertEquals(setAttribute.getId(), attributesModifiedEvent.getComponentId());
        final Map<String, String> attributesModifiedAttributes = toMap(attributesModifiedEvent.getAttributes());
        assertEquals(generatedFilename, attributesModifiedAttributes.get("filename"));
        assertEquals(HELLO_WORLD, attributesModifiedAttributes.get("greeting"));

        final ProvenanceEventDTO dropEvent = eventsByType.get(ProvenanceEventType.DROP.name()).get(0);
        assertEquals(terminate.getId(), dropEvent.getComponentId());
        final Map<String, String> dropAttributes = toMap(dropEvent.getAttributes());
        assertEquals(generatedFilename, dropAttributes.get("filename"));
        assertEquals(HELLO_WORLD, dropAttributes.get("greeting"));

        assertTrue(contentModifiedEvent.getEventId() > createEvent.getEventId());
        assertTrue(attributesModifiedEvent.getEventId() > contentModifiedEvent.getEventId());
        assertTrue(dropEvent.getEventId() > attributesModifiedEvent.getEventId());
    }

    private Map<String, String> toMap(final Collection<AttributeDTO> attributes) {
        final Map<String, String> map = new HashMap<>();
        attributes.forEach(dto -> map.put(dto.getName(), dto.getValue()));
        return map;
    }

    private ProvenanceSearchValueDTO createSearchValueDto(final String value) {
        final ProvenanceSearchValueDTO dto = new ProvenanceSearchValueDTO();
        dto.setInverse(false);
        dto.setValue(value);
        return dto;
    }

    @Test
    public void testChangeFlowVersion() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();
        final ConnectionEntity inputToOutput = getClientUtil().createConnection(inputPort, outputPort, statelessGroup.getId());

        final FlowRegistryClientEntity registryClient = registerClient();

        // Register the first version of the flow
        final VersionControlInformationEntity vci = getClientUtil().startVersionControl(statelessGroup, registryClient, "test-flows", "first-flow");
        waitFor(() -> VersionControlInformationDTO.UP_TO_DATE.equals(getClientUtil().getVersionControlState(statelessGroup.getId())) );

        // Update the flow
        getNifiClient().getConnectionClient().deleteConnection(inputToOutput);

        final ProcessorEntity reverseContents = getClientUtil().createProcessor(REVERSE_CONTENTS, statelessGroup.getId());
        getClientUtil().createConnection(inputPort, reverseContents, statelessGroup.getId());
        getClientUtil().createConnection(reverseContents, outputPort, SUCCESS, statelessGroup.getId());

        // Save v2 of the flow
        getClientUtil().saveFlowVersion(statelessGroup, registryClient, vci);
        waitFor(() -> VersionControlInformationDTO.UP_TO_DATE.equals(getClientUtil().getVersionControlState(statelessGroup.getId())) );

        // Let a FlowFile go through and verify the results
        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput.getId(), 1);
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());
        waitForQueueCount(outputToTerminate, 1);
        assertEquals(HELLO_WORLD_REVERSED, getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 0));
        getClientUtil().stopProcessor(generate);

        // Switch back to v1 while flow is running
        getClientUtil().changeFlowVersion(statelessGroup.getId(), "1");
        getClientUtil().startProcessor(generate);
        waitForQueueCount(outputToTerminate, 2);
        assertEquals(HELLO_WORLD, getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 1));
        getClientUtil().stopProcessor(generate);

        // Switch back to v2 while flow is running
        getClientUtil().changeFlowVersion(statelessGroup.getId(), "2");
        getClientUtil().startProcessor(generate);
        waitForQueueCount(outputToTerminate, 3);
        assertEquals(HELLO_WORLD_REVERSED, getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), 2));
    }
}
