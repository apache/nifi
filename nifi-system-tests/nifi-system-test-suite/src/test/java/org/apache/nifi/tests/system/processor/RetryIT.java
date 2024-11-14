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
package org.apache.nifi.tests.system.processor;

import org.apache.nifi.controller.BackoffMechanism;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RetryIT extends NiFiSystemIT {
    private static final int RETRY_COUNT = 2;

    @Test
    public void testSplitInputIntoTwoRemoveParentRetryChild() throws NiFiClientException, IOException, InterruptedException {
        // Create a GenerateFlowFile processor and a SplitByLine Processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity splitByLine = getClientUtil().createProcessor("SplitByLine");
        final ProcessorEntity terminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");

        // Configure split to retry once. Set backoff/penalty to 60 seconds to ensure that it is not re-processed before having a chance to verify the rollback
        enableRetries(splitByLine, Collections.singleton("success"), 60_000L);
        getClientUtil().updateProcessorProperties(generateFlowFile, Collections.singletonMap("Text", "abc\nxyz"));

        final ConnectionEntity generateToSplit = getClientUtil().createConnection(generateFlowFile, splitByLine, "success");
        final ConnectionEntity splitToTerminate = getClientUtil().createConnection(splitByLine, terminateFlowFile, "success");

        getClientUtil().startProcessor(generateFlowFile);

        waitForQueueCount(generateToSplit.getId(), 1);
        getClientUtil().startProcessor(splitByLine);

        waitFor(() -> {
            if (getConnectionQueueSize(generateToSplit.getId()) != 1) {
                return false;
            }

            final FlowFileEntity flowFile = getClientUtil().getQueueFlowFile(generateToSplit.getId(), 0);
            if (!Boolean.TRUE.equals(flowFile.getFlowFile().getPenalized())) {
                return false;
            }

            return getConnectionQueueSize(splitToTerminate.getId()) == 0;
        });

        final ProcessorStatusDTO statusDto = getNifiClient().getProcessorClient().getProcessor(splitByLine.getId()).getStatus();
        assertEquals(0, statusDto.getAggregateSnapshot().getFlowFilesIn());
        assertEquals(0L, statusDto.getAggregateSnapshot().getBytesIn());
        assertEquals(0, statusDto.getAggregateSnapshot().getFlowFilesOut());
        assertEquals(0L, statusDto.getAggregateSnapshot().getBytesOut());
    }

    @Test
    public void testRetryHappensTwiceThenFinishes() throws NiFiClientException, IOException, InterruptedException {
        //Create a GenerateFlowFile processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");

        //Create a PassThrough processor
        final ProcessorEntity passThrough = getClientUtil().createProcessor("PassThrough");

        //Configure the processor's success relationship to be retried twice with flow file penalization for a maximum of 1 ms
        enableRetries(passThrough, Collections.singleton("success"));

        //Create a connection between GenerateFlowFile and PassThrough processors
        final ConnectionEntity generateFlowFileAndPassThroughConnection = getClientUtil().createConnection(generateFlowFile, passThrough, "success");

        //Create a CountEvents processor with autoterminated relationship
        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        //Create a connection between PassThrough and CountEvents processors
        final ConnectionEntity passThroughAndCountEventsConnection = getClientUtil().createConnection(passThrough, countEvents, "success");


        // Wait for processors to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());
        getClientUtil().waitForValidProcessor(passThrough.getId());
        getClientUtil().waitForValidProcessor(countEvents.getId());

        //Generate flow file
        runProcessorOnce(generateFlowFile);
        getClientUtil().waitForStoppedProcessor(generateFlowFile.getId());
        waitForQueueCount(generateFlowFileAndPassThroughConnection.getId(), 1);

        assertEquals(0, getRetryCount(generateFlowFileAndPassThroughConnection));

        //Run PassThrough processor, this will trigger the first retry
        runProcessorOnce(passThrough);
        getClientUtil().waitForStoppedProcessor(passThrough.getId());
        waitForQueueCount(generateFlowFileAndPassThroughConnection.getId(), 1);

        assertEquals(1, getRetryCount(generateFlowFileAndPassThroughConnection));

        //Running PassThrough processor again, will trigger another retry
        runProcessorOnce(passThrough);
        getClientUtil().waitForStoppedProcessor(passThrough.getId());
        waitForQueueCount(generateFlowFileAndPassThroughConnection.getId(), 1);

        assertEquals(2, getRetryCount(generateFlowFileAndPassThroughConnection));

        //Running PassThrough processor again should finish retry and transfer flow file
        runProcessorOnce(passThrough);
        getClientUtil().waitForStoppedProcessor(passThrough.getId());
        waitForQueueCount(passThroughAndCountEventsConnection.getId(), 1);

        assertEquals(0, getRetryCount(passThroughAndCountEventsConnection));
        assertEquals(0, getConnectionQueueSize(generateFlowFileAndPassThroughConnection.getId()));
    }

    @Test
    public void testNoRetryHappensWithSourceTypeProcessors() throws NiFiClientException, IOException, InterruptedException {
        //Create a GenerateFlowFile processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");

        //Configure the processor's success relationship to be retried twice with flow file penalization for a maximum of 1 ms
        enableRetries(generateFlowFile, Collections.singleton("success"));

        //Create a CountEvents processor with autoterminated relationship
        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        //Create a connection between GenerateFlowFile and CountEvents processors
        final ConnectionEntity connection = getClientUtil().createConnection(generateFlowFile, countEvents, "success");

        // Wait for processors to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());
        getClientUtil().waitForValidProcessor(countEvents.getId());

        runProcessorOnce(generateFlowFile);
        waitForQueueCount(connection.getId(), 1);

        assertEquals(0, getRetryCount(connection));
    }

    private void runProcessorOnce(final ProcessorEntity processorEntity) throws NiFiClientException, IOException, InterruptedException {
        getNifiClient().getProcessorClient().runProcessorOnce(processorEntity);
        getClientUtil().waitForStoppedProcessor(processorEntity.getId());
    }

    @Test
    public void testRetryHappensWhenOnlyChildRelationshipHasRetryConfigured() throws NiFiClientException, IOException, InterruptedException {
        //Create a GenerateFlowFile processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        Map<String, String> properties = new HashMap<>();
        properties.put("Text", "abc\n123\nxyz\n321");
        properties.put("Batch Size", "1");
        getClientUtil().updateProcessorProperties(generateFlowFile, properties);

        //Create a SplitTextByLine processor
        final ProcessorEntity splitTextByLine = getClientUtil().createProcessor("SplitTextByLine");

        //Configure the processor's splits relationship to be retried twice with flow file penalization for a maximum of 1 ms
        enableRetries(splitTextByLine, Collections.singleton("splits"));

        //Create a connection between GenerateFlowFile and SplitTextByLine processors
        final ConnectionEntity generateFlowFileAndSplitTextByLineConnection = getClientUtil().createConnection(generateFlowFile, splitTextByLine, "success");

        //Create a CountEvents processor with autoterminated relationship
        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        //Create a connection between SplitTextByLine and CountEvents processors
        final ConnectionEntity splitTextByLineAndCountEventsOriginalConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "original");
        final ConnectionEntity splitTextByLineAndCountEventsSplitsConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "splits");
        final ConnectionEntity splitTextByLineAndCountEventsFailureConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "failure");


        // Wait for processors to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());
        getClientUtil().waitForValidProcessor(splitTextByLine.getId());
        getClientUtil().waitForValidProcessor(countEvents.getId());

        //Generate flow file
        runProcessorOnce(generateFlowFile);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(0, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Run SplitTextByLine processor, this will trigger the first retry
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(1, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again, will trigger another retry
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(2, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again should finish retry and transfer flow file
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(splitTextByLineAndCountEventsSplitsConnection.getId(), 4);

        assertEquals(0, getRetryCount(splitTextByLineAndCountEventsSplitsConnection));
        assertEquals(0, getConnectionQueueSize(generateFlowFileAndSplitTextByLineConnection.getId()));
        assertEquals(0, getConnectionQueueSize(splitTextByLineAndCountEventsFailureConnection.getId()));
        assertEquals(1, getConnectionQueueSize(splitTextByLineAndCountEventsOriginalConnection.getId()));
    }

    @Test
    public void testRetryHappensWhenOnlyParentRelationshipHasRetryConfigured() throws NiFiClientException, IOException, InterruptedException {
        //Create a GenerateFlowFile processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        Map<String, String> properties = new HashMap<>();
        properties.put("Text", "abc\n123\nxyz");
        properties.put("Batch Size", "1");
        getClientUtil().updateProcessorProperties(generateFlowFile, properties);

        //Create a SplitTextByLine processor
        final ProcessorEntity splitTextByLine = getClientUtil().createProcessor("SplitTextByLine");

        //Configure the processor's original relationship to be retried twice with flow file penalization for a maximum of 1 ms
        enableRetries(splitTextByLine, Collections.singleton("original"));

        //Create a connection between GenerateFlowFile and SplitTextByLine processors
        final ConnectionEntity generateFlowFileAndSplitTextByLineConnection = getClientUtil().createConnection(generateFlowFile, splitTextByLine, "success");

        //Create a CountEvents processor with autoterminated relationship
        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        //Create a connection between SplitTextByLine and CountEvents processors
        final ConnectionEntity splitTextByLineAndCountEventsOriginalConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "original");
        final ConnectionEntity splitTextByLineAndCountEventsSplitsConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "splits");
        final ConnectionEntity splitTextByLineAndCountEventsFailureConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "failure");


        // Wait for processors to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());
        getClientUtil().waitForValidProcessor(splitTextByLine.getId());
        getClientUtil().waitForValidProcessor(countEvents.getId());

        //Generate flow file
        runProcessorOnce(generateFlowFile);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(0, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Run SplitTextByLine processor, this will trigger the first retry
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(1, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again, will trigger another retry
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(2, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again should finish retry and transfer flow file
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(splitTextByLineAndCountEventsSplitsConnection.getId(), 3);

        assertEquals(0, getRetryCount(splitTextByLineAndCountEventsSplitsConnection));
        assertEquals(0, getConnectionQueueSize(generateFlowFileAndSplitTextByLineConnection.getId()));
        assertEquals(0, getConnectionQueueSize(splitTextByLineAndCountEventsFailureConnection.getId()));
        assertEquals(1, getConnectionQueueSize(splitTextByLineAndCountEventsOriginalConnection.getId()));
    }

    @Test
    public void testRetryHappensOnlyOnceWhenBothChildAndParentRelationshipHasRetryConfigured() throws NiFiClientException, IOException, InterruptedException {
        //Create a GenerateFlowFile processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        Map<String, String> properties = new HashMap<>();
        properties.put("Text", "abc\n123\nxyz\n321\ndfg");
        properties.put("Batch Size", "1");
        getClientUtil().updateProcessorProperties(generateFlowFile, properties);

        //Create a SplitTextByLine processor
        final ProcessorEntity splitTextByLine = getClientUtil().createProcessor("SplitTextByLine");

        //Configure the processor's original and splits relationships to be retried twice with flow file penalization for a maximum of 1 ms
        enableRetries(splitTextByLine, new HashSet<>(Arrays.asList("original", "splits")));

        //Create a connection between GenerateFlowFile and SplitTextByLine processors
        final ConnectionEntity generateFlowFileAndSplitTextByLineConnection = getClientUtil().createConnection(generateFlowFile, splitTextByLine, "success");

        //Create a CountEvents processor with autoterminated relationship
        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        //Create a connection between SplitTextByLine and CountEvents processors
        final ConnectionEntity splitTextByLineAndCountEventsOriginalConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "original");
        final ConnectionEntity splitTextByLineAndCountEventsSplitsConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "splits");
        final ConnectionEntity splitTextByLineAndCountEventsFailureConnection = getClientUtil().createConnection(splitTextByLine, countEvents, "failure");


        // Wait for processors to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());
        getClientUtil().waitForValidProcessor(splitTextByLine.getId());
        getClientUtil().waitForValidProcessor(countEvents.getId());

        //Generate flow file
        runProcessorOnce(generateFlowFile);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(0, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Run SplitTextByLine processor, this will trigger the first retry
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(1, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again, will trigger another retry
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(2, getRetryCount(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again should finish retry and transfer flow file
        runProcessorOnce(splitTextByLine);
        waitForQueueCount(splitTextByLineAndCountEventsSplitsConnection.getId(), 5);

        assertEquals(0, getRetryCount(splitTextByLineAndCountEventsSplitsConnection));
        assertEquals(0, getConnectionQueueSize(generateFlowFileAndSplitTextByLineConnection.getId()));
        assertEquals(0, getConnectionQueueSize(splitTextByLineAndCountEventsFailureConnection.getId()));
        assertEquals(1, getConnectionQueueSize(splitTextByLineAndCountEventsOriginalConnection.getId()));
    }

    private int getRetryCount(final ConnectionEntity connection) throws NiFiClientException, IOException {
        return getClientUtil().getQueueFlowFile(connection.getId(), 0).getFlowFile().getAttributes()
            .entrySet().stream()
            .filter(entry -> entry.getKey().startsWith("retryCount."))
            .map(Map.Entry::getValue)
            .findFirst()
            .map(Integer::parseInt)
            .orElse(0);
    }

    private void enableRetries(final ProcessorEntity processorEntity, final Set<String> relationships) throws NiFiClientException, IOException {
        enableRetries(processorEntity, relationships, 1);
    }

    private void enableRetries(final ProcessorEntity processorEntity, final Set<String> relationships, final long backoffMillis) throws NiFiClientException, IOException {
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setRetryCount(RETRY_COUNT);
        config.setMaxBackoffPeriod(backoffMillis + " ms");
        config.setBackoffMechanism(BackoffMechanism.PENALIZE_FLOWFILE.name());
        config.setRetriedRelationships(relationships);
        config.setPenaltyDuration(backoffMillis + " ms");
        getClientUtil().updateProcessorConfig(processorEntity, config);
    }
}
