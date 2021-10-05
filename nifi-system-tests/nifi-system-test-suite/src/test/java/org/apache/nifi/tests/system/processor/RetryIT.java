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

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RetryIT extends NiFiSystemIT {
    private static final int RETRY_COUNTS = 2;
    private static final String EXPECTED_NO_RETRY_COUNTS = null;
    private static final String EXPECTED_FIRST_RETRY = String.valueOf(RETRY_COUNTS - 1);
    private static final String EXPECTED_SECOND_RETRY = String.valueOf(RETRY_COUNTS);

    @Test
    public void testRetryHappensTwiceThenFinishes() throws NiFiClientException, IOException, InterruptedException {
        //Create a GenerateFlowFile processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");

        //Create a PassThrough processor
        final ProcessorEntity passThrough = getClientUtil().createProcessor("PassThrough");

        //Configure the processor's success relationship to be retried twice with flow file penalization for a maximum of 1 ms
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setRetryCounts(RETRY_COUNTS);
        config.setMaxBackoffPeriod("1 ms");
        config.setBackoffMechanism("PENALIZE_FLOWFILE");
        config.setRetriedRelationships(Collections.singleton("success"));
        config.setPenaltyDuration("1 ms");
        getClientUtil().updateProcessorConfig(passThrough, config);

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
        getNifiClient().getProcessorClient().runProcessorOnce(generateFlowFile);
        waitForQueueCount(generateFlowFileAndPassThroughConnection.getId(), 1);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(generateFlowFileAndPassThroughConnection));

        //Run PassThrough processor, this will trigger the first retry
        getNifiClient().getProcessorClient().runProcessorOnce(passThrough);
        waitForQueueCount(generateFlowFileAndPassThroughConnection.getId(), 1);

        assertEquals(EXPECTED_FIRST_RETRY, getRetryCounts(generateFlowFileAndPassThroughConnection));

        //Running PassThrough processor again, will trigger another retry
        getNifiClient().getProcessorClient().runProcessorOnce(passThrough);
        waitForQueueCount(generateFlowFileAndPassThroughConnection.getId(), 1);

        assertEquals(EXPECTED_SECOND_RETRY, getRetryCounts(generateFlowFileAndPassThroughConnection));

        //Running PassThrough processor again should finish retry and transfer flow file
        getNifiClient().getProcessorClient().runProcessorOnce(passThrough);
        waitForQueueCount(passThroughAndCountEventsConnection.getId(), 1);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(passThroughAndCountEventsConnection));
        assertEquals(0, getConnectionQueueSize(generateFlowFileAndPassThroughConnection.getId()));
    }

    @Test
    public  void testNoRetryHappensWithSourceTypeProcessors() throws NiFiClientException, IOException, InterruptedException {
        //Create a GenerateFlowFile processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");

        //Configure the processor's success relationship to be retried twice with flow file penalization for a maximum of 1 ms
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setRetryCounts(RETRY_COUNTS);
        config.setMaxBackoffPeriod("1 ms");
        config.setBackoffMechanism("PENALIZE_FLOWFILE");
        config.setRetriedRelationships(Collections.singleton("success"));
        config.setPenaltyDuration("1 ms");
        getClientUtil().updateProcessorConfig(generateFlowFile, config);

        //Create a CountEvents processor with autoterminated relationship
        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        //Create a connection between GenerateFlowFile and CountEvents processors
        final ConnectionEntity connection = getClientUtil().createConnection(generateFlowFile, countEvents, "success");


        // Wait for processors to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());
        getClientUtil().waitForValidProcessor(countEvents.getId());


        getNifiClient().getProcessorClient().runProcessorOnce(generateFlowFile);
        waitForQueueCount(connection.getId(), 1);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(connection));
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
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setRetryCounts(RETRY_COUNTS);
        config.setMaxBackoffPeriod("1 ms");
        config.setBackoffMechanism("PENALIZE_FLOWFILE");
        config.setRetriedRelationships(Collections.singleton("splits"));
        config.setPenaltyDuration("1 ms");
        getClientUtil().updateProcessorConfig(splitTextByLine, config);

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
        getNifiClient().getProcessorClient().runProcessorOnce(generateFlowFile);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Run SplitTextByLine processor, this will trigger the first retry
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_FIRST_RETRY, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again, will trigger another retry
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_SECOND_RETRY, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again should finish retry and transfer flow file
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(splitTextByLineAndCountEventsSplitsConnection.getId(), 4);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(splitTextByLineAndCountEventsSplitsConnection));
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
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setRetryCounts(RETRY_COUNTS);
        config.setMaxBackoffPeriod("1 ms");
        config.setBackoffMechanism("PENALIZE_FLOWFILE");
        config.setRetriedRelationships(Collections.singleton("original"));
        config.setPenaltyDuration("1 ms");
        getClientUtil().updateProcessorConfig(splitTextByLine, config);

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
        getNifiClient().getProcessorClient().runProcessorOnce(generateFlowFile);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Run SplitTextByLine processor, this will trigger the first retry
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_FIRST_RETRY, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again, will trigger another retry
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_SECOND_RETRY, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again should finish retry and transfer flow file
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(splitTextByLineAndCountEventsSplitsConnection.getId(), 3);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(splitTextByLineAndCountEventsSplitsConnection));
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
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setRetryCounts(RETRY_COUNTS);
        config.setMaxBackoffPeriod("1 ms");
        config.setBackoffMechanism("PENALIZE_FLOWFILE");
        config.setRetriedRelationships(new HashSet<>(Arrays.asList("original", "splits")));
        config.setPenaltyDuration("1 ms");
        getClientUtil().updateProcessorConfig(splitTextByLine, config);

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
        getNifiClient().getProcessorClient().runProcessorOnce(generateFlowFile);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Run SplitTextByLine processor, this will trigger the first retry
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_FIRST_RETRY, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again, will trigger another retry
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(generateFlowFileAndSplitTextByLineConnection.getId(), 1);

        assertEquals(EXPECTED_SECOND_RETRY, getRetryCounts(generateFlowFileAndSplitTextByLineConnection));

        //Running SplitTextByLine processor again should finish retry and transfer flow file
        getNifiClient().getProcessorClient().runProcessorOnce(splitTextByLine);
        waitForQueueCount(splitTextByLineAndCountEventsSplitsConnection.getId(), 5);

        assertEquals(EXPECTED_NO_RETRY_COUNTS, getRetryCounts(splitTextByLineAndCountEventsSplitsConnection));
        assertEquals(0, getConnectionQueueSize(generateFlowFileAndSplitTextByLineConnection.getId()));
        assertEquals(0, getConnectionQueueSize(splitTextByLineAndCountEventsFailureConnection.getId()));
        assertEquals(1, getConnectionQueueSize(splitTextByLineAndCountEventsOriginalConnection.getId()));
    }

    private String getRetryCounts(ConnectionEntity connection) throws NiFiClientException, IOException {
        return getClientUtil().getQueueFlowFile(connection.getId(), 0).getFlowFile().getAttributes().get("retryCounts");
    }
}
