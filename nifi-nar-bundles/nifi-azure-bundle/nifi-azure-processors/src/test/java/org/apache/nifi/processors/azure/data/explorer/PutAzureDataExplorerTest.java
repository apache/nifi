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
package org.apache.nifi.processors.azure.data.explorer;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.data.explorer.KustoIngestService;
import org.apache.nifi.services.azure.data.explorer.KustoIngestionResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PutAzureDataExplorerTest {

    private static final String SERVICE_ID = KustoIngestService.class.getName();

    private static final String DATABASE_NAME = "records";

    private static final String TABLE_NAME = "records";

    private static final String MAPPING_NAME = "records";

    private static final String DATA_FORMAT = "csv";

    private static final byte[] EMPTY = new byte[]{};

    @Mock
    private KustoIngestService kustoIngestService;

    @Mock
    private Map<Integer,List<String>> queryResult;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(PutAzureDataExplorer.class);
        runner.setValidateExpressionUsage(false);
        when(kustoIngestService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, kustoIngestService);
        runner.enableControllerService(kustoIngestService);
        queryResult = new HashMap<>();
        List<String> row = new ArrayList<>();
        row.add("test1");
        row.add("test2");
        row.add("test3");
        queryResult.put(0,row);
    }

    @Test
    void testProperties() {
        runner.assertNotValid();

        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.INGEST_SERVICE, SERVICE_ID);

        runner.assertValid();
    }

    @Test
    void testRunSuccessQueuedIngestion() {
        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.INGEST_SERVICE, SERVICE_ID);

        runner.enqueue(EMPTY);

        when(kustoIngestService.isTableReadable(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
        final KustoIngestionResult kustoIngestionResult = KustoIngestionResult.SUCCEEDED;
        when(kustoIngestService.ingestData(Mockito.any())).thenReturn(kustoIngestionResult);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureDataExplorer.SUCCESS);
    }

    @Test
    void testRunFailureQueuedIngestion() {
        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.INGEST_SERVICE, SERVICE_ID);

        runner.enqueue(EMPTY);

        when(kustoIngestService.isTableReadable(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
        final KustoIngestionResult kustoIngestionResult = KustoIngestionResult.FAILED;
        when(kustoIngestService.ingestData(Mockito.any())).thenReturn(kustoIngestionResult);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureDataExplorer.FAILURE);
    }

    @Test
    void testRunSuccessStreamingIngestion() {
        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.INGEST_SERVICE, SERVICE_ID);
        runner.setProperty(PutAzureDataExplorer.STREAMING_ENABLED, Boolean.TRUE.toString());

        runner.enqueue(EMPTY);

        when(kustoIngestService.isStreamingPolicyEnabled(Mockito.anyString())).thenReturn(true);
        when(kustoIngestService.isTableReadable(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
        final KustoIngestionResult kustoIngestionResult = KustoIngestionResult.SUCCEEDED;
        when(kustoIngestService.ingestData(Mockito.any())).thenReturn(kustoIngestionResult);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureDataExplorer.SUCCESS);
    }

    @Test
    void testRunFailureStreamingIngestion() {
        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.INGEST_SERVICE, SERVICE_ID);
        runner.setProperty(PutAzureDataExplorer.STREAMING_ENABLED, Boolean.TRUE.toString());

        runner.enqueue(EMPTY);

        when(kustoIngestService.isStreamingPolicyEnabled(Mockito.anyString())).thenReturn(true);
        when(kustoIngestService.isTableReadable(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
        final KustoIngestionResult kustoIngestionResult = KustoIngestionResult.FAILED;
        when(kustoIngestService.ingestData(Mockito.any())).thenReturn(kustoIngestionResult);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureDataExplorer.FAILURE);
    }
}
