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
import org.apache.nifi.services.azure.data.explorer.KustoQueryResponse;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PutAzureDataExplorerTest {

    private static final String SERVICE_ID = KustoIngestService.class.getName();

    private static final String DATABASE_NAME = "records";

    private static final String TABLE_NAME = "records";

    private static final String MAPPING_NAME = "records";

    private static final String DATA_FORMAT = "CSV";

    private static final byte[] EMPTY = new byte[]{};

    @Mock
    private KustoIngestService kustoIngestService;

    @Mock
    private List<List<Object>> queryResult;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(PutAzureDataExplorer.class);
        runner.setValidateExpressionUsage(false);
        when(kustoIngestService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, kustoIngestService);
        runner.enableControllerService(kustoIngestService);
        queryResult = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add("test1");
        row.add("test2");
        row.add("test3");
        queryResult.add(row);
    }

    @Test
    void testProperties() {
        runner.assertNotValid();

        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.ADX_SERVICE, SERVICE_ID);

        runner.assertValid();
    }

    @Test
    void testRunSuccessNonTransactional() throws URISyntaxException {
        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.ADX_SERVICE, SERVICE_ID);

        runner.enqueue(EMPTY);

        KustoQueryResponse kustoQueryResponse = Mockito.mock(KustoQueryResponse.class);
        when(kustoIngestService.executeQuery(Mockito.anyString(), Mockito.anyString())).thenReturn(kustoQueryResponse);
        final KustoIngestionResult kustoIngestionResult = KustoIngestionResult.SUCCEEDED;
        when(kustoIngestService.ingestData(Mockito.any())).thenReturn(kustoIngestionResult);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureDataExplorer.SUCCESS);
    }

    @Test
    void testRunFailureNonTransactional() throws URISyntaxException {
        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.ADX_SERVICE, SERVICE_ID);

        runner.enqueue(EMPTY);

        KustoQueryResponse kustoQueryResponse = Mockito.mock(KustoQueryResponse.class);
        when(kustoIngestService.executeQuery(Mockito.anyString(), Mockito.anyString())).thenReturn(kustoQueryResponse);
        final KustoIngestionResult kustoIngestionResult = KustoIngestionResult.SUCCEEDED;
        when(kustoIngestService.ingestData(Mockito.any())).thenReturn(kustoIngestionResult);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureDataExplorer.SUCCESS);
    }

    @Test
    void testRunSuccessStreaming() throws URISyntaxException{
        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.ADX_SERVICE, SERVICE_ID);
        runner.setProperty(PutAzureDataExplorer.IS_STREAMING_ENABLED, "true");

        runner.enqueue(EMPTY);

        KustoQueryResponse kustoQueryResponse = new KustoQueryResponse(queryResult);
        when(kustoIngestService.executeQuery(Mockito.anyString(), Mockito.anyString())).thenReturn(kustoQueryResponse);
        final KustoIngestionResult kustoIngestionResult = KustoIngestionResult.SUCCEEDED;
        when(kustoIngestService.ingestData(Mockito.any())).thenReturn(kustoIngestionResult);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureDataExplorer.SUCCESS);
    }

    @Test
    void testRunSuccessStreamingFailure() throws URISyntaxException{
        runner.setProperty(PutAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(PutAzureDataExplorer.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutAzureDataExplorer.MAPPING_NAME, MAPPING_NAME);
        runner.setProperty(PutAzureDataExplorer.DATA_FORMAT, DATA_FORMAT);
        runner.setProperty(PutAzureDataExplorer.ADX_SERVICE, SERVICE_ID);
        runner.setProperty(PutAzureDataExplorer.IS_STREAMING_ENABLED, "true");

        runner.enqueue(EMPTY);

        KustoQueryResponse kustoQueryResponse = new KustoQueryResponse(queryResult);
        when(kustoIngestService.executeQuery(Mockito.anyString(), Mockito.anyString())).thenReturn(kustoQueryResponse);
        final KustoIngestionResult kustoIngestionResult = KustoIngestionResult.FAILED;
        when(kustoIngestService.ingestData(Mockito.any())).thenReturn(kustoIngestionResult);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutAzureDataExplorer.FAILURE);
    }


}
