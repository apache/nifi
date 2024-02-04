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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.data.explorer.KustoQueryResponse;
import org.apache.nifi.services.azure.data.explorer.KustoQueryService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class QueryAzureDataExplorerTest {
    private static final String SERVICE_ID = KustoQueryService.class.getName();

    private static final String DATABASE_NAME = "records";

    private static final String QUERY = ".show table testing";

    private static final byte[] EMPTY = new byte[]{};

    private static final String ERROR_MESSAGE = "Results not found";

    private static final String EMPTY_ARRAY = "[]";

    @Mock
    private KustoQueryService kustoQueryService;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(QueryAzureDataExplorer.class);

        when(kustoQueryService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, kustoQueryService);
        runner.enableControllerService(kustoQueryService);
    }

    @Test
    void testProperties() {
        runner.assertNotValid();

        runner.setProperty(QueryAzureDataExplorer.KUSTO_QUERY_SERVICE, SERVICE_ID);
        runner.setProperty(QueryAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(QueryAzureDataExplorer.QUERY, QUERY);

        runner.assertValid();
    }

    @Test
    void testRunFailure() {
        runner.setProperty(QueryAzureDataExplorer.KUSTO_QUERY_SERVICE, SERVICE_ID);
        runner.setProperty(QueryAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(QueryAzureDataExplorer.QUERY, QUERY);

        runner.enqueue(EMPTY);

        final KustoQueryResponse kustoQueryResponse = new KustoQueryResponse(true, ERROR_MESSAGE);
        when(kustoQueryService.executeQuery(eq(DATABASE_NAME), eq(QUERY))).thenReturn(kustoQueryResponse);

        runner.run();

        runner.assertAllFlowFilesTransferred(QueryAzureDataExplorer.FAILURE);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryAzureDataExplorer.FAILURE).getFirst();
        flowFile.assertAttributeEquals(QueryAzureDataExplorer.QUERY_ERROR_MESSAGE, ERROR_MESSAGE);
        flowFile.assertAttributeEquals(QueryAzureDataExplorer.QUERY_EXECUTED, QUERY);
    }

    @Test
    void testRunSuccess() {
        runner.setProperty(QueryAzureDataExplorer.KUSTO_QUERY_SERVICE, SERVICE_ID);
        runner.setProperty(QueryAzureDataExplorer.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(QueryAzureDataExplorer.QUERY, QUERY);

        runner.enqueue(EMPTY);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(EMPTY_ARRAY.getBytes(StandardCharsets.UTF_8));
        final KustoQueryResponse kustoQueryResponse = new KustoQueryResponse(inputStream);
        when(kustoQueryService.executeQuery(eq(DATABASE_NAME), eq(QUERY))).thenReturn(kustoQueryResponse);

        runner.run();

        runner.assertAllFlowFilesTransferred(QueryAzureDataExplorer.SUCCESS);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryAzureDataExplorer.SUCCESS).getFirst();
        flowFile.assertAttributeEquals(QueryAzureDataExplorer.QUERY_EXECUTED, QUERY);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), QueryAzureDataExplorer.APPLICATION_JSON);
        flowFile.assertContentEquals(EMPTY_ARRAY);
    }
}
