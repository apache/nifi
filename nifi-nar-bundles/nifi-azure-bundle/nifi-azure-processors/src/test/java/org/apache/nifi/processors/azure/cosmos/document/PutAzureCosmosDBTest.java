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
package org.apache.nifi.processors.azure.cosmos.document;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PutAzureCosmosDBTest extends MockTestBase {

    private PutAzureCosmosDBWithMock processor;

    @Before
    public void setUp() throws Exception {
        processor = new PutAzureCosmosDBWithMock();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setIncomingConnection(false);
        testRunner.setNonLoopConnection(false);
    }

    @Test
    public void testPutProcessorConfigValidity() throws Exception {
        setBasicMockProperties(false);
        testRunner.assertNotValid();
        testRunner.setProperty(AzureCosmosDBUtils.URI, MOCK_URI);
        testRunner.assertNotValid();
        testRunner.setProperty(AzureCosmosDBUtils.DB_ACCESS_KEY,MOCK_DB_ACCESS_KEY);
        testRunner.assertValid();
        processor.setCosmosClient(null);
        processor.onScheduled(testRunner.getProcessContext());
        assertNotNull(processor.getCosmosClient());
    }

    @Test
    public void testPutProcessorConfigValidity_With_ConnectionService() throws Exception {
        setBasicMockProperties(true);
        testRunner.assertValid();
        processor.setCosmosClient(null);
        processor.onScheduled(testRunner.getProcessContext());
        assertNotNull(processor.getCosmosClient());
    }
    @Test
    public void testOnTrigger() throws Exception {
        prepareMockProcess();
        String document = String.format("{\"hello\": 2, \"%s\":\"x\"}", MOCK_PARTITION_FIELD_NAME);
        testRunner.enqueue(document.getBytes());
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutAzureCosmosDB.REL_SUCCESS, 1);
    }

    @Test
    public void testOnTriggerWithDocumentIntId() throws Exception {
        prepareMockProcess();
        String document = String.format("{\"id\": 2, \"%s\":\"x\"}", MOCK_PARTITION_FIELD_NAME);
        testRunner.enqueue(document.getBytes());
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutAzureCosmosDB.REL_SUCCESS, 1);
        CosmosContainer mockContainer = processor.getMockConainer();

        ArgumentCaptor<JsonNode> captor_1 = ArgumentCaptor.forClass(JsonNode.class);
        verify(mockContainer, atLeast(1)).upsertItem(captor_1.capture(), any(), any());

        // Test if the document to be sent has id field and its type is String.
        // Conversion of id field to String is required for Cosmos Document to accept
        JsonNode capturedDoc = captor_1.getValue();
        assertNotNull(capturedDoc.get("id"));
        assertFalse(capturedDoc.get("id").isNumber());

    }

    private void prepareMockProcess() throws Exception{
        // this setup connection service and basic mock properties
        setBasicMockProperties(true);
    }


}

class PutAzureCosmosDBWithMock extends PutAzureCosmosDB {

    static CosmosClient mockClient = mock(CosmosClient.class);
    static CosmosContainer mockContainer = mock(CosmosContainer.class);

    @Override
    protected void createCosmosClient(String uri, String accessKey, ConsistencyLevel clevel) {
        this.setCosmosClient(mockClient);
    }
    @Override
    protected void getCosmosDocumentContainer(final ProcessContext context) throws CosmosException {
        this.setContainer(mockContainer);
    }

    public CosmosContainer getMockConainer() {
        return mockContainer;
    }

}