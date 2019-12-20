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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosItemProperties;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;


public class PutCosmosDocumentTest extends MockTestBase {

    private PutCosmosDocumentWithMock processor;

    @Before
    public void setUp() throws Exception {
        processor = new PutCosmosDocumentWithMock();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setIncomingConnection(false);
        testRunner.setNonLoopConnection(false);
    }

    @Test
    public void testPutProcessorConfigValidity() throws Exception {
        setBasicMockProperties(false);
        testRunner.assertNotValid();
        testRunner.setProperty(PutCosmosDocument.URI,MOCK_URI);
        testRunner.assertNotValid();
        testRunner.setProperty(PutCosmosDocument.DB_ACCESS_KEY,MOCK_DB_ACCESS_KEY);
        testRunner.assertValid();
        processor.cosmosClient =null;
        processor.createClient(testRunner.getProcessContext());
        assertNotNull(processor.cosmosClient);
    }

    @Test
    public void testPutProcessorConfigValidity_With_ConnectionService() throws Exception {
        setBasicMockProperties(true);
        testRunner.assertValid();
        processor.cosmosClient =null;
        processor.createClient(testRunner.getProcessContext());
        assertNotNull(processor.cosmosClient);
    }
    @Test
    public void testOnTrigger() throws Exception {
        prepareMockProcess();
        String document = String.format("{\"hello\": 2, \"%s\":\"x\"}", MOCK_PARTITION_FIELD_NAME);
        testRunner.enqueue(document.getBytes());
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutCosmosDocument.REL_SUCCESS, 1);
    }

    @Test
    public void testOnTriggerWithDocumentIntId() throws Exception {
        prepareMockProcess();
        String document = String.format("{\"id\": 2, \"%s\":\"x\"}", MOCK_PARTITION_FIELD_NAME);
        testRunner.enqueue(document.getBytes());
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutCosmosDocument.REL_SUCCESS, 1);
        CosmosContainer mockContainer = processor.getMockConainer();

        ArgumentCaptor<CosmosItemProperties> captor_1 = ArgumentCaptor.forClass(CosmosItemProperties.class);
        verify(mockContainer, atLeast(1)).upsertItem(captor_1.capture());

        // Test if the document to be sent has id field and its type is String.
        // Conversion of id field is required for Cosmos Documentto accept
        CosmosItemProperties capturedDoc = captor_1.getValue();
        assertNotNull(capturedDoc.get("id"));
        assertTrue(capturedDoc.get("id") instanceof String);

    }

    private void prepareMockProcess() throws Exception{
        // this setup connection service and basic mock properties
        setBasicMockProperties(true);
    }


}

class PutCosmosDocumentWithMock extends PutCosmosDocument {

    static CosmosClient mockClient = mock(CosmosClient.class);
    static CosmosContainer mockContainer = mock(CosmosContainer.class);

    @Override
    protected void createDocumentClient(String uri, String accessKey, ConsistencyLevel clevel) throws Exception {
        // create a  mock DocumentClient
        this.cosmosClient =  mockClient;
    }
    @Override
    protected void getCosmosDocumentContainer(final ProcessContext context) throws CosmosClientException {
        this.container = mockContainer;
    }

    public CosmosContainer getMockConainer()
    {
        return mockContainer;
    }




}