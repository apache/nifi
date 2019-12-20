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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosItemProperties;
import com.azure.cosmos.FeedResponse;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class GetCosmosDocumentTest extends MockTestBase {

    private GetCosmosDocumentWithMock processor;

    @Before
    public void setUp() throws Exception {
        processor = new GetCosmosDocumentWithMock();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setIncomingConnection(false);
        testRunner.setNonLoopConnection(false);
    }


    @Test
    public void testProcessorConfigValidity() throws Exception {
        setBasicMockProperties(false);

        testRunner.setProperty(GetCosmosDocument.URI,MOCK_URI);
        testRunner.assertNotValid();
        testRunner.setProperty(GetCosmosDocument.DB_ACCESS_KEY,MOCK_DB_ACCESS_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(GetCosmosDocument.QUERY,MOCK_QUERY);
        testRunner.assertValid();
        processor.cosmosClient =null;
        processor.createClient(testRunner.getProcessContext());
        assertNotNull(processor.cosmosClient);
    }

    @Test
    public void testProcessorConfigValidity_With_ConnectionService() throws Exception {
        setBasicMockProperties(true);
        testRunner.setProperty(GetCosmosDocument.QUERY,MOCK_QUERY);
        testRunner.assertValid();
        processor.cosmosClient =null;
        processor.createClient(testRunner.getProcessContext());
        assertNotNull(processor.cosmosClient);
    }

    @Test
    public void testReadDocuments() throws Exception {
        prepareMockProcess();
        testRunner.enqueue(new byte[] {});
        testRunner.run();
        List<CosmosItemProperties> mockData = processor.getMockData();

        testRunner.assertAllFlowFilesTransferred(GetCosmosDocument.REL_SUCCESS, mockData.size());
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetCosmosDocument.REL_SUCCESS);
        for(int idx=0; idx < 10;  idx++) {
            MockFlowFile flowFile = flowFiles.get(idx);
            flowFile.assertContentEquals(mockData.get(idx).toJson());
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }
    }

    @Test
    public void testMultipleResultsInFlowFile() throws Exception {
        JsonParser parser = new JsonParser();
        prepareMockProcess();
        testRunner.setProperty(GetCosmosDocument.RESULTS_PER_FLOWFILE, "3");
        testRunner.enqueue(new byte[] {});
        testRunner.run();
        List<CosmosItemProperties> mockData = processor.getMockData();
        int expectedNumOfFlowFiles = (int) Math.ceil(mockData.size()/3.0);

        testRunner.assertAllFlowFilesTransferred(GetCosmosDocument.REL_SUCCESS, expectedNumOfFlowFiles); // 4 flowfils with each having [3,3,3,1] records.
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetCosmosDocument.REL_SUCCESS);
        for(int idx=0; idx < expectedNumOfFlowFiles;  idx++) {
            MockFlowFile flowFile = flowFiles.get(idx);
            JsonElement parsedJson = parser.parse(new String(flowFile.toByteArray()));
            JsonArray jArray = parsedJson.getAsJsonArray();
            if (idx < expectedNumOfFlowFiles-1) {
               assertEquals(3, jArray.size());  // all other than last flow file should have 3 Json records
            }
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }
    }

    private void prepareMockProcess() throws Exception{
        testRunner.setProperty(GetCosmosDocument.QUERY,MOCK_QUERY);
        // this setup connection service and basic mock properties
        setBasicMockProperties(true);
    }


}


@SuppressWarnings("unchecked")
class GetCosmosDocumentWithMock extends GetCosmosDocument {

    Iterator<FeedResponse<CosmosItemProperties>> _mockFeedResponse;
    List<CosmosItemProperties> _mockData;
    FeedResponse<CosmosItemProperties> _mockPage;

    @Override
    protected void createDocumentClient(String uri, String accessKey, ConsistencyLevel clevel) throws Exception {
        // create a  mock DocumentClient
        this.cosmosClient = mock(CosmosClient.class);

    }

    @Override
    protected void getCosmosDocumentContainer(final ProcessContext context) throws ProcessException {
        this.container = mock(CosmosContainer.class);
        _mockFeedResponse = mockResponseData();
        when(this.container.queryItems(anyString(), any())).thenReturn(_mockFeedResponse);
    }

    public List<CosmosItemProperties> getMockData() {
       return _mockData;
    }

    private Iterator<FeedResponse<CosmosItemProperties>> mockResponseData(){
        final List<FeedResponse<CosmosItemProperties>> mockResponse = new ArrayList<>();
        _mockData = new ArrayList<>();
        for(int i=0; i< 10; i++){
            CosmosItemProperties doc = new CosmosItemProperties();
            doc.setId("" + i);
            _mockData.add(doc);
        }
        try {
            Constructor<?> constructor = FeedResponse.class.getDeclaredConstructor(List.class, Map.class);
            constructor.setAccessible(true);
            Map<String, String> mockHeader = (Map<String, String>) mock(Map.class);
            _mockPage = (FeedResponse<CosmosItemProperties>) constructor.newInstance(_mockData, mockHeader);

        }catch(Exception e) {
            e.printStackTrace();
        }
        mockResponse.add(_mockPage);
        return mockResponse.listIterator();
    }
}