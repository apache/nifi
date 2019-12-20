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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.MockFlowFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosItemProperties;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class ITGetCosmosDocument extends ITAbstractCosmosDocument {

    private static final String TEST_COSMOS_QUERY = "select top 100 * from c";
    private static List<CosmosItemProperties> testData;
    private static int numOfTestData = 10;


    static {
        testData = new ArrayList<>();
        for(int i=0; i< numOfTestData; i++){
            JsonObject json =  new JsonObject();
            json.addProperty("id", ""+i);
            json.addProperty(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, "xyz"+i);
            CosmosItemProperties doc = new CosmosItemProperties(json.toString());
            testData.add(doc);
        }

        for(CosmosItemProperties doc : testData){
            try {
                container.upsertItem(doc);
            }catch(CosmosClientException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return GetCosmosDocument.class;
    }
    @Before
    public void setQuery()
    {
        runner.setProperty(GetCosmosDocument.QUERY, TEST_COSMOS_QUERY);
    }
    @After
    public void resetGetTestSpecificProperties()
    {
        runner.removeProperty(GetCosmosDocument.RESULTS_PER_FLOWFILE);
        runner.removeProperty(GetCosmosDocument.MAX_RESPONSE_PAGE_SIZE);
    }

    @Test
    public void testProcessorConfigValidity() {
        runner.run();
    }

    @Test
    public void testProcessorConfigValidityWithConnectionServiceController() throws Exception {
        this.configureCosmosConnectionControllerService();
        runner.run();
    }
    @Test
    public void testReadDocuments() {
        runner.enqueue(new byte[] {});
        runner.run();

        runner.assertAllFlowFilesTransferred(GetCosmosDocument.REL_SUCCESS, testData.size());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetCosmosDocument.REL_SUCCESS);
        for(int idx=0; idx < testData.size();  idx++) {
            MockFlowFile flowFile = flowFiles.get(idx);
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }
    }

    @Test
    public void testReadDocumentsWithResponsePageSize() {
        runner.setProperty(GetCosmosDocument.MAX_RESPONSE_PAGE_SIZE, "4");
        runner.enqueue(new byte[] {});
        runner.run();
        // regardless response page size,the same number of flow files should be generated as testReadDocuments
        runner.assertAllFlowFilesTransferred(GetCosmosDocument.REL_SUCCESS, testData.size());
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetCosmosDocument.REL_SUCCESS);
        for(int idx=0; idx < testData.size();  idx++) {
            MockFlowFile flowFile = flowFiles.get(idx);
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }
    }

    @Test
    public void testMultipleResultsInFlowFile() {
        JsonParser parser = new JsonParser();
        runner.setProperty(GetCosmosDocument.RESULTS_PER_FLOWFILE, "3");
        runner.enqueue(new byte[] {});
        runner.run();

        int expectedNumOfFlowFiles = (int) Math.ceil(testData.size()/3.0);

        runner.assertAllFlowFilesTransferred(GetCosmosDocument.REL_SUCCESS, expectedNumOfFlowFiles); // 4 flowfils with each having [3,3,3,1] records.
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetCosmosDocument.REL_SUCCESS);
        for(int idx=0; idx < expectedNumOfFlowFiles;  idx++) {
            MockFlowFile flowFile = flowFiles.get(idx);
            JsonElement parsedJson = parser.parse(new String(flowFile.toByteArray()));
            JsonArray jArray = parsedJson.getAsJsonArray();
            if (idx < expectedNumOfFlowFiles-1) {
               assertEquals(3, jArray.size());  // all other than the last flow file should have 3 Json records
            }
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        }

    }

    @Test
    public void testMultipleResultsInFlowFileWithResponsePageSizeSet() {
        JsonParser parser = new JsonParser();
        runner.setProperty(GetCosmosDocument.MAX_RESPONSE_PAGE_SIZE, "4"); // # of  response pages will be differ from the above test
        runner.setProperty(GetCosmosDocument.RESULTS_PER_FLOWFILE, "3");
        runner.enqueue(new byte[] {});
        runner.run();

        int expectedNumOfFlowFiles = (int) Math.ceil(testData.size()/3.0);

        runner.assertAllFlowFilesTransferred(GetCosmosDocument.REL_SUCCESS, expectedNumOfFlowFiles); // 4 flowfils with each having [3,3,3,1] records.
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetCosmosDocument.REL_SUCCESS);
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
 }