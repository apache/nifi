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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.azure.cosmos.CosmosException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;


public class ITGetCosmosDocumentRecord extends ITAbstractAzureCosmosDBDocument {

    private static final String TEST_COSMOS_QUERY = "select top 100 * from c";
    private static final String TEST_PARTITION_KEY = "category";
    private static List<JsonNode> testData;
    private static int numOfTestData = 10;
    private static RecordSchema SCHEMA;

    static {
        final ObjectMapper mapper = new ObjectMapper();
        final List<RecordField> testDataFields = new ArrayList<>();
        final RecordField idField = new RecordField("id", RecordFieldType.STRING.getDataType());
        final RecordField categoryField = new RecordField(TEST_PARTITION_KEY, RecordFieldType.INT.getDataType());
        final RecordField payloadField = new RecordField("payload", RecordFieldType.STRING.getDataType());
        testDataFields.add(idField);
        testDataFields.add(categoryField);
        testDataFields.add(payloadField);;
        SCHEMA = new SimpleRecordSchema(testDataFields);
        JsonNode doc = null;

        testData = new ArrayList<>();
        for (int i=0; i< numOfTestData; i++) {
            JsonObject json =  new JsonObject();
            json.addProperty("id", ""+i);
            json.addProperty(TEST_COSMOS_PARTITION_KEY_FIELD_NAME, MockTestBase.getRandomInt(1,4));
            json.addProperty("payload", RandomStringUtils.random(100, true, true));
            try {
                doc = mapper.readTree(json.toString());
            } catch(IOException exp) {
                exp.printStackTrace();
            }
            testData.add(doc);
        }

        for (JsonNode jdoc : testData) {
            try {
                container.upsertItem(jdoc);
            } catch(CosmosException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return GetAzureCosmosDBRecord.class;
    }
    @Before
    public void setupTest() throws Exception {
        runner.setProperty(GetAzureCosmosDBRecord.QUERY, TEST_COSMOS_QUERY);
        MockSchemaRegistry registry = new MockSchemaRegistry();
        JsonRecordSetWriter writer = new JsonRecordSetWriter();
        registry.addSchema("sample", SCHEMA);

        runner.addControllerService("writer", writer);
        runner.addControllerService("registry", registry);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.enableControllerService(registry);
        runner.enableControllerService(writer);
        runner.setProperty(GetAzureCosmosDBRecord.WRITER_FACTORY, "writer");
    }
    @Test
    public void testReadRecords() {
        runner.setVariable("schema.name", "sample");
        runner.enqueue(new byte[] {});
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractAzureCosmosDBProcessor.REL_SUCCESS);
        assertTrue(flowFiles.size() == 1);
        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        flowFile.assertAttributeEquals("record.count", String.valueOf(numOfTestData));

        JsonElement parsedJson = JsonParser.parseString(new String(flowFile.toByteArray()));
        JsonArray jArray = parsedJson.getAsJsonArray();
        assertTrue(jArray.size() == numOfTestData);
    }

    @Test
    public void testReadTop5Recrods() {
        // This test is equivalent to limit test of GetMongoRecord processor
        runner.setProperty(GetAzureCosmosDBRecord.QUERY, "select top 5 * from c");
        runner.setVariable("schema.name", "sample");
        runner.enqueue(new byte[] {});
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractAzureCosmosDBProcessor.REL_SUCCESS);
        assertTrue(flowFiles.size() == 1);
        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        flowFile.assertAttributeEquals("record.count", "5");

        JsonElement parsedJson = JsonParser.parseString(new String(flowFile.toByteArray()));
        JsonArray jArray = parsedJson.getAsJsonArray();
        assertTrue(jArray.size() == 5);
    }

    @Test
    public void testSortAndProjection() {
        runner.setProperty(GetAzureCosmosDBRecord.QUERY, "select c.id, c.category, c.payload from c order by c.id DESC");
        runner.setVariable("schema.name", "sample");
        runner.enqueue(new byte[] {});
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractAzureCosmosDBProcessor.REL_SUCCESS);
        assertTrue(flowFiles.size() == 1);
        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        flowFile.assertAttributeEquals("record.count", String.valueOf(numOfTestData));

        JsonElement parsedJson = JsonParser.parseString(new String(flowFile.toByteArray()));
        JsonArray jArray = parsedJson.getAsJsonArray();
        assertTrue(jArray.size() == numOfTestData);
    }
}
