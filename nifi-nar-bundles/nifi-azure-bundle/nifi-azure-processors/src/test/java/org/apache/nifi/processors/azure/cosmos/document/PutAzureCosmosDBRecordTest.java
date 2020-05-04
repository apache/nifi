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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import net.minidev.json.JSONObject;
public class PutAzureCosmosDBRecordTest extends MockTestBase {

    private MockPutAzureCosmosDBRecord processor;
    private MockRecordParser recordReader;

    private void setupRecordReader() throws InitializationException {
        recordReader = new MockRecordParser();
        if (testRunner != null) {
            testRunner.addControllerService("reader", recordReader);
            testRunner.enableControllerService(recordReader);
            testRunner.setProperty(PutAzureCosmosDBRecord.RECORD_READER_FACTORY, "reader");
        }

    }

    @Before
    public void setUp() throws Exception {
        processor = new MockPutAzureCosmosDBRecord();
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setIncomingConnection(false);
        testRunner.setNonLoopConnection(false);
    }

    @Test
    public void testPutCosmosRecordProcessorConfigValidity() throws Exception {
        setBasicMockProperties(false);
        testRunner.setProperty(AzureCosmosDBUtils.URI, MOCK_URI);
        testRunner.assertNotValid();
        testRunner.setProperty(AzureCosmosDBUtils.DB_ACCESS_KEY, MOCK_DB_ACCESS_KEY);

        testRunner.assertNotValid();

        setupRecordReader();
        testRunner.assertValid();
        processor.setCosmosClient(null);
        processor.onScheduled(testRunner.getProcessContext());
        assertNotNull(processor.getCosmosClient());
    }

    @Test
    public void testPutCosmosRecordProcessorConfigValidityWithConnectionService() throws Exception {
        setBasicMockProperties(true);
        testRunner.assertNotValid();
        // setup recordReader
        setupRecordReader();
        testRunner.assertValid();
        processor.setCosmosClient(null);
        processor.onScheduled(testRunner.getProcessContext());
        assertNotNull(processor.getCosmosClient());
    }

    @Test
    public void testOnTriggerWithFlatRecords() throws Exception {
        setupRecordReader();
        prepareMockTest();
        recordReader.addSchemaField("id", RecordFieldType.STRING);
        recordReader.addSchemaField(MOCK_PARTITION_FIELD_NAME, RecordFieldType.STRING);
        recordReader.addSchemaField("name", RecordFieldType.STRING);
        recordReader.addSchemaField("age", RecordFieldType.INT);
        recordReader.addSchemaField("sport", RecordFieldType.STRING);

        recordReader.addRecord("1", "A", "John Doe", 48, "Soccer");
        recordReader.addRecord("2", "B","Jane Doe", 47, "Tennis");
        recordReader.addRecord("3", "B", "Sally Doe", 47, "Curling");
        recordReader.addRecord("4", "A", "Jimmy Doe", 14, null);
        recordReader.addRecord("5", "C","Pizza Doe", 14, null);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutAzureCosmosDBRecord.REL_SUCCESS, 1);
        assertEquals(5, processor.getTestResults().size());
    }

    @Test
    public void testOnTriggerWithNestedRecords() throws Exception {
        setupRecordReader();
        prepareMockTest();
        recordReader.addSchemaField("id", RecordFieldType.STRING);
        recordReader.addSchemaField(MOCK_PARTITION_FIELD_NAME, RecordFieldType.STRING);

        final List<RecordField> personFields = new ArrayList<>();
        final RecordField nameField = new RecordField("name", RecordFieldType.STRING.getDataType());
        final RecordField ageField = new RecordField("age", RecordFieldType.INT.getDataType());
        final RecordField sportField = new RecordField("sport", RecordFieldType.STRING.getDataType());
        personFields.add(nameField);
        personFields.add(ageField);
        personFields.add(sportField);
        final RecordSchema personSchema = new SimpleRecordSchema(personFields);
        recordReader.addSchemaField("person", RecordFieldType.RECORD);

        recordReader.addRecord("1", "A", new MapRecord(personSchema, new HashMap<String,Object>() {
            private static final long serialVersionUID = -3185956498135742190L;
            {
                put("name", "John Doe");
                put("age", 48);
                put("sport", "Soccer");
            }
        }));
        recordReader.addRecord("2", "B", new MapRecord(personSchema, new HashMap<String,Object>() {
            private static final long serialVersionUID = 1L;
            {
                put("name", "Jane Doe");
                put("age", 47);
                put("sport", "Tennis");
            }
        }));
        recordReader.addRecord("3", "A", new MapRecord(personSchema, new HashMap<String,Object>() {
            private static final long serialVersionUID = -1329194249439570573L;
            {
                put("name", "Sally Doe");
                put("age", 47);
                put("sport", "Curling");
            }
        }));
        recordReader.addRecord("4", "C", new MapRecord(personSchema, new HashMap<String,Object>() {
            private static final long serialVersionUID = -1329194249439570574L;
            {
                put("name", "Jimmy Doe");
                put("age", 14);
                put("sport", null);
            }
        }));
        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutAzureCosmosDBRecord.REL_SUCCESS, 1);
        assertEquals(4, processor.getTestResults().size());
    }

    @Test
    public void testArrayConversion() throws Exception {
        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        // schema creation for test
        JsonObject schemaDef = new JsonObject();
        schemaDef.addProperty("type", "record");
        schemaDef.addProperty("name", "Test");
        JsonArray schemaArray = new JsonArray();
        JsonObject f1 = new JsonObject();
        f1.addProperty("type", "string");
        f1.addProperty("name", "id");
        schemaArray.add(f1);
        JsonObject f2 = new JsonObject();
        f2.addProperty("type", "string");
        f2.addProperty("name", "name");
        schemaArray.add(f2);
        JsonObject f3 = new JsonObject();
        f3.addProperty("type", "string");
        f3.addProperty("name", "sport");
        schemaArray.add(f3);
        JsonObject arrayDef = new JsonObject();
        arrayDef.addProperty("type", "array");
        arrayDef.addProperty("items", "string");
        JsonObject f4 = new JsonObject();
        f4.add("type", arrayDef);
        f4.addProperty("name", "arrayTest");
        schemaArray.add(f4);
        schemaDef.add("fields", schemaArray);

        // test data generation
        JsonObject testData = new JsonObject();
        testData.addProperty("id", UUID.randomUUID().toString());
        testData.addProperty("name", "John Doe");
        testData.addProperty("sport", "Soccer");
        JsonArray jarray = new JsonArray();
        jarray.add("a");
        jarray.add("b");
        jarray.add("c");
        testData.add("arrayTest", jarray);

        // setup registry and reader
        MockSchemaRegistry registry = new MockSchemaRegistry();
        RecordSchema rschema = AvroTypeUtil.createSchema(new Schema.Parser().parse(gson.toJson(schemaDef)));
        registry.addSchema("test", rschema);
        JsonTreeReader reader = new JsonTreeReader();
        testRunner.addControllerService("registry", registry);
        testRunner.addControllerService("reader", reader);
        testRunner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        testRunner.setProperty(PutAzureCosmosDBRecord.RECORD_READER_FACTORY, "reader");
        testRunner.enableControllerService(registry);
        testRunner.enableControllerService(reader);
        prepareMockTest();
        // override partiton key for this test case
        testRunner.setProperty(PutAzureCosmosDBRecord.PARTITION_KEY, "sport");

        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "test");

        testRunner.enqueue(gson.toJson(testData), attrs);
        testRunner.run();

        testRunner.assertTransferCount(PutAzureCosmosDBRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutAzureCosmosDBRecord.REL_SUCCESS, 1);
        List<Map<String, Object>> backendData = processor.getTestResults();
        assertEquals(1, backendData.size());
        //validate array data
        JSONObject arrayTestResult = new JSONObject();
        arrayTestResult.putAll(backendData.get(0));
        Object[] check  = (Object []) arrayTestResult.get("arrayTest");
        assertArrayEquals(new Object[]{"a", "b", "c"}, check);
    }
    private void prepareMockTest() throws Exception {
        // this setup connection service and basic mock properties
        setBasicMockProperties(true);
    }
}

class MockPutAzureCosmosDBRecord extends PutAzureCosmosDBRecord {

    static CosmosClient mockClient = mock(CosmosClient.class);
    static CosmosContainer mockContainer = mock(CosmosContainer.class);
    private List<Map<String, Object>> mockBackend = new ArrayList<>();

    @Override
    protected void createCosmosClient(final String uri, final String accessKey, final ConsistencyLevel clevel) {
        this.setCosmosClient(mockClient);
    }
    @Override
    protected void getCosmosDocumentContainer(final ProcessContext context) throws CosmosException {
        this.setContainer(mockContainer);
    }

    @Override
    protected void bulkInsert(List<Map<String, Object>> records ) throws CosmosException{
        this.mockBackend.addAll(records);
    }

    public List<Map<String, Object>> getTestResults() {
        return mockBackend;
    }


    public CosmosContainer getMockConainer() {
        return mockContainer;
    }




}