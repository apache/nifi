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

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

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

    @BeforeEach
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
        recordReader.addRecord("2", "B", "Jane Doe", 47, "Tennis");
        recordReader.addRecord("3", "B", "Sally Doe", 47, "Curling");
        recordReader.addRecord("4", "A", "Jimmy Doe", 14, null);
        recordReader.addRecord("5", "C", "Pizza Doe", 14, null);

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

        recordReader.addRecord("1", "A", new MapRecord(personSchema, new HashMap<>() {
            private static final long serialVersionUID = -3185956498135742190L;
            {
                put("name", "John Doe");
                put("age", 48);
                put("sport", "Soccer");
            }
        }));
        recordReader.addRecord("2", "B", new MapRecord(personSchema, new HashMap<>() {
            private static final long serialVersionUID = 1L;
            {
                put("name", "Jane Doe");
                put("age", 47);
                put("sport", "Tennis");
            }
        }));
        recordReader.addRecord("3", "A", new MapRecord(personSchema, new HashMap<>() {
            private static final long serialVersionUID = -1329194249439570573L;
            {
                put("name", "Sally Doe");
                put("age", 47);
                put("sport", "Curling");
            }
        }));
        recordReader.addRecord("4", "C", new MapRecord(personSchema, new HashMap<>() {
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
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNodeFactory nodeFactory = objectMapper.getNodeFactory();

        // schema creation for test
        final ObjectNode schemaDef = nodeFactory.objectNode();
        schemaDef.put("type", "record");
        schemaDef.put("name", "Test");
        final ArrayNode schemaArray = nodeFactory.arrayNode();
        final ObjectNode f1 = nodeFactory.objectNode();
        f1.put("type", "string");
        f1.put("name", "id");
        schemaArray.add(f1);
        final ObjectNode f2 = nodeFactory.objectNode();
        f2.put("type", "string");
        f2.put("name", "name");
        schemaArray.add(f2);
        final ObjectNode f3 = nodeFactory.objectNode();
        f3.put("type", "string");
        f3.put("name", "sport");
        schemaArray.add(f3);
        final ObjectNode arrayDef = nodeFactory.objectNode();
        arrayDef.put("type", "array");
        arrayDef.put("items", "string");
        final ObjectNode f4 = nodeFactory.objectNode();
        f4.set("type", arrayDef);
        f4.put("name", "arrayTest");
        schemaArray.add(f4);
        schemaDef.set("fields", schemaArray);

        // test data generation
        final ObjectNode testData = nodeFactory.objectNode();
        testData.put("id", UUID.randomUUID().toString());
        testData.put("name", "John Doe");
        testData.put("sport", "Soccer");
        final ArrayNode jarray = nodeFactory.arrayNode();
        jarray.add("a");
        jarray.add("b");
        jarray.add("c");
        testData.set("arrayTest", jarray);

        // setup registry and reader
        MockSchemaRegistry registry = new MockSchemaRegistry();
        RecordSchema rschema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schemaDef.toPrettyString()));
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

        testRunner.enqueue(testData.toPrettyString(), attrs);
        testRunner.run();

        testRunner.assertTransferCount(PutAzureCosmosDBRecord.REL_FAILURE, 0);
        testRunner.assertTransferCount(PutAzureCosmosDBRecord.REL_SUCCESS, 1);
        List<Map<String, Object>> backendData = processor.getTestResults();
        assertEquals(1, backendData.size());
        //validate array data
        final Map<?, ?> arrayTestResult = backendData.getFirst();
        Object[] check  = (Object[]) arrayTestResult.get("arrayTest");
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
    private final List<Map<String, Object>> mockBackend = new ArrayList<>();

    @Override
    protected void createCosmosClient(final String uri, final String accessKey, final ConsistencyLevel consistencyLevel) {
        this.setCosmosClient(mockClient);
    }
    @Override
    protected void getCosmosDocumentContainer(final ProcessContext context) throws CosmosException {
        this.setContainer(mockContainer);
    }

    @Override
    protected void bulkInsert(List<Map<String, Object>> records ) throws CosmosException {
        this.mockBackend.addAll(records);
    }

    public List<Map<String, Object>> getTestResults() {
        return mockBackend;
    }
}