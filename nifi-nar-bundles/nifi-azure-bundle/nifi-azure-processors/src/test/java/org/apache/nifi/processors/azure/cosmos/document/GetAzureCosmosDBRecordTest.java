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
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class GetAzureCosmosDBRecordTest extends MockTestBase {

    public static final String MOCK_QUERY2 = "select * from d";
    public static final int MOCK_DATA_NUM = 10;
    private MockGetAzureCosmosDBRecord processor;
    private static RecordSchema SCHEMA1;
    private static RecordSchema SCHEMA2;

    static {
        final List<RecordField> testDataFields = new ArrayList<>();
        final RecordField idField = new RecordField("id", RecordFieldType.STRING.getDataType());
        final RecordField categoryField = new RecordField(MOCK_PARTITION_FIELD_NAME, RecordFieldType.INT.getDataType());
        final RecordField payloadField = new RecordField("payload", RecordFieldType.STRING.getDataType());
        testDataFields.add(idField);
        testDataFields.add(categoryField);
        testDataFields.add(payloadField);
        SCHEMA1 = new SimpleRecordSchema(testDataFields);

        final List<RecordField> testDataFields2 = new ArrayList<>();
        final RecordField payloadField2 = new RecordField("payload", RecordFieldType.ARRAY.getDataType());
        testDataFields.add(idField);
        testDataFields.add(categoryField);
        testDataFields.add(payloadField2);
        SCHEMA2 = new SimpleRecordSchema(testDataFields2);
    }

    @Before
    public void setUp() throws Exception {
        processor = new MockGetAzureCosmosDBRecord();
        testRunner = TestRunners.newTestRunner(processor);

        // setup  schema registry, record writer, and schema for test cases
        MockSchemaRegistry registry = new MockSchemaRegistry();
        JsonRecordSetWriter writer = new JsonRecordSetWriter();

        registry.addSchema("sample", SCHEMA1);
        registry.addSchema("sample2", SCHEMA2);
        testRunner.addControllerService("registry", registry);
        testRunner.enableControllerService(registry);

        testRunner.addControllerService("writer", writer);
        testRunner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        testRunner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        testRunner.enableControllerService(writer);
        testRunner.setProperty(MockGetAzureCosmosDBRecord.WRITER_FACTORY, "writer");
    }


    @Test
    public void testProcessorConfigValidity() throws Exception {
        setBasicMockProperties(false);
        testRunner.setProperty(AzureCosmosDBUtils.URI,MOCK_URI);
        testRunner.assertNotValid();
        testRunner.setProperty(AzureCosmosDBUtils.DB_ACCESS_KEY,MOCK_DB_ACCESS_KEY);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureCosmosDBRecord.QUERY,MOCK_QUERY);
        testRunner.assertValid();
        processor.onScheduled(testRunner.getProcessContext());
        assertNotNull(processor.getCosmosClient());
    }

    @Test
    public void testProcessorConfigValidity_With_ConnectionService() throws Exception {
        setBasicMockProperties(true);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureCosmosDBRecord.QUERY,MOCK_QUERY);
        testRunner.assertValid();
        processor.onScheduled(testRunner.getProcessContext());
        assertNotNull(processor.getCosmosClient());
    }
}

class MockGetAzureCosmosDBRecord extends GetAzureCosmosDBRecord {

    static CosmosClient mockClient = mock(CosmosClient.class);
    static CosmosContainer mockContainer = mock(CosmosContainer.class);
    List<JsonNode> _mockData1;
    List<JsonNode> _mockData2;

    CosmosPagedIterable<JsonNode> mockPagedIterable;
    final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void createCosmosClient(final String uri, final String accessKey, final ConsistencyLevel clevel) {
        this.setCosmosClient(mockClient);
    }
    @Override
    protected void getCosmosDocumentContainer(final ProcessContext context) throws CosmosException {
        this.setContainer(mockContainer);
    }
}
