/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

package org.apache.nifi.elasticsearch.integration;

import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.ElasticSearchLookupService;
import org.apache.nifi.elasticsearch.TestControllerServiceProcessor;
import org.apache.nifi.elasticsearch.TestSchemaRegistry;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElasticSearchLookupService_IT extends AbstractElasticsearch_IT {
    private TestRunner runner;
    private ElasticSearchClientService service;
    private ElasticSearchLookupService lookupService;

    @BeforeAll
    public static void beforeAll() throws IOException {
        startTestcontainer();
        setupTestData();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        tearDownTestData();
        stopTestcontainer();
    }

    @BeforeEach
    void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new ElasticSearchClientServiceImpl();
        lookupService = new ElasticSearchLookupService();
        runner.addControllerService("Client Service", service);
        runner.addControllerService("Lookup Service", lookupService);
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, ELASTIC_HOST);
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000");
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000");
        runner.setProperty(service, ElasticSearchClientService.USERNAME, "elastic");
        runner.setProperty(service, ElasticSearchClientService.PASSWORD, ELASTIC_USER_PASSWORD);
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "Client Service");
        runner.setProperty(TestControllerServiceProcessor.LOOKUP_SERVICE, "Lookup Service");
        runner.setProperty(lookupService, ElasticSearchLookupService.CLIENT_SERVICE, "Client Service");
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "user_details");
        setTypeOnLookupService();

        try {
            runner.enableControllerService(service);
            runner.enableControllerService(lookupService);
        } catch (Exception ex) {
            throw ex;
        }

        service.refresh(null, null);
    }

    void setTypeOnLookupService() {
        if (getElasticMajorVersion() == 6) {
            runner.setProperty(lookupService, ElasticSearchLookupService.TYPE, ElasticSearchClientService_IT.TYPE);
        } else {
            runner.removeProperty(lookupService, ElasticSearchLookupService.TYPE);
        }
    }

    @Test
    void testValidity() throws Exception {
        setDefaultSchema();
        runner.assertValid();
    }

    private void setDefaultSchema() throws Exception {
        runner.disableControllerService(lookupService);
        SchemaRegistry registry = new TestSchemaRegistry();
        runner.addControllerService("registry", registry);
        runner.setProperty(lookupService, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.enableControllerService(registry);
        runner.enableControllerService(lookupService);
    }

    @Test
    void lookupById() throws LookupFailureException {
        Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("_id", "2");
        Optional<Record> result = lookupService.lookup(coordinates);

        assertNotNull(result);
        assertTrue(result.isPresent());
        Record record = result.get();
        assertEquals("jane.doe@company.com", record.getAsString("email"));
        assertEquals("098-765-4321", record.getAsString("phone"));
        assertEquals("GHIJK", record.getAsString("accessKey"));
    }

    @Test
    void testInvalidIdScenarios() {
        List<Map<String, Object>> coordinates = new ArrayList<>();
        Map<String, Object> temp = new HashMap<>();
        temp.put("_id", 1);
        coordinates.add(temp);
        temp = new HashMap<>();
        temp.put("_id", 1);
        temp.put("email", "john.smith@company.com");
        coordinates.add(temp);

        for (Map<String, Object> coordinate: coordinates) {
            Exception exception = null;

            try {
                lookupService.lookup(coordinate);
            } catch (Exception ex) {
                exception = ex;
            }

            assertNotNull(exception);
            assertTrue(exception instanceof LookupFailureException);
        }
    }

    @Test
    void lookupByQuery() throws LookupFailureException {
        Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("phone", "098-765-4321");
        coordinates.put("email", "jane.doe@company.com");
        Optional<Record> result = lookupService.lookup(coordinates);

        assertNotNull(result);
        assertTrue(result.isPresent());
        Record record = result.get();
        assertEquals("jane.doe@company.com", record.getAsString("email"));
        assertEquals("098-765-4321", record.getAsString("phone"));
        assertEquals("GHIJK", record.getAsString("accessKey"));
    }

    @Test
    void testNestedSchema() throws LookupFailureException {
        Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("subField.deeper.deepest.super_secret", "The sky is blue");
        coordinates.put("subField.deeper.secretz", "Buongiorno, mondo!!");
        coordinates.put("msg", "Hello, world");

        runner.disableControllerService(lookupService);
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "nested");
        setTypeOnLookupService();
        runner.enableControllerService(lookupService);

        Optional<Record> response = lookupService.lookup(coordinates);
        assertNotNull(response);
        assertTrue(response.isPresent());
        Record rec = response.get();
        assertEquals("Hello, world", rec.getValue("msg"));
        Record subRec = getSubRecord(rec, "subField");
        assertNotNull(subRec);
        Record deeper = getSubRecord(subRec, "deeper");
        assertNotNull(deeper);
        Record deepest = getSubRecord(deeper, "deepest");
        assertNotNull(deepest);
        assertEquals("The sky is blue", deepest.getAsString("super_secret"));
    }

    @Test
    void testDetectedSchema() throws LookupFailureException {
        runner.disableControllerService(lookupService);
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "complex");
        setTypeOnLookupService();
        runner.enableControllerService(lookupService);
        Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("_id", "1");

        Optional<Record> response = lookupService.lookup(coordinates);
        assertNotNull(response);
        assertTrue(response.isPresent());
        Record rec = response.get();
        Record subRec = getSubRecord(rec, "subField");

        Record r2 = new MapRecord(rec.getSchema(), new HashMap<>());
        RecordPath path = RecordPath.compile("/subField/longField");
        RecordPathResult result = path.evaluate(r2);
        result.getSelectedFields().findFirst().get().updateValue(1234567890L);

        assertNotNull(rec);
        assertNotNull(subRec);
        assertEquals("Hello, world", rec.getValue("msg"));
        assertNotNull(rec.getValue("subField"));
        assertEquals(100000, subRec.getValue("longField"));
        assertEquals("2018-04-10T12:18:05Z", subRec.getValue("dateField"));
    }

    public static Record getSubRecord(Record rec, String fieldName) {
        RecordSchema schema = rec.getSchema();
        RecordSchema subSchema = ((RecordDataType)schema.getField(fieldName).get().getDataType()).getChildSchema();
        return rec.getAsRecord(fieldName, subSchema);
    }

    @Test
    void testMappings() throws LookupFailureException {
        runner.disableControllerService(lookupService);
        runner.setProperty(lookupService, "$.subField.longField", "/longField2");
        runner.setProperty(lookupService, "$.subField.dateField", "/dateField2");
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "nested");
        setTypeOnLookupService();
        runner.enableControllerService(lookupService);

        Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("msg", "Hello, world");
        Optional<Record> result = lookupService.lookup(coordinates);
        assertTrue(result.isPresent());
        Record rec = result.get();
        Map<String, Object> entries = new HashMap<>();
        entries.put("dateField2", "2018-08-14T10:08:00Z");
        entries.put("longField2", 150000);
        entries.entrySet().forEach( (field) -> {
            Object value = rec.getValue(field.getKey());
            assertEquals(field.getValue(), value);
        });
    }
}
