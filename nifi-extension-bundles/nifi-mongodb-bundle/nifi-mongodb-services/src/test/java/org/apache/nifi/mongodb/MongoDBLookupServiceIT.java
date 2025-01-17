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

package org.apache.nifi.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class MongoDBLookupServiceIT extends AbstractMongoIT {
    private static final String DB_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());
    private static final String COL_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());

    private TestRunner runner;
    private MongoDBLookupService service;
    private MongoDBControllerService controllerService;
    private MongoDatabase db;
    private MongoCollection col;

    @BeforeEach
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestLookupServiceProcessor.class);
        service = new MongoDBLookupService();
        controllerService = new MongoDBControllerService();
        runner.addControllerService("Client Service", service);
        runner.addControllerService("Client Service 2", controllerService);
        runner.setProperty(TestLookupServiceProcessor.CLIENT_SERVICE, "Client Service");
        runner.setProperty(service, MongoDBLookupService.DATABASE_NAME, DB_NAME);
        runner.setProperty(service, MongoDBLookupService.COLLECTION_NAME, COL_NAME);
        runner.setProperty(controllerService, MongoDBControllerService.URI, MONGO_CONTAINER.getConnectionString());
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "message");
        runner.setProperty(service, MongoDBLookupService.CONTROLLER_SERVICE, "Client Service 2");
        SchemaRegistry registry = new StubSchemaRegistry();
        runner.addControllerService("registry", registry);
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "");
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.enableControllerService(registry);
        runner.enableControllerService(controllerService);
        runner.enableControllerService(service);

        db = controllerService.getDatabase(DB_NAME);
        col = db.getCollection(COL_NAME);
    }

    @AfterEach
    public void after() {
        db.drop();
        controllerService.onDisable();
    }

    @Test
    public void testInit() {
        runner.assertValid(service);

    }

    @Test
    public void testLookupSingle() throws Exception {
        runner.disableControllerService(service);
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "message");
        runner.enableControllerService(service);
        Document document = controllerService.convertJson("{ \"uuid\": \"x-y-z\", \"message\": \"Hello, world\" }");
        col.insertOne(document);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");
        Optional result = service.lookup(criteria);

        assertNotNull(result.get(), "The value was null.");
        assertEquals("Hello, world", result.get(), "The value was wrong.");

        Map<String, Object> clean = new HashMap<>();
        clean.putAll(criteria);
        col.deleteOne(new Document(clean));

        try {
            result = service.lookup(criteria);
        } catch (LookupFailureException ex) {
            fail();
        }

        assertFalse(result.isPresent());
    }

    @Test
    public void testWithSchemaRegistry() throws Exception {
        runner.assertValid();

        col.insertOne(new Document()
            .append("username", "john.smith")
            .append("password", "testing1234")
        );

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("username", "john.smith");
        Map<String, String> context = new HashMap<>();
        context.put("schema.name", "user");
        Optional result = service.lookup(criteria, context);
        assertTrue(result.isPresent());
        assertNotNull(result.get());
        MapRecord record = (MapRecord) result.get();

        assertEquals("john.smith", record.getAsString("username"));
        assertEquals("testing1234", record.getAsString("password"));

        /*
         * Test falling back on schema detection if a user doesn't specify the context argument
         */
        result = service.lookup(criteria);
        assertTrue(result.isPresent());
        assertNotNull(result.get());
        record = (MapRecord) result.get();

        assertEquals("john.smith", record.getAsString("username"));
        assertEquals("testing1234", record.getAsString("password"));
    }

    @Test
    public void testSchemaTextStrategy() throws Exception {
        byte[] contents = IOUtils.toByteArray(getClass().getResourceAsStream("/simple.avsc"));

        runner.disableControllerService(service);
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "");
        runner.setProperty(service, MongoDBLookupService.PROJECTION, "{ \"_id\": 0 }");
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_TEXT, "${schema.text}");
        runner.enableControllerService(service);
        runner.assertValid();

        col.insertOne(new Document().append("msg", "Testing1234"));

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("msg", "Testing1234");
        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.text", new String(contents));

        Optional results = service.lookup(criteria, attrs);
        assertNotNull(results);
        assertTrue(results.isPresent());
    }

    @Test
    public void testLookupRecord() throws Exception {
        runner.disableControllerService(service);
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "");
        runner.setProperty(service, MongoDBLookupService.PROJECTION, "{ \"_id\": 0 }");
        runner.enableControllerService(service);

        Date d = new Date();
        Timestamp ts = new Timestamp(new Date().getTime());
        List list = Arrays.asList("a", "b", "c", "d", "e");

        col.insertOne(new Document()
            .append("uuid", "x-y-z")
            .append("dateField", d)
            .append("longField", 10000L)
            .append("stringField", "Hello, world")
            .append("timestampField", ts)
            .append("decimalField", Double.MAX_VALUE / 2.0)
            .append("subrecordField", new Document()
                .append("nestedString", "test")
                .append("nestedLong", Long.valueOf(1000)))
            .append("arrayField", list)
        );

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");
        Optional result = service.lookup(criteria);

        assertNotNull(result.get(), "The value was null.");
        assertInstanceOf(MapRecord.class, result.get(), "The value was wrong.");
        MapRecord record = (MapRecord) result.get();
        RecordSchema subSchema = ((RecordDataType) record.getSchema().getField("subrecordField").get().getDataType()).getChildSchema();

        assertEquals("Hello, world", record.getValue("stringField"), "The value was wrong.");
        assertEquals("x-y-z", record.getValue("uuid"), "The value was wrong.");
        assertEquals(Long.valueOf(10000), record.getValue("longField"));
        assertEquals((Double.MAX_VALUE / 2.0), record.getValue("decimalField"));
        assertEquals(d, record.getValue("dateField"));
        assertEquals(ts.getTime(), ((Date) record.getValue("timestampField")).getTime());

        Record subRecord = record.getAsRecord("subrecordField", subSchema);
        assertNotNull(subRecord);
        assertEquals("test", subRecord.getValue("nestedString"));
        assertEquals(Long.valueOf(1000), subRecord.getValue("nestedLong"));
        assertEquals(list, record.getValue("arrayField"));

        Map<String, Object> clean = new HashMap<>();
        clean.putAll(criteria);
        col.deleteOne(new Document(clean));

        try {
            result = service.lookup(criteria);
        } catch (LookupFailureException ex) {
            fail();
        }

        assertFalse(result.isPresent());
    }

    @Test
    public void testServiceParameters() {
        Document document = controllerService.convertJson("{ \"uuid\": \"x-y-z\", \"message\": \"Hello, world\" }");
        col.insertOne(document);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");

        assertDoesNotThrow(() -> service.lookup(criteria));

        assertThrows(LookupFailureException.class, () -> service.lookup(new HashMap<>()));
    }
}
