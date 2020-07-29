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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MongoDBLookupServiceIT {
    private static final String DB_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());
    private static final String COL_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());

    private TestRunner runner;
    private MongoDBLookupService service;
    private MongoDBControllerService controllerService;
    private MongoDatabase db;
    private MongoCollection col;

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestLookupServiceProcessor.class);
        service = new MongoDBLookupService();
        controllerService = new MongoDBControllerService();
        runner.addControllerService("Client Service", service);
        runner.addControllerService("Client Service 2", controllerService);
        runner.setProperty(TestLookupServiceProcessor.CLIENT_SERVICE, "Client Service");
        runner.setProperty(service, MongoDBLookupService.DATABASE_NAME, DB_NAME);
        runner.setProperty(service, MongoDBLookupService.COLLECTION_NAME, COL_NAME);
        runner.setProperty(controllerService, MongoDBControllerService.URI, "mongodb://localhost:27017");
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

    @After
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

        Assert.assertNotNull("The value was null.", result.get());
        Assert.assertEquals("The value was wrong.", "Hello, world", result.get());

        Map<String, Object> clean = new HashMap<>();
        clean.putAll(criteria);
        col.deleteOne(new Document(clean));

        try {
            result = service.lookup(criteria);
        } catch (LookupFailureException ex) {
            Assert.fail();
        }

        Assert.assertTrue(!result.isPresent());
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
        Assert.assertTrue(result.isPresent());
        Assert.assertNotNull(result.get());
        MapRecord record = (MapRecord)result.get();

        Assert.assertEquals("john.smith", record.getAsString("username"));
        Assert.assertEquals("testing1234", record.getAsString("password"));

        /*
         * Test falling back on schema detection if a user doesn't specify the context argument
         */
        result = service.lookup(criteria);
        Assert.assertTrue(result.isPresent());
        Assert.assertNotNull(result.get());
        record = (MapRecord)result.get();

        Assert.assertEquals("john.smith", record.getAsString("username"));
        Assert.assertEquals("testing1234", record.getAsString("password"));
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
        Assert.assertNotNull(results);
        Assert.assertTrue(results.isPresent());
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
                .append("nestedLong", new Long(1000)))
            .append("arrayField", list)
        );

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");
        Optional result = service.lookup(criteria);

        Assert.assertNotNull("The value was null.", result.get());
        Assert.assertTrue("The value was wrong.", result.get() instanceof MapRecord);
        MapRecord record = (MapRecord)result.get();
        RecordSchema subSchema = ((RecordDataType)record.getSchema().getField("subrecordField").get().getDataType()).getChildSchema();

        Assert.assertEquals("The value was wrong.", "Hello, world", record.getValue("stringField"));
        Assert.assertEquals("The value was wrong.", "x-y-z", record.getValue("uuid"));
        Assert.assertEquals(new Long(10000), record.getValue("longField"));
        Assert.assertEquals((Double.MAX_VALUE / 2.0), record.getValue("decimalField"));
        Assert.assertEquals(d, record.getValue("dateField"));
        Assert.assertEquals(ts.getTime(), ((Date)record.getValue("timestampField")).getTime());

        Record subRecord = record.getAsRecord("subrecordField", subSchema);
        Assert.assertNotNull(subRecord);
        Assert.assertEquals("test", subRecord.getValue("nestedString"));
        Assert.assertEquals(new Long(1000), subRecord.getValue("nestedLong"));
        Assert.assertEquals(list, record.getValue("arrayField"));

        Map<String, Object> clean = new HashMap<>();
        clean.putAll(criteria);
        col.deleteOne(new Document(clean));

        try {
            result = service.lookup(criteria);
        } catch (LookupFailureException ex) {
            Assert.fail();
        }

        Assert.assertTrue(!result.isPresent());
    }

    @Test
    public void testServiceParameters() {
        Document document = controllerService.convertJson("{ \"uuid\": \"x-y-z\", \"message\": \"Hello, world\" }");
        col.insertOne(document);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");

        boolean error = false;
        try {
            service.lookup(criteria);
        } catch(Exception ex) {
            error = true;
        }

        Assert.assertFalse("An error was thrown when no error should have been thrown.", error);
        error = false;

        try {
            service.lookup(new HashMap());
        } catch (Exception ex) {
            error = true;
            Assert.assertTrue("The exception was the wrong type", ex instanceof LookupFailureException);
        }

        Assert.assertTrue("An error was not thrown when the input was empty", error);
    }
}
