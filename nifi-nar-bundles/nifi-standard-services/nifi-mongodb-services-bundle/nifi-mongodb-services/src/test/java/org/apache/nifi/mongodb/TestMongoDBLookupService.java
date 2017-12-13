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

import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Ignore("This is an integration test and requires a copy of MongoDB running on localhost")
public class TestMongoDBLookupService {
    private static final String DB_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());
    private static final String COL_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());

    private TestRunner runner;
    private MongoDBLookupService service;

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestLookupServiceProcessor.class);
        service = new MongoDBLookupService();
        runner.addControllerService("Client Service", service);
        runner.setProperty(service, MongoDBLookupService.DATABASE_NAME, DB_NAME);
        runner.setProperty(service, MongoDBLookupService.COLLECTION_NAME, COL_NAME);
        runner.setProperty(service, MongoDBLookupService.URI, "mongodb://localhost:27017");
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "message");
    }

    @After
    public void after() throws Exception {
        service.dropDatabase();
        service.onDisable();
    }

    @Test
    public void testInit() throws Exception {
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testLookupSingle() throws Exception {
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "message");
        runner.enableControllerService(service);
        Document document = service.convertJson("{ \"uuid\": \"x-y-z\", \"message\": \"Hello, world\" }");
        service.insert(document);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");
        Optional result = service.lookup(criteria);

        Assert.assertNotNull("The value was null.", result.get());
        Assert.assertEquals("The value was wrong.", "Hello, world", result.get());

        Map<String, Object> clean = new HashMap<>();
        clean.putAll(criteria);
        service.delete(new Document(clean));

        boolean error = false;
        try {
            service.lookup(criteria);
        } catch (LookupFailureException ex) {
            error = true;
        }

        Assert.assertTrue("An error should have been thrown.", error);
    }

    @Test
    public void testLookupRecord() throws Exception {
        runner.setProperty(service, MongoDBLookupService.LOOKUP_VALUE_FIELD, "");
        runner.enableControllerService(service);
        Document document = service.convertJson("{ \"uuid\": \"x-y-z\", \"message\": \"Hello, world\" }");
        service.insert(document);

        Map<String, Object> criteria = new HashMap<>();
        criteria.put("uuid", "x-y-z");
        Optional result = service.lookup(criteria);

        Assert.assertNotNull("The value was null.", result.get());
        Assert.assertTrue("The value was wrong.", result.get() instanceof MapRecord);
        MapRecord record = (MapRecord)result.get();
        Assert.assertEquals("The value was wrong.", "Hello, world", record.getAsString("message"));
        Assert.assertEquals("The value was wrong.", "x-y-z", record.getAsString("uuid"));

        Map<String, Object> clean = new HashMap<>();
        clean.putAll(criteria);
        service.delete(new Document(clean));

        boolean error = false;
        try {
            service.lookup(criteria);
        } catch (LookupFailureException ex) {
            error = true;
        }

        Assert.assertTrue("An error should have been thrown.", error);
    }

    @Test
    public void testServiceParameters() throws Exception {
        runner.enableControllerService(service);
        Document document = service.convertJson("{ \"uuid\": \"x-y-z\", \"message\": \"Hello, world\" }");
        service.insert(document);

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
