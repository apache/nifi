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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Ignore("This is an integration test and requires a copy of MongoDB running on localhost")
public class TestMongoDBControllerService {
    private static final String DB_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());
    private static final String COL_NAME = String.format("nifi_test-%d", Calendar.getInstance().getTimeInMillis());

    private TestRunner runner;
    private MongoDBControllerService service;

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new MongoDBControllerService();
        runner.addControllerService("Client Service", service);
        runner.setProperty(service, MongoDBControllerService.DATABASE_NAME, DB_NAME);
        runner.setProperty(service, MongoDBControllerService.COLLECTION_NAME, COL_NAME);
        runner.setProperty(service, MongoDBControllerService.URI, "mongodb://localhost:27017");
        runner.enableControllerService(service);
    }

    @After
    public void after() throws Exception {
        service.dropDatabase();
        service.onDisable();
    }

    @Test
    public void testInit() throws Exception {
        runner.assertValid(service);
    }

    @Test
    public void testBasicCRUD() throws Exception {
        Document doc = service.convertJson("{\n" +
                "\t\"uuid\": \"x-y-z\",\n" +
                "\t\"message\": \"Testing!\"\n" +
                "}");
        Document lookup = service.convertJson("{ \"uuid\": \"x-y-z\" }");
        Document update = service.convertJson("{\n" +
                "\t\"$set\": {\n" +
                "\t\t\"updatedBy\": \"testUser\"\n" +
                "\t}\n" +
                "}");

        service.insert(doc);
        Document result = service.findOne(lookup);

        Assert.assertNotNull("The result was null", result);
        Assert.assertEquals("The UUID did not match", result.getString("uuid"), "x-y-z");
        Assert.assertNotNull("The message block was missing", result.getString("message"));
        Assert.assertEquals("The message block did not match", result.getString("message"), "Testing!");

        service.update(lookup, update, false);

        result = service.findOne(lookup);

        Assert.assertNotNull("The result was null", result);
        Assert.assertEquals("The UUID did not match", result.getString("uuid"), "x-y-z");
        Assert.assertNotNull("The message block was missing", result.getString("message"));
        Assert.assertEquals("The message block did not match", result.getString("message"), "Testing!");
        Assert.assertNotNull("The updatedBy block was missing", result.getString("updatedBy"));
        Assert.assertEquals("The updatedBy block did not match", result.getString("updatedBy"), "testUser");

        service.delete(lookup);

        boolean exists = service.exists(lookup);

        Assert.assertFalse("After the delete, the document still existed", exists);
    }

    @Test
    public void testMultipleCRUD() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Document> sampleDocuments = new ArrayList<>();
        List<String> uuids = new ArrayList<>();
        Map<String, Object> mappings = new HashMap<>();
        Random random = new Random();
        int count = random.nextInt(1000);
        for (int x = 0; x < count; x++) {
            Map<String, Object> doc = new HashMap<>();
            String uuid = UUID.randomUUID().toString();
            String ts = Calendar.getInstance().getTime().toString();
            uuids.add(uuid);
            mappings.put(uuid, ts);

            doc.put("uuid", uuid);
            doc.put("timestamp", ts);
            doc.put("randomNumber", random.nextInt(10));

            String json = mapper.writeValueAsString(doc);
            sampleDocuments.add(service.convertJson(json));
        }

        service.insert(sampleDocuments);

        long docCount = service.count(service.convertJson("{}"));

        Assert.assertEquals("The counts did not match", docCount, count);
        for (String uuid : uuids) {
            Document lookup = service.convertJson(String.format("{ \"uuid\": \"%s\" }", uuid));
            Document result = service.findOne(lookup);
            Assert.assertNotNull("The document was not found", result);
            Assert.assertEquals("The uuid did not match", result.getString("uuid"), uuid);
            Assert.assertEquals("The timestamp did not match", result.getString("timestamp"), mappings.get(uuid));
        }

        Document query = service.convertJson("{ \"randomNumber\": 5 }");
        docCount = service.count(query);
        List<Document> results = service.findMany(query);

        Assert.assertTrue("Count should have been >= 1", docCount >= 1);
        Assert.assertNotNull("Result set was null", results);
        Assert.assertEquals("The counts did not match up", docCount, results.size());
    }

    @Test
    public void testUpsert() throws Exception {
        Document query = service.convertJson(String.format("{ \"uuid\": \"%s\" }", UUID.randomUUID().toString()));
        Document update = service.convertJson("{ \"$set\": { \"message\": \"Hello, world\"  } }");
        service.upsert(query, update);

        Document result = service.findOne(query);
        Assert.assertNotNull("No result returned", result);
        Assert.assertEquals("UUID did not match", result.getString("uuid"), query.getString("uuid"));
        Assert.assertEquals("Message did not match", result.getString("message"), "Hello, world");

        Map<String, String> mappings = new HashMap<>();
        for (int x = 0; x < 5; x++) {
            String fieldName = String.format("field_%d", x);
            String uuid = UUID.randomUUID().toString();
            mappings.put(fieldName, uuid);
            update = service.convertJson(String.format("{ \"$set\": { \"%s\": \"%s\" } }", fieldName, uuid));

            service.upsert(query, update);
        }

        result = service.findOne(query);

        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            Assert.assertEquals("Entry did not match.", entry.getValue(), result.getString(entry.getKey()));
        }
    }
}
