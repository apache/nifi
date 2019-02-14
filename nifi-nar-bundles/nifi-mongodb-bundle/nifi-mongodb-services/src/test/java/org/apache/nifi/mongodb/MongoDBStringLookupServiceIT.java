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
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Optional;

public class MongoDBStringLookupServiceIT {
    TestRunner runner;
    MongoDBControllerService controllerService;
    MongoDBStringLookupService stringLookupService;
    MongoDatabase database;
    MongoCollection<Document> collection;

    static final String DB = "string_lookup_test_db";
    static final String COL = "string_lookup_test_col";

    @Before
    public void setup() throws Exception {
        controllerService = new MongoDBControllerService();
        stringLookupService = new MongoDBStringLookupService();
        runner = TestRunners.newTestRunner(TestLookupServiceProcessor.class);
        runner.setVariable("database", DB);
        runner.setVariable("collection", COL);
        runner.addControllerService("clientService", controllerService);
        runner.addControllerService("lookupService", stringLookupService);
        runner.setProperty(controllerService, MongoDBClientService.URI, "mongodb://localhost:27017");
        runner.setProperty(stringLookupService, MongoDBStringLookupService.CONTROLLER_SERVICE, "clientService");
        runner.setProperty(stringLookupService, MongoDBStringLookupService.DB_NAME, "${database}");
        runner.setProperty(stringLookupService, MongoDBStringLookupService.COL_NAME, "${collection}");
        runner.enableControllerService(controllerService);
        runner.enableControllerService(stringLookupService);
        runner.setProperty(TestLookupServiceProcessor.CLIENT_SERVICE_STR, "lookupService");
        runner.assertValid();
        database = controllerService.getDatabase(DB);
        collection = database.getCollection(COL);
    }

    @After
    public void teardown() {
        database.drop();
    }

    @Test
    public void test() throws Exception {
        Document base = new Document("_id", "doc-1");
        collection.insertOne(base.append("message", "Hello, world"));
        Optional<String> result = stringLookupService.lookup(new HashMap<String, Object>(){{
            put(MongoDBStringLookupService.QUERY, base.toJson());
        }});

        assertTrue(result.isPresent());
        String _temp = result.get();
        Document parsed = Document.parse(_temp);
        assertEquals("Hello, world", parsed.get("message"));
    }
}
