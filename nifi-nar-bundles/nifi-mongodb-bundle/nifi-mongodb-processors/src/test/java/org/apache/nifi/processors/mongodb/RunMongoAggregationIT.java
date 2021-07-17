/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunMongoAggregationIT {

    private static final String MONGO_URI = "mongodb://localhost";
    private static final String DB_NAME   = String.format("agg_test-%s", Calendar.getInstance().getTimeInMillis());
    private static final String COLLECTION_NAME = "agg_test_data";
    private static final String AGG_ATTR = "mongo.aggregation.query";

    private TestRunner runner;
    private MongoClient mongoClient;
    private Map<String, Integer> mappings;
    private Calendar now = Calendar.getInstance();

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(RunMongoAggregation.class);
        runner.setVariable("uri", MONGO_URI);
        runner.setVariable("db", DB_NAME);
        runner.setVariable("collection", COLLECTION_NAME);
        runner.setProperty(AbstractMongoProcessor.URI, "${uri}");
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");
        runner.setProperty(RunMongoAggregation.QUERY_ATTRIBUTE, AGG_ATTR);

        mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));

        MongoCollection<Document> collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        String[] values = new String[] { "a", "b", "c" };
        mappings = new HashMap<>();

        for (int x = 0; x < values.length; x++) {
            for (int y = 0; y < x + 2; y++) {
                Document doc = new Document().append("val", values[x]).append("date", now.getTime());
                collection.insertOne(doc);
            }
            mappings.put(values[x], x + 2);
        }
    }

    @After
    public void teardown() {
        runner = null;
        mongoClient.getDatabase(DB_NAME).drop();
    }

    @Test
    public void testAggregation() throws Exception {
        final String queryInput = "[\n" +
                "    {\n" +
                "        \"$project\": {\n" +
                "            \"_id\": 0,\n" +
                "            \"val\": 1\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"$group\": {\n" +
                "            \"_id\": \"$val\",\n" +
                "            \"doc_count\": {\n" +
                "                \"$sum\": 1\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "]";
        runner.setProperty(RunMongoAggregation.QUERY, queryInput);
        runner.enqueue("test");
        runner.run(1, true, true);

        evaluateRunner(1);

        runner.clearTransferState();

        runner.setIncomingConnection(false);
        runner.run(); //Null parent flowfile
        evaluateRunner(0);

        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(RunMongoAggregation.REL_RESULTS);
        for (MockFlowFile mff : flowFiles) {
            String val = mff.getAttribute(AGG_ATTR);
            Assert.assertNotNull("Missing query attribute", val);
            Assert.assertEquals("Value was wrong", val, queryInput);
        }
    }

    @Test
    public void testExpressionLanguageSupport() throws Exception {
        runner.setVariable("fieldName", "$val");
        runner.setProperty(RunMongoAggregation.QUERY, "[\n" +
                "    {\n" +
                "        \"$project\": {\n" +
                "            \"_id\": 0,\n" +
                "            \"val\": 1\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"$group\": {\n" +
                "            \"_id\": \"${fieldName}\",\n" +
                "            \"doc_count\": {\n" +
                "                \"$sum\": 1\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "]");

        runner.enqueue("test");
        runner.run(1, true, true);
        evaluateRunner(1);
    }

    @Test
    public void testInvalidQuery(){
        runner.setProperty(RunMongoAggregation.QUERY, "[\n" +
            "    {\n" +
                "        \"$invalid_stage\": {\n" +
                "            \"_id\": 0,\n" +
                "            \"val\": 1\n" +
                "        }\n" +
                "    }\n" +
            "]"
        );
        runner.enqueue("test");
        runner.run(1, true, true);
        runner.assertTransferCount(RunMongoAggregation.REL_RESULTS, 0);
        runner.assertTransferCount(RunMongoAggregation.REL_ORIGINAL, 0);
        runner.assertTransferCount(RunMongoAggregation.REL_FAILURE, 1);
    }

    @Test
    public void testJsonTypes() throws IOException {

        runner.setProperty(RunMongoAggregation.JSON_TYPE, RunMongoAggregation.JSON_STANDARD);
        runner.setProperty(RunMongoAggregation.QUERY, "[ { \"$project\": { \"myArray\": [ \"$val\", \"$date\" ] } } ]");
        runner.enqueue("test");
        runner.run(1, true, true);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(RunMongoAggregation.REL_RESULTS);
        ObjectMapper mapper = new ObjectMapper();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        for (MockFlowFile mockFlowFile : flowFiles) {
            byte[] raw = runner.getContentAsByteArray(mockFlowFile);
            Map<String, List<String>> read = mapper.readValue(raw, Map.class);
            Assert.assertTrue(read.get("myArray").get(1).equalsIgnoreCase( format.format(now.getTime())));
        }

        runner.clearTransferState();

        runner.setProperty(RunMongoAggregation.JSON_TYPE, RunMongoAggregation.JSON_EXTENDED);
        runner.enqueue("test");
        runner.run(1, true, true);

        flowFiles = runner.getFlowFilesForRelationship(RunMongoAggregation.REL_RESULTS);
        for (MockFlowFile mockFlowFile : flowFiles) {
            byte[] raw = runner.getContentAsByteArray(mockFlowFile);
            Map<String, List<Long>> read = mapper.readValue(raw, Map.class);
            Assert.assertTrue(read.get("myArray").get(1) == now.getTimeInMillis());
        }
    }

    private void evaluateRunner(int original) throws IOException {
        runner.assertTransferCount(RunMongoAggregation.REL_RESULTS, mappings.size());
        runner.assertTransferCount(RunMongoAggregation.REL_ORIGINAL, original);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(RunMongoAggregation.REL_RESULTS);
        ObjectMapper mapper = new ObjectMapper();
        for (MockFlowFile mockFlowFile : flowFiles) {
            byte[] raw = runner.getContentAsByteArray(mockFlowFile);
            Map read = mapper.readValue(raw, Map.class);
            Assert.assertTrue("Value was not found", mappings.containsKey(read.get("_id")));

            String queryAttr = mockFlowFile.getAttribute(AGG_ATTR);
            Assert.assertNotNull("Query attribute was null.", queryAttr);
            Assert.assertTrue("Missing $project", queryAttr.contains("$project"));
            Assert.assertTrue("Missing $group", queryAttr.contains("$group"));
        }
    }

    @Test
    public void testClientService() throws Exception {
        MongoDBClientService clientService = new MongoDBControllerService();
        runner.addControllerService("clientService", clientService);
        runner.removeProperty(RunMongoAggregation.URI);
        runner.setProperty(clientService, MongoDBControllerService.URI, MONGO_URI);
        runner.setProperty(RunMongoAggregation.CLIENT_SERVICE, "clientService");
        runner.setProperty(RunMongoAggregation.QUERY, "[\n" +
                        "    {\n" +
                        "        \"$project\": {\n" +
                        "            \"_id\": 0,\n" +
                        "            \"val\": 1\n" +
                        "        }\n" +
                        "    }]");
        runner.enableControllerService(clientService);
        runner.assertValid();

        runner.enqueue("{}");
        runner.run();
        runner.assertTransferCount(RunMongoAggregation.REL_RESULTS, 9);
    }

    @Test
    public void testExtendedJsonSupport() throws Exception {
        String pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        //Let's put this a week from now to make sure that we're not getting too close to
        //the creation date
        Date nowish = new Date(now.getTime().getTime() + (7 * 24 * 60 * 60 * 1000));

        final String queryInput = "[\n" +
            "  {\n" +
            "    \"$match\": {\n" +
            "      \"date\": { \"$gte\": { \"$date\": \"2019-01-01T00:00:00Z\" }, \"$lte\": { \"$date\": \"" + simpleDateFormat.format(nowish) + "\" } }\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"$group\": {\n" +
            "      \"_id\": \"$val\",\n" +
            "      \"doc_count\": {\n" +
            "        \"$sum\": 1\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "]\n";

        runner.setProperty(RunMongoAggregation.QUERY, queryInput);
        runner.enqueue("test");
        runner.run(1, true, true);

        runner.assertTransferCount(RunMongoAggregation.REL_RESULTS, mappings.size());
    }

    @Test
    public void testEmptyResponse() throws Exception {
        final String queryInput = "[\n" +
                "  {\n" +
                "    \"$match\": {\n" +
                "      \"val\": \"no_exists\"\n" +
                "    }\n" +
                "  },\n" +
                "  {\n" +
                "    \"$group\": {\n" +
                "      \"_id\": \"null\",\n" +
                "      \"doc_count\": {\n" +
                "        \"$sum\": 1\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "]";

        runner.setProperty(RunMongoAggregation.QUERY, queryInput);
        runner.enqueue("test");
        runner.run(1, true, true);

        runner.assertTransferCount(RunMongoAggregation.REL_ORIGINAL, 1);
        runner.assertTransferCount(RunMongoAggregation.REL_FAILURE, 0);
        runner.assertTransferCount(RunMongoAggregation.REL_RESULTS, 1);
    }
}
