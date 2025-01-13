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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RunMongoAggregationIT extends AbstractMongoIT {

    private static final String DB_NAME   = String.format("agg_test-%s", Calendar.getInstance().getTimeInMillis());
    private static final String COLLECTION_NAME = "agg_test_data";
    private static final String AGG_ATTR = "mongo.aggregation.query";

    private TestRunner runner;
    private MongoClient mongoClient;
    private Map<String, Integer> mappings;
    private final Calendar now = Calendar.getInstance();
    private MongoDBControllerService clientService;

    @BeforeEach
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(RunMongoAggregation.class);
        runner.setEnvironmentVariableValue("uri", MONGO_CONTAINER.getConnectionString());
        runner.setEnvironmentVariableValue("db", DB_NAME);
        runner.setEnvironmentVariableValue("collection", COLLECTION_NAME);
        clientService = new MongoDBControllerService();
        runner.addControllerService("clientService", clientService);
        runner.setProperty(clientService, MongoDBControllerService.URI, MONGO_CONTAINER.getConnectionString());
        runner.setProperty(AbstractMongoProcessor.CLIENT_SERVICE, "clientService");
        runner.enableControllerService(clientService);
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");
        runner.setProperty(RunMongoAggregation.QUERY_ATTRIBUTE, AGG_ATTR);

        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());

        MongoCollection<Document> collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        String[] values = new String[] {"a", "b", "c"};
        mappings = new HashMap<>();

        for (int x = 0; x < values.length; x++) {
            for (int y = 0; y < x + 2; y++) {
                Document doc = new Document().append("val", values[x]).append("date", now.getTime());
                collection.insertOne(doc);
            }
            mappings.put(values[x], x + 2);
        }
    }

    @AfterEach
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
            assertNotNull("Missing query attribute", val);
            assertEquals(val, queryInput, "Value was wrong");
        }
    }

    @Test
    public void testExpressionLanguageSupport() throws Exception {
        runner.setEnvironmentVariableValue("fieldName", "$val");
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
    public void testInvalidQuery() {
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
        for (MockFlowFile mockFlowFile : flowFiles) {
            byte[] raw = runner.getContentAsByteArray(mockFlowFile);
            Map<String, List<String>> read = mapper.readValue(raw, Map.class);
            assertNotNull(read.get("myArray").get(1));
        }

        runner.clearTransferState();

        runner.setProperty(RunMongoAggregation.JSON_TYPE, RunMongoAggregation.JSON_EXTENDED);
        runner.enqueue("test");
        runner.run(1, true, true);

        flowFiles = runner.getFlowFilesForRelationship(RunMongoAggregation.REL_RESULTS);
        for (MockFlowFile mockFlowFile : flowFiles) {
            byte[] raw = runner.getContentAsByteArray(mockFlowFile);
            Map<String, List<Long>> read = mapper.readValue(raw, Map.class);
            assertEquals((long) read.get("myArray").get(1), now.getTimeInMillis());
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
            assertTrue(mappings.containsKey(read.get("_id")), "Value was not found");

            String queryAttr = mockFlowFile.getAttribute(AGG_ATTR);
            assertNotNull("Query attribute was null.", queryAttr);
            assertTrue(queryAttr.contains("$project"), "Missing $project");
            assertTrue(queryAttr.contains("$group"), "Missing $group");
        }
    }

    @Test
    public void testExtendedJsonSupport() throws Exception {
        String pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
        //Let's put this a week from now to make sure that we're not getting too close to
        //the creation date
        OffsetDateTime nowish = new Date(now.getTime().getTime() + (7 * 24 * 60 * 60 * 1000)).toInstant().atOffset(ZoneOffset.UTC);

        final String queryInput = "[\n" +
            "  {\n" +
            "    \"$match\": {\n" +
            "      \"date\": { \"$gte\": { \"$date\": \"2019-01-01T00:00:00Z\" }, \"$lte\": { \"$date\": \"" + dateTimeFormatter.format(nowish) + "\" } }\n" +
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
