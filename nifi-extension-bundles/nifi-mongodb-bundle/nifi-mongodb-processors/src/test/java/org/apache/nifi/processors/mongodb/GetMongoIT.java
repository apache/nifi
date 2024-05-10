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
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetMongoIT extends AbstractMongoIT {
    private static final String DB_NAME = GetMongoIT.class.getSimpleName().toLowerCase();
    private static final String COLLECTION_NAME = "test";

    private static final List<Document> DOCUMENTS;
    private static final Calendar CAL;

    static {
        CAL = Calendar.getInstance();
        DOCUMENTS = Arrays.asList(
                new Document("_id", "doc_1").append("a", 1).append("b", 2).append("c", 3),
                new Document("_id", "doc_2").append("a", 1).append("b", 2).append("c", 4).append("date_field", CAL.getTime()),
                new Document("_id", "doc_3").append("a", 1).append("b", 3)
        );
    }

    private TestRunner runner;
    private MongoClient mongoClient;
    private MongoDBControllerService clientService;

    @BeforeEach
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(GetMongo.class);
        runner.setEnvironmentVariableValue("uri", MONGO_CONTAINER.getConnectionString());
        runner.setEnvironmentVariableValue("db", DB_NAME);
        runner.setEnvironmentVariableValue("collection", COLLECTION_NAME);
        clientService = new MongoDBControllerService();
        runner.addControllerService("clientService", clientService);
        runner.setProperty(clientService, MongoDBControllerService.URI, MONGO_CONTAINER.getConnectionString());
        runner.setProperty(GetMongo.CLIENT_SERVICE, "clientService");
        runner.enableControllerService(clientService);
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");
        runner.setProperty(GetMongo.USE_PRETTY_PRINTING, GetMongo.YES_PP);
        runner.setIncomingConnection(false);

        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());

        MongoCollection<Document> collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        collection.insertMany(DOCUMENTS);
    }

    @AfterEach
    public void teardown() {
        runner = null;

        mongoClient.getDatabase(DB_NAME).drop();
    }

    @Test
    public void testValidators() {

        TestRunner runner = TestRunners.newTestRunner(GetMongo.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

        // missing uri, db, collection
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(2, results.size());
        Iterator<ValidationResult> it = results.iterator();
        assertTrue(it.next().toString().contains("is invalid because Mongo Database Name is required"));
        assertTrue(it.next().toString().contains("is invalid because Mongo Collection Name is required"));

        // missing query - is ok
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, DB_NAME);
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, COLLECTION_NAME);
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());

        // invalid query
        runner.setProperty(GetMongo.QUERY, "{a: x,y,z}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        assertTrue(results.iterator().next().toString().contains("is invalid because"));

        // invalid projection
        runner.setEnvironmentVariableValue("projection", "{a: x,y,z}");
        runner.setProperty(GetMongo.QUERY, "{\"a\": 1}");
        runner.setProperty(GetMongo.PROJECTION, "{a: z}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        assertTrue(results.iterator().next().toString().contains("is invalid"));

        // invalid sort
        runner.removeProperty(GetMongo.PROJECTION);
        runner.setProperty(GetMongo.SORT, "{a: x,y,z}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        assertTrue(results.iterator().next().toString().contains("is invalid"));
    }

    @Test
    public void testCleanJson() throws Exception {
        runner.setEnvironmentVariableValue("query", "{\"_id\": \"doc_2\"}");
        runner.setProperty(GetMongo.QUERY, "${query}");
        runner.setProperty(GetMongo.JSON_TYPE, GetMongo.JSON_STANDARD);
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        byte[] raw = runner.getContentAsByteArray(flowFiles.get(0));
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> parsed = mapper.readValue(raw, Map.class);

        assertTrue(parsed.get("date_field").getClass() == String.class);
    }

    @Test
    public void testReadOneDocument() {
        runner.setEnvironmentVariableValue("query", "{a: 1, b: 3}");
        runner.setProperty(GetMongo.QUERY, "${query}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(2).toJson());
    }

    @Test
    public void testReadMultipleDocuments() {
        runner.setProperty(GetMongo.QUERY, "{\"a\": {\"$exists\": \"true\"}}");
        runner.run();
        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 3);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        for (int i=0; i < flowFiles.size(); i++) {
            flowFiles.get(i).assertContentEquals(DOCUMENTS.get(i).toJson());
        }
    }

    @Test
    public void testProjection() {
        runner.setProperty(GetMongo.QUERY, "{\"a\": 1, \"b\": 3}");
        runner.setProperty(GetMongo.PROJECTION, "{\"_id\": 0, \"a\": 1}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        Document expected = new Document("a", 1);
        flowFiles.get(0).assertContentEquals(expected.toJson());
    }

    @Test
    public void testSort() {
        runner.setEnvironmentVariableValue("sort", "{a: -1, b: -1, c: 1}");
        runner.setProperty(GetMongo.QUERY, "{\"a\": {\"$exists\": \"true\"}}");
        runner.setProperty(GetMongo.SORT, "${sort}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(2).toJson());
        flowFiles.get(1).assertContentEquals(DOCUMENTS.get(0).toJson());
        flowFiles.get(2).assertContentEquals(DOCUMENTS.get(1).toJson());
    }

    @Test
    public void testLimit() {
        runner.setProperty(GetMongo.QUERY, "{\"a\": {\"$exists\": \"true\"}}");
        runner.setProperty(GetMongo.LIMIT, "${limit}");
        runner.setEnvironmentVariableValue("limit", "1");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(0).toJson());
    }

    @Test
    public void testResultsPerFlowfile() {
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "${results.per.flowfile}");
        runner.setEnvironmentVariableValue("results.per.flowfile", "2");
        runner.enqueue("{}");
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 2);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        assertTrue(results.get(0).getSize() > 0, "Flowfile was empty");
        assertEquals(results.get(0).getAttribute(CoreAttributes.MIME_TYPE.key()), "application/json", "Wrong mime type");
    }

    @Test
    public void testBatchSize() {
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "2");
        runner.setProperty(GetMongo.BATCH_SIZE, "${batch.size}");
        runner.setEnvironmentVariableValue("batch.size", "1");
        runner.enqueue("{}");
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 2);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        assertTrue(results.get(0).getSize() > 0, "Flowfile was empty");
        assertEquals(results.get(0).getAttribute(CoreAttributes.MIME_TYPE.key()), "application/json", "Wrong mime type");
    }

    @Test
    public void testConfigurablePrettyPrint() {
        runner.setProperty(GetMongo.JSON_TYPE, GetMongo.JSON_STANDARD);
        runner.setProperty(GetMongo.LIMIT, "1");
        runner.enqueue("{}");
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        byte[] raw = runner.getContentAsByteArray(flowFiles.get(0));
        String json = new String(raw);
        assertTrue(json.contains("\n"), "JSON did not have new lines.");
        runner.clearTransferState();
        runner.setProperty(GetMongo.USE_PRETTY_PRINTING, GetMongo.NO_PP);
        runner.enqueue("{}");
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        raw = runner.getContentAsByteArray(flowFiles.get(0));
        json = new String(raw);
        assertFalse(json.contains("\n"), "New lines detected");
    }

    private void testQueryAttribute(String attr, String expected) {
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        for (MockFlowFile mff : flowFiles) {
            String val = mff.getAttribute(attr);
            assertNotNull(val, "Missing query attribute");
            assertEquals(expected, val, "Value was wrong");
        }
    }

    @Test
    public void testQueryAttribute() {
        /*
         * Test original behavior; Manually set query of {}, no input
         */
        final String attr = "query.attr";
        final String queryValue = "{}";
        runner.setProperty(GetMongo.QUERY, queryValue);
        runner.setProperty(GetMongo.QUERY_ATTRIBUTE, attr);
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 3);
        testQueryAttribute(attr, queryValue);

        runner.clearTransferState();

        /*
         * Test original behavior; No Input/Empty val = {}
         */
        runner.removeProperty(GetMongo.QUERY);
        runner.setIncomingConnection(false);
        runner.run();
        testQueryAttribute(attr, queryValue);

        runner.clearTransferState();

        /*
         * Input flowfile with {} as the query
         */

        runner.setIncomingConnection(true);
        runner.enqueue("{}");
        runner.run();
        testQueryAttribute(attr, queryValue);

        /*
         * Input flowfile with invalid query
         */

        runner.clearTransferState();
        runner.enqueue("invalid query");
        runner.run();

        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 0);
    }

    /*
     * Query read behavior tests
     */
    @Test
    public void testReadQueryFromBodyWithEL() {
        Map attributes = new HashMap();
        attributes.put("field", "c");
        attributes.put("value", "4");
        String query = "{ \"${field}\": { \"$gte\": ${value}}}";
        runner.setIncomingConnection(true);
        runner.setProperty(GetMongo.QUERY, query);
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "10");
        runner.enqueue("test", attributes);
        runner.run(1, true, true);

        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
    }

    @Test
    public void testReadQueryFromBodyNoEL() {
        String query = "{ \"c\": { \"$gte\": 4 }}";
        runner.setIncomingConnection(true);
        runner.removeProperty(GetMongo.QUERY);
        runner.enqueue(query);
        runner.run(1, true, true);

        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);

    }

    @Test
    public void testReadQueryFromQueryParamNoConnection() {
        String query = "{ \"c\": { \"$gte\": 4 }}";
        runner.setProperty(GetMongo.QUERY, query);
        runner.setIncomingConnection(false);
        runner.run(1, true, true);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);

    }

    @Test
    public void testReadQueryFromQueryParamWithConnection() {
        String query = "{ \"c\": { \"$gte\": ${value} }}";
        Map<String, String> attrs = new HashMap<>();
        attrs.put("value", "4");

        runner.setProperty(GetMongo.QUERY, query);
        runner.setIncomingConnection(true);
        runner.enqueue("test", attrs);
        runner.run(1, true, true);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
    }

    @Test
    public void testQueryParamMissingWithNoFlowfile() {
        Exception ex = null;

        try {
            runner.assertValid();
            runner.setIncomingConnection(false);
            runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "1");
            runner.run(1, true, true);
        } catch (Exception pe) {
            ex = pe;
        }

        assertNull(ex, "An exception was thrown!");
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 3);
    }

    @Test
    public void testReadCharsetWithEL() {
        String query = "{ \"c\": { \"$gte\": 4 }}";
        Map<String, String> attrs = new HashMap<>();
        attrs.put("charset", "UTF-8");

        runner.setProperty(GetMongo.CHARSET, "${charset}");
        runner.setProperty(GetMongo.BATCH_SIZE, "2");
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "2");

        runner.setIncomingConnection(true);
        runner.enqueue(query, attrs);
        runner.run(1, true, true);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
    }

    @Test
    public void testKeepOriginalAttributes() {
        final String query = "{ \"c\": { \"$gte\": 4 }}";
        final Map<String, String> attributesMap = new HashMap<>(1);
        attributesMap.put("property.1", "value-1");

        runner.setIncomingConnection(true);
        runner.removeProperty(GetMongo.QUERY);
        runner.enqueue(query, attributesMap);

        runner.run(1, true, true);

        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS).get(0);
        assertTrue(flowFile.getAttributes().containsKey("property.1"));
        flowFile.assertAttributeEquals("property.1", "value-1");
    }
    /*
     * End query read behavior tests
     */

    @Test
    public void testDBAttributes() {
        runner.enqueue("{}");
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 3);
        List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        for (MockFlowFile ff : ffs) {
            String db = ff.getAttribute(GetMongo.DB_NAME);
            String col = ff.getAttribute(GetMongo.COL_NAME);
            assertNotNull(db);
            assertNotNull(col);
            assertEquals(DB_NAME, db);
            assertEquals(COLLECTION_NAME, col);
        }
    }

    @Test
    public void testDateFormat() throws Exception {
        runner.setIncomingConnection(true);
        runner.setProperty(GetMongo.JSON_TYPE, GetMongo.JSON_STANDARD);
        runner.setProperty(GetMongo.DATE_FORMAT, "yyyy-MM-dd");
        runner.enqueue("{ \"_id\": \"doc_2\" }");
        runner.run();

        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS).get(0);
        byte[] content = runner.getContentAsByteArray(ff);
        String json = new String(content);
        Map<String, Object> result = new ObjectMapper().readValue(json, Map.class);

        Pattern format = Pattern.compile("([\\d]{4})-([\\d]{2})-([\\d]{2})");

        assertTrue(result.containsKey("date_field"));
        assertTrue(format.matcher((String) result.get("date_field")).matches());
    }

    @Test
    public void testClientService() throws Exception {
        MongoDBClientService clientService = new MongoDBControllerService();
        runner.addControllerService("clientService", clientService);
        runner.setProperty(clientService, MongoDBControllerService.URI, MONGO_CONTAINER.getConnectionString());
        runner.setProperty(GetMongo.CLIENT_SERVICE, "clientService");
        runner.enableControllerService(clientService);
        runner.assertValid();

        runner.enqueue("{}");
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 3);
    }

    @Test
    public void testInvalidQueryGoesToFailure() {
        //Test environment properties mode
        runner.setEnvironmentVariableValue("badattribute", "<<?>>");
        runner.setProperty(GetMongo.QUERY, "${badattribute}");
        runner.run();
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);

        runner.clearTransferState();

        //Test that it doesn't blow up with env properties values holding a proper value
        runner.setEnvironmentVariableValue("badattribute", "{}");
        runner.run();
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 3);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);

        runner.clearTransferState();

        //Test a bad flowfile attribute
        runner.setIncomingConnection(true);
        runner.setProperty(GetMongo.QUERY, "${badfromff}");
        runner.enqueue("<<?>>", new HashMap<String, String>() {{
            put("badfromff", "{\"prop\":}");
        }});
        runner.run();
        runner.assertTransferCount(GetMongo.REL_FAILURE, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);

        runner.clearTransferState();

        //Test for regression on a good query from a flowfile attribute
        runner.setIncomingConnection(true);
        runner.setProperty(GetMongo.QUERY, "${badfromff}");
        runner.enqueue("<<?>>", new HashMap<String, String>() {{
            put("badfromff", "{}");
        }});
        runner.run();
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 3);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);

        runner.clearTransferState();
        runner.removeProperty(GetMongo.QUERY);

        //Test for regression against the body w/out any EL involved.
        runner.enqueue("<<?>>");
        runner.run();
        runner.assertTransferCount(GetMongo.REL_FAILURE, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);
    }
}
