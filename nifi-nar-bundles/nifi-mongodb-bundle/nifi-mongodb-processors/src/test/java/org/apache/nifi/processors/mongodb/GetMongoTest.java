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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;

@Ignore("Integration tests that cause failures in some environments. Require that they be run from Maven to run the embedded mongo maven plugin. Maven Plugin also fails in my CentOS 7 environment.")
public class GetMongoTest {
    private static final String MONGO_URI = "mongodb://localhost";
    private static final String DB_NAME = GetMongoTest.class.getSimpleName().toLowerCase();
    private static final String COLLECTION_NAME = "test";

    private static final List<Document> DOCUMENTS;
    private static final Calendar CAL;

    static {
        CAL = Calendar.getInstance();
        DOCUMENTS = Lists.newArrayList(
            new Document("_id", "doc_1").append("a", 1).append("b", 2).append("c", 3),
            new Document("_id", "doc_2").append("a", 1).append("b", 2).append("c", 4).append("date_field", CAL.getTime()),
            new Document("_id", "doc_3").append("a", 1).append("b", 3)
        );
    }

    private TestRunner runner;
    private MongoClient mongoClient;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(GetMongo.class);
        runner.setVariable("uri", MONGO_URI);
        runner.setVariable("db", DB_NAME);
        runner.setVariable("collection", COLLECTION_NAME);
        runner.setProperty(AbstractMongoProcessor.URI, "${uri}");
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");

        mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));

        MongoCollection<Document> collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        collection.insertMany(DOCUMENTS);
    }

    @After
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
        Assert.assertEquals(3, results.size());
        Iterator<ValidationResult> it = results.iterator();
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo URI is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Database Name is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Collection Name is required"));

        // missing query - is ok
        runner.setProperty(AbstractMongoProcessor.URI, MONGO_URI);
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, DB_NAME);
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, COLLECTION_NAME);
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());

        // invalid query
        runner.setProperty(GetMongo.QUERY, "{a: x,y,z}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.iterator().next().toString().contains("is invalid because"));

        // invalid projection
        runner.setVariable("projection", "{a: x,y,z}");
        runner.setProperty(GetMongo.QUERY, "{a: 1}");
        runner.setProperty(GetMongo.PROJECTION, "${projection}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.iterator().next().toString().matches("'Projection' .* is invalid because org.bson.json.JsonParseException"));

        // invalid sort
        runner.removeProperty(GetMongo.PROJECTION);
        runner.setProperty(GetMongo.SORT, "{a: x,y,z}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.iterator().next().toString().matches("'Sort' .* is invalid because org.bson.json.JsonParseException"));
    }

    @Test
    public void testCleanJson() throws Exception {
        runner.setVariable("query", "{\"_id\": \"doc_2\"}");
        runner.setProperty(GetMongo.QUERY, "${query}");
        runner.setProperty(GetMongo.JSON_TYPE, GetMongo.JSON_STANDARD);
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        byte[] raw = runner.getContentAsByteArray(flowFiles.get(0));
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> parsed = mapper.readValue(raw, Map.class);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        Assert.assertTrue(parsed.get("date_field").getClass() == String.class);
        Assert.assertTrue(((String)parsed.get("date_field")).startsWith(format.format(CAL.getTime())));
    }

    @Test
    public void testReadOneDocument() throws Exception {
        runner.setVariable("query", "{a: 1, b: 3}");
        runner.setProperty(GetMongo.QUERY, "${query}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(2).toJson());
    }

    @Test
    public void testReadMultipleDocuments() throws Exception {
        runner.setProperty(GetMongo.QUERY, "{a: {$exists: true}}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        for (int i=0; i < flowFiles.size(); i++) {
            flowFiles.get(i).assertContentEquals(DOCUMENTS.get(i).toJson());
        }
    }

    @Test
    public void testProjection() throws Exception {
        runner.setProperty(GetMongo.QUERY, "{a: 1, b: 3}");
        runner.setProperty(GetMongo.PROJECTION, "{_id: 0, a: 1}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        Document expected = new Document("a", 1);
        flowFiles.get(0).assertContentEquals(expected.toJson());
    }

    @Test
    public void testSort() throws Exception {
        runner.setVariable("sort", "{a: -1, b: -1, c: 1}");
        runner.setProperty(GetMongo.QUERY, "{a: {$exists: true}}");
        runner.setProperty(GetMongo.SORT, "${sort}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(2).toJson());
        flowFiles.get(1).assertContentEquals(DOCUMENTS.get(0).toJson());
        flowFiles.get(2).assertContentEquals(DOCUMENTS.get(1).toJson());
    }

    @Test
    public void testLimit() throws Exception {
        runner.setProperty(GetMongo.QUERY, "{a: {$exists: true}}");
        runner.setProperty(GetMongo.LIMIT, "1");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(0).toJson());
    }

    @Test
    public void testResultsPerFlowfile() throws Exception {
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "2");
        runner.run();
        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 2);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        Assert.assertTrue("Flowfile was empty", results.get(0).getSize() > 0);
        Assert.assertEquals("Wrong mime type", results.get(0).getAttribute(CoreAttributes.MIME_TYPE.key()), "application/json");
    }
}
