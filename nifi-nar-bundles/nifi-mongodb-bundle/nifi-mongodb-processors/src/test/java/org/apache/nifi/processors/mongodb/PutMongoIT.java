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
package org.apache.nifi.processors.mongodb;

import com.mongodb.client.MongoCursor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PutMongoIT extends MongoWriteTestBase {
    @Before
    public void setup() {
        super.setup(PutMongo.class);
    }

    @Override
    @After
    public void teardown() {
        super.teardown();
    }

    private byte[] documentToByteArray(Document doc) {
        return doc.toJson().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testValidators() {
        TestRunner runner = TestRunners.newTestRunner(PutMongo.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

        // missing uri, db, collection
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(2, results.size());
        Iterator<ValidationResult> it = results.iterator();
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Database Name is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Collection Name is required"));

        // invalid write concern
        runner.setProperty(AbstractMongoProcessor.URI, MONGO_URI);
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, COLLECTION_NAME);
        runner.setProperty(PutMongo.WRITE_CONCERN, "xyz");
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.iterator().next().toString().matches("'Write Concern' .* is invalid because Given value not found in allowed set .*"));

        // valid write concern
        runner.setProperty(PutMongo.WRITE_CONCERN, PutMongo.WRITE_CONCERN_UNACKNOWLEDGED);
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testQueryAndUpdateKey() {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        runner.setProperty(PutMongo.UPDATE_QUERY, "{}");
        runner.assertNotValid();
    }

    @Test
    public void testNoQueryAndNoUpdateKey() {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.removeProperty(PutMongo.UPDATE_QUERY);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "");
        runner.assertNotValid();
    }

    @Test
    public void testBlankUpdateKey() {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "  ");
        runner.assertNotValid();
    }

    @Test
    public void testUpdateQuery() {
        TestRunner runner = init(PutMongo.class);
        Document document = new Document()
            .append("name", "John Smith")
            .append("department", "Engineering");
        collection.insertOne(document);
        String updateBody = "{\n" +
            "\t\"$set\": {\n" +
            "\t\t\"email\": \"john.smith@test.com\",\n" +
            "\t\t\"grade\": \"Sr. Principle Eng.\"\n" +
            "\t},\n" +
            "\t\"$inc\": {\n" +
            "\t\t\"writes\": 1\n" +
            "\t}\n" +
            "}";
        Map<String, String> attr = new HashMap<>();
        attr.put("mongo.update.query", document.toJson());
        runner.setProperty(PutMongo.UPDATE_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPDATE_QUERY, "${mongo.update.query}");
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody, attr);
        updateTests(runner, document);
    }

    @Test
    public void testUpdateBySimpleKey() {
        TestRunner runner = init(PutMongo.class);
        Document document = new Document()
            .append("name", "John Smith")
            .append("department", "Engineering");
        collection.insertOne(document);

        String updateBody = "{\n" +
            "\t\"name\": \"John Smith\",\n" +
            "\t\"$set\": {\n" +
            "\t\t\"email\": \"john.smith@test.com\",\n" +
            "\t\t\"grade\": \"Sr. Principle Eng.\"\n" +
            "\t},\n" +
            "\t\"$inc\": {\n" +
            "\t\t\"writes\": 1\n" +
            "\t}\n" +
            "}";
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "name");
        runner.setProperty(PutMongo.UPDATE_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody);
        updateTests(runner, document);
    }

    @Test
    public void testUpdateWithFullDocByKeys() {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "name,department");
        testUpdateFullDocument(runner);
    }

    @Test
    public void testUpdateWithFullDocByQuery() {
        TestRunner runner = init(PutMongo.class);
        String query = "{ \"name\": \"John Smith\"}";
        runner.setProperty(PutMongo.UPDATE_QUERY, query);
        testUpdateFullDocument(runner);
    }

    private void testUpdateFullDocument(TestRunner runner) {
        Document document = new Document()
                .append("name", "John Smith")
                .append("department", "Engineering");
        collection.insertOne(document);
        String updateBody = "{\n" +
                "\t\"name\": \"John Smith\",\n" +
                "\t\"department\": \"Engineering\",\n" +
                "\t\"contacts\": {\n" +
                "\t\t\"phone\": \"555-555-5555\",\n" +
                "\t\t\"email\": \"john.smith@test.com\",\n" +
                "\t\t\"twitter\": \"@JohnSmith\"\n" +
                "\t}\n" +
                "}";
        runner.setProperty(PutMongo.UPDATE_MODE, PutMongo.UPDATE_WITH_DOC);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody);
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        MongoCursor<Document> cursor = collection.find(document).iterator();
        Document found = cursor.next();
        Assert.assertEquals(found.get("name"), document.get("name"));
        Assert.assertEquals(found.get("department"), document.get("department"));
        Document contacts = (Document)found.get("contacts");
        Assert.assertNotNull(contacts);
        Assert.assertEquals(contacts.get("twitter"), "@JohnSmith");
        Assert.assertEquals(contacts.get("email"), "john.smith@test.com");
        Assert.assertEquals(contacts.get("phone"), "555-555-5555");
        Assert.assertEquals(collection.count(document), 1);
    }

    @Test
    public void testUpdateByComplexKey() {
        TestRunner runner = init(PutMongo.class);
        Document document = new Document()
                .append("name", "John Smith")
                .append("department", "Engineering")
                .append("contacts", new Document().append("email", "john.smith@test.com")
                .append("phone", "555-555-5555"));
        collection.insertOne(document);
        String updateBody = "{\n" +
                "\t\"contacts.phone\": \"555-555-5555\",\n" +
                "\t\"contacts.email\": \"john.smith@test.com\",\n" +
                "\t\"$set\": {\n" +
                "\t\t\"contacts.twitter\": \"@JohnSmith\"\n" +
                "\t},\n" +
                "\t\"$inc\": {\n" +
                "\t\t\"writes\": 1\n" +
                "\t}\n" +
                "}";
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "contacts.phone,contacts.email");
        runner.setProperty(PutMongo.UPDATE_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody);
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        MongoCursor<Document> iterator = collection.find(new Document("name", "John Smith")).iterator();
        Assert.assertTrue("Document did not come back.", iterator.hasNext());
        Document val = iterator.next();
        Map contacts = (Map)val.get("contacts");
        Assert.assertNotNull(contacts);
        Assert.assertTrue(contacts.containsKey("twitter") && contacts.get("twitter").equals("@JohnSmith"));
        Assert.assertTrue(val.containsKey("writes") && val.get("writes").equals(1));
    }

    private void updateTests(TestRunner runner, Document document) {
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        MongoCursor<Document> iterator = collection.find(document).iterator();
        Assert.assertTrue("Document did not come back.", iterator.hasNext());
        Document val = iterator.next();
        Assert.assertTrue(val.containsKey("email") && val.get("email").equals("john.smith@test.com"));
        Assert.assertTrue(val.containsKey("grade") && val.get("grade").equals("Sr. Principle Eng."));
        Assert.assertTrue(val.containsKey("writes") && val.get("writes").equals(1));
    }

    @Test
    public void testInsertOne() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        Document doc = DOCUMENTS.get(0);
        byte[] bytes = documentToByteArray(doc);

        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).get(0);
        out.assertContentEquals(bytes);

        // verify 1 doc inserted into the collection
        assertEquals(1, collection.count());
        assertEquals(doc, collection.find().first());
    }

    @Test
    public void testInsertMany() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        for (Document doc : DOCUMENTS) {
            runner.enqueue(documentToByteArray(doc));
        }
        runner.run(3);

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS);
        for (int i=0; i < flowFiles.size(); i++) {
            flowFiles.get(i).assertContentEquals(DOCUMENTS.get(i).toJson());
        }

        // verify 3 docs inserted into the collection
        assertEquals(3, collection.count());
    }

    @Test
    public void testInsertWithDuplicateKey() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        // pre-insert one document
        collection.insertOne(DOCUMENTS.get(0));

        for (Document doc : DOCUMENTS) {
            runner.enqueue(documentToByteArray(doc));
        }
        runner.run(3);

        // first doc failed, other 2 succeeded
        runner.assertTransferCount(PutMongo.REL_FAILURE, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_FAILURE).get(0);
        out.assertContentEquals(documentToByteArray(DOCUMENTS.get(0)));

        runner.assertTransferCount(PutMongo.REL_SUCCESS, 2);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS);
        for (int i=0; i < flowFiles.size(); i++) {
            flowFiles.get(i).assertContentEquals(DOCUMENTS.get(i+1).toJson());
        }

        // verify 2 docs inserted into the collection for a total of 3
        assertEquals(3, collection.count());
    }

    /**
     * Verifies that 'update' does not insert if 'upsert' if false.
     * @see #testUpsert()
     */
    @Test
    public void testUpdateDoesNotInsert() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        Document doc = DOCUMENTS.get(0);
        byte[] bytes = documentToByteArray(doc);

        runner.setProperty(PutMongo.MODE, "update");
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).get(0);
        out.assertContentEquals(bytes);

        // nothing was in collection, so nothing to update since upsert defaults to false
        assertEquals(0, collection.count());
    }

    /**
     * Verifies that 'update' does insert if 'upsert' is true.
     * @see #testUpdateDoesNotInsert()
     */
    @Test
    public void testUpsert() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        Document doc = DOCUMENTS.get(0);
        byte[] bytes = documentToByteArray(doc);

        runner.setProperty(PutMongo.MODE, "update");
        runner.setProperty(PutMongo.UPSERT, "true");
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).get(0);
        out.assertContentEquals(bytes);

        // verify 1 doc inserted into the collection
        assertEquals(1, collection.count());
        assertEquals(doc, collection.find().first());
    }

    @Test
    public void testUpdate() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        Document doc = DOCUMENTS.get(0);

        // pre-insert document
        collection.insertOne(doc);

        // modify the object
        doc.put("abc", "123");
        doc.put("xyz", "456");
        doc.remove("c");

        byte[] bytes = documentToByteArray(doc);

        runner.setProperty(PutMongo.MODE, "update");
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).get(0);
        out.assertContentEquals(bytes);

        assertEquals(1, collection.count());
        assertEquals(doc, collection.find().first());
    }

    @Test
    public void testUpsertWithOperators() throws Exception {
        TestRunner runner = init(PutMongo.class);
        String upsert = "{\n" +
                "  \"_id\": \"Test\",\n" +
                "  \"$push\": {\n" +
                "     \"testArr\": { \"msg\": \"Hi\" }\n" +
                "  }\n" +
                "}";
        runner.setProperty(PutMongo.UPDATE_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        runner.setProperty(PutMongo.MODE, "update");
        runner.setProperty(PutMongo.UPSERT, "true");
        for (int x = 0; x < 3; x++) {
            runner.enqueue(upsert.getBytes());
        }
        runner.run(3, true, true);
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 3);

        Document query = new Document("_id", "Test");
        Document result = collection.find(query).first();
        List array = (List)result.get("testArr");
        Assert.assertNotNull("Array was empty", array);
        Assert.assertEquals("Wrong size", array.size(), 3);
        for (int index = 0; index < array.size(); index++) {
            Document doc = (Document)array.get(index);
            String msg = doc.getString("msg");
            Assert.assertNotNull("Msg was null", msg);
            Assert.assertEquals("Msg had wrong value", msg, "Hi");
        }
    }

    /*
     * Start NIFI-4759 Regression Tests
     *
     * 2 issues with ID field:
     *
     * * Assumed _id is the update key, causing failures when the user configured a different one in the UI.
     * * Treated _id as a string even when it is an ObjectID sent from another processor as a string value.
     *
     * Expected behavior:
     *
     * * update key field should work no matter what (legal) value it is set to be.
     * * _ids that are ObjectID should become real ObjectIDs when added to Mongo.
     * * _ids that are arbitrary strings should be still go in as strings.
     *
     */
    @Test
    public void testNiFi_4759_Regressions() {
        TestRunner runner = init(PutMongo.class);
        String[] upserts = new String[]{
                "{ \"_id\": \"12345\", \"$set\": { \"msg\": \"Hello, world\" } }",
                "{ \"_id\": \"5a5617b9c1f5de6d8276e87d\", \"$set\": { \"msg\": \"Hello, world\" } }",
                "{ \"updateKey\": \"12345\", \"$set\": { \"msg\": \"Hello, world\" } }"
        };

        String[] updateKeyProps = new String[] { "_id", "_id", "updateKey" };
        Object[] updateKeys = new Object[] { "12345", new ObjectId("5a5617b9c1f5de6d8276e87d"), "12345" };
        int index = 0;

        runner.setProperty(PutMongo.UPDATE_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.MODE, "update");
        runner.setProperty(PutMongo.UPSERT, "true");

        final int LIMIT = 2;

        for (String upsert : upserts) {
            runner.setProperty(PutMongo.UPDATE_QUERY_KEY, updateKeyProps[index]);
            for (int x = 0; x < LIMIT; x++) {
                runner.enqueue(upsert);
            }
            runner.run(LIMIT, true, true);
            runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
            runner.assertTransferCount(PutMongo.REL_SUCCESS, LIMIT);

            Document query = new Document(updateKeyProps[index], updateKeys[index]);
            Document result = collection.find(query).first();
            Assert.assertNotNull("Result was null", result);
            Assert.assertEquals("Count was wrong", 1, collection.count(query));
            runner.clearTransferState();
            index++;
        }
    }

    @Test
    public void testClientService() throws Exception {
        MongoDBClientService clientService = new MongoDBControllerService();
        TestRunner runner = init(PutMongo.class);
        runner.addControllerService("clientService", clientService);
        runner.removeProperty(PutMongo.URI);
        runner.setProperty(clientService, MongoDBControllerService.URI, MONGO_URI);
        runner.setProperty(PutMongo.CLIENT_SERVICE, "clientService");
        runner.enableControllerService(clientService);
        runner.assertValid();

        runner.enqueue("{ \"msg\": \"Hello, world\" }");
        runner.run();
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
    }
}
