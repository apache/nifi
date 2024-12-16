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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.mongodb.AbstractMongoProcessor.UpdateMethod;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PutMongoIT extends MongoWriteTestBase {
    @BeforeEach
    public void setup() {
        super.setup(PutMongo.class);
    }

    @Override
    @AfterEach
    public void teardown() {
        super.teardown();
    }

    private byte[] documentToByteArray(Document doc) {
        return doc.toJson().getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testValidators() throws Exception {
        TestRunner runner = init(PutMongo.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

        // missing uri, db, collection
        runner.enqueue(new byte[0]);
        runner.removeProperty(PutMongo.DATABASE_NAME);
        runner.removeProperty(PutMongo.COLLECTION_NAME);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(2, results.size());
        Iterator<ValidationResult> it = results.iterator();
        assertTrue(it.next().toString().contains("is invalid because Mongo Database Name is required"));
        assertTrue(it.next().toString().contains("is invalid because Mongo Collection Name is required"));

        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, COLLECTION_NAME);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");

        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());
    }

    @Test
    public void testQueryAndUpdateKey() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        runner.setProperty(PutMongo.UPDATE_QUERY, "{}");
        runner.assertNotValid();
    }

    @Test
    public void testNoQueryAndNoUpdateKey() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.removeProperty(PutMongo.UPDATE_QUERY);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "");
        runner.assertNotValid();
    }

    @Test
    public void testBlankUpdateKeyInUpdateMode() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "  ");
        runner.assertNotValid();
    }

    @Test
    public void testUpdateOneWithOperatorByQuery() throws Exception {
        TestRunner runner = init(PutMongo.class);
        Document document = new Document()
            .append("name", "John Smith")
            .append("department", "Engineering");
        collection.insertOne(document);
        Document updateBody = new Document(Map.of(
            "$set", Map.of(
                "email", "john.smith@test.com",
                "grade", "Sr. Principle Eng."
            ),
            "$inc", Map.of(
                "writes", 1
            )
        ));
        Map<String, String> attr = new HashMap<>();
        attr.put("mongo.update.query", document.toJson());
        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPDATE_QUERY, "${mongo.update.query}");
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody.toJson(), attr);
        updateOneTests(runner, document);
    }
    @Test
    public void testUpdateManyWithOperatorByQuery() throws Exception {
        TestRunner runner = init(PutMongo.class);
        Map<String, String> docKeys = Map.of(
            "name", "John Smith",
            "department", "Engineering"
        );

        collection.insertOne(new Document(docKeys));
        collection.insertOne(new Document(docKeys));
        Document updateBody = new Document(Map.of(
            "$set", Map.of(
                "email", "john.smith@test.com",
                "grade", "Sr. Principle Eng."
            ),
            "$inc", Map.of(
                "writes", 1
            )
        ));
        Document search = new Document(docKeys);
        Map<String, String> attr = new HashMap<>();
        attr.put("mongo.update.query", search.toJson());
        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.UPDATE_METHOD, UpdateMethod.UPDATE_MANY);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPDATE_QUERY, "${mongo.update.query}");
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody.toJson(), attr);
        updateManyTests(runner, search, 2);
    }

    @Test
    public void testUpdateOneBySimpleKey() throws Exception {
        TestRunner runner = init(PutMongo.class);
        Document document = new Document()
            .append("name", "John Smith")
            .append("department", "Engineering");
        collection.insertOne(document);

        Document updateBody = new Document(Map.of(
            "name", "John Smith",
            "$set", Map.of(
                "email", "john.smith@test.com",
                "grade", "Sr. Principle Eng."
            ),
            "$inc", Map.of(
                "writes", 1
            )
        ));
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "name");
        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody.toJson());
        updateOneTests(runner, document);
    }


    @Test
    public void testUpdateManyWithOperatorBySimpleKey() throws Exception {
        TestRunner runner = init(PutMongo.class);
        Map<String, String> docKeys = Map.of(
            "name", "John Smith",
            "department", "Engineering"
        );

        collection.insertOne(new Document(docKeys));
        collection.insertOne(new Document(docKeys));

        Document updateBody = new Document(Map.of(
            "name", "John Smith",
            "$set", Map.of(
                "email", "john.smith@test.com",
                "grade", "Sr. Principle Eng."
            ),
            "$inc", Map.of(
                "writes", 1
            )
        ));
        Document search = new Document(docKeys);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "name");
        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.UPDATE_METHOD, UpdateMethod.UPDATE_MANY);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody.toJson());
        updateManyTests(runner, search, 2);
    }

    @Test
    public void testUpdateWithFullDocByKeys() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "name,department");
        testUpdateFullDocument(runner);
    }

    @Test
    public void testUpdateWithFullDocByQuery() throws Exception {
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
        Document updateBody = new Document(Map.of(
            "name", "John Smith",
            "department", "Engineering",
            "contacts", Map.of(
                "phone", "555-555-5555",
                "email", "john.smith@test.com",
                "twitter", "@JohnSmith"
            )
        ));
        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_DOC);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody.toJson());
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertAttributeNotExists(PutMongo.ATTRIBUTE_UPSERT_ID);
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(1));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(1));

        MongoCursor<Document> cursor = collection.find(document).iterator();
        Document found = cursor.next();
        assertEquals(found.get("name"), document.get("name"));
        assertEquals(found.get("department"), document.get("department"));
        Document contacts = (Document) found.get("contacts");
        assertNotNull(contacts);
        assertEquals(contacts.get("twitter"), "@JohnSmith");
        assertEquals(contacts.get("email"), "john.smith@test.com");
        assertEquals(contacts.get("phone"), "555-555-5555");
        assertEquals(collection.countDocuments(document), 1);
    }

    @Test
    public void testUpdateOneByComplexKey() throws Exception {
        TestRunner runner = init(PutMongo.class);
        Document document = new Document()
                .append("name", "John Smith")
                .append("department", "Engineering")
                .append("contacts", new Document().append("email", "john.smith@test.com")
                .append("phone", "555-555-5555"));
        collection.insertOne(document);
        Document updateBody = new Document(Map.of(
            "contacts.phone", "555-555-5555",
            "contacts.email", "john.smith@test.com",
            "$set", Map.of(
                "contacts.twitter", "@JohnSmith"
            ),
            "$inc", Map.of(
                "writes", 1
            )
        ));
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "contacts.phone,contacts.email");
        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody.toJson());
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertAttributeNotExists(PutMongo.ATTRIBUTE_UPSERT_ID);
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(1));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(1));

        MongoCursor<Document> iterator = collection.find(new Document("name", "John Smith")).iterator();
        assertTrue(iterator.hasNext(), "Document did not come back.");
        Document val = iterator.next();
        Map contacts = (Map) val.get("contacts");
        assertNotNull(contacts);
        assertTrue(contacts.containsKey("twitter") && contacts.get("twitter").equals("@JohnSmith"));
        assertTrue(val.containsKey("writes") && val.get("writes").equals(1));
    }

    @Test
    public void testUpdateManyWithOperatorByComplexKey() throws Exception {
        TestRunner runner = init(PutMongo.class);
        Map<String, Object> data = Map.of("name", "John Smith",
            "department", "Engineering",
            "contacts", Map.of(
                "email", "john.smith@test.com",
                "phone", "555-555-5555"
                )
        );
        collection.insertOne(new Document(data));
        collection.insertOne(new Document(data));
        Document updateBody = new Document(Map.of(
            "contacts.phone", "555-555-5555",
            "contacts.email", "john.smith@test.com",
            "$set", Map.of(
                "contacts.twitter", "@JohnSmith"
            ),
            "$inc", Map.of(
                "writes", 1
            )
        ));
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "contacts.phone,contacts.email");
        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.UPDATE_METHOD, UpdateMethod.UPDATE_MANY);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setValidateExpressionUsage(true);
        runner.enqueue(updateBody.toJson());
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertAttributeNotExists(PutMongo.ATTRIBUTE_UPSERT_ID);
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(2));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(2));

        MongoCursor<Document> iterator = collection.find(new Document("name", "John Smith")).iterator();
        for (int i = 1; i <= 2; i++) {
            assertTrue(iterator.hasNext(), "Document %d did not come back.".formatted(i));
            Document val = iterator.next();
            Map contacts = (Map) val.get("contacts");
            assertNotNull(contacts, "Document %d's contacts null".formatted(i));
            assertTrue(contacts.containsKey("twitter") && contacts.get("twitter").equals("@JohnSmith"), "Document %d's twitter invalid".formatted(i));
            assertTrue(val.containsKey("writes") && val.get("writes").equals(1), "Document %d's writes invalid".formatted(i));

        }
    }

    private void updateOneTests(TestRunner runner, Document document) {
        updateManyTests(runner, document, 1);
    }

    private void updateManyTests(TestRunner runner, Document document, int updateCount) {
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);
        MongoCursor<Document> iterator = collection.find(document).iterator();
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertAttributeNotExists(PutMongo.ATTRIBUTE_UPSERT_ID);
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(updateCount));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(updateCount));
        for (int i = 1; i <= updateCount; i++) {
            assertTrue(iterator.hasNext(), "Document number %s did not come back.".formatted(i));
            Document val = iterator.next();
            assertTrue(val.containsKey("email") && val.get("email").equals("john.smith@test.com"));
            assertTrue(val.containsKey("grade") && val.get("grade").equals("Sr. Principle Eng."));
            assertTrue(val.containsKey("writes") && val.get("writes").equals(1));
        }
    }

    @Test
    public void testInsertOne() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        Document doc = DOCUMENTS.getFirst();
        byte[] bytes = documentToByteArray(doc);

        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertContentEquals(bytes);

        // verify 1 doc inserted into the collection
        assertEquals(1, collection.countDocuments());
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
        for (int i = 0; i < flowFiles.size(); i++) {
            flowFiles.get(i).assertContentEquals(DOCUMENTS.get(i).toJson());
        }

        // verify 3 docs inserted into the collection
        assertEquals(3, collection.countDocuments());
    }

    @Test
    public void testInsertWithDuplicateKey() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        // pre-insert one document
        collection.insertOne(DOCUMENTS.getFirst());

        for (Document doc : DOCUMENTS) {
            runner.enqueue(documentToByteArray(doc));
        }
        runner.run(3);

        // first doc failed, other 2 succeeded
        runner.assertTransferCount(PutMongo.REL_FAILURE, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_FAILURE).getFirst();
        out.assertContentEquals(documentToByteArray(DOCUMENTS.getFirst()));

        runner.assertTransferCount(PutMongo.REL_SUCCESS, 2);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS);
        for (int i = 0; i < flowFiles.size(); i++) {
            flowFiles.get(i).assertContentEquals(DOCUMENTS.get(i + 1).toJson());
        }

        // verify 2 docs inserted into the collection for a total of 3
        assertEquals(3, collection.countDocuments());
    }

    /**
     * Verifies that 'update' does not insert if 'upsert' if false.
     * @see #testUpsert()
     */
    @Test
    public void testUpdateDoesNotInsert() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        Document doc = DOCUMENTS.getFirst();
        byte[] bytes = documentToByteArray(doc);

        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertContentEquals(bytes);
        out.assertAttributeNotExists(PutMongo.ATTRIBUTE_UPSERT_ID);
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(0));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(0));

        // nothing was in collection, so nothing to update since upsert defaults to false
        assertEquals(0, collection.countDocuments());
    }

    /**
     * Verifies that 'update' does insert if 'upsert' is true.
     * @see #testUpdateDoesNotInsert()
     */
    @Test
    public void testUpsert() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        Document doc = DOCUMENTS.getFirst();
        byte[] bytes = documentToByteArray(doc);

        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPSERT, "true");
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertContentEquals(bytes);

        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPSERT_ID, doc.getString("_id"));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(0));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(0));

        // verify 1 doc inserted into the collection
        assertEquals(1, collection.countDocuments());
        assertEquals(doc, collection.find().first());
    }

    @Test
    public void testUpsertWithOid() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        byte[] bytes = documentToByteArray(oidDocument);

        runner.setProperty(PutMongo.MODE, "update");
        runner.setProperty(PutMongo.UPSERT, "true");
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertContentEquals(bytes);

        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPSERT_ID, oidDocument.getObjectId("_id").toString());
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(0));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(0));

        // verify 1 doc inserted into the collection
        assertEquals(1, collection.countDocuments());
        assertEquals(oidDocument, collection.find().first());
    }

    @Test
    public void testUpdate() throws Exception {
        TestRunner runner = init(PutMongo.class);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        Document doc = DOCUMENTS.getFirst();

        // pre-insert document
        collection.insertOne(doc);

        // modify the object
        doc.put("abc", "123");
        doc.put("xyz", "456");
        doc.remove("c");

        byte[] bytes = documentToByteArray(doc);

        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.enqueue(bytes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutMongo.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS).getFirst();
        out.assertContentEquals(bytes);
        out.assertAttributeNotExists(PutMongo.ATTRIBUTE_UPSERT_ID);
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(1));
        out.assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(1));
        assertEquals(1, collection.countDocuments());
        assertEquals(doc, collection.find().first());
    }

    @Test
    public void testUpsertWithOperators() throws Exception {
        TestRunner runner = init(PutMongo.class);
        Document upsert = new Document(Map.of(
            "_id", "Test",
            "$push", Map.of(
                "testArr", Map.of(
                    "msg", "Hi"
                )
            )
        ));

        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "_id");
        runner.setProperty(PutMongo.MODE, "update");
        runner.setProperty(PutMongo.UPSERT, "true");
        for (int x = 0; x < 3; x++) {
            runner.enqueue(upsert.toJson());
        }
        runner.run(3, true, true);
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 3);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(PutMongo.REL_SUCCESS);
        MockFlowFile upsertOutput = flowFilesForRelationship.removeFirst();
        upsertOutput.assertAttributeEquals(PutMongo.ATTRIBUTE_UPSERT_ID, "Test");

        // test next flow files for update attributes
        for (int i = 0; i < flowFilesForRelationship.size(); i++) {
            flowFilesForRelationship.get(i).assertAttributeNotExists(PutMongo.ATTRIBUTE_UPSERT_ID);
            flowFilesForRelationship.get(i).assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(1));
            flowFilesForRelationship.get(i).assertAttributeEquals(PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(1));
        }

        Document query = new Document("_id", "Test");
        Document result = collection.find(query).first();
        List array = (List) result.get("testArr");
        assertNotNull(array, "Array was empty");
        assertEquals(3, array.size(), "Wrong size");
        for (int index = 0; index < array.size(); index++) {
            Document doc = (Document) array.get(index);
            String msg = doc.getString("msg");
            assertNotNull(msg, "Msg was null");
            assertEquals(msg, "Hi", "Msg had wrong value");
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
    public void testNiFi_4759_Regressions() throws Exception {
        TestRunner runner = init(PutMongo.class);

        List<Document> upserts = List.of(
            new Document(Map.of(
                "_id", "12345",
                "$set", Map.of(
                    "msg", "Hello, world"
                )
            )),
            new Document(Map.of(
                "_id", "5a5617b9c1f5de6d8276e87d",
                "$set", Map.of(
                    "msg", "Hello, world"
                )
            )),
            new Document(Map.of(
                "updateKey", "12345",
                "$set", Map.of(
                    "msg", "Hello, world"
                )
            ))
        );

        String[] updateKeyProps = new String[] {"_id", "_id", "updateKey"};
        Object[] updateKeys = new Object[] {"12345", new ObjectId("5a5617b9c1f5de6d8276e87d"), "12345"};

        runner.setProperty(PutMongo.UPDATE_OPERATION_MODE, PutMongo.UPDATE_WITH_OPERATORS);
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPSERT, "true");

        final int LIMIT = 2;

        for (int index = 0; index < upserts.size(); index++) {
            Document upsert = upserts.get(index);
            runner.setProperty(PutMongo.UPDATE_QUERY_KEY, updateKeyProps[index]);
            for (int x = 0; x < LIMIT; x++) {
                runner.enqueue(upsert.toJson());
            }
            runner.run(LIMIT, true, true);
            runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
            runner.assertTransferCount(PutMongo.REL_SUCCESS, LIMIT);

            Document query = new Document(updateKeyProps[index], updateKeys[index]);
            Document result = collection.find(query).first();

            assertNotNull(result, "Result was null");
            assertEquals(1, collection.countDocuments(query), "Count was wrong");
            runner.clearTransferState();
        }
    }
}
