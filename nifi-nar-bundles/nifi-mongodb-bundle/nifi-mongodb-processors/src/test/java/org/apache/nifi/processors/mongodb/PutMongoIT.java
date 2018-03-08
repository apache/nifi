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

import org.apache.nifi.components.ValidationResult;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PutMongoIT extends MongoWriteTestBase {
    @Before
    public void setup() {
        super.setup(PutMongo.class);
    }

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
        Assert.assertEquals(3, results.size());
        Iterator<ValidationResult> it = results.iterator();
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo URI is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Database Name is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Collection Name is required"));

        // invalid write concern
        runner.setProperty(AbstractMongoProcessor.URI, MONGO_URI);
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, DATABASE_NAME);
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, COLLECTION_NAME);
        runner.setProperty(PutMongo.WRITE_CONCERN, "xyz");
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
    public void testInsertOne() throws Exception {
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
        String upsert = "{\n" +
                "  \"_id\": \"Test\",\n" +
                "  \"$push\": {\n" +
                "     \"testArr\": { \"msg\": \"Hi\" }\n" +
                "  }\n" +
                "}";
        runner.setProperty(PutMongo.UPDATE_MODE, PutMongo.UPDATE_WITH_OPERATORS);
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
}
