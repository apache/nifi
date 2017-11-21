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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
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
public class PutMongoTest {
    private static final String MONGO_URI = "mongodb://localhost";
    private static final String DATABASE_NAME = PutMongoTest.class.getSimpleName().toLowerCase();
    private static final String COLLECTION_NAME = "test";

    private static final List<Document> DOCUMENTS = Lists.newArrayList(
        new Document("_id", "doc_1").append("a", 1).append("b", 2).append("c", 3),
        new Document("_id", "doc_2").append("a", 1).append("b", 2).append("c", 4),
        new Document("_id", "doc_3").append("a", 1).append("b", 3)
        );

    private TestRunner runner;
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(PutMongo.class);
        runner.setVariable("uri", MONGO_URI);
        runner.setVariable("db", DATABASE_NAME);
        runner.setVariable("collection", COLLECTION_NAME);
        runner.setProperty(AbstractMongoProcessor.URI, "${uri}");
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");

        mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));

        collection = mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME);
    }

    @After
    public void teardown() {
        runner = null;

        mongoClient.getDatabase(DATABASE_NAME).drop();
    }

    private byte[] documentToByteArray(Document doc) {
        return doc.toJson().getBytes(UTF_8);
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
}
