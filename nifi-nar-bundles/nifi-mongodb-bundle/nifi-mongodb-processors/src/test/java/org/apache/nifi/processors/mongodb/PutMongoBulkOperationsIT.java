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

import org.apache.nifi.util.TestRunner;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PutMongoBulkOperationsIT extends MongoWriteTestBase {

    @BeforeEach
    public void setup() {
        super.setup(PutMongoBulkOperations.class);
    }

    @Override
    @AfterEach
    public void teardown() {
        super.teardown();
    }

    @Test
    public void testBulkWriteInsert() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);

        StringBuffer doc = new StringBuffer();
        doc.append("[");
        for (int i = 0; i < DOCUMENTS.size(); i++) {
            if (i > 0) {
                doc.append(", ");
            }
            doc.append("{\"insertOne\": {\"document\": ");
            doc.append(DOCUMENTS.get(i).toJson());
            doc.append("}}");
        }
        doc.append("]");
        runner.enqueue(doc.toString());
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        assertEquals(3, collection.countDocuments());
        Document doc1 = collection.find(new Document().append("_id", "doc_2")).first();
        assertNotNull(doc1);
        assertEquals(4, doc1.getInteger("c", 0));
    }

    @Test
    public void testBulkWriteUpdateOne() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);

        collection.insertMany(DOCUMENTS);

        runner.enqueue("[{\"updateOne\": {\"filter\": {\"_id\": {\"$in\": [\"doc_1\", \"doc_2\"]}}, \"update\": {\"$set\": {\"z\": 42}}}}]");
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        assertEquals(1, collection.countDocuments(new Document().append("z", 42)));
    }

    @Test
    public void testBulkWriteUpdateMany() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);

        collection.insertMany(DOCUMENTS);

        runner.enqueue("[{\"updateMany\": {\"filter\": {\"_id\": {\"$in\": [\"doc_1\", \"doc_2\"]}}, \"update\": {\"$set\": {\"z\": 42}}}}]");
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        assertEquals(2, collection.countDocuments(new Document().append("z", 42)));
    }

    @Test
    public void testBulkWriteReplaceOne() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);

        collection.insertMany(DOCUMENTS);

        runner.enqueue("[{\"replaceOne\": {\"filter\": {\"_id\": \"doc_1\"}, \"replacement\": {\"_id\": \"doc_1\", \"z\": 42}}}]");
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        assertEquals(1, collection.countDocuments(new Document().append("z", 42)));
        Document doc1 = collection.find(new Document().append("_id", "doc_1")).first();
        assertNotNull(doc1);
        assertEquals(42, doc1.getInteger("z", 0));
        assertNull(doc1.get("a"));
    }

    @Test
    public void testBulkWriteDeleteOne() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);

        collection.insertMany(DOCUMENTS);

        runner.enqueue("[{\"deleteOne\": {\"filter\": {\"_id\": {\"$in\": [\"doc_1\", \"doc_2\"]}}}}]");
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        assertEquals(2, collection.countDocuments());
        assertEquals(0, collection.countDocuments(new Document().append("z", 42)));
    }

    @Test
    public void testBulkWriteDeleteMany() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);

        collection.insertMany(DOCUMENTS);

        runner.enqueue("[{\"deleteMany\": {\"filter\": {\"_id\": {\"$in\": [\"doc_1\", \"doc_2\"]}}}}]");
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        assertEquals(1, collection.countDocuments());
        assertEquals(0, collection.countDocuments(new Document().append("z", 42)));
    }

    @Test
    public void testInvalid() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);

        runner.enqueue("[{\"whatever\": {\"filter\": {\"_id\": {\"$in\": [\"doc_1\", \"doc_2\"]}}}}]");
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 1);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 0);
    }

    @Test
    public void testBulkWriteOrderedAsIs() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);
        runner.setProperty(PutMongoBulkOperations.ORDERED, "true"); // default, still

        StringBuffer doc = new StringBuffer();
        doc.append("[");
        // inserting same ID twice fails w/in mongo, not before, so we can really test transactions and ordering
        doc.append("{\"insertOne\": {\"document\": {\"_id\": \"doc_1\"}}},{\"insertOne\": {\"document\": ");
        doc.append(DOCUMENTS.get(0).toJson());
        doc.append("}}]");
        runner.enqueue(doc.toString());
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 1);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 0);

        assertEquals(1, collection.countDocuments());
    }

    @Test
    public void testBulkWriteOrderedNoTransaction() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);
        runner.setProperty(PutMongoBulkOperations.ORDERED, "true"); // default, still

        StringBuffer doc = new StringBuffer();
        doc.append("[");
        doc.append("{\"insertOne\": {\"document\": ");
        doc.append(DOCUMENTS.get(0).toJson());
        // inserting same ID twice fails w/in mongo, not before, so we can really test transactions and ordering
        doc.append("}}, {\"insertOne\": {\"document\": {\"_id\": \"doc_1\"}}}]");
        runner.enqueue(doc.toString());
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 1);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 0);

        assertEquals(1, collection.countDocuments());
    }

    @Test
    public void testBulkWriteUnordered() throws Exception {
        final TestRunner runner = init(PutMongoBulkOperations.class);
        runner.setProperty(PutMongoBulkOperations.ORDERED, "false");

        StringBuffer doc = new StringBuffer();
        doc.append("[");
        // inserting same ID twice fails w/in mongo, not before, so we can really test transactions and ordering
        doc.append("{\"insertOne\": {\"document\": {\"_id\": \"doc_1\"}}},{\"insertOne\": {\"document\": ");
        doc.append(DOCUMENTS.get(0).toJson());
        doc.append("}}]");
        runner.enqueue(doc.toString());
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 1);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 0);

        assertEquals(1, collection.countDocuments());
    }

}
