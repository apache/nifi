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

import org.apache.nifi.util.TestRunner;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeleteMongoIT extends MongoWriteTestBase {
    @BeforeEach
    public void setup() {
        super.setup(DeleteMongo.class);
        collection.insertMany(DOCUMENTS);
    }

    @AfterEach
    public void teardown() {
        super.teardown();
    }

    private void testOne(TestRunner runner, String query, Map<String, String> attrs) {
        runner.enqueue(query, attrs);
        runner.run(1, true);
        runner.assertTransferCount(DeleteMongo.REL_FAILURE, 0);
        runner.assertTransferCount(DeleteMongo.REL_SUCCESS, 1);

        assertEquals(0, collection.countDocuments(Document.parse(query)),
                "Found a document that should have been deleted.");
    }

    @Test
    public void testDeleteOne() throws Exception {
        TestRunner runner = init(DeleteMongo.class);
        String query = "{ \"_id\": \"doc_1\" }";
        runner.setProperty(DeleteMongo.DELETE_MODE, DeleteMongo.DELETE_ONE);
        testOne(runner, query, new HashMap<>());
        Map<String, String> attrs = new HashMap<>();
        attrs.put("mongodb.delete.mode", "one");
        runner.setProperty(DeleteMongo.DELETE_MODE, DeleteMongo.DELETE_ATTR);
        query = "{ \"_id\": \"doc_2\" }";
        runner.clearTransferState();
        testOne(runner, query, attrs);
    }

    private void manyTest(TestRunner runner, String query, Map<String, String> attrs) {
        runner.enqueue(query, attrs);
        runner.run(1, true);
        runner.assertTransferCount(DeleteMongo.REL_FAILURE, 0);
        runner.assertTransferCount(DeleteMongo.REL_SUCCESS, 1);

        assertEquals(0, collection.countDocuments(Document.parse(query)), "Found a document that should have been deleted.");
        assertEquals(1, collection.countDocuments(Document.parse("{}")), "One document should have been left.");
    }

    @Test
    public void testDeleteMany() throws Exception {
        TestRunner runner = init(DeleteMongo.class);
        String query = "{\n" +
                "\t\"_id\": {\n" +
                "\t\t\"$in\": [\"doc_1\", \"doc_2\"]\n" +
                "\t}\n" +
                "}";
        runner.setProperty(DeleteMongo.DELETE_MODE, DeleteMongo.DELETE_MANY);
        manyTest(runner, query, new HashMap<>());

        runner.setProperty(DeleteMongo.DELETE_MODE, DeleteMongo.DELETE_ATTR);
        Map<String, String> attrs = new HashMap<>();
        attrs.put("mongodb.delete.mode", "many");
        collection.drop();
        collection.insertMany(DOCUMENTS);
        runner.clearTransferState();
        manyTest(runner, query, attrs);
    }

    @Test
    public void testFailOnNoDeleteOptions() throws Exception {
        TestRunner runner = init(DeleteMongo.class);
        String query = "{ \"_id\": \"doc_4\"} ";
        runner.enqueue(query);
        runner.run(1, true);
        runner.assertTransferCount(DeleteMongo.REL_FAILURE, 1);
        runner.assertTransferCount(DeleteMongo.REL_SUCCESS, 0);

        assertEquals(3, collection.countDocuments(Document.parse("{}")), "A document was deleted");

        runner.setProperty(DeleteMongo.FAIL_ON_NO_DELETE, DeleteMongo.NO_FAIL);
        runner.clearTransferState();
        runner.enqueue(query);
        runner.run(1, true, true);


        runner.assertTransferCount(DeleteMongo.REL_FAILURE, 0);
        runner.assertTransferCount(DeleteMongo.REL_SUCCESS, 1);

        assertEquals(3, collection.countDocuments(Document.parse("{}")), "A document was deleted");
    }
}
