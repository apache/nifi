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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

public class MongoWriteTestBase {
    protected static final String MONGO_URI = "mongodb://localhost";
    protected static final String COLLECTION_NAME = "test";
    protected String DATABASE_NAME;

    protected static final List<Document> DOCUMENTS = Arrays.asList(
            new Document("_id", "doc_1").append("a", 1).append("b", 2).append("c", 3),
            new Document("_id", "doc_2").append("a", 1).append("b", 2).append("c", 4),
            new Document("_id", "doc_3").append("a", 1).append("b", 3)
    );

    protected MongoClient mongoClient;
    protected MongoCollection<Document> collection;

    public void setup(Class processor) {
        DATABASE_NAME = processor.getSimpleName().toLowerCase();
        mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));
        collection = mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME);
    }

    public TestRunner init(Class processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setVariable("uri", MONGO_URI);
        runner.setVariable("db", DATABASE_NAME);
        runner.setVariable("collection", COLLECTION_NAME);
        runner.setProperty(AbstractMongoProcessor.URI, "${uri}");
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");
        return runner;
    }

    public void teardown() {
        mongoClient.getDatabase(DATABASE_NAME).drop();
    }
}
