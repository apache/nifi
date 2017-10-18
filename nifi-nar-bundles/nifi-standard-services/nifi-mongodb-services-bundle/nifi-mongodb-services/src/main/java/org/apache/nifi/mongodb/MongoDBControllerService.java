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

package org.apache.nifi.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Tags({"mongo", "mongodb", "service"})
@CapabilityDescription(
    "Provides a controller service that wraps most of the functionality of the MongoDB driver."
)
public class MongoDBControllerService extends AbstractMongoDBControllerService implements MongoDBClientService {
    private MongoDatabase db;
    private MongoCollection<Document> col;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, InterruptedException {
        this.createClient(context);
        this.db = this.mongoClient.getDatabase(context.getProperty(MongoDBControllerService.DATABASE_NAME).getValue());
        this.col = this.db.getCollection(context.getProperty(MongoDBControllerService.COLLECTION_NAME).getValue());
    }

    @OnDisabled
    public void onDisable() {
        this.mongoClient.close();
    }

    @Override
    public long count(Document query) {
        return this.col.count(query);
    }

    @Override
    public void delete(Document query) {
        this.col.deleteMany(query);
    }

    @Override
    public boolean exists(Document query) {
        return this.col.count(query) > 0;
    }

    public Document findOne(Document query) {
        MongoCursor<Document> cursor  = this.col.find(query).limit(1).iterator();
        Document retVal = cursor.next();
        cursor.close();

        return retVal;
    }

    @Override
    public List<Document> findMany(Document query) {
        return findMany(query, null, -1);
    }

    @Override
    public List<Document> findMany(Document query, int limit) {
        return findMany(query, null, limit);
    }

    @Override
    public List<Document> findMany(Document query, Document sort, int limit) {
        FindIterable<Document> fi = this.col.find(query);
        if (limit > 0) {
            fi = fi.limit(limit);
        }
        if (sort != null) {
            fi = fi.sort(sort);
        }
        MongoCursor<Document> cursor = fi.iterator();
        List<Document> retVal = new ArrayList<>();
        while (cursor.hasNext()) {
            retVal.add(cursor.next());
        }
        cursor.close();

        return retVal;
    }

    @Override
    public void insert(Document doc) {
        this.col.insertOne(doc);
    }

    @Override
    public void insert(List<Document> docs) {
        this.col.insertMany(docs);
    }

    @Override
    public void update(Document query, Document update, boolean multiple) {
        if (multiple) {
            this.col.updateMany(query, update);
        } else {
            this.col.updateOne(query, update);
        }
    }

    @Override
    public void update(Document query, Document update) {
        update(query, update, true);
    }

    @Override
    public void updateOne(Document query, Document update) {
        this.update(query, update, false);
    }

    @Override
    public void upsert(Document query, Document update) {
        this.col.updateOne(query, update, new UpdateOptions().upsert(true));
    }

    @Override
    public void dropDatabase() {
        this.db.drop();
        this.col = null;
    }

    @Override
    public void dropCollection() {
        this.col.drop();
        this.col = null;
    }
}
