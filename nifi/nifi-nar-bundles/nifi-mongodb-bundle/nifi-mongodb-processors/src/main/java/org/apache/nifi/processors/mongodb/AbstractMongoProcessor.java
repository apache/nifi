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

import java.io.IOException;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public abstract class AbstractMongoProcessor extends AbstractProcessor {
    protected static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
        .name("Mongo URI")
        .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    protected static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("Mongo Database Name")
        .description("The name of the database to use")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    protected static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
        .name("Mongo Collection Name")
        .description("The name of the collection to use")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected MongoClient mongoClient;

    @OnScheduled
    public final void createClient(ProcessContext context) throws IOException {
        if (mongoClient != null) {
            closeClient();
        }

        getLogger().info("Creating MongoClient");

        try {
            final String uri = context.getProperty(URI).getValue();
            mongoClient = new MongoClient(new MongoClientURI(uri));
        } catch (Exception e) {
            getLogger().error("Failed to schedule PutMongo due to {}", new Object[] { e }, e);
            throw e;
        }
    }

    @OnStopped
    public final void closeClient() {
        if (mongoClient != null) {
            getLogger().info("Closing MongoClient");
            mongoClient.close();
            mongoClient = null;
        }
    }

    protected MongoDatabase getDatabase(final ProcessContext context) {
        final String databaseName = context.getProperty(DATABASE_NAME).getValue();
        return mongoClient.getDatabase(databaseName);
    }

    protected MongoCollection<Document> getCollection(final ProcessContext context) {
        final String collectionName = context.getProperty(COLLECTION_NAME).getValue();
        return getDatabase(context).getCollection(collectionName);
    }
}
