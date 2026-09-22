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
package org.apache.nifi.cdc.mongodb.processors;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads the documents a collection already holds, in the order of their identifiers and in batches. Reading in
 * that order is what lets an interrupted snapshot carry on from the last identifier it committed instead of
 * starting over.
 */
class SnapshotReader {

    private static final String ID = "_id";

    private final MongoDBClientService clientService;
    private final String databaseName;
    private final String collectionName;
    private final int batchSize;

    SnapshotReader(final MongoDBClientService clientService, final String databaseName, final String collectionName, final int batchSize) {
        this.clientService = clientService;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.batchSize = batchSize;
    }

    /**
     * The time the server is at right now. The snapshot is taken as of this moment and the change stream carries on
     * from it, so that a change made while the snapshot runs is either already in the documents it reads or arrives
     * afterwards as an event. A change can be both, which is why a consumer has to tolerate a repeat.
     */
    BsonTimestamp currentClusterTime() {
        final Document reply = clientService.getDatabase(databaseName).runCommand(new Document("ping", 1));
        final BsonTimestamp operationTime = reply.get("operationTime", BsonTimestamp.class);
        if (operationTime == null) {
            throw new IllegalStateException("The server did not report an operation time. Change streams and the initial snapshot "
                    + "need a replica set or a sharded cluster.");
        }
        return operationTime;
    }

    List<BsonDocument> readBatch(final BsonValue afterId) {
        final Bson filter = afterId == null ? new BsonDocument() : Filters.gt(ID, afterId);
        return clientService.getDatabase(databaseName)
                .getCollection(collectionName, BsonDocument.class)
                .find(filter)
                .sort(Sorts.ascending(ID))
                .limit(batchSize)
                .into(new ArrayList<>());
    }

    int getBatchSize() {
        return batchSize;
    }

    String getDatabaseName() {
        return databaseName;
    }

    String getCollectionName() {
        return collectionName;
    }
}
