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

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.nifi.cdc.mongodb.event.EventMapper;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.concurrent.TimeUnit;

/**
 * Opens the change stream cursor for the watched scope. A stored resume token decides where the stream starts;
 * without one it starts at the configured point in time, or at the current moment.
 */
class StreamOpener {

    private final MongoDBClientService clientService;
    private final StreamOptions options;

    StreamOpener(final MongoDBClientService clientService, final StreamOptions options) {
        this.clientService = clientService;
        this.options = options;
    }

    MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> open(final String resumeTokenData, final BsonTimestamp startAtOperationTime) {
        ChangeStreamIterable<BsonDocument> stream = switch (options.scope()) {
            case COLLECTION -> clientService.getDatabase(options.databaseName())
                    .getCollection(options.collectionName(), BsonDocument.class)
                    .watch(options.pipeline());
            case DATABASE -> clientService.getDatabase(options.databaseName())
                    .watch(options.pipeline(), BsonDocument.class);
        };

        final BsonDocument resumeToken = EventMapper.resumeToken(resumeTokenData);
        if (resumeToken != null) {
            // startAfter continues from the token like resumeAfter, and unlike resumeAfter it also works when the
            // token belongs to an invalidate event, so the stream survives a dropped or renamed collection.
            stream = stream.startAfter(resumeToken);
        } else if (startAtOperationTime != null) {
            stream = stream.startAtOperationTime(startAtOperationTime);
        }

        return stream
                .fullDocument(options.fullDocument())
                .fullDocumentBeforeChange(options.fullDocumentBeforeChange())
                // Bounds how long tryNext() waits on an idle stream, so that a trigger returns promptly.
                .maxAwaitTime(options.maxAwaitTimeMillis(), TimeUnit.MILLISECONDS)
                .cursor();
    }
}
