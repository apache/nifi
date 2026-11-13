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

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.bson.BsonDocument;

import java.util.List;

/**
 * Everything the watch call needs that does not change while the processor runs. {@code collectionName} is null
 * for database scope. Where the stream starts is decided when the cursor is opened, because an initial snapshot
 * only learns its handover point while it runs.
 */
record StreamOptions(
        WatchScope scope,
        String databaseName,
        String collectionName,
        List<BsonDocument> pipeline,
        FullDocument fullDocument,
        FullDocumentBeforeChange fullDocumentBeforeChange,
        long maxAwaitTimeMillis) {

    /**
     * Names what the stored resume token belongs to. A token from one namespace is meaningless for another, so the
     * processor refuses to continue when this changes.
     */
    String source() {
        return scope == WatchScope.COLLECTION
                ? String.format("collection:%s.%s", databaseName, collectionName)
                : String.format("database:%s", databaseName);
    }
}
