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
package org.apache.nifi.cdc.mongodb.event;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;

/**
 * Builds change stream events for the tests by decoding the document a server sends with the driver codec, so the
 * tests work on the same shape as production.
 */
public final class ChangeEvents {

    private ChangeEvents() {
    }

    public static ChangeStreamDocument<BsonDocument> decode(final String eventJson) {
        final Codec<ChangeStreamDocument<BsonDocument>> codec =
                ChangeStreamDocument.createCodec(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
        return codec.decode(new BsonDocumentReader(BsonDocument.parse(eventJson)), DecoderContext.builder().build());
    }

    /**
     * The event the server sends when the watched collection is dropped or renamed. It carries no namespace and no
     * document key.
     */
    public static ChangeStreamDocument<BsonDocument> invalidate(final String resumeToken) {
        return decode("""
                {
                  "_id": {"_data": "%s"},
                  "operationType": "invalidate",
                  "clusterTime": {"$timestamp": {"t": 1700000000, "i": 2}},
                  "wallTime": {"$date": "2023-11-14T22:13:20Z"}
                }
                """.formatted(resumeToken));
    }

    public static ChangeStreamDocument<BsonDocument> insert(final String resumeToken) {
        return decode("""
                {
                  "_id": {"_data": "%s"},
                  "operationType": "insert",
                  "clusterTime": {"$timestamp": {"t": 1700000000, "i": 1}},
                  "wallTime": {"$date": "2023-11-14T22:13:20Z"},
                  "ns": {"db": "lab", "coll": "orders"},
                  "documentKey": {"_id": 1},
                  "fullDocument": {"_id": 1}
                }
                """.formatted(resumeToken));
    }
}
