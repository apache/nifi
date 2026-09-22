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

import org.apache.nifi.serialization.record.Record;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The events are the documents a change stream delivers, decoded with the driver codec, so the test reads the
 * same shape the server sends.
 */
class EventMapperTest {

    private static final String RESUME_TOKEN = "8265E2C0B0000000012B0429296E1404";

    private final EventMapper mapper = new EventMapper(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());

    @Test
    void insertIsMappedWithTheFullDocument() {
        final Record record = map("""
                {
                  "_id": {"_data": "%s"},
                  "operationType": "insert",
                  "clusterTime": {"$timestamp": {"t": 1700000000, "i": 3}},
                  "wallTime": {"$date": "2023-11-14T22:13:20Z"},
                  "ns": {"db": "lab", "coll": "orders"},
                  "documentKey": {"_id": 7},
                  "fullDocument": {"_id": 7, "total": 42, "note": "first"}
                }
                """.formatted(RESUME_TOKEN));

        assertEquals("insert", record.getValue(EventMapper.FIELD_OPERATION));
        assertEquals("lab", record.getValue(EventMapper.FIELD_DATABASE));
        assertEquals("orders", record.getValue(EventMapper.FIELD_COLLECTION));
        assertEquals("{\"_id\": 7}", record.getValue(EventMapper.FIELD_DOCUMENT_KEY));
        assertEquals("{\"_id\": 7, \"total\": 42, \"note\": \"first\"}", record.getValue(EventMapper.FIELD_FULL_DOCUMENT));
        assertNull(record.getValue(EventMapper.FIELD_UPDATED_FIELDS));
        assertNull(record.getValue(EventMapper.FIELD_REMOVED_FIELDS));
        assertNull(record.getValue(EventMapper.FIELD_TXN_NUMBER));
        assertNull(record.getValue(EventMapper.FIELD_FULL_DOCUMENT_BEFORE_CHANGE));
        assertEquals(new Timestamp(1700000000000L), record.getValue(EventMapper.FIELD_WALL_TIME));
        assertEquals(RESUME_TOKEN, record.getValue(EventMapper.FIELD_RESUME_TOKEN));
    }

    @Test
    void updateCarriesTheChangedAndRemovedFields() {
        final Record record = map("""
                {
                  "_id": {"_data": "%s"},
                  "operationType": "update",
                  "clusterTime": {"$timestamp": {"t": 1700000005, "i": 1}},
                  "wallTime": {"$date": "2023-11-14T22:13:25Z"},
                  "ns": {"db": "lab", "coll": "orders"},
                  "documentKey": {"_id": 7},
                  "updateDescription": {"updatedFields": {"total": 43}, "removedFields": ["note", "draft"]},
                  "txnNumber": {"$numberLong": "12"}
                }
                """.formatted(RESUME_TOKEN));

        assertEquals("update", record.getValue(EventMapper.FIELD_OPERATION));
        assertEquals("{\"total\": 43}", record.getValue(EventMapper.FIELD_UPDATED_FIELDS));
        assertArrayEquals(new Object[]{"note", "draft"}, record.getAsArray(EventMapper.FIELD_REMOVED_FIELDS));
        assertNull(record.getValue(EventMapper.FIELD_FULL_DOCUMENT));
        assertEquals(12L, record.getValue(EventMapper.FIELD_TXN_NUMBER));
    }

    @Test
    void replaceCarriesTheNewDocument() {
        final Record record = map("""
                {
                  "_id": {"_data": "%s"},
                  "operationType": "replace",
                  "clusterTime": {"$timestamp": {"t": 1700000006, "i": 1}},
                  "ns": {"db": "lab", "coll": "orders"},
                  "documentKey": {"_id": 7},
                  "fullDocument": {"_id": 7, "total": 99}
                }
                """.formatted(RESUME_TOKEN));

        assertEquals("replace", record.getValue(EventMapper.FIELD_OPERATION));
        assertEquals("{\"_id\": 7, \"total\": 99}", record.getValue(EventMapper.FIELD_FULL_DOCUMENT));
        assertNull(record.getValue(EventMapper.FIELD_WALL_TIME));
    }

    @Test
    void deleteCarriesOnlyTheDocumentKey() {
        final Record record = map("""
                {
                  "_id": {"_data": "%s"},
                  "operationType": "delete",
                  "clusterTime": {"$timestamp": {"t": 1700000007, "i": 1}},
                  "ns": {"db": "lab", "coll": "orders"},
                  "documentKey": {"_id": 7}
                }
                """.formatted(RESUME_TOKEN));

        assertEquals("delete", record.getValue(EventMapper.FIELD_OPERATION));
        assertEquals("{\"_id\": 7}", record.getValue(EventMapper.FIELD_DOCUMENT_KEY));
        assertNull(record.getValue(EventMapper.FIELD_FULL_DOCUMENT));
    }

    @Test
    void invalidateHasNoNamespaceAndNoDocumentKey() {
        final Record record = map("""
                {
                  "_id": {"_data": "%s"},
                  "operationType": "invalidate",
                  "clusterTime": {"$timestamp": {"t": 1700000008, "i": 1}}
                }
                """.formatted(RESUME_TOKEN));

        assertEquals("invalidate", record.getValue(EventMapper.FIELD_OPERATION));
        assertNull(record.getValue(EventMapper.FIELD_DATABASE));
        assertNull(record.getValue(EventMapper.FIELD_COLLECTION));
        assertNull(record.getValue(EventMapper.FIELD_DOCUMENT_KEY));
        assertEquals(RESUME_TOKEN, record.getValue(EventMapper.FIELD_RESUME_TOKEN));
    }

    @Test
    void preImageIsMappedWhenTheServerSendsIt() {
        final Record record = map("""
                {
                  "_id": {"_data": "%s"},
                  "operationType": "update",
                  "clusterTime": {"$timestamp": {"t": 1700000009, "i": 1}},
                  "ns": {"db": "lab", "coll": "orders"},
                  "documentKey": {"_id": 7},
                  "updateDescription": {"updatedFields": {"total": 50}, "removedFields": []},
                  "fullDocumentBeforeChange": {"_id": 7, "total": 43}
                }
                """.formatted(RESUME_TOKEN));

        assertEquals("{\"_id\": 7, \"total\": 43}", record.getValue(EventMapper.FIELD_FULL_DOCUMENT_BEFORE_CHANGE));
        assertArrayEquals(new Object[0], record.getAsArray(EventMapper.FIELD_REMOVED_FIELDS));
    }

    /**
     * Canonical mode writes every BSON type with its name, so a consumer can turn the string back into the exact
     * document. Relaxed mode drops that for readability.
     */
    @Test
    void canonicalModeKeepsTheBsonTypes() {
        final String event = """
                {
                  "_id": {"_data": "%s"},
                  "operationType": "insert",
                  "clusterTime": {"$timestamp": {"t": 1700000000, "i": 1}},
                  "ns": {"db": "lab", "coll": "orders"},
                  "documentKey": {"_id": 7},
                  "fullDocument": {"_id": 7, "total": 42}
                }
                """.formatted(RESUME_TOKEN);

        final EventMapper canonical = new EventMapper(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
        assertEquals("{\"_id\": {\"$numberInt\": \"7\"}, \"total\": {\"$numberInt\": \"42\"}}",
                canonical.map(ChangeEvents.decode(event)).getValue(EventMapper.FIELD_FULL_DOCUMENT));

        assertEquals("{\"_id\": 7, \"total\": 42}", map(event).getValue(EventMapper.FIELD_FULL_DOCUMENT));
    }

    @Test
    void clusterTimePacksSecondsAndIncrementSoThatItSorts() {
        assertEquals((1700000000L << 32) | 3L, EventMapper.clusterTime(new BsonTimestamp(1700000000, 3)));
        assertEquals(0L, EventMapper.clusterTime(null));

        final long earlier = EventMapper.clusterTime(new BsonTimestamp(1700000000, 9));
        final long later = EventMapper.clusterTime(new BsonTimestamp(1700000001, 1));
        assertTrue(earlier < later);
    }

    @Test
    void resumeTokenSurvivesTheRoundTripThroughState() {
        assertEquals(RESUME_TOKEN, EventMapper.resumeTokenData(EventMapper.resumeToken(RESUME_TOKEN)));
        assertNull(EventMapper.resumeToken(null));
        assertNull(EventMapper.resumeTokenData(null));
        assertNull(EventMapper.resumeTokenData(BsonDocument.parse("{\"other\": 1}")));
    }

    private Record map(final String eventJson) {
        return mapper.map(ChangeEvents.decode(eventJson));
    }

}
