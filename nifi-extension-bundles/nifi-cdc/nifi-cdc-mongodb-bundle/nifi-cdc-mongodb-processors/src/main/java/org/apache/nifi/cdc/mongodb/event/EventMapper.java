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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.json.JsonWriterSettings;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a change stream event into a Record. Documents keep their BSON types by being written as Extended JSON
 * strings rather than being mapped to record fields, so a collection without a fixed shape stays readable.
 */
public class EventMapper {

    public static final String FIELD_OPERATION = "operation";
    public static final String FIELD_DATABASE = "database";
    public static final String FIELD_COLLECTION = "collection";
    public static final String FIELD_DOCUMENT_KEY = "document_key";
    public static final String FIELD_FULL_DOCUMENT = "full_document";
    public static final String FIELD_FULL_DOCUMENT_BEFORE_CHANGE = "full_document_before_change";
    public static final String FIELD_UPDATED_FIELDS = "updated_fields";
    public static final String FIELD_REMOVED_FIELDS = "removed_fields";
    public static final String FIELD_CLUSTER_TIME = "cluster_time";
    public static final String FIELD_WALL_TIME = "wall_time";
    public static final String FIELD_TXN_NUMBER = "txn_number";
    public static final String FIELD_RESUME_TOKEN = "resume_token";

    /** The operation of a record that comes from the initial snapshot rather than from a change. */
    public static final String OPERATION_READ = "read";

    private static final String RESUME_TOKEN_DATA = "_data";
    private static final String ID = "_id";

    private static final RecordSchema EVENT_SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField(FIELD_OPERATION, RecordFieldType.STRING.getDataType(), false),
            new RecordField(FIELD_DATABASE, RecordFieldType.STRING.getDataType()),
            new RecordField(FIELD_COLLECTION, RecordFieldType.STRING.getDataType()),
            new RecordField(FIELD_DOCUMENT_KEY, RecordFieldType.STRING.getDataType()),
            new RecordField(FIELD_FULL_DOCUMENT, RecordFieldType.STRING.getDataType()),
            new RecordField(FIELD_FULL_DOCUMENT_BEFORE_CHANGE, RecordFieldType.STRING.getDataType()),
            new RecordField(FIELD_UPDATED_FIELDS, RecordFieldType.STRING.getDataType()),
            new RecordField(FIELD_REMOVED_FIELDS, RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
            new RecordField(FIELD_CLUSTER_TIME, RecordFieldType.LONG.getDataType(), false),
            new RecordField(FIELD_WALL_TIME, RecordFieldType.TIMESTAMP.getDataType()),
            new RecordField(FIELD_TXN_NUMBER, RecordFieldType.LONG.getDataType()),
            // Null for the records of the initial snapshot: they are documents that were already there, not changes.
            new RecordField(FIELD_RESUME_TOKEN, RecordFieldType.STRING.getDataType())));

    private final JsonWriterSettings jsonWriterSettings;

    public EventMapper(final JsonWriterSettings jsonWriterSettings) {
        this.jsonWriterSettings = jsonWriterSettings;
    }

    public RecordSchema getEventSchema() {
        return EVENT_SCHEMA;
    }

    public Record map(final ChangeStreamDocument<BsonDocument> event) {
        final MongoNamespace namespace = event.getNamespace();
        final UpdateDescription updateDescription = event.getUpdateDescription();

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(FIELD_OPERATION, event.getOperationTypeString());
        values.put(FIELD_DATABASE, namespace == null ? event.getDatabaseName() : namespace.getDatabaseName());
        values.put(FIELD_COLLECTION, namespace == null ? null : namespace.getCollectionName());
        values.put(FIELD_DOCUMENT_KEY, toJson(event.getDocumentKey()));
        values.put(FIELD_FULL_DOCUMENT, toJson(event.getFullDocument()));
        values.put(FIELD_FULL_DOCUMENT_BEFORE_CHANGE, toJson(event.getFullDocumentBeforeChange()));
        values.put(FIELD_UPDATED_FIELDS, updateDescription == null ? null : toJson(updateDescription.getUpdatedFields()));
        values.put(FIELD_REMOVED_FIELDS, removedFields(updateDescription));
        values.put(FIELD_CLUSTER_TIME, clusterTime(event.getClusterTime()));
        values.put(FIELD_WALL_TIME, wallTime(event.getWallTime()));
        values.put(FIELD_TXN_NUMBER, txnNumber(event.getTxnNumber()));
        values.put(FIELD_RESUME_TOKEN, resumeTokenData(event.getResumeToken()));

        return new MapRecord(EVENT_SCHEMA, values);
    }

    /**
     * A document read by the initial snapshot. It is not a change, so it carries no resume token and no wall clock
     * time; its cluster time is the moment the snapshot is consistent with, which is where the change stream then
     * carries on.
     */
    public Record mapSnapshotDocument(final BsonDocument document, final String databaseName, final String collectionName,
                                      final BsonTimestamp snapshotTime) {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(FIELD_OPERATION, OPERATION_READ);
        values.put(FIELD_DATABASE, databaseName);
        values.put(FIELD_COLLECTION, collectionName);
        values.put(FIELD_DOCUMENT_KEY, toJson(documentKey(document)));
        values.put(FIELD_FULL_DOCUMENT, toJson(document));
        values.put(FIELD_FULL_DOCUMENT_BEFORE_CHANGE, null);
        values.put(FIELD_UPDATED_FIELDS, null);
        values.put(FIELD_REMOVED_FIELDS, null);
        values.put(FIELD_CLUSTER_TIME, clusterTime(snapshotTime));
        values.put(FIELD_WALL_TIME, null);
        values.put(FIELD_TXN_NUMBER, null);
        values.put(FIELD_RESUME_TOKEN, null);

        return new MapRecord(EVENT_SCHEMA, values);
    }

    public static BsonDocument documentKey(final BsonDocument document) {
        return new BsonDocument(ID, document.get(ID));
    }

    /**
     * The seconds and the increment of a BSON timestamp packed into one long, the same order the server compares
     * them in, so that sorting the field sorts the events.
     */
    public static long clusterTime(final BsonTimestamp clusterTime) {
        return clusterTime == null ? 0L : clusterTime.getValue();
    }

    /**
     * A resume token is a single string wrapped in a document. Only the string is carried in the record and in
     * processor state; {@link #resumeToken(String)} wraps it again.
     */
    public static String resumeTokenData(final BsonDocument resumeToken) {
        if (resumeToken == null || !resumeToken.containsKey(RESUME_TOKEN_DATA)) {
            return null;
        }
        return resumeToken.getString(RESUME_TOKEN_DATA).getValue();
    }

    public static BsonDocument resumeToken(final String data) {
        return data == null ? null : new BsonDocument(RESUME_TOKEN_DATA, new BsonString(data));
    }

    private String toJson(final BsonDocument document) {
        return document == null ? null : document.toJson(jsonWriterSettings);
    }

    private static Object[] removedFields(final UpdateDescription updateDescription) {
        if (updateDescription == null || updateDescription.getRemovedFields() == null) {
            return null;
        }
        return updateDescription.getRemovedFields().toArray(new String[0]);
    }

    private static Timestamp wallTime(final BsonDateTime wallTime) {
        return wallTime == null ? null : new Timestamp(wallTime.getValue());
    }

    private static Long txnNumber(final BsonInt64 txnNumber) {
        return txnNumber == null ? null : txnNumber.getValue();
    }
}
