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

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.cdc.mongodb.event.EventMapper;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@TriggerSerially
@PrimaryNodeOnly
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"mongodb", "cdc", "change stream", "replication", "event"})
@CapabilityDescription("Retrieves Change Data Capture (CDC) events from MongoDB using Change Streams. The processor reads insert, update, "
        + "replace, delete and invalidate events for one collection or for every collection of a database, and writes them as records "
        + "using the configured Record Writer, one FlowFile per batch. Each record has the fields operation, database, collection, "
        + "document_key, full_document, full_document_before_change, updated_fields, removed_fields, cluster_time, wall_time, txn_number "
        + "and resume_token. Documents are written as Extended JSON strings. An aggregation pipeline can be given, and the server "
        + "applies it before sending the events, so events that do not match never travel. The resume token is stored in the same "
        + "transaction as the FlowFiles of a batch, so a failure replays events but never skips them. The processor never writes to the "
        + "source database; it needs only the changeStream and find privileges on the watched scope. MongoDB 6.0 or later, running as a "
        + "replica set or a sharded cluster, is required. Without stored state the stream starts where Start Position says, by default "
        + "at the moment the processor is started, so changes made before that are not captured; Initial Snapshot writes the documents "
        + "the collection already holds first, as records with the operation 'read', and then carries on with the changes made from the "
        + "moment the snapshot started. An invalidate event, which the server "
        + "sends when the watched collection is dropped or renamed, is written like any other event and the stream continues after it. "
        + "When the server is unreachable the processor waits longer after every failed attempt, up to a minute, and continues from the "
        + "last committed position once the server answers again.")
@Stateful(scopes = Scope.CLUSTER, description = "The resume token of the last change event written to a FlowFile is stored so that the "
        + "processor resumes from the same position after a restart or a change of the primary node, together with the scope the token "
        + "belongs to. A token is meaningless for another scope, database or collection, so the processor refuses to start when those "
        + "change and asks for the state to be cleared. While an initial snapshot runs, the moment it is consistent with and the "
        + "identifier of the last document it wrote are stored as well, so that a processor stopped in the middle of a snapshot carries "
        + "on from there instead of reading the collection again.")
@WritesAttributes({
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_DATABASE, description = "Database the events belong to"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_COLLECTION, description = "Collection the events belong to"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_FIRST_RESUME_TOKEN, description = "Resume token of the first change event in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, description = "Resume token of the last change event in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_LAG_MILLIS,
                description = "Milliseconds between the wall clock time of the last change event in the FlowFile and the time the FlowFile was written"),
        @WritesAttribute(attribute = CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, description = "Number of records written to the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type reported by the Record Writer")
})
public class CaptureChangeMongoDB extends AbstractProcessor implements VerifiableProcessor {

    public static final String ATTRIBUTE_DATABASE = "mongodb.database";
    public static final String ATTRIBUTE_COLLECTION = "mongodb.collection";
    public static final String ATTRIBUTE_FIRST_RESUME_TOKEN = "cdc.first.resume.token";
    public static final String ATTRIBUTE_LAST_RESUME_TOKEN = "cdc.last.resume.token";
    public static final String ATTRIBUTE_LAG_MILLIS = "cdc.lag.millis";
    public static final String ATTRIBUTE_RECORD_COUNT = "record.count";

    private static final int MINIMUM_MAJOR_VERSION = 6;
    private static final String SHARDED_CLUSTER_MESSAGE = "isdbgrid";
    private static final String INVALIDATE_OPERATION = "invalidate";
    private static final String ID_FIELD = "_id";

    /** Canonical Extended JSON, so that an identifier kept in state comes back as exactly the same BSON value. */
    private static final JsonWriterSettings STATE_JSON_SETTINGS = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    private static final int HISTORY_LOST_CODE = 286;
    private static final String HISTORY_LOST_CODE_NAME = "ChangeStreamHistoryLost";
    private static final int CHANGE_STREAM_FATAL_CODE = 280;
    private static final String RESUME_TOKEN_NOT_FOUND = "resume token was not found";

    private static final long INITIAL_BACKOFF_MILLIS = 1_000L;
    private static final long MAXIMUM_BACKOFF_MILLIS = 60_000L;
    private static final long REPEATED_FAILURE_LOG_INTERVAL_MILLIS = 60_000L;

    private static final List<String> REQUIRED_ACTIONS = List.of("changeStream", "find");
    private static final List<String> WRITE_ACTIONS =
            List.of("insert", "update", "remove", "dropCollection", "dropDatabase", "createCollection", "collMod", "renameCollectionSameDB");

    /** The stages MongoDB accepts in the pipeline of a change stream. */
    static final List<String> ALLOWED_STAGES =
            List.of("$match", "$project", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith");

    static final AllowableValue START_POSITION_NOW = new AllowableValue("now", "Now",
            "Start with the changes made from the moment the processor is started. Changes made before that are not captured.");
    static final AllowableValue START_POSITION_TIMESTAMP = new AllowableValue("timestamp", "Timestamp",
            "Start with the changes made from the given point in time, as far back as the oplog of the server reaches.");
    static final AllowableValue START_POSITION_INITIAL_SNAPSHOT = new AllowableValue("initial-snapshot", "Initial Snapshot",
            "Write the documents the collection already holds first, as records with the operation 'read', and then carry on with the "
                    + "changes made from the moment the snapshot started. A change made while the snapshot runs can appear twice, once "
                    + "in the snapshot and once as an event, so the consumer has to tolerate a repeat. Collection scope only.");

    // The values are the ones the server understands, so that the driver enum can be built straight from them.
    static final AllowableValue FULL_DOCUMENT_DEFAULT = new AllowableValue("default", "Default",
            "Update events carry only the changed fields.");
    static final AllowableValue FULL_DOCUMENT_UPDATE_LOOKUP = new AllowableValue("updateLookup", "Update Lookup",
            "Update events also carry the document as it is when the event is read, which is not necessarily how it was right after "
                    + "the change. Costs one read per event on the server.");
    static final AllowableValue FULL_DOCUMENT_WHEN_AVAILABLE = new AllowableValue("whenAvailable", "When Available",
            "Update events carry the document as it was right after the change when the collection keeps post-images, and nothing "
                    + "when it does not.");
    static final AllowableValue FULL_DOCUMENT_REQUIRED = new AllowableValue("required", "Required",
            "Like When Available, but the server reports an error instead of sending an event without the document.");

    static final AllowableValue BEFORE_CHANGE_OFF = new AllowableValue("off", "Off",
            "The version before the change is never sent.");
    static final AllowableValue BEFORE_CHANGE_WHEN_AVAILABLE = new AllowableValue("whenAvailable", "When Available",
            "The version before the change is sent when the collection keeps pre-images, and left out when it does not.");
    static final AllowableValue BEFORE_CHANGE_REQUIRED = new AllowableValue("required", "Required",
            "Like When Available, but the server reports an error instead of sending an event without the earlier version.");

    static final AllowableValue EXTENDED_JSON_RELAXED = new AllowableValue("relaxed", "Relaxed",
            "Readable JSON: numbers and strings look like JSON numbers and strings.");
    static final AllowableValue EXTENDED_JSON_CANONICAL = new AllowableValue("canonical", "Canonical",
            "Every BSON type is written with its type, for example {\"$numberInt\": \"7\"}, so nothing is lost.");

    static final AllowableValue HISTORY_LOST_FAIL = new AllowableValue("fail", "Fail",
            "Report an error and keep the stored resume token. The flow stops making progress until an administrator decides what to do, "
                    + "so that no change is passed over without anyone noticing.");
    static final AllowableValue HISTORY_LOST_RESTART_FROM_NOW = new AllowableValue("restart-from-now", "Restart From Now",
            "Discard the stored resume token and continue with the changes made from now on. The changes between the stored position and "
                    + "now are lost; a warning records how the stream was restarted.");

    static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Client Service")
            .description("The MongoDB client service that provides the connection to the replica set or the sharded cluster. "
                    + "Credentials and connection timeouts are configured there.")
            .identifiesControllerService(MongoDBClientService.class)
            .required(true)
            .build();

    static final PropertyDescriptor WATCH_SCOPE = new PropertyDescriptor.Builder()
            .name("Watch Scope")
            .description("Whether to capture the changes of one collection or of every collection of a database. Changing this, the "
                    + "database or the collection makes the stored resume token meaningless, so the processor state has to be cleared "
                    + "before the processor starts again.")
            .required(true)
            .allowableValues(WatchScope.class)
            .defaultValue(WatchScope.COLLECTION)
            .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .description("Name of the database to watch.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("Collection Name")
            .description("Name of the collection to watch.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(WATCH_SCOPE, WatchScope.COLLECTION)
            .build();

    static final PropertyDescriptor PIPELINE = new PropertyDescriptor.Builder()
            .name("Pipeline")
            .description("An aggregation pipeline the server applies to the change events before sending them, as a JSON array, for "
                    + "example [{\"$match\": {\"operationType\": \"insert\"}}]. The work happens on the server, so events that do not "
                    + "match never travel. MongoDB allows only these stages on a change stream: " + String.join(", ", ALLOWED_STAGES)
                    + ". The pipeline sees the change event, not the document, so a condition on a field of the document reads like "
                    + "{\"fullDocument.status\": \"open\"}.")
            .required(false)
            .addValidator(CaptureChangeMongoDB::validatePipeline)
            .build();

    static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder()
            .name("Start Position")
            .description("Where the stream starts when there is no stored resume token. Once a token is stored this is ignored, and the "
                    + "processor always continues from the stored position.")
            .required(true)
            .allowableValues(START_POSITION_NOW, START_POSITION_TIMESTAMP, START_POSITION_INITIAL_SNAPSHOT)
            .defaultValue(START_POSITION_NOW.getValue())
            .build();

    static final PropertyDescriptor SNAPSHOT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Snapshot Batch Size")
            .description("How many documents the initial snapshot reads and writes at a time. Each batch is committed on its own, so a "
                    + "processor that is stopped in the middle of a snapshot carries on from the last committed document.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .dependsOn(START_POSITION, START_POSITION_INITIAL_SNAPSHOT)
            .build();

    static final PropertyDescriptor START_TIMESTAMP = new PropertyDescriptor.Builder()
            .name("Start Timestamp")
            .description("The point in time the stream starts at, in seconds since the epoch. The server can only start there while its "
                    + "oplog still reaches back that far.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .dependsOn(START_POSITION, START_POSITION_TIMESTAMP)
            .build();

    static final PropertyDescriptor FULL_DOCUMENT = new PropertyDescriptor.Builder()
            .name("Full Document")
            .description("Whether the current version of the changed document is sent with an update event. Insert and replace events "
                    + "always carry the document; delete events never do.")
            .required(true)
            .allowableValues(FULL_DOCUMENT_DEFAULT, FULL_DOCUMENT_UPDATE_LOOKUP, FULL_DOCUMENT_WHEN_AVAILABLE, FULL_DOCUMENT_REQUIRED)
            .defaultValue(FULL_DOCUMENT_DEFAULT.getValue())
            .build();

    static final PropertyDescriptor FULL_DOCUMENT_BEFORE_CHANGE = new PropertyDescriptor.Builder()
            .name("Full Document Before Change")
            .description("Whether the version of the document before the change is sent with update, replace and delete events. The "
                    + "server only has it for collections where pre-images are turned on, which is a change a database administrator "
                    + "makes: db.runCommand({collMod: \"<collection>\", changeStreamPreAndPostImages: {enabled: true}}).")
            .required(true)
            .allowableValues(BEFORE_CHANGE_OFF, BEFORE_CHANGE_WHEN_AVAILABLE, BEFORE_CHANGE_REQUIRED)
            .defaultValue(BEFORE_CHANGE_OFF.getValue())
            .build();

    static final PropertyDescriptor EXTENDED_JSON_MODE = new PropertyDescriptor.Builder()
            .name("Extended JSON Mode")
            .description("How the documents are written into the record fields. Relaxed is easier to read and turns numbers into JSON "
                    + "numbers; Canonical keeps every BSON type exactly, so the value can be converted back without loss.")
            .required(true)
            .allowableValues(EXTENDED_JSON_RELAXED, EXTENDED_JSON_CANONICAL)
            .defaultValue(EXTENDED_JSON_RELAXED.getValue())
            .build();

    static final PropertyDescriptor MAX_EVENTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Max Events Per FlowFile")
            .description("The number of change events after which the FlowFile is completed and transferred, even if the stream has more "
                    + "events available.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_BATCH_DURATION = new PropertyDescriptor.Builder()
            .name("Max Batch Duration")
            .description("The time after which the FlowFile is completed and transferred, even if fewer events than Max Events Per "
                    + "FlowFile have been read. Keeps the delay of an event bounded while the collection changes slowly.")
            .required(true)
            .defaultValue("5 s")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_AWAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Await Time")
            .description("How long the server holds the request open while the stream has no new event. A trigger that finds nothing "
                    + "returns after this time, so this is the longest a single trigger blocks.")
            .required(true)
            .defaultValue("1 s")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor ON_HISTORY_LOST = new PropertyDescriptor.Builder()
            .name("On History Lost")
            .description("What to do when the stored resume token is no longer in the oplog, which happens when the processor was stopped, "
                    + "or could not keep up, for longer than the oplog of the server covers. The changes from that period cannot be read "
                    + "any more.")
            .required(true)
            .allowableValues(HISTORY_LOST_FAIL, HISTORY_LOST_RESTART_FROM_NOW)
            .defaultValue(HISTORY_LOST_FAIL.getValue())
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("The Record Writer used to write the change events to a FlowFile.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing the change events read from the change stream")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLIENT_SERVICE,
            WATCH_SCOPE,
            DATABASE_NAME,
            COLLECTION_NAME,
            PIPELINE,
            START_POSITION,
            START_TIMESTAMP,
            SNAPSHOT_BATCH_SIZE,
            FULL_DOCUMENT,
            FULL_DOCUMENT_BEFORE_CHANGE,
            EXTENDED_JSON_MODE,
            MAX_EVENTS_PER_FLOWFILE,
            MAX_BATCH_DURATION,
            MAX_AWAIT_TIME,
            ON_HISTORY_LOST,
            RECORD_WRITER);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile StreamOpener streamOpener;
    private volatile EventMapper eventMapper;
    private volatile RecordSetWriterFactory writerFactory;
    private volatile Map<String, String> namespaceAttributes;
    private volatile String transitUri;
    private volatile int maxEventsPerFlowFile;
    private volatile long maxBatchDurationMillis;

    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor;

    /**
     * The position the stream is reopened from after a failure: the last committed resume token, or, before the
     * first commit, the position the cursor was opened at, so that reopening after a failed batch does not step
     * over the events of that batch.
     */
    private volatile String resumeFrom;

    /** The resume token currently held in processor state. */
    private volatile String committedToken;

    private volatile String onHistoryLost;
    private volatile String streamSource;

    private volatile BsonTimestamp configuredStartTime;

    private volatile SnapshotReader snapshotReader;
    private volatile boolean snapshotPending;
    private volatile String snapshotStartTime;
    private volatile String snapshotLastId;

    private long backoffMillis;
    private long nextAttemptMillis;
    private boolean failing;
    private long lastFailureLogMillis;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        final boolean snapshot = START_POSITION_INITIAL_SNAPSHOT.getValue().equals(context.getProperty(START_POSITION).getValue());
        if (snapshot && WatchScope.DATABASE.getValue().equals(context.getProperty(WATCH_SCOPE).getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(START_POSITION.getDisplayName())
                    .input(START_POSITION_INITIAL_SNAPSHOT.getDisplayName())
                    .valid(false)
                    .explanation(String.format("reads the documents of one collection, so it needs %s to be %s",
                            WATCH_SCOPE.getDisplayName(), WatchScope.COLLECTION.getDisplayName()))
                    .build());
        }
        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final MongoDBClientService clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
        final StateMap storedState = context.getStateManager().getState(Scope.CLUSTER);

        final boolean snapshotConfigured = START_POSITION_INITIAL_SNAPSHOT.getValue().equals(context.getProperty(START_POSITION).getValue());
        snapshotStartTime = storedState.get(StateKeys.SNAPSHOT_START_TIME);
        snapshotLastId = storedState.get(StateKeys.SNAPSHOT_LAST_ID);
        snapshotPending = snapshotConfigured && !Boolean.parseBoolean(storedState.get(StateKeys.SNAPSHOT_DONE));
        snapshotReader = snapshotConfigured
                ? new SnapshotReader(clientService,
                        context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue(),
                        context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions().getValue(),
                        context.getProperty(SNAPSHOT_BATCH_SIZE).asInteger())
                : null;

        configuredStartTime = START_POSITION_TIMESTAMP.getValue().equals(context.getProperty(START_POSITION).getValue())
                ? new BsonTimestamp(context.getProperty(START_TIMESTAMP).asInteger(), 0)
                : null;

        final StreamOptions options = buildStreamOptions(context);

        streamOpener = new StreamOpener(clientService, options);
        eventMapper = new EventMapper(JsonWriterSettings.builder().outputMode(extendedJsonMode(context)).build());
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        namespaceAttributes = options.scope() == WatchScope.COLLECTION
                ? Map.of(ATTRIBUTE_DATABASE, options.databaseName(), ATTRIBUTE_COLLECTION, options.collectionName())
                : Map.of(ATTRIBUTE_DATABASE, options.databaseName());
        transitUri = buildTransitUri(clientService.getURI(), options);
        streamSource = options.source();
        maxEventsPerFlowFile = context.getProperty(MAX_EVENTS_PER_FLOWFILE).asInteger();
        maxBatchDurationMillis = context.getProperty(MAX_BATCH_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);
        onHistoryLost = context.getProperty(ON_HISTORY_LOST).getValue();

        backoffMillis = 0L;
        nextAttemptMillis = 0L;
        failing = false;

        committedToken = storedState.get(StateKeys.RESUME_TOKEN);
        resumeFrom = committedToken;

        final String storedSource = storedState.get(StateKeys.STREAM_SOURCE);
        if (storedSource != null && !storedSource.equals(streamSource)) {
            throw new ProcessException(String.format(
                    "The stored position belongs to %s and cannot be used for %s. Clear the state of the processor to start watching "
                            + "%s, keeping in mind that the changes made in the meantime are not captured.",
                    storedSource, streamSource, streamSource));
        }
    }

    private StreamOptions buildStreamOptions(final ProcessContext context) {
        final WatchScope scope = context.getProperty(WATCH_SCOPE).asAllowableValue(WatchScope.class);
        final String collectionName = scope == WatchScope.COLLECTION
                ? context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions().getValue()
                : null;
        return new StreamOptions(
                scope,
                context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue(),
                collectionName,
                parsePipeline(context.getProperty(PIPELINE).getValue()),
                FullDocument.fromString(context.getProperty(FULL_DOCUMENT).getValue()),
                FullDocumentBeforeChange.fromString(context.getProperty(FULL_DOCUMENT_BEFORE_CHANGE).getValue()),
                context.getProperty(MAX_AWAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
    }

    private static JsonMode extendedJsonMode(final ProcessContext context) {
        return EXTENDED_JSON_CANONICAL.getValue().equals(context.getProperty(EXTENDED_JSON_MODE).getValue())
                ? JsonMode.EXTENDED
                : JsonMode.RELAXED;
    }

    static List<BsonDocument> parsePipeline(final String pipeline) {
        if (pipeline == null || pipeline.isBlank()) {
            return List.of();
        }
        return BsonArray.parse(pipeline).stream().map(BsonValue::asDocument).toList();
    }

    private static ValidationResult validatePipeline(final String subject, final String input, final ValidationContext context) {
        final ValidationResult.Builder result = new ValidationResult.Builder().subject(subject).input(input);
        if (input == null || input.isBlank()) {
            return result.valid(true).build();
        }

        final List<BsonValue> stages;
        try {
            stages = BsonArray.parse(input);
        } catch (final Exception e) {
            return result.valid(false).explanation("is not a JSON array of pipeline stages: " + e.getMessage()).build();
        }

        for (final BsonValue stage : stages) {
            if (!stage.isDocument() || stage.asDocument().size() != 1) {
                return result.valid(false).explanation("every stage must be an object with exactly one operator, for example "
                        + "{\"$match\": {\"operationType\": \"insert\"}}").build();
            }
            final String operator = stage.asDocument().getFirstKey();
            if (!ALLOWED_STAGES.contains(operator)) {
                return result.valid(false).explanation(String.format("MongoDB does not allow %s on a change stream; allowed are %s",
                        operator, String.join(", ", ALLOWED_STAGES))).build();
            }
        }
        return result.valid(true).build();
    }

    @OnStopped
    public void stop() {
        closeCursor();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (currentTimeMillis() < nextAttemptMillis) {
            context.yield();
            return;
        }

        if (snapshotPending) {
            readSnapshot(context, session);
            return;
        }

        final MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> currentCursor;
        try {
            currentCursor = getOrOpenCursor();
        } catch (final Exception e) {
            closeCursor();
            if (isHistoryLost(e)) {
                onHistoryLost(context, e);
            } else {
                backOff();
                logFailure(String.format("Opening the change stream of %s failed", transitUri), e);
            }
            context.yield();
            return;
        }

        // Events taken from the cursor are either committed to a FlowFile together with their resume token, or the
        // cursor is discarded and reopened from the last committed token, so that no event is passed over.
        final EventBatch batch = new EventBatch(session, writerFactory, eventMapper, getLogger(), transitUri, namespaceAttributes);
        boolean completed = false;
        try {
            final boolean invalidated = read(currentCursor, batch);

            if (batch.isEmpty()) {
                confirmIdleProgress(session, currentCursor);
                context.yield();
            } else {
                batch.transfer(REL_SUCCESS);
                commit(session, batch.getLastResumeToken());
                getLogger().debug("Wrote {} change events from {}", batch.getEventCount(), transitUri);
            }
            completed = true;
            failing = false;
            resetBackoff();

            if (invalidated) {
                // The server ends the stream after an invalidate. The next trigger opens a new one with startAfter
                // on the invalidate token, which is the only way to carry on past it.
                getLogger().info("The change stream of {} was invalidated, the collection was dropped or renamed; "
                        + "continuing after the invalidate event", transitUri);
                closeCursor();
            }
        } catch (final Exception e) {
            backOff();
            logFailure(String.format("Reading changes from %s failed; the change stream will be reopened from the last committed resume token",
                    transitUri), e);
            context.yield();
        } finally {
            if (!completed) {
                batch.rollback();
                closeCursor();
            }
        }
    }

    /**
     * Writes one batch of the documents the collection already holds. The time the snapshot is consistent with is
     * taken before the first document is read, so a change made while the snapshot runs is either already in what it
     * reads or arrives afterwards from the change stream.
     */
    private void readSnapshot(final ProcessContext context, final ProcessSession session) {
        final EventBatch batch = new EventBatch(session, writerFactory, eventMapper, getLogger(), transitUri, namespaceAttributes);
        boolean completed = false;
        try {
            if (snapshotStartTime == null) {
                snapshotStartTime = Long.toString(snapshotReader.currentClusterTime().getValue());
                getLogger().info("Starting the initial snapshot of {}", transitUri);
            }

            final BsonValue afterId = snapshotLastId == null ? null : BsonDocument.parse(snapshotLastId).get(ID_FIELD);
            final List<BsonDocument> documents = snapshotReader.readBatch(afterId);
            final boolean lastBatch = documents.size() < snapshotReader.getBatchSize();

            if (!documents.isEmpty()) {
                final BsonTimestamp snapshotTime = new BsonTimestamp(Long.parseLong(snapshotStartTime));
                for (final BsonDocument document : documents) {
                    batch.write(eventMapper.mapSnapshotDocument(document, snapshotReader.getDatabaseName(),
                            snapshotReader.getCollectionName(), snapshotTime));
                }
                batch.transfer(REL_SUCCESS);
            }

            final String lastId = documents.isEmpty()
                    ? snapshotLastId
                    : EventMapper.documentKey(documents.getLast()).toJson(STATE_JSON_SETTINGS);
            session.setState(snapshotState(lastBatch, lastId), Scope.CLUSTER);
            session.commitAsync(() -> {
                snapshotLastId = lastId;
                if (lastBatch) {
                    snapshotPending = false;
                    getLogger().info("The initial snapshot of {} is complete; continuing with the changes made since it started",
                            transitUri);
                }
            }, this::onCommitFailure);

            completed = true;
            failing = false;
            resetBackoff();
            if (!lastBatch) {
                getLogger().debug("Wrote {} documents of the initial snapshot of {}", documents.size(), transitUri);
            }
        } catch (final Exception e) {
            backOff();
            logFailure(String.format("Reading the initial snapshot of %s failed; the batch will be read again", transitUri), e);
            context.yield();
        } finally {
            if (!completed) {
                batch.rollback();
            }
        }
    }

    private Map<String, String> snapshotState(final boolean lastBatch, final String lastId) {
        final Map<String, String> state = new HashMap<>();
        state.put(StateKeys.STREAM_SOURCE, streamSource);
        state.put(StateKeys.SNAPSHOT_START_TIME, snapshotStartTime);
        if (lastBatch) {
            state.put(StateKeys.SNAPSHOT_DONE, Boolean.TRUE.toString());
        } else if (lastId != null) {
            state.put(StateKeys.SNAPSHOT_LAST_ID, lastId);
        }
        return state;
    }

    /**
     * Reads events until the batch is full, the time is up or the stream has nothing more for now. Returns whether
     * the batch ends with an invalidate event.
     */
    private boolean read(final MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> currentCursor, final EventBatch batch) throws IOException {
        final long deadline = currentTimeMillis() + maxBatchDurationMillis;
        while (batch.getEventCount() < maxEventsPerFlowFile && currentTimeMillis() < deadline) {
            final ChangeStreamDocument<BsonDocument> event = currentCursor.tryNext();
            if (event == null) {
                return false;
            }
            batch.write(event);
            if (INVALIDATE_OPERATION.equals(event.getOperationTypeString())) {
                return true;
            }
        }
        return false;
    }

    /**
     * An idle stream still moves forward: the server reports a resume token for the point it has read up to. Storing
     * it keeps the position of a quiet collection close to the end of the oplog, so that a long pause does not leave
     * the processor with a token the server has already discarded.
     */
    private void confirmIdleProgress(final ProcessSession session, final MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> currentCursor)
            throws IOException {
        final String resumeToken = EventMapper.resumeTokenData(currentCursor.getResumeToken());
        if (resumeToken != null && !resumeToken.equals(committedToken)) {
            commit(session, resumeToken);
        }
    }

    private void commit(final ProcessSession session, final String resumeToken) throws IOException {
        final Map<String, String> state = new HashMap<>();
        state.put(StateKeys.RESUME_TOKEN, resumeToken);
        state.put(StateKeys.STREAM_SOURCE, streamSource);
        if (snapshotStartTime != null) {
            // Keep the snapshot marked as done, so that a restart does not read the whole collection again.
            state.put(StateKeys.SNAPSHOT_START_TIME, snapshotStartTime);
            state.put(StateKeys.SNAPSHOT_DONE, Boolean.TRUE.toString());
        }
        session.setState(state, Scope.CLUSTER);
        session.commitAsync(() -> {
            committedToken = resumeToken;
            resumeFrom = resumeToken;
        }, this::onCommitFailure);
    }

    private void onCommitFailure(final Throwable failure) {
        getLogger().error("Committing change events from {} failed; the change stream will be reopened from the last committed resume token",
                transitUri, failure);
        closeCursor();
    }

    /**
     * The stored token is gone from the oplog, so the changes between it and now cannot be read from this server.
     * Either the flow stops here, or the position is given up on purpose and the stream restarts at the present.
     */
    private void onHistoryLost(final ProcessContext context, final Exception failure) {
        if (HISTORY_LOST_RESTART_FROM_NOW.getValue().equals(onHistoryLost)) {
            getLogger().warn("The resume token stored for {} is no longer in the oplog of the server. On History Lost is set to '{}', so the "
                    + "stored position is discarded and the stream restarts with the changes made from now on. The changes since the stored "
                    + "position are lost.", transitUri, HISTORY_LOST_RESTART_FROM_NOW.getDisplayName(), failure);
            try {
                context.getStateManager().clear(Scope.CLUSTER);
            } catch (final IOException e) {
                getLogger().error("Clearing the stored resume token of {} failed", transitUri, e);
                backOff();
                return;
            }
            committedToken = null;
            resumeFrom = null;
            resetBackoff();
            return;
        }

        backOff();
        logFailure(String.format("The resume token stored for %s is no longer in the oplog of the server, so the changes since that position "
                + "cannot be read. Nothing is discarded: either restore the oplog, or set On History Lost to '%s' to give up the position and "
                + "continue with the changes made from now on", transitUri, HISTORY_LOST_RESTART_FROM_NOW.getDisplayName()), failure);
    }

    /**
     * The server reports a position it cannot serve any more in two ways: ChangeStreamHistoryLost when the oplog no
     * longer reaches back that far, and the more general ChangeStreamFatalError with "resume token was not found"
     * when the token itself is not in the oplog. Other fatal stream errors keep the ordinary retry.
     */
    private static boolean isHistoryLost(final Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (!(current instanceof MongoCommandException commandException)) {
                continue;
            }
            if (commandException.getErrorCode() == HISTORY_LOST_CODE || HISTORY_LOST_CODE_NAME.equals(commandException.getErrorCodeName())) {
                return true;
            }
            if (commandException.getErrorCode() == CHANGE_STREAM_FATAL_CODE
                    && commandException.getErrorMessage() != null
                    && commandException.getErrorMessage().contains(RESUME_TOKEN_NOT_FOUND)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Waits longer after every failure, up to a minute, so that a server that is down or unreachable is not asked
     * again on every trigger. The wait is given up as soon as a trigger succeeds.
     */
    private void backOff() {
        backoffMillis = backoffMillis == 0L ? INITIAL_BACKOFF_MILLIS : Math.min(backoffMillis * 2, MAXIMUM_BACKOFF_MILLIS);
        nextAttemptMillis = currentTimeMillis() + backoffMillis;
    }

    private void resetBackoff() {
        backoffMillis = 0L;
        nextAttemptMillis = 0L;
    }

    /**
     * Logs a failure as an error the first time, then at a limited rate while triggers keep failing, so that an
     * outage does not flood the bulletin board. Every trigger still retries.
     */
    private void logFailure(final String message, final Throwable failure) {
        final long now = currentTimeMillis();
        if (!failing) {
            failing = true;
            lastFailureLogMillis = now;
            getLogger().error("{}; retrying in {} ms", message, backoffMillis, failure);
        } else if (now - lastFailureLogMillis >= REPEATED_FAILURE_LOG_INTERVAL_MILLIS) {
            lastFailureLogMillis = now;
            getLogger().warn("{}; still retrying in {} ms: {}", message, backoffMillis, failure.toString());
        } else {
            getLogger().debug("{}", message, failure);
        }
    }

    /**
     * Factory method for the change stream cursor, overridable for tests.
     */
    protected MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> openCursor(final String resumeTokenData) {
        return streamOpener.open(resumeTokenData, streamStartTime());
    }

    /**
     * Where a stream without a stored token begins. After an initial snapshot that is the moment the snapshot is
     * consistent with, so the changes made while it ran are read; the value is only known once the snapshot has
     * started, which is why it is resolved here and not when the processor is scheduled.
     */
    private BsonTimestamp streamStartTime() {
        if (snapshotStartTime != null) {
            return new BsonTimestamp(Long.parseLong(snapshotStartTime));
        }
        return configuredStartTime;
    }

    /**
     * The clock the batch deadline and the backoff are measured against, overridable for tests.
     */
    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    private MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> getOrOpenCursor() {
        if (cursor == null) {
            cursor = openCursor(resumeFrom);
            if (resumeFrom == null) {
                // Remember where the stream begins before any event is read, so that a batch that fails to commit
                // is read again instead of being skipped.
                resumeFrom = EventMapper.resumeTokenData(cursor.getResumeToken());
            }
        }
        return cursor;
    }

    private void closeCursor() {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (final Exception e) {
                getLogger().debug("Closing the change stream of {} failed", transitUri, e);
            }
            cursor = null;
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final MongoDBClientService clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();

        final Document hello;
        try {
            hello = clientService.getDatabase(databaseName).runCommand(new Document("hello", 1));
            results.add(result("Connect to MongoDB", ConfigVerificationResult.Outcome.SUCCESSFUL,
                    String.format("Connected to %s", hello.get("me", "the server"))));
        } catch (final Exception e) {
            verificationLogger.error("Connecting to MongoDB failed", e);
            results.add(result("Connect to MongoDB", ConfigVerificationResult.Outcome.FAILED,
                    String.format("Could not connect: %s", e.getMessage())));
            return results;
        }

        results.add(verifyDeployment(hello));
        results.add(verifyServerVersion(clientService, databaseName, verificationLogger));
        results.add(verifyPrivileges(clientService, context, verificationLogger));

        final StreamOptions options = buildStreamOptions(context);
        if (options.fullDocumentBeforeChange() != FullDocumentBeforeChange.OFF) {
            results.add(verifyPreImages(clientService, options, verificationLogger));
        }
        return results;
    }

    /**
     * The processor only reads, so the user needs changeStream and find on the watched scope and nothing more. A user
     * that may also write is not an error, but it is worth saying out loud.
     */
    private ConfigVerificationResult verifyPrivileges(final MongoDBClientService clientService, final ProcessContext context,
                                                      final ComponentLog verificationLogger) {
        final String step = "Privileges sufficient";
        final StreamOptions options = buildStreamOptions(context);

        final Document authInfo;
        try {
            authInfo = clientService.getDatabase("admin")
                    .runCommand(new Document("connectionStatus", 1).append("showPrivileges", true))
                    .get("authInfo", Document.class);
        } catch (final Exception e) {
            verificationLogger.warn("Reading the privileges of the user failed", e);
            return result(step, ConfigVerificationResult.Outcome.SKIPPED,
                    String.format("Could not read the privileges of the user: %s", e.getMessage()));
        }

        final List<Document> users = authInfo == null ? List.of() : authInfo.getList("authenticatedUsers", Document.class, List.of());
        if (users.isEmpty()) {
            return result(step, ConfigVerificationResult.Outcome.SKIPPED,
                    "The connection is not authenticated, so the server applies no privileges");
        }

        final Set<String> actions = new HashSet<>();
        for (final Document privilege : authInfo.getList("authenticatedUserPrivileges", Document.class, List.of())) {
            if (coversWatchedScope(privilege.get("resource", Document.class), options)) {
                actions.addAll(privilege.getList("actions", String.class, List.of()));
            }
        }

        final List<String> missing = REQUIRED_ACTIONS.stream().filter(action -> !actions.contains(action)).toList();
        if (!missing.isEmpty()) {
            return result(step, ConfigVerificationResult.Outcome.FAILED,
                    String.format("The user is missing %s on %s", String.join(" and ", missing), options.source()));
        }

        final List<String> writeActions = WRITE_ACTIONS.stream().filter(actions::contains).toList();
        if (!writeActions.isEmpty()) {
            return result(step, ConfigVerificationResult.Outcome.SUCCESSFUL,
                    String.format("changeStream and find are granted. The user may also write to the source (%s); the processor never "
                            + "does, but a read-only user is the safer choice", String.join(", ", writeActions)));
        }
        return result(step, ConfigVerificationResult.Outcome.SUCCESSFUL, "changeStream and find are granted, and the user cannot write");
    }

    private static boolean coversWatchedScope(final Document resource, final StreamOptions options) {
        if (resource == null) {
            return false;
        }
        if (Boolean.TRUE.equals(resource.getBoolean("anyResource"))) {
            return true;
        }
        final String database = resource.getString("db");
        final String collection = resource.getString("collection");
        if (database == null || collection == null) {
            return false;
        }
        // An empty name stands for every database or every collection.
        final boolean databaseMatches = database.isEmpty() || database.equals(options.databaseName());
        final boolean collectionMatches = collection.isEmpty()
                || (options.scope() == WatchScope.COLLECTION && collection.equals(options.collectionName()));
        return databaseMatches && collectionMatches;
    }

    /**
     * The version before the change only exists for collections where a database administrator turned pre-images on.
     */
    private ConfigVerificationResult verifyPreImages(final MongoDBClientService clientService, final StreamOptions options,
                                                     final ComponentLog verificationLogger) {
        final String step = "Pre-images available";
        if (options.scope() != WatchScope.COLLECTION) {
            return result(step, ConfigVerificationResult.Outcome.SKIPPED,
                    "With database scope every collection needs pre-images of its own, which is not checked here");
        }

        try {
            final Document collectionInfo = clientService.getDatabase(options.databaseName()).listCollections()
                    .filter(new Document("name", options.collectionName())).first();
            if (collectionInfo == null) {
                return result(step, ConfigVerificationResult.Outcome.FAILED,
                        String.format("The collection %s does not exist", options.source()));
            }

            final Document collectionOptions = collectionInfo.get("options", Document.class);
            final Document preImages = collectionOptions == null ? null : collectionOptions.get("changeStreamPreAndPostImages", Document.class);
            if (preImages == null || !Boolean.TRUE.equals(preImages.getBoolean("enabled"))) {
                return result(step, ConfigVerificationResult.Outcome.FAILED, String.format(
                        "Full Document Before Change is set, but the collection does not keep pre-images. A database administrator turns "
                                + "them on with db.runCommand({collMod: \"%s\", changeStreamPreAndPostImages: {enabled: true}}); only "
                                + "changes made after that have a pre-image.", options.collectionName()));
            }
            return result(step, ConfigVerificationResult.Outcome.SUCCESSFUL, "The collection keeps pre-images");
        } catch (final Exception e) {
            verificationLogger.warn("Reading the collection options failed", e);
            return result(step, ConfigVerificationResult.Outcome.SKIPPED,
                    String.format("Could not read the options of the collection: %s", e.getMessage()));
        }
    }

    private ConfigVerificationResult verifyDeployment(final Document hello) {
        final String replicaSetName = hello.getString("setName");
        if (replicaSetName != null) {
            return result("Change Streams available", ConfigVerificationResult.Outcome.SUCCESSFUL,
                    String.format("Connected to the replica set %s", replicaSetName));
        }
        if (SHARDED_CLUSTER_MESSAGE.equals(hello.getString("msg"))) {
            return result("Change Streams available", ConfigVerificationResult.Outcome.SUCCESSFUL, "Connected to a sharded cluster");
        }
        return result("Change Streams available", ConfigVerificationResult.Outcome.FAILED,
                "The server is a standalone server. Change Streams need a replica set or a sharded cluster.");
    }

    private ConfigVerificationResult verifyServerVersion(final MongoDBClientService clientService, final String databaseName,
                                                         final ComponentLog verificationLogger) {
        final String version;
        final int majorVersion;
        try {
            final Document buildInfo = clientService.getDatabase(databaseName).runCommand(new Document("buildInfo", 1));
            version = buildInfo.getString("version");
            majorVersion = buildInfo.getList("versionArray", Number.class).get(0).intValue();
        } catch (final Exception e) {
            verificationLogger.warn("Reading the server version failed", e);
            return result("Server version supported", ConfigVerificationResult.Outcome.SKIPPED,
                    String.format("Could not read the server version, the user may not be allowed to run buildInfo: %s", e.getMessage()));
        }

        if (majorVersion < MINIMUM_MAJOR_VERSION) {
            return result("Server version supported", ConfigVerificationResult.Outcome.FAILED,
                    String.format("MongoDB %s is older than the required version %d.0", version, MINIMUM_MAJOR_VERSION));
        }
        return result("Server version supported", ConfigVerificationResult.Outcome.SUCCESSFUL, String.format("MongoDB %s", version));
    }

    private static ConfigVerificationResult result(final String step, final ConfigVerificationResult.Outcome outcome, final String explanation) {
        return new ConfigVerificationResult.Builder()
                .verificationStepName(step)
                .outcome(outcome)
                .explanation(explanation)
                .build();
    }

    /**
     * The connection string without any credentials that may be embedded in it, so that provenance never carries a password.
     */
    private static String buildTransitUri(final String uri, final StreamOptions options) {
        final String withoutCredentials = uri == null ? "" : uri.replaceAll("://[^@/]*@", "://");
        final String withoutTrailingSlash = withoutCredentials.endsWith("/")
                ? withoutCredentials.substring(0, withoutCredentials.length() - 1)
                : withoutCredentials;
        final String namespace = options.scope() == WatchScope.COLLECTION
                ? String.format("%s.%s", options.databaseName(), options.collectionName())
                : options.databaseName();
        return String.format("%s/%s", withoutTrailingSlash, namespace);
    }
}
