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

import com.github.dockerjava.api.DockerClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the processor against a real single node replica set. The subclasses pick the server version.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractCaptureChangeMongoDBIT {

    private static final String DATABASE = "lab";
    private static final String COLLECTION = "orders";
    private static final int MAX_TRIGGERS = 40;
    private static final long RECOVERY_TIMEOUT_MILLIS = 90_000L;
    private static final int SNAPSHOT_DOCUMENTS = 50_000;

    private MongoDBContainer container;
    private MongoClient client;
    private MongoCollection<Document> collection;
    private TestRunner runner;

    protected abstract String getImage();

    @BeforeAll
    void startServer() {
        container = new MongoDBContainer(DockerImageName.parse(getImage())).withReplicaSet();
        container.start();
        client = MongoClients.create(container.getReplicaSetUrl());
    }

    @AfterAll
    void stopServer() {
        if (client != null) {
            client.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    @BeforeEach
    void createRunner() throws Exception {
        client.getDatabase(DATABASE).drop();
        collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
        // A change stream can only be opened on a collection that exists.
        collection.insertOne(new Document("_id", "seed"));

        runner = TestRunners.newTestRunner(new CaptureChangeMongoDB());

        final MongoDBControllerService clientService = new MongoDBControllerService();
        runner.addControllerService("client-service", clientService);
        // Short timeouts, so that a server that does not answer fails the trigger instead of blocking it.
        runner.setProperty(clientService, MongoDBClientService.URI,
                container.getReplicaSetUrl() + "?connectTimeoutMS=2000&socketTimeoutMS=2000&serverSelectionTimeoutMS=2000");
        runner.enableControllerService(clientService);

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.setProperty(writer, JsonRecordSetWriter.OUTPUT_GROUPING, JsonRecordSetWriter.OUTPUT_ONELINE.getValue());
        runner.enableControllerService(writer);

        runner.setProperty(CaptureChangeMongoDB.CLIENT_SERVICE, "client-service");
        runner.setProperty(CaptureChangeMongoDB.RECORD_WRITER, "record-writer");
        runner.setProperty(CaptureChangeMongoDB.DATABASE_NAME, DATABASE);
        runner.setProperty(CaptureChangeMongoDB.COLLECTION_NAME, COLLECTION);
        runner.setProperty(CaptureChangeMongoDB.MAX_AWAIT_TIME, "250 ms");
        runner.setProperty(CaptureChangeMongoDB.MAX_BATCH_DURATION, "1 s");
    }

    @Test
    void everyOperationIsCapturedInOrder() {
        startStream();

        collection.insertOne(new Document("_id", 1).append("total", 10));
        collection.updateOne(Filters.eq("_id", 1), Updates.set("total", 20));
        collection.replaceOne(Filters.eq("_id", 1), new Document("_id", 1).append("total", 30));
        collection.deleteOne(Filters.eq("_id", 1));

        final List<Document> records = readRecords(4);

        assertEquals(List.of("insert", "update", "replace", "delete"),
                records.stream().map(record -> record.getString("operation")).toList());
        records.forEach(record -> {
            assertEquals(DATABASE, record.getString("database"));
            assertEquals(COLLECTION, record.getString("collection"));
            assertEquals("{\"_id\": 1}", record.getString("document_key"));
            assertNotNull(record.getString("resume_token"));
            assertTrue(record.get("cluster_time", Number.class).longValue() > 0L);
        });

        assertEquals("{\"_id\": 1, \"total\": 10}", records.get(0).getString("full_document"));
        assertEquals("{\"total\": 20}", records.get(1).getString("updated_fields"));
        assertEquals("{\"_id\": 1, \"total\": 30}", records.get(2).getString("full_document"));
        assertNull(records.get(3).getString("full_document"));
    }

    @Test
    void changesMadeWhileTheProcessorIsStoppedAreReadAfterTheRestart() throws Exception {
        startStream();
        collection.insertOne(new Document("_id", 0));
        assertEquals(1, readRecords(1).size());
        assertNotNull(runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.RESUME_TOKEN));

        runner.run(1, true, false);
        runner.clearTransferState();

        for (int id = 1; id <= 100; id++) {
            collection.insertOne(new Document("_id", id));
        }

        runner.run(1, false, true);
        final List<Document> records = readRecords(100);

        assertEquals(100, records.size());
        assertEquals(List.of(), records.stream().filter(record -> !"insert".equals(record.getString("operation"))).toList());
        assertEquals(IntStream.rangeClosed(1, 100).boxed().toList(),
                records.stream().map(record -> Document.parse(record.getString("document_key")).getInteger("_id")).toList());
    }

    /**
     * The processor must be usable with a read-only user, so none of what it does may be a write. The counters are
     * compared around the reading of the events.
     */
    @Test
    void readingChangesWritesNothingToTheServer() {
        startStream();
        collection.insertOne(new Document("_id", 1));
        collection.updateOne(Filters.eq("_id", 1), Updates.set("total", 5));
        collection.deleteOne(Filters.eq("_id", 1));

        final Document before = opcounters();
        assertEquals(3, readRecords(3).size());
        final Document after = opcounters();

        for (final String counter : List.of("insert", "update", "delete")) {
            assertEquals(before.get(counter, Number.class).longValue(), after.get(counter, Number.class).longValue(),
                    String.format("the processor performed a %s on the server", counter));
        }
    }

    /**
     * Dropping the watched collection ends the stream with an invalidate. The events are delivered, and the stream
     * carries on afterwards, so a collection that is dropped and created again keeps being captured.
     */
    @Test
    void droppingTheCollectionInvalidatesTheStreamAndTheStreamCarriesOn() {
        startStream();
        collection.insertOne(new Document("_id", 1));
        collection.drop();

        final List<Document> beforeInvalidate = readUntil(record -> "invalidate".equals(record.getString("operation")));
        final List<String> operations = beforeInvalidate.stream().map(record -> record.getString("operation")).toList();
        assertEquals("insert", operations.getFirst());
        assertEquals("invalidate", operations.getLast());

        client.getDatabase(DATABASE).getCollection(COLLECTION).insertOne(new Document("_id", 2));

        final List<Document> afterInvalidate = readRecords(1);
        assertEquals(1, afterInvalidate.size());
        assertEquals("insert", afterInvalidate.getFirst().getString("operation"));
        assertEquals("{\"_id\": 2}", afterInvalidate.getFirst().getString("document_key"));
    }

    /**
     * Every event of a multi document transaction carries the same transaction number, so a consumer can tell that
     * they belong together.
     */
    @Test
    void theEventsOfATransactionShareTheTransactionNumber() {
        startStream();

        try (ClientSession session = client.startSession()) {
            session.startTransaction();
            collection.insertOne(session, new Document("_id", 1));
            collection.insertOne(session, new Document("_id", 2));
            collection.insertOne(session, new Document("_id", 3));
            session.commitTransaction();
        }

        final List<Document> records = readRecords(3);
        assertEquals(3, records.size());

        final List<Long> transactionNumbers = records.stream()
                .map(record -> record.get("txn_number", Number.class))
                .map(number -> number == null ? null : number.longValue())
                .distinct()
                .toList();
        assertEquals(1, transactionNumbers.size(), "all events of the transaction must carry the same txn_number");
        assertNotNull(transactionNumbers.getFirst(), "the events of a transaction must carry a txn_number");
    }

    /**
     * While the server does not answer the processor keeps failing and waiting, and once it answers again it
     * continues from the last committed position without losing a change.
     */
    @Test
    void anOutageIsSurvivedWithoutLosingChanges() throws Exception {
        startStream();
        for (int id = 1; id <= 5; id++) {
            collection.insertOne(new Document("_id", id));
        }
        assertEquals(5, readRecords(5).size());

        final DockerClient docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(container.getContainerId()).exec();
        try {
            runner.run(1, false, false);
            runner.run(1, false, false);
            runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 0);
        } finally {
            docker.unpauseContainerCmd(container.getContainerId()).exec();
        }

        for (int id = 6; id <= 10; id++) {
            collection.insertOne(new Document("_id", id));
        }

        final List<Document> records = readRecordsWaiting(5);
        assertEquals(IntStream.rangeClosed(6, 10).boxed().toList(),
                records.stream().map(record -> Document.parse(record.getString("document_key")).getInteger("_id")).toList());
    }

    /**
     * A position the server cannot serve any more. With the default setting the stored token is kept and the
     * problem is reported, so that nobody loses changes without noticing.
     */
    @Test
    void aPositionTheServerCannotServeIsReportedAndKept() throws Exception {
        final String lostToken = stopWithAPositionTheServerCannotServe();

        runner.run(1, false, true);

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 0);
        assertEquals(lostToken, runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.RESUME_TOKEN),
                "the stored position must be kept, so that the loss is not hidden");
        assertTrue(runner.getLogger().getErrorMessages().stream()
                        .anyMatch(message -> message.getMsg().contains("no longer in the oplog")),
                "the operator must be told that the stored position is gone");
    }

    @Test
    void aPositionTheServerCannotServeCanBeGivenUpToCarryOn() throws Exception {
        final String lostToken = stopWithAPositionTheServerCannotServe();
        runner.setProperty(CaptureChangeMongoDB.ON_HISTORY_LOST, CaptureChangeMongoDB.HISTORY_LOST_RESTART_FROM_NOW.getValue());

        runner.run(1, false, true);
        assertNull(runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.RESUME_TOKEN),
                "the position the server cannot serve must be given up");

        runner.run(1, false, false);
        collection.insertOne(new Document("_id", "after-restart"));

        final List<Document> records = readRecords(1);
        assertEquals(1, records.size());
        assertEquals("{\"_id\": \"after-restart\"}", records.getFirst().getString("document_key"));
        assertNotEquals(lostToken, runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.RESUME_TOKEN));
    }

    /**
     * Leaves the processor stopped with a stored resume token the server rejects. The token is a real one whose
     * timestamp is moved far back, which is what the server sees after the oplog has passed the stored position;
     * filling a small oplog does not work in a test because the server truncates it in the background.
     */
    private String stopWithAPositionTheServerCannotServe() throws Exception {
        startStream();
        collection.insertOne(new Document("_id", "before"));

        final List<Document> records = readRecords(1);
        assertEquals(1, records.size());
        final String realToken = records.getFirst().getString("resume_token");
        assertTrue(realToken.startsWith("82"), "unexpected resume token format: " + realToken);

        runner.run(1, true, false);
        runner.clearTransferState();

        final String backdatedToken = realToken.substring(0, 2) + "60000000" + realToken.substring(10);
        runner.getStateManager().setState(Map.of(StateKeys.RESUME_TOKEN, backdatedToken), Scope.CLUSTER);
        return backdatedToken;
    }

    @Test
    void databaseScopeCapturesEveryCollectionOfTheDatabase() {
        runner.removeProperty(CaptureChangeMongoDB.COLLECTION_NAME);
        runner.setProperty(CaptureChangeMongoDB.WATCH_SCOPE, WatchScope.DATABASE);
        startStream();

        collection.insertOne(new Document("_id", 1));
        client.getDatabase(DATABASE).getCollection("invoices").insertOne(new Document("_id", 2));

        final List<Document> records = readRecords(2);

        assertEquals(List.of(COLLECTION, "invoices"), records.stream().map(record -> record.getString("collection")).toList());
        records.forEach(record -> assertEquals(DATABASE, record.getString("database")));
    }

    /**
     * The pipeline runs on the server, so the events it drops never reach the processor. Nothing is filtered here,
     * which is why seeing only the matching events proves the server did the work.
     */
    @Test
    void aPipelineKeepsTheUnwantedEventsOnTheServer() {
        runner.setProperty(CaptureChangeMongoDB.PIPELINE, "[{\"$match\": {\"operationType\": \"delete\"}}]");
        startStream();

        for (int id = 1; id <= 5; id++) {
            collection.insertOne(new Document("_id", id));
        }
        collection.deleteOne(Filters.eq("_id", 3));

        final List<Document> records = readRecords(1);

        assertEquals(1, records.size(), "only the delete may arrive, the five inserts must be dropped by the server");
        assertEquals("delete", records.getFirst().getString("operation"));
        assertEquals("{\"_id\": 3}", records.getFirst().getString("document_key"));
    }

    @Test
    void updateLookupAddsTheDocumentToAnUpdateEvent() {
        runner.setProperty(CaptureChangeMongoDB.FULL_DOCUMENT, CaptureChangeMongoDB.FULL_DOCUMENT_UPDATE_LOOKUP.getValue());
        startStream();

        collection.insertOne(new Document("_id", 1).append("total", 10));
        collection.updateOne(Filters.eq("_id", 1), Updates.set("total", 20));

        final List<Document> records = readRecords(2);
        assertEquals("update", records.get(1).getString("operation"));
        assertEquals("{\"total\": 20}", records.get(1).getString("updated_fields"));
        assertEquals("{\"_id\": 1, \"total\": 20}", records.get(1).getString("full_document"));
    }

    /**
     * The version before the change only exists where the collection keeps pre-images; turning them on is a change
     * a database administrator makes, never the processor.
     */
    @Test
    void preImagesAreDeliveredWhenTheCollectionKeepsThem() {
        client.getDatabase(DATABASE).runCommand(new Document("collMod", COLLECTION)
                .append("changeStreamPreAndPostImages", new Document("enabled", true)));
        runner.setProperty(CaptureChangeMongoDB.FULL_DOCUMENT_BEFORE_CHANGE, CaptureChangeMongoDB.BEFORE_CHANGE_REQUIRED.getValue());
        startStream();

        collection.insertOne(new Document("_id", 1).append("total", 10));
        collection.updateOne(Filters.eq("_id", 1), Updates.set("total", 20));

        final List<Document> records = readRecords(2);
        assertEquals("{\"_id\": 1, \"total\": 10}", records.get(1).getString("full_document_before_change"));
    }

    @Test
    void canonicalModeKeepsTheBsonTypes() {
        runner.setProperty(CaptureChangeMongoDB.EXTENDED_JSON_MODE, CaptureChangeMongoDB.EXTENDED_JSON_CANONICAL.getValue());
        startStream();

        collection.insertOne(new Document("_id", 1).append("total", 42));

        final List<Document> records = readRecords(1);
        assertEquals("{\"_id\": {\"$numberInt\": \"1\"}, \"total\": {\"$numberInt\": \"42\"}}",
                records.getFirst().getString("full_document"));
    }

    /**
     * Start Position Timestamp reaches back into the oplog, so changes made before the processor was ever started
     * are captured.
     */
    @Test
    void startPositionTimestampReadsTheChangesMadeBeforeTheFirstStart() throws Exception {
        // The property has a resolution of one second, so the next second is waited for; otherwise the changes made
        // while the test was setting up would fall on the start position as well.
        final long startSeconds = Instant.now().getEpochSecond() + 1;
        Thread.sleep(Math.max(0L, startSeconds * 1000L - System.currentTimeMillis()) + 100L);

        collection.insertOne(new Document("_id", "made-before-the-first-start"));

        runner.setProperty(CaptureChangeMongoDB.START_POSITION, CaptureChangeMongoDB.START_POSITION_TIMESTAMP.getValue());
        runner.setProperty(CaptureChangeMongoDB.START_TIMESTAMP, Long.toString(startSeconds));

        runner.run(1, false, true);
        final List<Document> records = readRecords(1);

        assertEquals(1, records.size());
        assertEquals("{\"_id\": \"made-before-the-first-start\"}", records.getFirst().getString("document_key"));
    }

    @Test
    void theInitialSnapshotWritesTheDocumentsAndThenCarriesOnWithTheChanges() {
        collection.deleteMany(new Document());
        for (int id = 1; id <= 10; id++) {
            collection.insertOne(new Document("_id", id).append("total", id));
        }
        configureSnapshot(4);

        runner.run(1, false, true);
        final List<Document> snapshot = readRecords(10);

        assertEquals(10, snapshot.size());
        snapshot.forEach(record -> {
            assertEquals("read", record.getString("operation"));
            assertEquals(DATABASE, record.getString("database"));
            assertEquals(COLLECTION, record.getString("collection"));
            assertNull(record.getString("resume_token"), "a document that was already there is not a change");
            assertTrue(record.get("cluster_time", Number.class).longValue() > 0L);
        });
        assertEquals(IntStream.rangeClosed(1, 10).boxed().toList(), idsOf(snapshot), "the snapshot reads in the order of the identifiers");
        assertEquals("{\"_id\": 1, \"total\": 1}", snapshot.getFirst().getString("full_document"));

        collection.updateOne(Filters.eq("_id", 1), Updates.set("total", 99));

        // The stream carries on at the moment the snapshot was taken, which is the moment of the last write before
        // it, so that write can arrive a second time as an event. That repeat is the at-least-once guarantee.
        final List<Document> changes = readRecordsWaitingFor(record -> "update".equals(record.getString("operation")));
        final Document update = changes.stream()
                .filter(record -> "update".equals(record.getString("operation"))).findFirst().orElseThrow();
        assertEquals(1, idOf(update));
        assertEquals("{\"total\": 99}", update.getString("updated_fields"));
        assertNotNull(update.getString("resume_token"));
        assertEquals(List.of(), changes.stream().filter(record -> "read".equals(record.getString("operation"))).toList(),
                "the snapshot is over, so nothing more is read from the collection");
    }

    /**
     * Scenario 8: while the snapshot runs the collection keeps changing. Every document that was there when the
     * snapshot started has to come out, either as a document it read or as a change that followed.
     */
    @Test
    void theInitialSnapshotLosesNothingWhileTheCollectionIsWrittenTo() {
        collection.deleteMany(new Document());
        final List<Document> documents = new ArrayList<>();
        for (int id = 1; id <= SNAPSHOT_DOCUMENTS; id++) {
            documents.add(new Document("_id", id).append("total", id));
            if (documents.size() == 1000) {
                collection.insertMany(documents);
                documents.clear();
            }
        }
        configureSnapshot(1000);

        runner.run(1, false, true);

        final List<Document> records = new ArrayList<>();
        for (int trigger = 0; trigger < SNAPSHOT_DOCUMENTS; trigger++) {
            collect(records);
            if (trigger == 3) {
                // changes made in the middle of the snapshot, to documents it has already passed and to new ones
                collection.updateOne(Filters.eq("_id", 1), Updates.set("total", -1));
                collection.deleteOne(Filters.eq("_id", 2));
                collection.insertOne(new Document("_id", SNAPSHOT_DOCUMENTS + 1).append("total", 0));
            }
            if (records.size() >= SNAPSHOT_DOCUMENTS && snapshotIsDone()) {
                break;
            }
        }

        final Set<Integer> read = records.stream()
                .filter(record -> "read".equals(record.getString("operation")))
                .map(AbstractCaptureChangeMongoDBIT::idOf)
                .collect(Collectors.toSet());
        final List<Integer> missing = IntStream.rangeClosed(1, SNAPSHOT_DOCUMENTS).boxed()
                .filter(id -> !read.contains(id)).toList();
        assertEquals(List.of(), missing, "every document that was there when the snapshot started must be read");
        assertTrue(snapshotIsDone(), "the snapshot must finish");

        // the changes made while the snapshot ran arrive from the change stream afterwards
        final List<Document> changes = readRecordsWaitingFor(record -> "delete".equals(record.getString("operation")));
        assertTrue(changes.stream().anyMatch(record -> "update".equals(record.getString("operation")) && idOf(record) == 1),
                "the update made during the snapshot must arrive");
        assertTrue(changes.stream().anyMatch(record -> "delete".equals(record.getString("operation")) && idOf(record) == 2),
                "the delete made during the snapshot must arrive");
    }

    /**
     * A snapshot that is stopped halfway carries on from the last document it committed, so nothing is read twice
     * and nothing is skipped.
     */
    @Test
    void anInterruptedSnapshotCarriesOnWhereItStopped() throws Exception {
        collection.deleteMany(new Document());
        final List<Document> documents = new ArrayList<>();
        for (int id = 1; id <= 5000; id++) {
            documents.add(new Document("_id", id));
        }
        collection.insertMany(documents);
        configureSnapshot(1000);

        runner.run(3, false, true);
        final List<Document> firstPart = new ArrayList<>();
        drain(firstPart);
        assertEquals(3000, firstPart.size(), "three batches of a thousand");
        assertNotNull(runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.SNAPSHOT_LAST_ID));
        assertNull(runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.SNAPSHOT_DONE));

        runner.run(1, true, false);
        drain(firstPart);
        runner.clearTransferState();

        runner.run(1, false, true);
        final List<Document> secondPart = new ArrayList<>();
        for (int trigger = 0; trigger < 10 && !snapshotIsDone(); trigger++) {
            collect(secondPart);
        }

        final List<Document> all = new ArrayList<>(firstPart);
        all.addAll(secondPart);
        final List<Integer> ids = idsOf(all.stream().filter(record -> "read".equals(record.getString("operation"))).toList());
        assertEquals(IntStream.rangeClosed(1, 5000).boxed().toList(), ids,
                "every document exactly once and in order, so the snapshot carried on instead of starting again");
    }

    @Test
    void verifyReportsTheConnectionTheDeploymentTheVersionAndThePrivileges() {
        final List<ConfigVerificationResult> results =
                ((CaptureChangeMongoDB) runner.getProcessor()).verify(runner.getProcessContext(), runner.getLogger(), Map.of());

        assertEquals(List.of("Connect to MongoDB", "Change Streams available", "Server version supported", "Privileges sufficient"),
                results.stream().map(ConfigVerificationResult::getVerificationStepName).toList());
        results.forEach(result -> assertNotEquals(ConfigVerificationResult.Outcome.FAILED, result.getOutcome(),
                result.getVerificationStepName() + ": " + result.getExplanation()));
    }

    /**
     * Asking for the version before the change on a collection that does not keep pre-images is a configuration
     * mistake the check has to catch, because the server would only fail later, event by event.
     */
    @Test
    void verifyReportsAMissingPreImageConfiguration() {
        runner.setProperty(CaptureChangeMongoDB.FULL_DOCUMENT_BEFORE_CHANGE, CaptureChangeMongoDB.BEFORE_CHANGE_REQUIRED.getValue());

        final List<ConfigVerificationResult> results =
                ((CaptureChangeMongoDB) runner.getProcessor()).verify(runner.getProcessContext(), runner.getLogger(), Map.of());

        final ConfigVerificationResult preImages = results.stream()
                .filter(result -> "Pre-images available".equals(result.getVerificationStepName()))
                .findFirst().orElseThrow();
        assertEquals(ConfigVerificationResult.Outcome.FAILED, preImages.getOutcome());
        assertTrue(preImages.getExplanation().contains("collMod"), preImages.getExplanation());
    }

    /**
     * Opens the change stream, so that the changes made afterwards fall inside the window the processor watches.
     */
    private void startStream() {
        runner.run(1, false, true);
        runner.clearTransferState();
    }

    private List<Document> readRecords(final int expected) {
        final List<Document> records = new ArrayList<>();
        for (int trigger = 0; trigger < MAX_TRIGGERS && records.size() < expected; trigger++) {
            collect(records);
        }
        return records;
    }

    private List<Document> readUntil(final Predicate<Document> last) {
        final List<Document> records = new ArrayList<>();
        for (int trigger = 0; trigger < MAX_TRIGGERS && records.stream().noneMatch(last); trigger++) {
            collect(records);
        }
        return records;
    }

    /**
     * Keeps triggering while the processor is waiting between attempts, so that the recovery after an outage is
     * given the time the growing wait asks for.
     */
    private List<Document> readRecordsWaiting(final int expected) throws InterruptedException {
        final List<Document> records = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + RECOVERY_TIMEOUT_MILLIS;
        while (records.size() < expected && System.currentTimeMillis() < deadline) {
            collect(records);
            if (records.size() < expected) {
                Thread.sleep(500L);
            }
        }
        return records;
    }

    private void configureSnapshot(final int batchSize) {
        runner.setProperty(CaptureChangeMongoDB.START_POSITION, CaptureChangeMongoDB.START_POSITION_INITIAL_SNAPSHOT.getValue());
        runner.setProperty(CaptureChangeMongoDB.SNAPSHOT_BATCH_SIZE, Integer.toString(batchSize));
    }

    private boolean snapshotIsDone() {
        try {
            return Boolean.parseBoolean(runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.SNAPSHOT_DONE));
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<Document> readRecordsWaitingFor(final Predicate<Document> wanted) {
        final List<Document> records = new ArrayList<>();
        for (int trigger = 0; trigger < MAX_TRIGGERS && records.stream().noneMatch(wanted); trigger++) {
            collect(records);
        }
        return records;
    }

    /**
     * Takes whatever is already queued without triggering again.
     */
    private void drain(final List<Document> records) {
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS)) {
            flowFile.getContent().lines().filter(line -> !line.isBlank()).map(Document::parse).forEach(records::add);
        }
        runner.clearTransferState();
    }

    private static List<Integer> idsOf(final List<Document> records) {
        return records.stream().map(AbstractCaptureChangeMongoDBIT::idOf).toList();
    }

    private static int idOf(final Document record) {
        return Document.parse(record.getString("document_key")).getInteger("_id");
    }

    private void collect(final List<Document> records) {
        runner.run(1, false, false);
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS)) {
            flowFile.getContent().lines().filter(line -> !line.isBlank()).map(Document::parse).forEach(records::add);
        }
        runner.clearTransferState();
    }

    private Document opcounters() {
        return client.getDatabase("admin").runCommand(new Document("serverStatus", 1)).get("opcounters", Document.class);
    }
}
