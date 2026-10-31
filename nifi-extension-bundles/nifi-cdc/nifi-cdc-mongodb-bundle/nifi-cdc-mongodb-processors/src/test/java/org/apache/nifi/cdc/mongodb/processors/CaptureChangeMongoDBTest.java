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
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.nifi.cdc.mongodb.event.ChangeEvents;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the processor with a fake cursor and a clock the test moves, so the batch loop, the stored resume token
 * and the behaviour after a failure are covered without a server.
 */
class CaptureChangeMongoDBTest {

    private static final String INITIAL_TOKEN = "token-at-open";
    private static final long INITIAL_BACKOFF_MILLIS = 1_000L;

    private final TestableProcessor processor = new TestableProcessor();

    @Test
    void eventsAreWrittenAsOneFlowFileAndTheLastTokenIsStored() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN, "t1", "t2", "t3"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run();

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, "3");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_FIRST_RESUME_TOKEN, "t1");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, "t3");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_DATABASE, "lab");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_COLLECTION, "orders");
        flowFile.assertAttributeExists(CaptureChangeMongoDB.ATTRIBUTE_LAG_MILLIS);

        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t3", Scope.CLUSTER);
        assertEquals(Collections.singletonList(null), processor.openedFrom);
    }

    @Test
    void maxEventsPerFlowFileEndsTheBatch() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN, "t1", "t2", "t3", "t4"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.setProperty(CaptureChangeMongoDB.MAX_EVENTS_PER_FLOWFILE, "2");

        runner.run();

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, "2");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, "t2");
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t2", Scope.CLUSTER);
    }

    @Test
    void anIdleStreamStoresThePositionTheServerReports() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run();

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, INITIAL_TOKEN, Scope.CLUSTER);
    }

    @Test
    void aStoredTokenIsUsedToReopenTheStreamAfterARestart() throws Exception {
        processor.addCursor(cursor("ignored", "t9"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.getStateManager().setState(Map.of(StateKeys.RESUME_TOKEN, "stored-token"), Scope.CLUSTER);

        runner.run();

        assertEquals(List.of("stored-token"), processor.openedFrom);
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t9", Scope.CLUSTER);
    }

    /**
     * A stream that breaks in the middle of a batch must leave nothing behind: no FlowFile, no stored token, and
     * the reopened stream starts where the failed batch started, so the events come again.
     */
    @Test
    void aFailedBatchIsDiscardedAndItsEventsAreReadAgain() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN, "t1", "t2", "t3").failAfter(2));
        processor.addCursor(cursor(INITIAL_TOKEN, "t1", "t2", "t3"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run(1, false, true);
        processor.advance(INITIAL_BACKOFF_MILLIS);
        runner.run(1, false, false);

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, "3");
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_FIRST_RESUME_TOKEN, "t1");

        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t3", Scope.CLUSTER);
        assertEquals(Arrays.asList(null, INITIAL_TOKEN), processor.openedFrom);
        assertTrue(processor.opened.getFirst().isClosed(), "the broken cursor must be closed");
    }

    /**
     * The same guarantee when the Record Writer is what fails.
     */
    @Test
    void aBatchThatCannotBeWrittenLeavesNoTokenBehind() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN, "t1", "t2", "t3"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false, 2));

        runner.run();

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 0);
        runner.getStateManager().assertStateNotSet(Scope.CLUSTER);
        assertTrue(processor.opened.getFirst().isClosed(), "the cursor must be closed so that it is reopened");
    }

    @Test
    void theTransitUriDoesNotRepeatThePasswordOfTheClientService() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN, "t1"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run();

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final String transitUri = provenanceEvents.getFirst().getTransitUri();
        assertFalse(transitUri.contains("s3cret"), "transit URI must not carry the password: " + transitUri);
        assertEquals("mongodb://mongo.example:27017/lab.orders", transitUri);
    }

    /**
     * The server ends the stream after an invalidate. The event is delivered like any other, and the next stream
     * carries on from its token.
     */
    @Test
    void anInvalidateIsDeliveredAndTheStreamCarriesOnAfterIt() throws Exception {
        final FakeChangeStreamCursor invalidated = new FakeChangeStreamCursor(INITIAL_TOKEN,
                List.of(ChangeEvents.insert("t1"), ChangeEvents.invalidate("t2")));
        processor.addCursor(invalidated);
        processor.addCursor(cursor("t2", "t3"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run(1, false, true);
        runner.run(1, false, false);

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS);
        flowFiles.getFirst().assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_RECORD_COUNT, "2");
        flowFiles.getFirst().assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, "t2");
        flowFiles.get(1).assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_LAST_RESUME_TOKEN, "t3");

        assertTrue(invalidated.isClosed(), "the invalidated cursor must be closed");
        assertEquals(Arrays.asList(null, "t2"), processor.openedFrom);
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t3", Scope.CLUSTER);
    }

    /**
     * With the default On History Lost the stored position is kept, so that nobody loses changes without noticing.
     */
    @Test
    void historyLostKeepsTheStoredTokenByDefault() throws Exception {
        processor.addOpenFailure(historyLost());
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.getStateManager().setState(Map.of(StateKeys.RESUME_TOKEN, "old-token"), Scope.CLUSTER);

        runner.run();

        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "old-token", Scope.CLUSTER);
        assertEquals(List.of("old-token"), processor.openedFrom);
    }

    /**
     * A server that cannot find the token reports the general fatal stream error rather than the history error, so
     * that message has to count as a lost position too.
     */
    @Test
    void aTokenTheServerCannotFindCountsAsALostPosition() throws Exception {
        processor.addOpenFailure(changeStreamFatalError("cannot resume stream; the resume token was not found"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.setProperty(CaptureChangeMongoDB.ON_HISTORY_LOST, CaptureChangeMongoDB.HISTORY_LOST_RESTART_FROM_NOW.getValue());
        runner.getStateManager().setState(Map.of(StateKeys.RESUME_TOKEN, "old-token"), Scope.CLUSTER);

        runner.run();

        assertNull(runner.getStateManager().getState(Scope.CLUSTER).get(StateKeys.RESUME_TOKEN),
                "a position the server cannot find must be given up");
    }

    /**
     * Any other fatal stream error stays an ordinary failure, so a stored position is never given up by accident.
     */
    @Test
    void anotherFatalStreamErrorKeepsTheStoredPosition() throws Exception {
        processor.addOpenFailure(changeStreamFatalError("the change stream cannot be opened for another reason"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.setProperty(CaptureChangeMongoDB.ON_HISTORY_LOST, CaptureChangeMongoDB.HISTORY_LOST_RESTART_FROM_NOW.getValue());
        runner.getStateManager().setState(Map.of(StateKeys.RESUME_TOKEN, "old-token"), Scope.CLUSTER);

        runner.run();

        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "old-token", Scope.CLUSTER);
    }

    @Test
    void historyLostCanGiveUpThePositionAndRestartFromNow() throws Exception {
        processor.addOpenFailure(historyLost());
        processor.addCursor(cursor("fresh", "t1"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.setProperty(CaptureChangeMongoDB.ON_HISTORY_LOST, CaptureChangeMongoDB.HISTORY_LOST_RESTART_FROM_NOW.getValue());
        runner.getStateManager().setState(Map.of(StateKeys.RESUME_TOKEN, "old-token"), Scope.CLUSTER);

        runner.run(1, false, true);
        runner.run(1, false, false);

        assertEquals(Arrays.asList("old-token", null), processor.openedFrom);
        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 1);
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t1", Scope.CLUSTER);
    }

    /**
     * While the server is unreachable the processor waits longer after every attempt, and it stops waiting as soon
     * as a trigger succeeds.
     */
    @Test
    void failuresAreRetriedWithAGrowingWait() throws Exception {
        processor.addOpenFailure(new MongoException("the server is unreachable"));
        processor.addOpenFailure(new MongoException("the server is unreachable"));
        processor.addCursor(cursor(INITIAL_TOKEN, "t1", "t2"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        // one event per FlowFile, so the trigger after the successful one has something left to read
        runner.setProperty(CaptureChangeMongoDB.MAX_EVENTS_PER_FLOWFILE, "1");

        runner.run(1, false, true);
        assertEquals(1, processor.openedFrom.size());

        processor.advance(INITIAL_BACKOFF_MILLIS - 1);
        runner.run(1, false, false);
        assertEquals(1, processor.openedFrom.size(), "the server must not be asked again before the wait is over");

        processor.advance(1);
        runner.run(1, false, false);
        assertEquals(2, processor.openedFrom.size());

        // the wait doubled, so the same step is not enough this time
        processor.advance(INITIAL_BACKOFF_MILLIS);
        runner.run(1, false, false);
        assertEquals(2, processor.openedFrom.size(), "the wait must grow after every failure");

        processor.advance(INITIAL_BACKOFF_MILLIS);
        runner.run(1, false, false);
        assertEquals(3, processor.openedFrom.size());
        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 1);

        // the successful trigger gives up the wait, so the next one reads the remaining event without waiting
        runner.run(1, false, false);
        runner.assertTransferCount(CaptureChangeMongoDB.REL_SUCCESS, 2);
        runner.getStateManager().assertStateEquals(StateKeys.RESUME_TOKEN, "t2", Scope.CLUSTER);
    }

    @Test
    void aPipelineIsCheckedAgainstWhatMongoDBAllowsOnAChangeStream() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.setProperty(CaptureChangeMongoDB.PIPELINE, "[{\"$match\": {\"operationType\": \"insert\"}}]");
        runner.assertValid();

        runner.setProperty(CaptureChangeMongoDB.PIPELINE, "[{\"$match\": {\"operationType\": \"insert\"}}, {\"$project\": {\"ns\": 1}}]");
        runner.assertValid();

        runner.setProperty(CaptureChangeMongoDB.PIPELINE, "[{\"$group\": {\"_id\": \"$ns\"}}]");
        runner.assertNotValid();

        runner.setProperty(CaptureChangeMongoDB.PIPELINE, "{\"$match\": {}}");
        runner.assertNotValid();

        runner.setProperty(CaptureChangeMongoDB.PIPELINE, "[{\"$match\": {}, \"$project\": {}}]");
        runner.assertNotValid();

        runner.setProperty(CaptureChangeMongoDB.PIPELINE, "not json");
        runner.assertNotValid();
    }

    @Test
    void collectionNameIsNotAskedForWithDatabaseScope() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN, "t1"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.removeProperty(CaptureChangeMongoDB.COLLECTION_NAME);
        runner.setProperty(CaptureChangeMongoDB.WATCH_SCOPE, WatchScope.DATABASE);
        runner.assertValid();

        runner.run();

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangeMongoDB.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CaptureChangeMongoDB.ATTRIBUTE_DATABASE, "lab");
        flowFile.assertAttributeNotExists(CaptureChangeMongoDB.ATTRIBUTE_COLLECTION);
    }

    /**
     * A resume token belongs to the scope it was read from. Continuing with it somewhere else would silently read
     * the wrong changes, so the processor refuses to start and says what to do.
     */
    @Test
    void aStoredPositionFromAnotherScopeStopsTheProcessor() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN, "t1"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));
        runner.getStateManager().setState(
                Map.of(StateKeys.RESUME_TOKEN, "stored-token", StateKeys.STREAM_SOURCE, "collection:lab.invoices"), Scope.CLUSTER);

        final Throwable failure = assertThrows(Throwable.class, runner::run);
        assertTrue(messageOf(failure).contains("collection:lab.invoices"), messageOf(failure));
        assertTrue(messageOf(failure).contains("Clear the state"), messageOf(failure));
        assertEquals(List.of(), processor.openedFrom, "the stream must not be opened at all");
    }

    @Test
    void theScopeTheTokenBelongsToIsStoredWithIt() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN, "t1"));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.run();

        runner.getStateManager().assertStateEquals(StateKeys.STREAM_SOURCE, "collection:lab.orders", Scope.CLUSTER);
    }

    /**
     * The initial snapshot reads the documents of one collection, so it cannot be combined with database scope.
     */
    @Test
    void theInitialSnapshotIsRefusedWithDatabaseScope() throws Exception {
        processor.addCursor(cursor(INITIAL_TOKEN));
        final TestRunner runner = createRunner(new MockRecordWriter("header", false));

        runner.setProperty(CaptureChangeMongoDB.START_POSITION, CaptureChangeMongoDB.START_POSITION_INITIAL_SNAPSHOT.getValue());
        runner.assertValid();

        runner.removeProperty(CaptureChangeMongoDB.COLLECTION_NAME);
        runner.setProperty(CaptureChangeMongoDB.WATCH_SCOPE, WatchScope.DATABASE);
        runner.assertNotValid();
    }

    private static String messageOf(final Throwable failure) {
        final StringBuilder messages = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            messages.append(current.getMessage()).append(' ');
        }
        return messages.toString();
    }

    private TestRunner createRunner(final MockRecordWriter writer) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final FakeMongoDBClientService clientService = new FakeMongoDBClientService();
        runner.addControllerService("client-service", clientService);
        runner.enableControllerService(clientService);

        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(CaptureChangeMongoDB.CLIENT_SERVICE, "client-service");
        runner.setProperty(CaptureChangeMongoDB.RECORD_WRITER, "record-writer");
        runner.setProperty(CaptureChangeMongoDB.DATABASE_NAME, "lab");
        runner.setProperty(CaptureChangeMongoDB.COLLECTION_NAME, "orders");
        return runner;
    }

    private static FakeChangeStreamCursor cursor(final String initialResumeToken, final String... eventTokens) {
        return new FakeChangeStreamCursor(initialResumeToken, Stream.of(eventTokens).map(ChangeEvents::insert).toList());
    }

    private static MongoCommandException historyLost() {
        return new MongoCommandException(BsonDocument.parse("""
                {
                  "ok": 0,
                  "code": 286,
                  "codeName": "ChangeStreamHistoryLost",
                  "errmsg": "Resume of change stream was not possible, as the resume point may no longer be in the oplog."
                }
                """), new ServerAddress());
    }

    private static MongoCommandException changeStreamFatalError(final String message) {
        return new MongoCommandException(new BsonDocument()
                .append("ok", new BsonInt32(0))
                .append("code", new BsonInt32(280))
                .append("codeName", new BsonString("ChangeStreamFatalError"))
                .append("errmsg", new BsonString(message)), new ServerAddress());
    }

    private static class TestableProcessor extends CaptureChangeMongoDB {

        private final List<Supplier<MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>>>> openings = new ArrayList<>();
        private final List<FakeChangeStreamCursor> opened = new ArrayList<>();
        private final List<String> openedFrom = new ArrayList<>();
        private long now = System.currentTimeMillis();
        private int index;

        void addCursor(final FakeChangeStreamCursor cursor) {
            openings.add(() -> {
                opened.add(cursor);
                return cursor;
            });
        }

        void addOpenFailure(final RuntimeException failure) {
            openings.add(() -> {
                throw failure;
            });
        }

        void advance(final long millis) {
            now += millis;
        }

        @Override
        protected MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> openCursor(final String resumeTokenData) {
            openedFrom.add(resumeTokenData);
            return openings.get(index++).get();
        }

        @Override
        protected long currentTimeMillis() {
            return now;
        }
    }
}
