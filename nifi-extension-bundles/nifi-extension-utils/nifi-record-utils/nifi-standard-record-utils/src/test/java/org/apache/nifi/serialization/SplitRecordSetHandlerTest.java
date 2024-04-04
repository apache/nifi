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
package org.apache.nifi.serialization;

import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitRecordSetHandlerTest {
    private static final int MAXIMUM_CHUNK_SIZE = 3;
    private static final Throwable CAUSE = new RuntimeException("Test");

    private TestableSplitRecordSetHandler testSubject;
    private SplitRecordSetHandler.RecordHandlerResult result;

    @BeforeEach
    public void setUp() {
        testSubject = new TestableSplitRecordSetHandler(MAXIMUM_CHUNK_SIZE);
    }

    @Test
    public void testEmptyBatch() throws Exception {
        executeHandlerWithRecordSet(0);

        assertSuccessfulHandling(0);
        assertChunkSizeIs(0);
    }

    @Test
    public void testSuccessWhenSingleNonFullChunk() throws Exception {
        setHandlerToSuccess();
        executeHandlerWithRecordSet(2);

        assertSuccessfulHandling(1);
        assertChunkSizeIs(2);
    }

    @Test
    public void testFailureWhenSingleNonFullChunk() throws Exception {
        setHandlerToFail();
        executeHandlerWithRecordSet(2);

        assertChunkSizeIs(2);
        assertFailedHandling(0);
        Assertions.assertEquals(1, testSubject.getHandleChunkCalls().size()); // Tried once
    }

    @Test
    public void testSuccessWhenSingleFullChunk() throws Exception {
        setHandlerToSuccess();
        executeHandlerWithRecordSet(MAXIMUM_CHUNK_SIZE);

        assertSuccessfulHandling(1);
        assertChunkSizeIs(MAXIMUM_CHUNK_SIZE);
    }

    @Test
    public void testFailureWhenSingleFullChunk() throws Exception {
        setHandlerToFail();
        executeHandlerWithRecordSet(MAXIMUM_CHUNK_SIZE);

        assertChunkSizeIs(MAXIMUM_CHUNK_SIZE);
        assertFailedHandling(0);
        Assertions.assertEquals(1, testSubject.getHandleChunkCalls().size()); // Tried once
    }

    @Test
    public void testSuccessWhenMultipleChunks() throws Exception {
        setHandlerToSuccess();
        executeHandlerWithRecordSet(5);

        assertSuccessfulHandling(2);
        assertChunkSizeIs(5);
    }

    @Test
    public void testFailureWhenMultipleChunks() throws Exception {
        setHandlerToSequence(true, false);
        executeHandlerWithRecordSet(3 * MAXIMUM_CHUNK_SIZE - 1);

        assertFailedHandling(1);
        Assertions.assertEquals(2, testSubject.getHandleChunkCalls().size());
    }

    @Test
    public void testSuccessWhenNumerousChunks() throws Exception {
        setHandlerToSuccess();
        executeHandlerWithRecordSet(31);

        assertSuccessfulHandling(11);
        Assertions.assertEquals(31, testSubject.getChunks().size());
    }

    @Test
    public void testStartAtLaterPoint() throws Exception {
        setHandlerToSuccess();
        executeHandlerWithRecordSet(MAXIMUM_CHUNK_SIZE * 3 - 1, 2);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertNull(result.getThrowable());
        Assertions.assertEquals(3, result.getSuccessfulChunks()); //
        Assertions.assertIterableEquals(Arrays.asList(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE), testSubject.getHandleChunkCalls());
        Assertions.assertEquals(MAXIMUM_CHUNK_SIZE * 3 - 1, testSubject.getChunks().size());
    }

    @Test
    public void testStartAtAPointAfterLastChunk() throws Exception {
        setHandlerToSuccess();
        executeHandlerWithRecordSet(MAXIMUM_CHUNK_SIZE * 3 - 1, 9);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertNull(result.getThrowable());
        Assertions.assertEquals(3, result.getSuccessfulChunks()); //
        Assertions.assertIterableEquals(Arrays.asList(Boolean.TRUE, Boolean.TRUE, Boolean.TRUE), testSubject.getHandleChunkCalls());
    }

    @Test
    public void testStartAtLaterPointAndFail() throws Exception {
        setHandlerToSequence(true, true, false);
        executeHandlerWithRecordSet(MAXIMUM_CHUNK_SIZE * 4 - 1, 1);

        assertFailedHandling(2);
        Assertions.assertIterableEquals(Arrays.asList(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE), testSubject.getHandleChunkCalls());
    }

    private void executeHandlerWithRecordSet(final int recordSetSize) throws Exception {
        result = testSubject.handle(getRecordSet(recordSetSize));
    }

    private void executeHandlerWithRecordSet(final int recordSetSize, final int alreadyProcessedChunks) throws Exception {
        result = testSubject.handle(getRecordSet(recordSetSize), alreadyProcessedChunks);
    }

    private void assertSuccessfulHandling(final int expectedNumberOfSuccessfulChunks) {
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertNull(result.getThrowable());
        Assertions.assertEquals(expectedNumberOfSuccessfulChunks, result.getSuccessfulChunks());
        final List<Boolean> expectedCalls = new ArrayList<>();

        for (int i = 0; i < expectedNumberOfSuccessfulChunks; i++) {
            expectedCalls.add(false);
        }

        Assertions.assertIterableEquals(expectedCalls, testSubject.getHandleChunkCalls());
    }

    private void assertFailedHandling(final int expectedNumberOfSuccessfulChunks) {
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(CAUSE, result.getThrowable().getCause());
        Assertions.assertEquals(expectedNumberOfSuccessfulChunks, result.getSuccessfulChunks());
    }

    private void assertChunkSizeIs(final int expectedSize) {
        Assertions.assertEquals(expectedSize, testSubject.getChunks().size());
    }

    private void setHandlerToSuccess() {
        testSubject.setUpHandleChunkResults(true);
    }

    private void setHandlerToFail() {
        testSubject.setUpHandleChunkResults(false);
    }

    private void setHandlerToSequence(Boolean... sequence) {
        testSubject.setUpHandleChunkResults(sequence);
    }

    private RecordSet getRecordSet(int size) {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("payload", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);
        final List<Record> records = new ArrayList<>();

        for (int i = 1; i <= size; i++) {
            final Map<String, Object> values = new HashMap<>();
            values.put("id", 1);
            values.put("payload", "value");
            final Record record = new MapRecord(schema, values);
            records.add(record);
        }

        final RecordSet recordSet = new ListRecordSet(schema, records);
        return recordSet;
    }

    private static class TestableSplitRecordSetHandler extends SplitRecordSetHandler {
        private final List<Record> chunks = new ArrayList<>();
        private final List<Boolean> handleChunkCalls = new ArrayList<>();
        private List<Boolean> handleChunkResults;

        protected TestableSplitRecordSetHandler(final int maximumChunkSize) {
            super(maximumChunkSize);
        }

        @Override
        protected void handleChunk(final boolean wasBatchAlreadyProcessed) throws SplitRecordSetHandlerException {
            handleChunkCalls.add(wasBatchAlreadyProcessed);
            final int index = Math.min(handleChunkCalls.size() - 1, handleChunkResults.size() - 1);

            if (!handleChunkResults.get(index)) {
                throw new SplitRecordSetHandlerException(CAUSE);
            }
        }

        @Override
        protected void addToChunk(final Record record) {
            chunks.add(record);
        }

        List<Record> getChunks() {
            return chunks;
        }

        List<Boolean> getHandleChunkCalls() {
            return handleChunkCalls;
        }

        void setUpHandleChunkResults(final Boolean... results) {
            handleChunkResults = Arrays.asList(results);
        }
    }
}