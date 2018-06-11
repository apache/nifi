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

package org.apache.nifi.processors.kudu;

import java.util.stream.Collectors;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.apache.nifi.util.Tuple;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.nifi.processors.kudu.TestPutKudu.ResultCode.OK;
import static org.apache.nifi.processors.kudu.TestPutKudu.ResultCode.FAIL;
import static org.apache.nifi.processors.kudu.TestPutKudu.ResultCode.EXCEPTION;

public class TestPutKudu {

    public static final String DEFAULT_TABLE_NAME = "Nifi-Kudu-Table";
    public static final String DEFAULT_MASTERS = "testLocalHost:7051";
    public static final String SKIP_HEAD_LINE = "false";
    public static final String TABLE_SCHEMA = "id,stringVal,num32Val,doubleVal";

    private TestRunner testRunner;
    private MockPutKudu processor;
    private MockRecordParser readerFactory;

    @Before
    public void setUp() {
        processor = new MockPutKudu();
        testRunner = TestRunners.newTestRunner(processor);
        setUpTestRunner(testRunner);
    }

    private void setUpTestRunner(TestRunner testRunner) {
        testRunner.setProperty(PutKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        testRunner.setProperty(PutKudu.KUDU_MASTERS, DEFAULT_MASTERS);
        testRunner.setProperty(PutKudu.SKIP_HEAD_LINE, SKIP_HEAD_LINE);
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");
        testRunner.setProperty(PutKudu.INSERT_OPERATION, OperationType.INSERT.toString());
    }

    @After
    public void close() {
        testRunner = null;
    }

    private void createRecordReader(int numOfRecord) throws InitializationException {

        readerFactory = new MockRecordParser();
        readerFactory.addSchemaField("id", RecordFieldType.INT);
        readerFactory.addSchemaField("stringVal", RecordFieldType.STRING);
        readerFactory.addSchemaField("num32Val", RecordFieldType.INT);
        readerFactory.addSchemaField("doubleVal", RecordFieldType.DOUBLE);

        for (int i=0; i < numOfRecord; i++) {
            readerFactory.addRecord(i, "val_" + i, 1000 + i, 100.88 + i);
        }

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
    }

    @Test
    public void testWriteKuduWithDefaults() throws IOException, InitializationException {
        createRecordReader(100);

        final String filename = "testWriteKudu-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);

        // verify the successful flow file has the expected content & attributes
        final MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), filename);
        mockFlowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "100");
        mockFlowFile.assertContentEquals("trigger");

        // verify we generated a provenance event
        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        Assert.assertEquals(1, provEvents.size());

        // verify it was a SEND event with the correct URI
        final ProvenanceEventRecord provEvent = provEvents.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
    }

    @Test
    public void testInvalidReaderShouldRouteToFailure() throws InitializationException, SchemaNotFoundException, MalformedRecordException, IOException {
        createRecordReader(0);

        // simulate throwing an IOException when the factory creates a reader which is what would happen when
        // invalid Avro is passed to the Avro reader factory
        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenThrow(new IOException("NOT AVRO"));

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");

        final String filename = "testInvalidAvroShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }

    @Test
    public void testValidSchemaShouldBeSuccessful() throws InitializationException, IOException {
        createRecordReader(10);
        final String filename = "testValidSchemaShouldBeSuccessful-" + System.currentTimeMillis();

        // don't provide my.schema as an attribute
        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("my.schema", TABLE_SCHEMA);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
    }

    @Test
    public void testMalformedRecordExceptionFromReaderShouldRouteToFailure() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        createRecordReader(10);

        final RecordReader recordReader = mock(RecordReader.class);
        when(recordReader.nextRecord()).thenThrow(new MalformedRecordException("ERROR"));

        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }

    @Test
    public void testReadAsStringAndWriteAsInt() throws InitializationException, IOException {
        createRecordReader(0);
        // add the favorite color as a string
        readerFactory.addRecord(1, "name0", "0", "89.89");

        final String filename = "testReadAsStringAndWriteAsInt-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
    }

    @Test
    public void testMissingColumInReader() throws InitializationException, IOException {
        createRecordReader(0);
        readerFactory.addRecord( "name0", "0", "89.89"); //missing id

        final String filename = "testMissingColumInReader-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }

    @Test
    public void testSkipHeadLineTrue() throws InitializationException, IOException {
        createRecordReader(100);
        testRunner.setProperty(PutKudu.SKIP_HEAD_LINE, "true");

        final String filename = "testSkipHeadLineTrue-" + System.currentTimeMillis();

        final Map<String,String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);

        MockFlowFile flowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);
        flowFiles.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "99");
    }

    @Test
    public void testInsertManyFlowFiles() throws Exception {
        createRecordReader(50);
        final String content1 = "{ \"field1\" : \"value1\", \"field2\" : \"valu11\" }";
        final String content2 = "{ \"field1\" : \"value1\", \"field2\" : \"value11\" }";
        final String content3 = "{ \"field1\" : \"value3\", \"field2\" : \"value33\" }";

        testRunner.enqueue(content1.getBytes());
        testRunner.enqueue(content2.getBytes());
        testRunner.enqueue(content3.getBytes());

        testRunner.run(3);

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS);

        flowFiles.get(0).assertContentEquals(content1.getBytes());
        flowFiles.get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");

        flowFiles.get(1).assertContentEquals(content2.getBytes());
        flowFiles.get(1).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");

        flowFiles.get(2).assertContentEquals(content3.getBytes());
        flowFiles.get(2).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testUpsertFlowFiles() throws Exception {
        createRecordReader(50);
        testRunner.setProperty(PutKudu.INSERT_OPERATION, OperationType.UPSERT.toString());
        testRunner.enqueue("string".getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);

        flowFile.assertContentEquals("string".getBytes());
        flowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testBuildRow() {
        buildPartialRow((long) 1, "foo", (short) 10);
    }

    @Test
    public void testBuildPartialRowNullable() {
        buildPartialRow((long) 1, null, (short) 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildPartialRowNullPrimaryKey() {
        buildPartialRow(null, "foo", (short) 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildPartialRowNotNullable() {
        buildPartialRow((long) 1, "foo", null);
    }

    private void buildPartialRow(Long id, String name, Short age) {
        final Schema kuduSchema = new Schema(Arrays.asList(
            new ColumnSchemaBuilder("id", Type.INT64).key(true).build(),
            new ColumnSchemaBuilder("name", Type.STRING).nullable(true).build(),
            new ColumnSchemaBuilder("age", Type.INT16).nullable(false).build(),
            new ColumnSchemaBuilder("updated_at", Type.UNIXTIME_MICROS).nullable(false).build(),
            new ColumnSchemaBuilder("score", Type.DECIMAL).nullable(true).typeAttributes(
                new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(9).scale(0).build()
            ).build()));

        final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("id", RecordFieldType.BIGINT.getDataType()),
            new RecordField("name", RecordFieldType.STRING.getDataType()),
            new RecordField("age", RecordFieldType.SHORT.getDataType()),
            new RecordField("updated_at", RecordFieldType.BIGINT.getDataType()),
            new RecordField("score", RecordFieldType.LONG.getDataType())));

        Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("age", age);
        values.put("updated_at", System.currentTimeMillis() * 1000);
        values.put("score", 10000L);
        new PutKudu().buildPartialRow(
            kuduSchema,
            kuduSchema.newPartialRow(),
            new MapRecord(schema, values),
            schema.getFieldNames()
        );
    }

    private Tuple<Insert, OperationResponse> insert(boolean success) {
        Insert insert = mock(Insert.class);
        OperationResponse response = mock(OperationResponse.class, Mockito.RETURNS_DEEP_STUBS);
        when(response.hasRowError()).thenReturn(!success);
        if (!success) {
            when(response.getRowError().getOperation()).thenReturn(insert);
        }
        return new Tuple<>(insert, response);
    }

    enum ResultCode {
        OK,
        FAIL,
        EXCEPTION
    }

    private LinkedList<OperationResponse> queueInsert(MockPutKudu kudu, KuduSession session, boolean sync, ResultCode... results) throws Exception {
        LinkedList<OperationResponse> responses = new LinkedList<>();
        for (ResultCode result : results) {
            boolean ok = result == OK;
            Tuple<Insert, OperationResponse> tuple = insert(ok);
            kudu.queue(tuple.getKey());

            if (result == EXCEPTION) {
                when(session.apply(tuple.getKey())).thenThrow(mock(KuduException.class));
                // Stop processing the rest of the records on the first exception
                break;
            } else {
                responses.add(tuple.getValue());
                if (sync) {
                    when(session.apply(tuple.getKey())).thenReturn(ok ? null : tuple.getValue());

                    // In AUTO_FLUSH_SYNC mode, PutKudu immediately knows when an operation has failed.
                    // In that case, it does not process the rest of the records in the FlowFile.
                    if (result == FAIL) break;
                }
            }
        }
        return responses;
    }

    private static <T> void stubSlices(OngoingStubbing<T> stubbing, List<T> slices) {
        for (T slice : slices) {
            stubbing = stubbing.thenReturn(slice);
        }
    }

    private void testKuduPartialFailure(FlushMode flushMode, int batchSize) throws Exception {
        final int numFlowFiles = 4;
        final int numRecordsPerFlowFile = 3;
        final ResultCode[][] flowFileResults = new ResultCode[][] {
            new ResultCode[]{OK, OK, FAIL},

            // The last operation will not be submitted to Kudu if flush mode is AUTO_FLUSH_SYNC
            new ResultCode[]{OK, FAIL, OK},

            // Everything's okay
            new ResultCode[]{OK, OK, OK},

            // The last operation will not be submitted due to an exception from apply() call
            new ResultCode[]{OK, EXCEPTION, OK},
        };

        KuduSession session = mock(KuduSession.class);
        when(session.getFlushMode()).thenReturn(flushMode);
        MockPutKudu putKudu = new MockPutKudu(session);

        List<List<OperationResponse>> flowFileResponses = new ArrayList<>();
        boolean sync = flushMode == FlushMode.AUTO_FLUSH_SYNC;
        for (ResultCode[] results : flowFileResults) {
            flowFileResponses.add(queueInsert(putKudu, session, sync, results));
        }

        switch (flushMode) {
            case AUTO_FLUSH_SYNC:
                // flush() or close() returns an empty list
                when(session.close()).thenReturn(Collections.emptyList());
                break;
            case AUTO_FLUSH_BACKGROUND:
                // close() will be called for each batch of FlowFiles, however we do not check
                // the return value of it. Instead, we should check the pending errors of the session
                // as buffered operations may have already been flushed.
                when(session.close()).thenReturn(Collections.emptyList());

                List<RowErrorsAndOverflowStatus> pendingErrorResponses = new ArrayList<>();
                while (!flowFileResponses.isEmpty()) {
                    int sliceSize = Math.min(batchSize, flowFileResponses.size());
                    List<List<OperationResponse>> slice = flowFileResponses.subList(0, sliceSize);

                    RowErrorsAndOverflowStatus pendingErrorResponse = mock(RowErrorsAndOverflowStatus.class);
                    RowError[] rowErrors = slice.stream()
                        .flatMap(List::stream)
                        .filter(OperationResponse::hasRowError)
                        .map(OperationResponse::getRowError)
                        .toArray(RowError[]::new);
                    when(pendingErrorResponse.getRowErrors()).thenReturn(rowErrors);
                    pendingErrorResponses.add(pendingErrorResponse);

                    flowFileResponses = flowFileResponses.subList(sliceSize, flowFileResponses.size());
                }

                stubSlices(when(session.getPendingErrors()), pendingErrorResponses);
                break;
            case MANUAL_FLUSH:
                // close() will be called at the end of a batch, but flush() will also be called
                // whenever the mutation buffer of KuduSession becomes full. In PutKudu, we set
                // the size of the mutation buffer to match batchSize, so flush() is called only
                // when a FlowFile more than one record.
                List<List<OperationResponse>> flushes = new ArrayList<>();
                List<List<OperationResponse>> closes = new ArrayList<>();

                while (!flowFileResponses.isEmpty()) {
                    int sliceSize = Math.min(batchSize, flowFileResponses.size());
                    List<List<OperationResponse>> slice = flowFileResponses.subList(0, sliceSize);
                    flowFileResponses = flowFileResponses.subList(sliceSize, flowFileResponses.size());

                    List<OperationResponse> batch = new ArrayList<>();
                    for (OperationResponse response : slice.stream().flatMap(List::stream).collect(Collectors.toList())) {
                        if (batch.size() == batchSize) {
                            flushes.add(batch);
                            batch = new ArrayList<>();
                        }
                        batch.add(response);
                    }
                    if (flowFileResponses.isEmpty() && batch.size() == batchSize) {
                        // To handle the case where PutKudu ends the batch with flush()
                        // instead of close() due to the exception from the subsequent apply call.
                        flushes.add(batch);
                    } else if (batch.size() > 0) {
                        closes.add(batch);
                    }

                    if (!flushes.isEmpty()) stubSlices(when(session.flush()), flushes);
                    if (!closes.isEmpty()) stubSlices(when(session.close()), closes);
                }
                break;
        }

        testRunner = TestRunners.newTestRunner(putKudu);
        createRecordReader(numRecordsPerFlowFile);
        setUpTestRunner(testRunner);
        testRunner.setProperty(PutKudu.FLUSH_MODE, flushMode.name());
        testRunner.setProperty(PutKudu.BATCH_SIZE, String.valueOf(batchSize));

        IntStream.range(0, numFlowFiles).forEach(i -> testRunner.enqueue(""));
        testRunner.run(numFlowFiles);

        testRunner.assertTransferCount(PutKudu.REL_FAILURE, 3);
        List<MockFlowFile> failedFlowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_FAILURE);
        failedFlowFiles.get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "2");
        failedFlowFiles.get(1).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, sync ? "1" : "2");
        failedFlowFiles.get(2).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "1");

        testRunner.assertTransferCount(PutKudu.REL_SUCCESS, 1);
        testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "3");
    }

    private void testKuduPartialFailure(FlushMode flushMode) throws Exception {
      // Test against different batch sizes (up until the point where every record can be buffered at once)
      for (int i = 1; i <= 11; i++) {
          testKuduPartialFailure(flushMode, i);
      }
    }

    @Test
    public void testKuduPartialFailuresOnAutoFlushSync() throws Exception {
        testKuduPartialFailure(FlushMode.AUTO_FLUSH_SYNC);
    }

    @Test
    public void testKuduPartialFailuresOnAutoFlushBackground() throws Exception {
        testKuduPartialFailure(FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    @Test
    public void testKuduPartialFailuresOnManualFlush() throws Exception {
        testKuduPartialFailure(FlushMode.MANUAL_FLUSH);
    }
}
