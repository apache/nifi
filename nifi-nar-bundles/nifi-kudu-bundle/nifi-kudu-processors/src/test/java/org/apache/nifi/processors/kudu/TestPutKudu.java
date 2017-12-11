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
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

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
        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
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

        final RecordReader recordReader = Mockito.mock(RecordReader.class);
        when(recordReader.nextRecord()).thenThrow(new MalformedRecordException("ERROR"));

        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
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
}
