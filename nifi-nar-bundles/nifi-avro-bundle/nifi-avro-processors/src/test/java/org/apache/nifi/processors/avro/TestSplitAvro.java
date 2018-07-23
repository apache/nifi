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
package org.apache.nifi.processors.avro;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSplitAvro {

    static final String META_KEY1 = "metaKey1";
    static final String META_KEY2 = "metaKey2";
    static final String META_KEY3 = "metaKey3";

    static final String META_VALUE1 = "metaValue1";
    static final long META_VALUE2 = Long.valueOf(1234567);
    static final String META_VALUE3 = "metaValue3";

    private Schema schema;
    private ByteArrayOutputStream users;

    @Before
    public void setup() throws IOException {
        this.users = new ByteArrayOutputStream();
        this.schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
        createUsers(100, users);
    }

    void createUsers(final int numUsers, final ByteArrayOutputStream users) throws IOException {
        final List<GenericRecord> userList = new ArrayList<>();
        for (int i=0; i < numUsers; i++) {
            final GenericRecord user = new GenericData.Record(schema);
            user.put("name", "name" + i);
            user.put("favorite_number", i);
            userList.add(user);
        }

        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))) {
            dataFileWriter.setMeta(META_KEY1, META_VALUE1);
            dataFileWriter.setMeta(META_KEY2, META_VALUE2);
            dataFileWriter.setMeta(META_KEY3, META_VALUE3.getBytes("UTF-8"));

            dataFileWriter.create(schema, users);
            for (GenericRecord user : userList) {
                dataFileWriter.append(user);
            }
            dataFileWriter.flush();
        }
    }

    @Test
    public void testRecordSplitWithNoIncomingRecords() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        createUsers(0, out);

        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 0);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);
    }

    @Test
    public void testRecordSplitDatafileOutputWithSingleRecords() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());

        final String filename = "users.avro";
        runner.enqueue(users.toByteArray(), new HashMap<String,String>() {{
            put(CoreAttributes.FILENAME.key(), filename);
        }});
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 100);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);
        final MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(SplitAvro.REL_ORIGINAL).get(0);
        originalFlowFile.assertAttributeExists(FRAGMENT_ID.key());
        originalFlowFile.assertAttributeEquals(FRAGMENT_COUNT.key(), "100");
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);
        checkDataFileSplitSize(flowFiles, 1, true);
        final String fragmentIdentifier = flowFiles.get(0).getAttribute("fragment.identifier");
        IntStream.range(0, flowFiles.size()).forEach((i) -> {
            MockFlowFile flowFile = flowFiles.get(i);
            assertEquals(i, Integer.parseInt(flowFile.getAttribute("fragment.index")));
            assertEquals(fragmentIdentifier, flowFile.getAttribute("fragment.identifier"));
            assertEquals(flowFiles.size(), Integer.parseInt(flowFile.getAttribute(FRAGMENT_COUNT.key())));
            assertEquals(filename, flowFile.getAttribute("segment.original.filename"));
        });
    }

    @Test
    public void testRecordSplitDatafileOutputWithMultipleRecords() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.OUTPUT_SIZE, "20");

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 5);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        runner.getFlowFilesForRelationship(SplitAvro.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT.key(), "5");
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);
        checkDataFileSplitSize(flowFiles, 20, true);
    }

    @Test
    public void testRecordSplitDatafileOutputWithSplitSizeLarger() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.OUTPUT_SIZE, "200");

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 1);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        runner.getFlowFilesForRelationship(SplitAvro.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT.key(), "1");
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);
        checkDataFileSplitSize(flowFiles, 100, true);
    }

    @Test
    public void testRecordSplitDatafileOutputWithoutMetadata() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.TRANSFER_METADATA, "false");

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 100);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        runner.getFlowFilesForRelationship(SplitAvro.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT.key(), "100");
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);
        checkDataFileSplitSize(flowFiles, 1, false);

        for (final MockFlowFile flowFile : flowFiles) {
            try (final ByteArrayInputStream in = new ByteArrayInputStream(flowFile.toByteArray());
                 final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {
                Assert.assertFalse(reader.getMetaKeys().contains(META_KEY1));
                Assert.assertFalse(reader.getMetaKeys().contains(META_KEY2));
                Assert.assertFalse(reader.getMetaKeys().contains(META_KEY3));
            }
        }
    }

    @Test
    public void testRecordSplitBareOutputWithSingleRecords() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.OUTPUT_STRATEGY, SplitAvro.BARE_RECORD_OUTPUT);

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 100);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        runner.getFlowFilesForRelationship(SplitAvro.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT.key(), "100");
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);

        checkBareRecordsSplitSize(flowFiles, 1, true);
    }

    @Test
    public void testRecordSplitBareOutputWithMultipleRecords() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.OUTPUT_STRATEGY, SplitAvro.BARE_RECORD_OUTPUT);
        runner.setProperty(SplitAvro.OUTPUT_SIZE, "20");

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 5);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        runner.getFlowFilesForRelationship(SplitAvro.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT.key(), "5");
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);

        checkBareRecordsSplitSize(flowFiles, 20, true);
    }

    @Test
    public void testRecordSplitBareOutputWithoutMetadata() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.OUTPUT_STRATEGY, SplitAvro.BARE_RECORD_OUTPUT);
        runner.setProperty(SplitAvro.OUTPUT_SIZE, "20");
        runner.setProperty(SplitAvro.TRANSFER_METADATA, "false");

        runner.enqueue(users.toByteArray());
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 5);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 0);

        runner.getFlowFilesForRelationship(SplitAvro.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT.key(), "5");
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SplitAvro.REL_SPLIT);

        checkBareRecordsSplitSize(flowFiles, 20, false);

        for (final MockFlowFile flowFile : flowFiles) {
            Assert.assertFalse(flowFile.getAttributes().containsKey(META_KEY1));
            Assert.assertFalse(flowFile.getAttributes().containsKey(META_KEY2));
            Assert.assertFalse(flowFile.getAttributes().containsKey(META_KEY3));
        }
    }

    @Test
    public void testFailure() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitAvro());
        runner.setProperty(SplitAvro.OUTPUT_SIZE, "200");

        runner.enqueue("not avro".getBytes("UTF-8"));
        runner.run();

        runner.assertTransferCount(SplitAvro.REL_SPLIT, 0);
        runner.assertTransferCount(SplitAvro.REL_ORIGINAL, 0);
        runner.assertTransferCount(SplitAvro.REL_FAILURE, 1);
    }

    private void checkBareRecordsSplitSize(final List<MockFlowFile> flowFiles, final int expectedRecordsPerSplit, final boolean checkMetadata) throws IOException {
        for (final MockFlowFile flowFile : flowFiles) {
            try (final ByteArrayInputStream in = new ByteArrayInputStream(flowFile.toByteArray())) {
                final DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
                final Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);

                int count = 0;
                GenericRecord record = reader.read(null, decoder);
                try {
                    while (record != null) {
                        Assert.assertNotNull(record.get("name"));
                        Assert.assertNotNull(record.get("favorite_number"));
                        count++;
                        record = reader.read(record, decoder);
                    }
                } catch (EOFException eof) {
                    // expected
                }
                assertEquals(expectedRecordsPerSplit, count);
            }

            if (checkMetadata) {
                Assert.assertTrue(flowFile.getAttributes().containsKey(META_KEY1));
                Assert.assertTrue(flowFile.getAttributes().containsKey(META_KEY2));
                Assert.assertTrue(flowFile.getAttributes().containsKey(META_KEY3));
            }
        }
    }

    private void checkDataFileSplitSize(List<MockFlowFile> flowFiles, int expectedRecordsPerSplit, boolean checkMetadata) throws IOException {
        for (final MockFlowFile flowFile : flowFiles) {
            try (final ByteArrayInputStream in = new ByteArrayInputStream(flowFile.toByteArray());
                final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {

                int count = 0;
                GenericRecord record = null;
                while (reader.hasNext()) {
                    record = reader.next(record);
                    Assert.assertNotNull(record.get("name"));
                    Assert.assertNotNull(record.get("favorite_number"));
                    count++;
                }
                assertEquals(expectedRecordsPerSplit, count);

                if (checkMetadata) {
                    assertEquals(META_VALUE1, reader.getMetaString(META_KEY1));
                    assertEquals(META_VALUE2, reader.getMetaLong(META_KEY2));
                    assertEquals(META_VALUE3, new String(reader.getMeta(META_KEY3), "UTF-8"));
                }
            }
        }
    }

    private void checkDataFileTotalSize(List<MockFlowFile> flowFiles, int expectedTotalRecords) throws IOException {
        int count = 0;
        for (final MockFlowFile flowFile : flowFiles) {
            try (final ByteArrayInputStream in = new ByteArrayInputStream(flowFile.toByteArray());
                 final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {

                GenericRecord record = null;
                while (reader.hasNext()) {
                    record = reader.next(record);
                    Assert.assertNotNull(record.get("name"));
                    Assert.assertNotNull(record.get("favorite_number"));
                    count++;
                }
            }
        }
        assertEquals(expectedTotalRecords, count);
    }

}
