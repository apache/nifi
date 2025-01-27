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
package org.apache.nifi.processors.standard;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.bin.InsertionLocation;
import org.apache.nifi.processors.standard.merge.AttributeStrategyUtil;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMergeContent {

    /**
     * This test will verify that if we have a FlowFile larger than the Max Size for a Bin, it will go into its
     * own bin and immediately be processed as its own bin.
     */
    @Test
    public void testFlowFileLargerThanBin() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_BIN_PACK);
        runner.setProperty(MergeContent.MIN_ENTRIES, "2");
        runner.setProperty(MergeContent.MAX_ENTRIES, "2");
        runner.setProperty(MergeContent.MIN_SIZE, "1 KB");
        runner.setProperty(MergeContent.MAX_SIZE, "5 KB");

        runner.enqueue(new byte[1026]); // add flowfile that fits within the bin limits
        runner.enqueue(new byte[1024 * 6]); // add flowfile that is larger than the bin limit
        runner.run(2); // run twice so that we have a chance to create two bins (though we shouldn't create 2, because only 1 bin will be full)

        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 1);
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        assertEquals(runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst().getAttribute(CoreAttributes.UUID.key()),
                runner.getFlowFilesForRelationship(MergeContent.REL_ORIGINAL).getFirst().getAttribute(MergeContent.MERGE_UUID_ATTRIBUTE));

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assertEquals(1024 * 6, bundle.getSize());

        // Queue should not be empty because the first FlowFile will be transferred back to the input queue
        // when we run out @OnStopped logic, since it won't be transferred to any bin.
        runner.assertQueueNotEmpty();
    }

    @Test
    public void testSimpleAvroConcat() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");
        runner.setProperty(MergeContent.MIN_ENTRIES, "3");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_AVRO);

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/TestMergeContent/user.avsc"));

        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        final GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");

        final GenericRecord user3 = new GenericData.Record(schema);
        user3.put("name", "John");
        user3.put("favorite_number", 5);
        user3.put("favorite_color", "blue");

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        final ByteArrayOutputStream out1 = serializeAvroRecord(schema, user1, datumWriter);
        final ByteArrayOutputStream out2 = serializeAvroRecord(schema, user2, datumWriter);
        final ByteArrayOutputStream out3 = serializeAvroRecord(schema, user3, datumWriter);

        runner.enqueue(out1.toByteArray());
        runner.enqueue(out2.toByteArray());
        runner.enqueue(out3.toByteArray());

        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");

        // create a reader for the merged content
        byte[] data = runner.getContentAsByteArray(bundle);
        final Map<String, GenericRecord> users = getGenericRecordMap(data, schema, "name");

        assertEquals(3, users.size());
        assertTrue(users.containsKey("Alyssa"));
        assertTrue(users.containsKey("Ben"));
        assertTrue(users.containsKey("John"));
    }

    @Test
    public void testAvroConcatWithDifferentSchemas() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");
        runner.setProperty(MergeContent.MIN_ENTRIES, "3");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_AVRO);

        final Schema schema1 = new Schema.Parser().parse(new File("src/test/resources/TestMergeContent/user.avsc"));
        final Schema schema2 = new Schema.Parser().parse(new File("src/test/resources/TestMergeContent/place.avsc"));

        final GenericRecord record1 = new GenericData.Record(schema1);
        record1.put("name", "Alyssa");
        record1.put("favorite_number", 256);

        final GenericRecord record2 = new GenericData.Record(schema2);
        record2.put("name", "Some Place");

        final GenericRecord record3 = new GenericData.Record(schema1);
        record3.put("name", "John");
        record3.put("favorite_number", 5);
        record3.put("favorite_color", "blue");

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema1);
        final ByteArrayOutputStream out1 = serializeAvroRecord(schema1, record1, datumWriter);
        final ByteArrayOutputStream out2 = serializeAvroRecord(schema2, record2, datumWriter);
        final ByteArrayOutputStream out3 = serializeAvroRecord(schema1, record3, datumWriter);

        runner.enqueue(out1.toByteArray());
        runner.enqueue(out2.toByteArray());
        runner.enqueue(out3.toByteArray());

        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 1);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");

        final byte[] data = runner.getContentAsByteArray(bundle);
        final Map<String, GenericRecord> users = getGenericRecordMap(data, schema1, "name");
        assertEquals(2, users.size());
        assertTrue(users.containsKey("Alyssa"));
        assertTrue(users.containsKey("John"));

        final MockFlowFile failure = runner.getFlowFilesForRelationship(MergeContent.REL_FAILURE).getFirst();
        final byte[] failureData = runner.getContentAsByteArray(failure);
        final Map<String, GenericRecord> places = getGenericRecordMap(failureData, schema2, "name");
        assertEquals(1, places.size());
        assertTrue(places.containsKey("Some Place"));
    }

    @Test
    public void testAvroConcatWithDifferentMetadataDoNotMerge() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");
        runner.setProperty(MergeContent.MIN_ENTRIES, "3");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_AVRO);
        runner.setProperty(MergeContent.METADATA_STRATEGY, MergeContent.METADATA_STRATEGY_DO_NOT_MERGE);

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/TestMergeContent/user.avsc"));

        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
        final Map<String, String> userMeta1 = Map.of("test_metadata1", "Test 1");

        final GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        final Map<String, String> userMeta2 = Map.of("test_metadata1", "Test 2"); // Test non-matching values

        final GenericRecord user3 = new GenericData.Record(schema);
        user3.put("name", "John");
        user3.put("favorite_number", 5);
        user3.put("favorite_color", "blue");
        final Map<String, String> userMeta3 = Map.of("test_metadata1", "Test 1", "test_metadata2", "Test"); // Test unique

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        final ByteArrayOutputStream out1 = serializeAvroRecord(schema, user1, datumWriter, userMeta1);
        final ByteArrayOutputStream out2 = serializeAvroRecord(schema, user2, datumWriter, userMeta2);
        final ByteArrayOutputStream out3 = serializeAvroRecord(schema, user3, datumWriter, userMeta3);

        runner.enqueue(out1.toByteArray());
        runner.enqueue(out2.toByteArray());
        runner.enqueue(out3.toByteArray());

        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 2);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");

        // create a reader for the merged content
        byte[] data = runner.getContentAsByteArray(bundle);
        final Map<String, GenericRecord> users = getGenericRecordMap(data, schema, "name");

        assertEquals(1, users.size());
        assertTrue(users.containsKey("Alyssa"));
    }

    @Test
    public void testAvroConcatWithDifferentMetadataIgnore() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");
        runner.setProperty(MergeContent.MIN_ENTRIES, "3");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_AVRO);
        runner.setProperty(MergeContent.METADATA_STRATEGY, MergeContent.METADATA_STRATEGY_IGNORE);

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/TestMergeContent/user.avsc"));

        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
        final Map<String, String> userMeta1 = Map.of("test_metadata1", "Test 1");

        final GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        final Map<String, String> userMeta2 = Map.of("test_metadata1", "Test 2"); // Test non-matching values

        final GenericRecord user3 = new GenericData.Record(schema);
        user3.put("name", "John");
        user3.put("favorite_number", 5);
        user3.put("favorite_color", "blue");
        final Map<String, String> userMeta3 = Map.of("test_metadata1", "Test 1", "test_metadata2", "Test"); // Test unique

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        final ByteArrayOutputStream out1 = serializeAvroRecord(schema, user1, datumWriter, userMeta1);
        final ByteArrayOutputStream out2 = serializeAvroRecord(schema, user2, datumWriter, userMeta2);
        final ByteArrayOutputStream out3 = serializeAvroRecord(schema, user3, datumWriter, userMeta3);

        runner.enqueue(out1.toByteArray());
        runner.enqueue(out2.toByteArray());
        runner.enqueue(out3.toByteArray());

        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");

        // create a reader for the merged content
        byte[] data = runner.getContentAsByteArray(bundle);
        final Map<String, GenericRecord> users = getGenericRecordMap(data, schema, "name");

        assertEquals(3, users.size());
        assertTrue(users.containsKey("Alyssa"));
        assertTrue(users.containsKey("Ben"));
        assertTrue(users.containsKey("John"));
    }

    @Test
    public void testAvroConcatWithDifferentMetadataUseFirst() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");
        runner.setProperty(MergeContent.MIN_ENTRIES, "3");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_AVRO);
        runner.setProperty(MergeContent.METADATA_STRATEGY, MergeContent.METADATA_STRATEGY_USE_FIRST);

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/TestMergeContent/user.avsc"));

        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
        final Map<String, String> userMeta1 = Map.of("test_metadata1", "Test 1");

        final GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        final Map<String, String> userMeta2 = Map.of("test_metadata1", "Test 2"); // Test non-matching values

        final GenericRecord user3 = new GenericData.Record(schema);
        user3.put("name", "John");
        user3.put("favorite_number", 5);
        user3.put("favorite_color", "blue");
        final Map<String, String> userMeta3 = Map.of("test_metadata1", "Test 1", "test_metadata2", "Test"); // Test unique

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        final ByteArrayOutputStream out1 = serializeAvroRecord(schema, user1, datumWriter, userMeta1);
        final ByteArrayOutputStream out2 = serializeAvroRecord(schema, user2, datumWriter, userMeta2);
        final ByteArrayOutputStream out3 = serializeAvroRecord(schema, user3, datumWriter, userMeta3);

        runner.enqueue(out1.toByteArray());
        runner.enqueue(out2.toByteArray());
        runner.enqueue(out3.toByteArray());

        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");

        // create a reader for the merged content
        byte[] data = runner.getContentAsByteArray(bundle);
        final Map<String, GenericRecord> users = getGenericRecordMap(data, schema, "name");

        assertEquals(3, users.size());
        assertTrue(users.containsKey("Alyssa"));
        assertTrue(users.containsKey("Ben"));
        assertTrue(users.containsKey("John"));
    }

    @Test
    public void testAvroConcatWithDifferentMetadataKeepCommon() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");
        runner.setProperty(MergeContent.MIN_ENTRIES, "3");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_AVRO);
        runner.setProperty(MergeContent.METADATA_STRATEGY, MergeContent.METADATA_STRATEGY_ALL_COMMON);

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/TestMergeContent/user.avsc"));

        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
        final Map<String, String> userMeta1 = Map.of("test_metadata1", "Test 1");

        final GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        final Map<String, String> userMeta2 = Map.of("test_metadata1", "Test 2"); // Test non-matching values

        final GenericRecord user3 = new GenericData.Record(schema);
        user3.put("name", "John");
        user3.put("favorite_number", 5);
        user3.put("favorite_color", "blue");
        final Map<String, String> userMeta3 = Map.of("test_metadata1", "Test 1", "test_metadata2", "Test"); // Test unique

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        final ByteArrayOutputStream out1 = serializeAvroRecord(schema, user1, datumWriter, userMeta1);
        final ByteArrayOutputStream out2 = serializeAvroRecord(schema, user2, datumWriter, userMeta2);
        final ByteArrayOutputStream out3 = serializeAvroRecord(schema, user3, datumWriter, userMeta3);

        runner.enqueue(out1.toByteArray());
        runner.enqueue(out2.toByteArray());
        runner.enqueue(out3.toByteArray());

        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 1);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");

        // create a reader for the merged content
        byte[] data = runner.getContentAsByteArray(bundle);
        final Map<String, GenericRecord> users = getGenericRecordMap(data, schema, "name");

        assertEquals(2, users.size());
        assertTrue(users.containsKey("Alyssa"));
        assertTrue(users.containsKey("John"));
    }

    private Map<String, GenericRecord> getGenericRecordMap(byte[] data, Schema schema, String key) throws IOException {
        // create a reader for the merged contet
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        SeekableByteArrayInput input = new SeekableByteArrayInput(data);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, datumReader);

        // read all the records into a map to verify all the records are there
        Map<String, GenericRecord> records = new HashMap<>();
        while (dataFileReader.hasNext()) {
            GenericRecord user = dataFileReader.next();
            records.put(user.get(key).toString(), user);
        }
        return records;
    }

    private ByteArrayOutputStream serializeAvroRecord(Schema schema, GenericRecord user2, DatumWriter<GenericRecord> datumWriter) throws IOException {
        return serializeAvroRecord(schema, user2, datumWriter, null);
    }

    private ByteArrayOutputStream serializeAvroRecord(Schema schema, GenericRecord user2, DatumWriter<GenericRecord> datumWriter, Map<String, String> metadata) throws IOException {
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        DataFileWriter<GenericRecord> dataFileWriter2 = new DataFileWriter<>(datumWriter);
        if (metadata != null) {
            metadata.forEach(dataFileWriter2::setMeta);
        }
        dataFileWriter2.create(schema, out2);
        dataFileWriter2.append(user2);
        dataFileWriter2.close();
        return out2;
    }

    @Test
    public void testSimpleBinaryConcat() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);

        createFlowFiles(runner);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertContentEquals("Hello, World!".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/plain-text");

        runner.getFlowFilesForRelationship(MergeContent.REL_ORIGINAL).forEach(
                ff -> assertEquals(bundle.getAttribute(CoreAttributes.UUID.key()), ff.getAttribute(MergeContent.MERGE_UUID_ATTRIBUTE)));
    }

    @Test
    public void testSimpleBinaryConcatSingleBin() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.MAX_BIN_COUNT, "1");

        createFlowFiles(runner);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertContentEquals("Hello, World!");
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
    }

    @Test
    public void testSimpleBinaryConcatWithTextDelimiters() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.DELIMITER_STRATEGY, MergeContent.DELIMITER_STRATEGY_TEXT);
        runner.setProperty(MergeContent.HEADER, "@");
        runner.setProperty(MergeContent.DEMARCATOR, "#");
        runner.setProperty(MergeContent.FOOTER, "$");

        createFlowFiles(runner);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertContentEquals("@Hello#, #World!$".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
    }

    @Test
    public void testSimpleBinaryConcatWithTextDelimitersHeaderOnly() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.DELIMITER_STRATEGY, MergeContent.DELIMITER_STRATEGY_TEXT);
        runner.setProperty(MergeContent.HEADER, "@");

        createFlowFiles(runner);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertContentEquals("@Hello, World!".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
    }

    @Test
    public void testSimpleBinaryConcatWithFileDelimiters() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.DELIMITER_STRATEGY, MergeContent.DELIMITER_STRATEGY_FILENAME);
        runner.setProperty(MergeContent.HEADER, "${header}");
        runner.setProperty(MergeContent.DEMARCATOR, "${demarcator}");
        runner.setProperty(MergeContent.FOOTER, "${footer}");


        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
        attributes.put("header", "src/test/resources/TestMergeContent/head");
        attributes.put("demarcator", "src/test/resources/TestMergeContent/demarcate");
        attributes.put("footer", "src/test/resources/TestMergeContent/foot");

        runner.enqueue("Hello".getBytes(StandardCharsets.UTF_8), attributes);
        runner.enqueue(", ".getBytes(StandardCharsets.UTF_8), attributes);
        runner.enqueue("World!".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertContentEquals("(|)Hello***, ***World!___".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
    }

    @Test
    void testSimpleBinaryConcatNoDelimiters() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.DELIMITER_STRATEGY, MergeContent.DELIMITER_STRATEGY_NONE);
        // set dependent values to ensure they're not used; see NIFI-12561
        runner.setProperty(MergeContent.HEADER, "aHeader");
        runner.setProperty(MergeContent.DEMARCATOR, "; ");
        runner.setProperty(MergeContent.FOOTER, "aFooter");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");

        runner.enqueue("First", attributes);
        runner.enqueue("Second", attributes);
        runner.enqueue("Third", attributes);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertContentEquals("FirstSecondThird");
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
    }

    @Test
    public void testTextDelimitersValidation() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.DELIMITER_STRATEGY, MergeContent.DELIMITER_STRATEGY_TEXT);
        runner.setProperty(MergeContent.HEADER, "");
        runner.setProperty(MergeContent.DEMARCATOR, "");
        runner.setProperty(MergeContent.FOOTER, "");

        Collection<ValidationResult> results = new HashSet<>();
        ProcessContext context = runner.getProcessContext();
        if (context instanceof MockProcessContext mockContext) {
            results = mockContext.validate();
        }

        assertEquals(3, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("cannot be empty"));
        }
    }

    @Test
    public void testFileDelimitersValidation() {
        final String doesNotExistFile = "src/test/resources/TestMergeContent/does_not_exist";
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.DELIMITER_STRATEGY, MergeContent.DELIMITER_STRATEGY_FILENAME);
        runner.setProperty(MergeContent.HEADER, doesNotExistFile);
        runner.setProperty(MergeContent.DEMARCATOR, doesNotExistFile);
        runner.setProperty(MergeContent.FOOTER, doesNotExistFile);

        Collection<ValidationResult> results = new HashSet<>();
        ProcessContext context = runner.getProcessContext();
        if (context instanceof MockProcessContext) {
            MockProcessContext mockContext = (MockProcessContext) context;
            results = mockContext.validate();
        }

        assertEquals(3, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because File " + new File(doesNotExistFile) + " does not exist"));
        }
    }

    @Test
    public void testMimeTypeIsOctetStreamIfConflictingWithBinaryConcat() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);

        createFlowFiles(runner);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/zip");
        runner.enqueue(new byte[0], attributes);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 4);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertContentEquals("Hello, World!".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
    }

    @Test
    public void testOldestBinIsExpired() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 day");
        runner.setProperty(MergeContent.MAX_BIN_COUNT, "50");
        runner.setProperty(MergeContent.MIN_ENTRIES, "10");
        runner.setProperty(MergeContent.MAX_ENTRIES, "10");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.CORRELATION_ATTRIBUTE_NAME, "correlationId");

        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < 49; i++) {
            attrs.put("correlationId", String.valueOf(i));

            for (int j = 0; j < 5; j++) {
                runner.enqueue(new byte[0], attrs);
            }
        }

        runner.run();

        runner.assertQueueNotEmpty(); // sessions rolled back.
        runner.assertTransferCount(MergeContent.REL_MERGED, 0);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 0);

        attrs.remove("correlationId");

        runner.clearTransferState();

        // Run a single iteration but do not perform the @OnStopped action because
        // we do not want to purge our Bin Manager. This causes some bins to get
        // created. We then enqueue a FlowFile with no correlation id. We do it this
        // way because if we just run a single iteration, then all FlowFiles will be
        // pulled in at once, and we don't know if the first bin to be created will
        // have 5 FlowFiles or 1 FlowFile, since this one that we are about to enqueue
        // will be in a separate bin.
        runner.run(1, false, true);
        runner.enqueue(new byte[0], attrs);

        // Add one more FlowFile, with a unique correlation id so that it creates the 51st bin.
        // This should trigger the oldest bin to be evicted.
        attrs.put("correlationId", "abc");
        runner.enqueue(new byte[0], attrs);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 5);
    }

    @Test
    public void testSimpleBinaryConcatWaitsForMin() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);
        runner.setProperty(MergeContent.MIN_SIZE, "20 KB");

        createFlowFiles(runner);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 0);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 0);
    }

    @Test
    public void testZip() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_ZIP);

        createFlowFiles(runner);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        try (final InputStream rawIn = new ByteArrayInputStream(runner.getContentAsByteArray(bundle)); final ZipInputStream in = new ZipInputStream(rawIn)) {
            assertNotNull(in.getNextEntry());
            final byte[] part1 = IOUtils.toByteArray(in);
            assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), part1);

            in.getNextEntry();
            final byte[] part2 = IOUtils.toByteArray(in);
            assertArrayEquals(", ".getBytes(StandardCharsets.UTF_8), part2);

            in.getNextEntry();
            final byte[] part3 = IOUtils.toByteArray(in);
            assertArrayEquals("World!".getBytes(StandardCharsets.UTF_8), part3);
        }
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/zip");
    }

    @Test
    public void testZipException() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_ZIP);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
        attributes.put("filename", "duplicate-filename.txt");

        runner.enqueue("Hello".getBytes(StandardCharsets.UTF_8), attributes);
        runner.enqueue(", ".getBytes(StandardCharsets.UTF_8), attributes);
        runner.enqueue("World!".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 2);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);
    }

    @Test
    public void testTar() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_TAR);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");

        attributes.put(CoreAttributes.FILENAME.key(), "AShortFileName");
        runner.enqueue("Hello".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(CoreAttributes.FILENAME.key(), "ALongerrrFileName");
        runner.enqueue(", ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(CoreAttributes.FILENAME.key(), "AReallyLongggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggFileName");
        runner.enqueue("World!".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        try (final InputStream rawIn = new ByteArrayInputStream(runner.getContentAsByteArray(bundle)); final TarArchiveInputStream in = new TarArchiveInputStream(rawIn)) {
            ArchiveEntry entry = in.getNextEntry();
            assertNotNull(entry);
            assertEquals("AShortFileName", entry.getName());
            final byte[] part1 = IOUtils.toByteArray(in);
            assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), part1);

            entry = in.getNextEntry();
            assertEquals("ALongerrrFileName", entry.getName());
            final byte[] part2 = IOUtils.toByteArray(in);
            assertArrayEquals(", ".getBytes(StandardCharsets.UTF_8), part2);

            entry = in.getNextEntry();
            assertEquals("AReallyLongggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggFileName", entry.getName());
            final byte[] part3 = IOUtils.toByteArray(in);
            assertArrayEquals("World!".getBytes(StandardCharsets.UTF_8), part3);
        }
        bundle.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/tar");
    }

    @Test
    public void testFlowFileStream() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MIN_ENTRIES, "2");
        runner.setProperty(MergeContent.MAX_ENTRIES, "2");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_FLOWFILE_STREAM_V3);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("path", "folder");
        runner.enqueue(Paths.get("src/test/resources/TestUnpackContent/folder/cal.txt"), attributes);
        runner.enqueue(Paths.get("src/test/resources/TestUnpackContent/folder/date.txt"), attributes);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 2);

        final MockFlowFile merged = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        merged.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), StandardFlowFileMediaType.VERSION_3.getMediaType());
    }

    @Test
    public void testDefragment() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDefragmentWithFragmentCountOnLastFragmentOnly() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");
        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDefragmentWithFragmentCountOnMiddleFragment() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final String fragmentId = "Fragment Id";

        runner.enqueue("Fragment 1 without count ".getBytes(StandardCharsets.UTF_8), Map.of(MergeContent.FRAGMENT_ID_ATTRIBUTE, fragmentId,
                MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1"));

        runner.enqueue("Fragment 2 with count ".getBytes(StandardCharsets.UTF_8), Map.of(MergeContent.FRAGMENT_ID_ATTRIBUTE, fragmentId,
                    MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2", MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "3"));

        runner.enqueue("Fragment 3 without count".getBytes(StandardCharsets.UTF_8), Map.of(MergeContent.FRAGMENT_ID_ATTRIBUTE, fragmentId, MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3"));

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assembled.assertContentEquals("Fragment 1 without count Fragment 2 with count Fragment 3 without count".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDefragmentWithDifferentFragmentCounts() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final String fragmentId = "Fragment Id";

        runner.enqueue("Fragment 1 with count ".getBytes(StandardCharsets.UTF_8), Map.of(MergeContent.FRAGMENT_ID_ATTRIBUTE,
                fragmentId, MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1", MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "2"));

        runner.enqueue("Fragment 2 with count ".getBytes(StandardCharsets.UTF_8), Map.of(MergeContent.FRAGMENT_ID_ATTRIBUTE, fragmentId,
                MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2", MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "3"));

        runner.enqueue("Fragment 3 without count".getBytes(StandardCharsets.UTF_8), Map.of(MergeContent.FRAGMENT_ID_ATTRIBUTE, fragmentId,
                MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3"));

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 0);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 3);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 0);
    }

    @Test
    public void testDefragmentDuplicateFragment() throws IOException, InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);
        // enqueue a duplicate fragment
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run(1, false);

        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes(StandardCharsets.UTF_8));

        runner.clearTransferState();
        Thread.sleep(1_100L);
        runner.run();
        runner.assertTransferCount(MergeContent.REL_FAILURE, 1);
        runner.assertTransferCount(MergeContent.REL_MERGED, 0);
    }

    @Test
    public void testDefragmentWithTooManyFragments() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDefragmentWithTooFewFragments() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "2 secs");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "5");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run(1, false);

        while (true) {
            try {
                Thread.sleep(3000L);
                break;
            } catch (final InterruptedException ignored) {
            }
        }
        runner.run(1);

        runner.assertTransferCount(MergeContent.REL_FAILURE, 4);
    }

    @Test
    public void testDefragmentOutOfOrder() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes(StandardCharsets.UTF_8));

        runner.getFlowFilesForRelationship(MergeContent.REL_ORIGINAL).forEach(
                ff -> assertEquals(assembled.getAttribute(CoreAttributes.UUID.key()), ff.getAttribute(MergeContent.MERGE_UUID_ATTRIBUTE)));
    }

    @Test
    public void testDefragmentMultipleMingledSegments() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");

        final Map<String, String> secondAttrs = new HashMap<>();
        secondAttrs.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "TWO");
        secondAttrs.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "3");

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");
        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);
        secondAttrs.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");
        runner.enqueue("No x ".getBytes(StandardCharsets.UTF_8), secondAttrs);
        secondAttrs.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("in ".getBytes(StandardCharsets.UTF_8), secondAttrs);
        secondAttrs.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("Nixon".getBytes(StandardCharsets.UTF_8), secondAttrs);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run(1);

        runner.assertTransferCount(MergeContent.REL_MERGED, 2);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(0);
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes(StandardCharsets.UTF_8));
        final MockFlowFile assembledTwo = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).get(1);
        assembledTwo.assertContentEquals("No x in Nixon".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testDefragmentOldStyleAttributes() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("fragment.identifier", "1");
        attributes.put("fragment.count", "4");
        attributes.put("fragment.index", "1");
        attributes.put("segment.original.filename", "originalfilename");

        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put("fragment.index", "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put("fragment.index", "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);
        attributes.put("fragment.index", "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes(StandardCharsets.UTF_8));
        assembled.assertAttributeEquals(CoreAttributes.FILENAME.key(), "originalfilename");
    }

    @Test
    public void testDefragmentMultipleOnTriggers() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_DEFRAGMENT);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MergeContent.FRAGMENT_ID_ATTRIBUTE, "1");
        attributes.put(MergeContent.FRAGMENT_COUNT_ATTRIBUTE, "4");
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");
        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run();

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "2");
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run();

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "3");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run();

        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "4");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile assembled = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        assembled.assertContentEquals("A Man A Plan A Canal Panama".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testMergeBasedOnCorrelation() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_BIN_PACK);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 min");
        runner.setProperty(MergeContent.CORRELATION_ATTRIBUTE_NAME, "attr");
        runner.setProperty(MergeContent.MAX_ENTRIES, "3");
        runner.setProperty(MergeContent.MIN_ENTRIES, "1");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr", "b");
        runner.enqueue("A Man ".getBytes(StandardCharsets.UTF_8), attributes);
        runner.enqueue("A Plan ".getBytes(StandardCharsets.UTF_8), attributes);

        attributes.put("attr", "c");
        runner.enqueue("A Canal ".getBytes(StandardCharsets.UTF_8), attributes);

        attributes.put("attr", "b");
        runner.enqueue("Panama".getBytes(StandardCharsets.UTF_8), attributes);

        runner.run(2);

        runner.assertTransferCount(MergeContent.REL_MERGED, 2);

        final List<MockFlowFile> mergedFiles = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        final MockFlowFile merged1 = mergedFiles.get(0);
        final MockFlowFile merged2 = mergedFiles.get(1);

        final String attr1 = merged1.getAttribute("attr");
        final String attr2 = merged2.getAttribute("attr");

        if ("c".equals(attr1)) {
            assertEquals("b", attr2);
            merged1.assertContentEquals("A Canal ", StandardCharsets.UTF_8);
            merged2.assertContentEquals("A Man A Plan Panama", StandardCharsets.UTF_8);
        } else {
            assertEquals("b", attr1);
            assertEquals("c", attr2);
            merged1.assertContentEquals("A Man A Plan Panama", StandardCharsets.UTF_8);
            merged2.assertContentEquals("A Canal ", StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testMaxBinAge() throws InterruptedException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MERGE_STRATEGY, MergeContent.MERGE_STRATEGY_BIN_PACK);
        runner.setProperty(MergeContent.MAX_BIN_AGE, "2 sec");
        runner.setProperty(MergeContent.CORRELATION_ATTRIBUTE_NAME, "attr");
        runner.setProperty(MergeContent.MAX_ENTRIES, "500");
        runner.setProperty(MergeContent.MIN_ENTRIES, "500");

        for (int i = 0; i < 50; i++) {
            runner.enqueue(new byte[0]);
        }

        runner.run(5, false);

        runner.assertTransferCount(MergeContent.REL_MERGED, 0);
        runner.clearTransferState();

        Thread.sleep(3000L);

        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
    }

    @Test
    public void testUniqueAttributes() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(AttributeStrategyUtil.ATTRIBUTE_STRATEGY, AttributeStrategyUtil.ATTRIBUTE_STRATEGY_ALL_UNIQUE);
        runner.setProperty(MergeContent.MAX_SIZE, "2 B");
        runner.setProperty(MergeContent.MIN_SIZE, "2 B");

        final Map<String, String> attr1 = new HashMap<>();
        attr1.put("abc", "xyz");
        attr1.put("xyz", "123");
        attr1.put("hello", "good-bye");

        final Map<String, String> attr2 = new HashMap<>();
        attr2.put("abc", "xyz");
        attr2.put("xyz", "321");
        attr2.put("world", "aaa");

        runner.enqueue(new byte[1], attr1);
        runner.enqueue(new byte[1], attr2);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile outFile = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();

        outFile.assertAttributeEquals("abc", "xyz");
        outFile.assertAttributeEquals("hello", "good-bye");
        outFile.assertAttributeEquals("world", "aaa");
        outFile.assertAttributeNotExists("xyz");
    }

    @Test
    public void testCommonAttributesOnly() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(AttributeStrategyUtil.ATTRIBUTE_STRATEGY, AttributeStrategyUtil.ATTRIBUTE_STRATEGY_ALL_COMMON);
        runner.setProperty(MergeContent.MAX_SIZE, "2 B");
        runner.setProperty(MergeContent.MIN_SIZE, "2 B");

        final Map<String, String> attr1 = new HashMap<>();
        attr1.put("abc", "xyz");
        attr1.put("xyz", "123");
        attr1.put("hello", "good-bye");

        final Map<String, String> attr2 = new HashMap<>();
        attr2.put("abc", "xyz");
        attr2.put("xyz", "321");
        attr2.put("world", "aaa");

        runner.enqueue(new byte[1], attr1);
        runner.enqueue(new byte[1], attr2);
        runner.run();

        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        final MockFlowFile outFile = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();

        outFile.assertAttributeEquals("abc", "xyz");
        outFile.assertAttributeNotExists("hello");
        outFile.assertAttributeNotExists("world");
        outFile.assertAttributeNotExists("xyz");

        final Set<String> uuids = new HashSet<>();
        for (final MockFlowFile mff : runner.getFlowFilesForRelationship(MergeContent.REL_ORIGINAL)) {
            uuids.add(mff.getAttribute(CoreAttributes.UUID.key()));
        }
        uuids.add(outFile.getAttribute(CoreAttributes.UUID.key()));

        assertEquals(3, uuids.size());
    }

    @Test
    public void testCountAttribute() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MAX_BIN_AGE, "1 sec");
        runner.setProperty(MergeContent.MERGE_FORMAT, MergeContent.MERGE_FORMAT_CONCAT);

        createFlowFiles(runner);
        runner.run(2);

        runner.assertQueueEmpty();
        runner.assertTransferCount(MergeContent.REL_MERGED, 1);
        runner.assertTransferCount(MergeContent.REL_FAILURE, 0);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 3);

        final MockFlowFile bundle = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED).getFirst();
        bundle.assertContentEquals("Hello, World!".getBytes(StandardCharsets.UTF_8));
        bundle.assertAttributeEquals(MergeContent.MERGE_COUNT_ATTRIBUTE, "3");
        bundle.assertAttributeExists(MergeContent.MERGE_BIN_AGE_ATTRIBUTE);
    }

    @Test
    public void testLeavesSmallBinUnmerged() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MIN_ENTRIES, "5");
        runner.setProperty(MergeContent.MAX_ENTRIES, "5");
        runner.setProperty(MergeContent.MAX_BIN_COUNT, "3");

        for (int i = 0; i < 17; i++) {
            runner.enqueue(i + "\n");
        }

        runner.run(5);

        runner.assertTransferCount(MergeContent.REL_MERGED, 3);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 15);
        assertEquals(2, runner.getQueueSize().getObjectCount());
    }

    @Test
    public void testBinTerminationTriggerStartOfBin() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MIN_ENTRIES, "10");
        runner.setProperty(MergeContent.MAX_ENTRIES, "25");
        runner.setProperty(MergeContent.MAX_BIN_COUNT, "3");
        runner.setProperty(MergeContent.BIN_TERMINATION_CHECK, "${termination:equals('true')}");
        runner.setProperty(MergeContent.FLOWFILE_INSERTION_STRATEGY, InsertionLocation.FIRST_IN_NEW_BIN);

        // Enqueue 5 FlowFiles, followed by a FlowFile with the 'termination' attribute set to 'true'.
        // Do this 3 times.
        int flowFileIndex = 0;
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 6; i++) {
                final Map<String, String> attributes = (i == 5) ? Map.of("termination", "true") : Collections.emptyMap();
                runner.enqueue((flowFileIndex++) + "\n", attributes);
            }
        }

        runner.run(2);

        runner.assertTransferCount(MergeContent.REL_MERGED, 3);

        // We should get out 17 because the last FlowFile has not yet been transferred out, since it is the start of a new bin.
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 17);

        final List<MockFlowFile> merged = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        merged.getFirst().assertContentEquals("0\n1\n2\n3\n4\n");
        merged.get(1).assertContentEquals("5\n6\n7\n8\n9\n10\n");
        merged.get(2).assertContentEquals("11\n12\n13\n14\n15\n16\n");
    }

    @Test
    public void testBinTerminationTriggerEndOfBin() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MIN_ENTRIES, "10");
        runner.setProperty(MergeContent.MAX_ENTRIES, "25");
        runner.setProperty(MergeContent.MAX_BIN_COUNT, "3");
        runner.setProperty(MergeContent.BIN_TERMINATION_CHECK, "${termination:equals('true')}");
        runner.setProperty(MergeContent.FLOWFILE_INSERTION_STRATEGY, InsertionLocation.LAST_IN_BIN);

        // Enqueue 5 FlowFiles, followed by a FlowFile with the 'termination' attribute set to 'true'.
        // Do this 3 times.
        int flowFileIndex = 0;
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 6; i++) {
                final Map<String, String> attributes = (i == 5) ? Map.of("termination", "true") : Collections.emptyMap();
                runner.enqueue((flowFileIndex++) + "\n", attributes);
            }
        }

        runner.run(2);

        runner.assertTransferCount(MergeContent.REL_MERGED, 3);

        // We should get out 18 because the last FlowFile ended the bin and was transferred out.
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 18);

        final List<MockFlowFile> merged = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        merged.getFirst().assertContentEquals("0\n1\n2\n3\n4\n5\n");
        merged.get(1).assertContentEquals("6\n7\n8\n9\n10\n11\n");
        merged.get(2).assertContentEquals("12\n13\n14\n15\n16\n17\n");
    }

    @Test
    public void testBinTerminationTriggerIsolated() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MIN_ENTRIES, "10");
        runner.setProperty(MergeContent.MAX_ENTRIES, "25");
        runner.setProperty(MergeContent.MAX_BIN_COUNT, "3");
        runner.setProperty(MergeContent.BIN_TERMINATION_CHECK, "${termination:equals('true')}");
        runner.setProperty(MergeContent.FLOWFILE_INSERTION_STRATEGY, InsertionLocation.ISOLATED);

        // Enqueue 5 FlowFiles, followed by a FlowFile with the 'termination' attribute set to 'true'.
        // Do this 3 times.
        int flowFileIndex = 0;
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 6; i++) {
                final Map<String, String> attributes = (i == 5) ? Map.of("termination", "true") : Collections.emptyMap();
                runner.enqueue((flowFileIndex++) + "\n", attributes);
            }
        }

        runner.run(2);

        runner.assertTransferCount(MergeContent.REL_MERGED, 6);

        // We should get out 18 because the last FlowFile ended the bin and was transferred out.
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 18);

        final List<MockFlowFile> merged = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        merged.getFirst().assertContentEquals("0\n1\n2\n3\n4\n");
        merged.get(1).assertContentEquals("5\n");
        merged.get(2).assertContentEquals("6\n7\n8\n9\n10\n");
        merged.get(3).assertContentEquals("11\n");
        merged.get(4).assertContentEquals("12\n13\n14\n15\n16\n");
        merged.get(5).assertContentEquals("17\n");
    }

    @Test
    public void testTerminationTriggerWithCorrelationAttribute() {
        final TestRunner runner = TestRunners.newTestRunner(new MergeContent());
        runner.setProperty(MergeContent.MIN_ENTRIES, "10");
        runner.setProperty(MergeContent.MAX_ENTRIES, "25");
        runner.setProperty(MergeContent.MAX_BIN_COUNT, "3");
        runner.setProperty(MergeContent.BIN_TERMINATION_CHECK, "${termination:equals('true')}");
        runner.setProperty(MergeContent.FLOWFILE_INSERTION_STRATEGY, InsertionLocation.FIRST_IN_NEW_BIN);
        runner.setProperty(MergeContent.CORRELATION_ATTRIBUTE_NAME, "correlation");

        // Enqueue FlowFiles. This should create 2 bins:
        // '1' should get values 0, 1
        // Then, '2' should get value 2
        // Then, FlowFile with content '3' should be the start of a new bin
        // Then, FlowFile with content '4' should be teh start of another new bin
        // Then, we add 10 additional FlowFiles to bin 2 in order to fill it without a termination signal
        // This should leave the FlowFile with content '4' in the bin, but not transferred out.
        final Map<String, String> terminationAttributes = Map.of("termination", "true", "correlation", "1");
        final Map<String, String> correlation1 = Map.of("correlation", "1");
        final Map<String, String> correlation2 = Map.of("correlation", "2");

        runner.enqueue("0\n", correlation1);
        runner.enqueue("1\n", correlation1);
        runner.enqueue("2\n", correlation2);
        runner.enqueue("3\n", terminationAttributes);
        runner.enqueue("4\n", terminationAttributes);

        for (int i = 0; i < 10; i++) {
            runner.enqueue(i + "\n", correlation2);
        }

        runner.run(2);

        runner.assertTransferCount(MergeContent.REL_MERGED, 3);
        runner.assertTransferCount(MergeContent.REL_ORIGINAL, 14);

        final List<MockFlowFile> merged = runner.getFlowFilesForRelationship(MergeContent.REL_MERGED);
        merged.getFirst().assertContentEquals("0\n1\n");
        merged.get(1).assertContentEquals("3\n");
        merged.get(2).assertContentEquals("2\n0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
    }

    private void createFlowFiles(final TestRunner testRunner) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/plain-text");
        // add 'fragment.index' attribute to ensure non-defragment mode operates correctly even when index is present
        attributes.put(MergeContent.FRAGMENT_INDEX_ATTRIBUTE, "1");

        testRunner.enqueue("Hello", attributes);
        testRunner.enqueue(", ", attributes);
        testRunner.enqueue("World!", attributes);
    }

}
