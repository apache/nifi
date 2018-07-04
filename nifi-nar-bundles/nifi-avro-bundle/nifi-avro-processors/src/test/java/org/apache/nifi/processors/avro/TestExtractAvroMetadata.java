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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestExtractAvroMetadata {

    static final String AVRO_SCHEMA_ATTR = "avro.schema";
    static final String AVRO_CODEC_ATTR = "avro.codec";

    @Test
    public void testDefaultExtraction() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
        final ByteArrayOutputStream out = getOutputStreamWithOneUser(schema);
        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_FINGERPRINT_ATTR, "b2d1d8d3de2833ce");
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_TYPE_ATTR, Schema.Type.RECORD.getName());
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_NAME_ATTR, "User");
        flowFile.assertAttributeNotExists(AVRO_SCHEMA_ATTR);
        flowFile.assertAttributeNotExists(ExtractAvroMetadata.ITEM_COUNT_ATTR);
    }

    @Test
    public void testExtractionWithItemCount() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());
        runner.setProperty(ExtractAvroMetadata.COUNT_ITEMS, "true");

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
        final ByteArrayOutputStream out = getOutputStreamWithMultipleUsers(schema, 6000); // creates 2 blocks
        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ExtractAvroMetadata.ITEM_COUNT_ATTR, "6000");
    }

    @Test
    public void testExtractionWithZeroUsers() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());
        runner.setProperty(ExtractAvroMetadata.COUNT_ITEMS, "true");

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
        final ByteArrayOutputStream out = getOutputStreamWithMultipleUsers(schema, 0);
        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_FINGERPRINT_ATTR, "b2d1d8d3de2833ce");
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_TYPE_ATTR, Schema.Type.RECORD.getName());
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_NAME_ATTR, "User");
        flowFile.assertAttributeEquals(ExtractAvroMetadata.ITEM_COUNT_ATTR, "0");
    }

    @Test
    public void testExtractionWithMD5() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());
        runner.setProperty(ExtractAvroMetadata.FINGERPRINT_ALGORITHM, ExtractAvroMetadata.MD5);

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
        final ByteArrayOutputStream out = getOutputStreamWithOneUser(schema);
        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_FINGERPRINT_ATTR, "3c6a7bee8994be20314dd28c6a3af4f2");
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_TYPE_ATTR, Schema.Type.RECORD.getName());
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_NAME_ATTR, "User");
        flowFile.assertAttributeNotExists(AVRO_SCHEMA_ATTR);
    }

    @Test
    public void testExtractionWithSHA256() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());
        runner.setProperty(ExtractAvroMetadata.FINGERPRINT_ALGORITHM, ExtractAvroMetadata.SHA_256);

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
        final ByteArrayOutputStream out = getOutputStreamWithOneUser(schema);
        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_FINGERPRINT_ATTR, "683f8f51ecd208038f4f0d39820ee9dd0ef3e32a3bee9371de0a2016d501b113");
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_TYPE_ATTR, Schema.Type.RECORD.getName());
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_NAME_ATTR, "User");
        flowFile.assertAttributeNotExists(AVRO_SCHEMA_ATTR);
    }

    @Test
    public void testExtractionWithMetadataKey() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());
        runner.setProperty(ExtractAvroMetadata.METADATA_KEYS, AVRO_SCHEMA_ATTR); // test dynamic attribute avro.schema

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
        final ByteArrayOutputStream out = getOutputStreamWithOneUser(schema);
        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeExists(ExtractAvroMetadata.SCHEMA_FINGERPRINT_ATTR);
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_TYPE_ATTR, Schema.Type.RECORD.getName());
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_NAME_ATTR, "User");
        flowFile.assertAttributeEquals(AVRO_SCHEMA_ATTR, schema.toString());
    }

    @Test
    public void testExtractionWithMetadataKeysWhitespace() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());
        runner.setProperty(ExtractAvroMetadata.METADATA_KEYS, "foo, bar,   " + AVRO_SCHEMA_ATTR); // test dynamic attribute avro.schema

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));
        final ByteArrayOutputStream out = getOutputStreamWithOneUser(schema);
        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeExists(ExtractAvroMetadata.SCHEMA_FINGERPRINT_ATTR);
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_TYPE_ATTR, Schema.Type.RECORD.getName());
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_NAME_ATTR, "User");
        flowFile.assertAttributeEquals(AVRO_SCHEMA_ATTR, schema.toString());
    }

    @Test
    public void testExtractionWithNonRecordSchema() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());
        runner.setProperty(ExtractAvroMetadata.COUNT_ITEMS, "true");

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/array.avsc"));

        final GenericData.Array<String> data = new GenericData.Array<>(schema, Arrays.asList("one", "two", "three"));
        final DatumWriter<GenericData.Array<String>> datumWriter = new GenericDatumWriter<>(schema);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataFileWriter<GenericData.Array<String>> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, out);
        dataFileWriter.append(data);
        dataFileWriter.append(data);
        dataFileWriter.close();

        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeExists(ExtractAvroMetadata.SCHEMA_FINGERPRINT_ATTR);
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_TYPE_ATTR, Schema.Type.ARRAY.getName());
        flowFile.assertAttributeEquals(ExtractAvroMetadata.SCHEMA_NAME_ATTR, "array");
        flowFile.assertAttributeEquals(ExtractAvroMetadata.ITEM_COUNT_ATTR, "2"); // number of arrays, not elements
    }

    @Test
    public void testExtractionWithCodec() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());
        runner.setProperty(ExtractAvroMetadata.METADATA_KEYS, AVRO_CODEC_ATTR); // test dynamic attribute avro.codec

        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/array.avsc"));

        final GenericData.Array<String> data = new GenericData.Array<>(schema, Arrays.asList("one", "two", "three"));
        final DatumWriter<GenericData.Array<String>> datumWriter = new GenericDatumWriter<>(schema);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataFileWriter<GenericData.Array<String>> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.deflateCodec(1));
        dataFileWriter.create(schema, out);
        dataFileWriter.append(data);
        dataFileWriter.close();

        runner.enqueue(out.toByteArray());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractAvroMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("avro.codec", "deflate");
    }

    @Test
    public void testExtractionWithBadInput() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractAvroMetadata());

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write("not avro".getBytes("UTF-8"));
        out.flush();

        runner.enqueue(out.toByteArray());
        runner.run();
        runner.assertAllFlowFilesTransferred(ExtractAvroMetadata.REL_FAILURE, 1);
    }

    private ByteArrayOutputStream getOutputStreamWithOneUser(Schema schema) throws IOException {
        final GenericRecord user = new GenericData.Record(schema);
        user.put("name", "Alyssa");
        user.put("favorite_number", 256);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        return AvroTestUtil.serializeAvroRecord(schema, datumWriter, user);
    }

    private ByteArrayOutputStream getOutputStreamWithMultipleUsers(Schema schema, int numUsers) throws IOException {
        final GenericRecord[] users = new GenericRecord[numUsers];
        for (int i=0; i < numUsers; i++) {
            final GenericRecord user = new GenericData.Record(schema);
            user.put("name", "user" + i);
            user.put("favorite_number", i);
            users[i] = user;
        }

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        return AvroTestUtil.serializeAvroRecord(schema, datumWriter, users);
    }
}
