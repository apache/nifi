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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestConvertAvroToJSON {

    @Test
    public void testSingleAvroMessage() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertAvroToJSON());
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));

        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        final ByteArrayOutputStream out1 = AvroTestUtil.serializeAvroRecord(schema, datumWriter, user1);
        runner.enqueue(out1.toByteArray());

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToJSON.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertAvroToJSON.REL_SUCCESS).get(0);
        out.assertContentEquals("{\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": null}");
    }

    @Test
    public void testMultipleAvroMessages() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertAvroToJSON());
        final Schema schema = new Schema.Parser().parse(new File("src/test/resources/user.avsc"));

        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        final GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "George");
        user2.put("favorite_number", 1024);
        user2.put("favorite_color", "red");

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        final ByteArrayOutputStream out1 = AvroTestUtil.serializeAvroRecord(schema, datumWriter, user1, user2);
        runner.enqueue(out1.toByteArray());

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToJSON.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertAvroToJSON.REL_SUCCESS).get(0);
        out.assertContentEquals("[{\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": null},{\"name\": \"George\", \"favorite_number\": 1024, \"favorite_color\": \"red\"}]");
    }

    @Test
    public void testNonJsonHandledProperly() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ConvertAvroToJSON());
        runner.enqueue("hello".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertAvroToJSON.REL_FAILURE, 1);
    }

}
