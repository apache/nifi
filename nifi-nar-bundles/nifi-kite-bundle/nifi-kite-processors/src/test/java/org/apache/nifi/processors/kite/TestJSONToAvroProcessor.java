/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.nifi.processors.kite.AbstractKiteConvertProcessor.CodecType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.nifi.processors.kite.TestUtil.streamFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJSONToAvroProcessor {

    public static final Schema SCHEMA = SchemaBuilder.record("Test").fields()
            .requiredLong("id")
            .requiredString("color")
            .optionalDouble("price")
            .endRecord();

    public static final String JSON_CONTENT = ""
            + "{\"id\": 1,\"color\": \"green\"}"
            + "{\"id\": \"120V\", \"color\": \"blue\"}\n" // invalid, ID is a string
            + "{\"id\": 10, \"color\": 15.23}\n" // invalid, color as double
            + "{\"id\": 20, \"color\": 34}\n" // invalid, color as int
            + "{\"id\": 2, \"color\": \"grey\", \"price\": 12.95 }";

    public static final String FAILURE_CONTENT = ""
            + "{\"id\": \"120V\", \"color\": \"blue\"}\n"
            + "{\"id\": 10, \"color\": 15.23}\n"
            + "{\"id\": 20, \"color\": 34}\n";

    public static final String FAILURE_SUMMARY = ""
            + "Cannot convert field id: Cannot convert to long: \"120V\", "
            + "Cannot convert field color: Cannot convert to string: 15.23 (1 similar failure)";

    @Test
    public void testBasicConversion() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertJSONToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertJSONToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.enqueue(streamFor(JSON_CONTENT));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        assertEquals(2, converted, "Should convert 2 rows");
        assertEquals(3, errors, "Should reject 3 rows");

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 1);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("incompatible").get(0);
        String failureContent = new String(runner.getContentAsByteArray(incompatible),
                StandardCharsets.UTF_8);
        assertEquals(
                JSON_CONTENT, failureContent, "Should reject an invalid string and double");
        assertEquals(
                FAILURE_SUMMARY, incompatible.getAttribute("errors"), "Should accumulate error messages");
    }

    @Test
    public void testBasicConversionWithCompression() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertJSONToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertJSONToAvro.SCHEMA, SCHEMA.toString());
        runner.setProperty(AbstractKiteConvertProcessor.COMPRESSION_TYPE, CodecType.NONE.toString());
        runner.assertValid();

        runner.enqueue(streamFor(JSON_CONTENT));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        assertEquals(2, converted, "Should convert 2 rows");
        assertEquals(3, errors, "Should reject 3 rows");

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 1);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("incompatible").get(0);
        String failureContent = new String(runner.getContentAsByteArray(incompatible),
                StandardCharsets.UTF_8);
        assertEquals(
                JSON_CONTENT, failureContent, "Should reject an invalid string and double");
        assertEquals(
                FAILURE_SUMMARY, incompatible.getAttribute("errors"), "Should accumulate error messages");
    }

    @Test
    public void testOnlyErrors() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertJSONToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertJSONToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.enqueue(streamFor(FAILURE_CONTENT));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        assertEquals(0, converted, "Should convert 0 rows");
        assertEquals(3, errors, "Should reject 1 row");

        runner.assertTransferCount("success", 0);
        runner.assertTransferCount("failure", 1);
        runner.assertTransferCount("incompatible", 0);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("failure").get(0);
        assertEquals(
                FAILURE_SUMMARY, incompatible.getAttribute("errors"), "Should set an error message");
    }

    @Test
    public void testEmptyContent() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertJSONToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertJSONToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.enqueue(streamFor(""));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        assertEquals(0, converted, "Should convert 0 rows");
        assertEquals(0, errors, "Should reject 0 row");

        runner.assertTransferCount("success", 0);
        runner.assertTransferCount("failure", 1);
        runner.assertTransferCount("incompatible", 0);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("failure").get(0);
        assertEquals(
                "No incoming records", incompatible.getAttribute("errors"), "Should set an error message");
    }

    @Test
    public void testBasicConversionNoErrors() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertJSONToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertJSONToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.enqueue(streamFor(
                "{\"id\": 1,\"color\": \"green\"}\n" +
                        "{\"id\": 2, \"color\": \"grey\", \"price\": 12.95 }"));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        assertEquals(2, converted, "Should convert 2 rows");
        assertEquals(0, errors, "Should reject 0 row");

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 0);
    }
}
