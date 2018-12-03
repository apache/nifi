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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.processors.kite.AbstractKiteConvertProcessor.CodecType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.nifi.processors.kite.TestUtil.streamFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCSVToAvroProcessor {

    public static final Schema SCHEMA = SchemaBuilder.record("Test").fields()
            .requiredLong("id")
            .requiredString("color")
            .optionalDouble("price")
            .endRecord();

    public static final String CSV_CONTENT = ""
            + "1,green\n"
            + ",blue,\n" + // invalid, ID is missing
            "2,grey,12.95";

    public static final String FAILURE_CONTENT = ""
            + ",blue,\n"; // invalid, ID is missing

    public static final String TSV_CONTENT = ""
            + "1\tgreen\n"
            + "\tblue\t\n" + // invalid, ID is missing
            "2\tgrey\t12.95";

    public static final String FAILURE_SUMMARY = "" +
            "Field id: cannot make \"long\" value: '': Field id type:LONG pos:0 not set and has no default value";


    /**
     * Test for a schema that is not a JSON but does not throw exception when trying to parse as an URI
     */
    @Test
    public void testSchemeValidation() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.setProperty(ConvertCSVToAvro.SCHEMA, "column1;column2");
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, "src/test/resources/Shapes_header.csv.avro");
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, "file:" + new File("src/test/resources/Shapes_header.csv.avro").getAbsolutePath());
        runner.assertValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, "");
        runner.assertNotValid();
    }

    /**
     * Basic test for tab separated files, similar to #test
     */
    @Test
    public void testTabSeparatedConversion() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, SCHEMA.toString());
        runner.setProperty(ConvertCSVToAvro.DELIMITER, "\\t");
        runner.assertValid();

        runner.enqueue(streamFor(TSV_CONTENT));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 2 rows", 2, converted);
        Assert.assertEquals("Should reject 1 row", 1, errors);

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 1);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("incompatible").get(0);
        String failureContent = new String(runner.getContentAsByteArray(incompatible),
                StandardCharsets.UTF_8);

        Assert.assertEquals("Should reject an invalid string and double",
                TSV_CONTENT, failureContent);
        Assert.assertEquals("Should accumulate error messages",
                FAILURE_SUMMARY, incompatible.getAttribute("errors"));
    }

    @Test
    public void testBasicConversion() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.enqueue(streamFor(CSV_CONTENT));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 2 rows", 2, converted);
        Assert.assertEquals("Should reject 1 row", 1, errors);

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 1);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("incompatible").get(0);
        String failureContent = new String(runner.getContentAsByteArray(incompatible),
                StandardCharsets.UTF_8);
        Assert.assertEquals("Should reject an invalid string and double",
                CSV_CONTENT, failureContent);
        Assert.assertEquals("Should accumulate error messages",
                FAILURE_SUMMARY, incompatible.getAttribute("errors"));
    }

    @Test
    public void testBasicConversionWithCompression() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, SCHEMA.toString());
        runner.setProperty(AbstractKiteConvertProcessor.COMPRESSION_TYPE, CodecType.DEFLATE.toString());
        runner.assertValid();

        runner.enqueue(streamFor(CSV_CONTENT));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 2 rows", 2, converted);
        Assert.assertEquals("Should reject 1 row", 1, errors);

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 1);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("incompatible").get(0);
        String failureContent = new String(runner.getContentAsByteArray(incompatible),
                StandardCharsets.UTF_8);
        Assert.assertEquals("Should reject an invalid string and double",
                CSV_CONTENT, failureContent);
        Assert.assertEquals("Should accumulate error messages",
                FAILURE_SUMMARY, incompatible.getAttribute("errors"));
    }

    @Test
    public void testAlternateCharset() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.setProperty(ConvertCSVToAvro.SCHEMA, SCHEMA.toString());
        runner.setProperty(ConvertCSVToAvro.CHARSET, "utf16");
        runner.assertValid();

        runner.enqueue(streamFor(CSV_CONTENT, Charset.forName("UTF-16")));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 2 rows", 2, converted);
        Assert.assertEquals("Should reject 1 row", 1, errors);

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 1);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("incompatible").get(0);
        Assert.assertEquals("Should accumulate error messages",
                FAILURE_SUMMARY, incompatible.getAttribute("errors"));
    }

    @Test
    public void testOnlyErrors() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.enqueue(streamFor(FAILURE_CONTENT));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 0 rows", 0, converted);
        Assert.assertEquals("Should reject 1 row", 1, errors);

        runner.assertTransferCount("success", 0);
        runner.assertTransferCount("failure", 1);
        runner.assertTransferCount("incompatible", 0);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("failure").get(0);
        Assert.assertEquals("Should set an error message",
                FAILURE_SUMMARY, incompatible.getAttribute("errors"));
    }

    @Test
    public void testEmptyContent() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.enqueue(streamFor(""));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 0 rows", 0, converted);
        Assert.assertEquals("Should reject 0 row", 0, errors);

        runner.assertTransferCount("success", 0);
        runner.assertTransferCount("failure", 1);
        runner.assertTransferCount("incompatible", 0);

        MockFlowFile incompatible = runner.getFlowFilesForRelationship("failure").get(0);
        Assert.assertEquals("Should set an error message",
                "No incoming records", incompatible.getAttribute("errors"));
    }

    @Test
    public void testBasicConversionNoErrors() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.enqueue(streamFor("1,green\n2,blue,\n3,grey,12.95"));
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 3 rows", 3, converted);
        Assert.assertEquals("Should reject 0 row", 0, errors);

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 0);
    }

    @Test
    public void testExpressionLanguageBasedCSVProperties() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(ConvertCSVToAvro.class);
        runner.assertNotValid();
        runner.setProperty(ConvertCSVToAvro.SCHEMA, SCHEMA.toString());
        runner.assertValid();

        runner.setProperty(ConvertCSVToAvro.DELIMITER, "${csv.delimiter}");
        runner.setProperty(ConvertCSVToAvro.QUOTE, "${csv.quote}");

        HashMap<String, String> flowFileAttributes = new HashMap<String,String>();
        flowFileAttributes.put("csv.delimiter", "|");
        flowFileAttributes.put("csv.quote", "~");

        runner.enqueue(streamFor("1|green\n2|~blue|field~|\n3|grey|12.95"), flowFileAttributes);
        runner.run();

        long converted = runner.getCounterValue("Converted records");
        long errors = runner.getCounterValue("Conversion errors");
        Assert.assertEquals("Should convert 3 rows", 3, converted);
        Assert.assertEquals("Should reject 0 row", 0, errors);

        runner.assertTransferCount("success", 1);
        runner.assertTransferCount("failure", 0);
        runner.assertTransferCount("incompatible", 0);

        final InputStream in = new ByteArrayInputStream(runner.getFlowFilesForRelationship("success").get(0).toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            assertTrue(dataFileReader.hasNext());
            GenericRecord record = dataFileReader.next();
            assertEquals(1L, record.get("id"));
            assertEquals("green", record.get("color").toString());
            assertNull(record.get("price"));

            assertTrue(dataFileReader.hasNext());
            record = dataFileReader.next();
            assertEquals(2L, record.get("id"));
            assertEquals("blue|field", record.get("color").toString());
            assertNull(record.get("price"));

            assertTrue(dataFileReader.hasNext());
            record = dataFileReader.next();
            assertEquals(3L, record.get("id"));
            assertEquals("grey", record.get("color").toString());
            assertEquals(12.95, record.get("price"));
        }
    }
}
