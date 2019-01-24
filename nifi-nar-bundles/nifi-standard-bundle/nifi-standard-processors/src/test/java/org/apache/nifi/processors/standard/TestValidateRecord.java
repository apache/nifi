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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestValidateRecord {

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(ValidateRecord.class);
    }

    @Test
    public void testColumnsOrder() throws InitializationException {
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        runner.setProperty(csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        runner.setProperty(csvReader, CSVUtils.TRAILING_DELIMITER, "false");
        runner.enableControllerService(csvReader);

        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("writer", csvWriter);
        runner.setProperty(csvWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(csvWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");

        final String content = "fieldA,fieldB,fieldC,fieldD,fieldE,fieldF\nvalueA,valueB,valueC,valueD,valueE,valueF\nvalueA,valueB,valueC,valueD,valueE,valueF\n";
        runner.enqueue(content);
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateRecord.REL_VALID, 1);
        runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).get(0).assertContentEquals(content);
    }


    @Test
    public void testWriteFailureRoutesToFaliure() throws InitializationException {
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        runner.setProperty(csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        runner.setProperty(csvReader, CSVUtils.TRAILING_DELIMITER, "false");
        runner.enableControllerService(csvReader);

        MockRecordWriter writer = new MockRecordWriter("header", false, 1);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");

        final String content = "fieldA,fieldB,fieldC,fieldD,fieldE,fieldF\nvalueA,valueB,valueC,valueD,valueE,valueF\nvalueA,valueB,valueC,valueD,valueE,valueF\n";
        runner.enqueue(content);
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateRecord.REL_FAILURE, 1);
    }

    @Test
    public void testAppropriateServiceUsedForInvalidRecords() throws InitializationException, UnsupportedEncodingException, IOException {
        final String schema = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc")), "UTF-8");

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_TEXT, schema);
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "false");
        runner.setProperty(csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        runner.setProperty(csvReader, CSVUtils.TRAILING_DELIMITER, "false");
        runner.enableControllerService(csvReader);

        final MockRecordWriter validWriter = new MockRecordWriter("valid", false);
        runner.addControllerService("writer", validWriter);
        runner.enableControllerService(validWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");

        final String content = "1, John Doe\n"
            + "2, Jane Doe\n"
            + "Three, Jack Doe\n";

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).get(0);
        validFlowFile.assertAttributeEquals("record.count", "2");
        validFlowFile.assertContentEquals("valid\n"
            + "1,John Doe\n"
            + "2,Jane Doe\n");

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).get(0);
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        invalidFlowFile.assertContentEquals("invalid\n\"Three\",\"Jack Doe\"\n");
    }

    @Test
    public void testStrictTypeCheck() throws InitializationException, IOException {
        final String validateSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string-fields.avsc")), "UTF-8");

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "csv-header-derived");
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        runner.setProperty(csvReader, CSVUtils.IGNORE_CSV_HEADER, "true");
        runner.setProperty(csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        runner.setProperty(csvReader, CSVUtils.TRAILING_DELIMITER, "false");
        runner.enableControllerService(csvReader);

        final JsonRecordSetWriter validWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", validWriter);
        runner.setProperty(validWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(validWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(ValidateRecord.SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "true");

        // The validationSchema expects 'id' to be int, but CSVReader reads it as 'string'
        // with strict type check, the type difference is not allowed.
        final String content = "id, firstName, lastName\n"
                + "1, John, Doe\n"
                + "2, Jane, Doe\n"
                + "Three, Jack, Doe\n";

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 0);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).get(0);
        invalidFlowFile.assertAttributeEquals("record.count", "3");
        final String expectedInvalidContents = "invalid\n"
                + "\"1\",\"John\",\"Doe\"\n"
                + "\"2\",\"Jane\",\"Doe\"\n"
                + "\"Three\",\"Jack\",\"Doe\"\n";
        invalidFlowFile.assertContentEquals(expectedInvalidContents);
    }

    @Test
    public void testNonStrictTypeCheckWithAvroWriter() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        final String validateSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string-fields.avsc")), "UTF-8");

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "csv-header-derived");
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        runner.setProperty(csvReader, CSVUtils.IGNORE_CSV_HEADER, "true");
        runner.setProperty(csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        runner.setProperty(csvReader, CSVUtils.TRAILING_DELIMITER, "false");
        runner.enableControllerService(csvReader);

        final AvroRecordSetWriter validWriter = new AvroRecordSetWriter();
        runner.addControllerService("writer", validWriter);
        runner.setProperty(validWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(validWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(ValidateRecord.SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "false");

        // The validationSchema expects 'id' to be int, but CSVReader reads it as 'string'
        // with non-strict type check, the type difference should be accepted, and results should be written as 'int'.
        final String content = "id, firstName, lastName\n"
                + "1, John, Doe\n"
                + "2, Jane, Doe\n"
                + "Three, Jack, Doe\n";

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final AvroReader avroReader = new AvroReader();
        runner.addControllerService("avroReader", avroReader);
        runner.setProperty(avroReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.enableControllerService(avroReader);
        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).get(0);
        try (
        final ByteArrayInputStream resultContentStream = new ByteArrayInputStream(validFlowFile.toByteArray());
        final RecordReader recordReader = avroReader.createRecordReader(validFlowFile.getAttributes(), resultContentStream, runner.getLogger());
        ) {
            final RecordSchema resultSchema = recordReader.getSchema();
            assertEquals(3, resultSchema.getFieldCount());

            // The id field should be an int field.
            final Optional<RecordField> idField = resultSchema.getField("id");
            assertTrue(idField.isPresent());
            assertEquals(RecordFieldType.INT, idField.get().getDataType().getFieldType());

            validFlowFile.assertAttributeEquals("record.count", "2");

            Record record = recordReader.nextRecord();
            assertEquals(1, record.getValue("id"));
            assertEquals("John", record.getValue("firstName"));
            assertEquals("Doe", record.getValue("lastName"));

            record = recordReader.nextRecord();
            assertEquals(2, record.getValue("id"));
            assertEquals("Jane", record.getValue("firstName"));
            assertEquals("Doe", record.getValue("lastName"));
        }

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).get(0);
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        final String expectedInvalidContents = "invalid\n"
                + "\"Three\",\"Jack\",\"Doe\"\n";
        invalidFlowFile.assertContentEquals(expectedInvalidContents);
    }

    /**
     * This test case demonstrates the limitation on JsonRecordSetWriter type-coercing when strict type check is disabled.
     * Since WriteJsonResult.writeRawRecord doesn't use record schema,
     * type coercing does not happen with JsonWriter even if strict type check is disabled.
     *
     * E.g. When an input "1" as string is given, and output field schema is int:
     * <ul>
     * <li>Expected result: "id": 1 (without quote)</li>
     * <li>Actual result: "id": "1" (with quote)</li>
     * </ul>
     */
    @Test
    public void testNonStrictTypeCheckWithJsonWriter() throws InitializationException, IOException {
        final String validateSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string-fields.avsc")), "UTF-8");

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "csv-header-derived");
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        runner.setProperty(csvReader, CSVUtils.IGNORE_CSV_HEADER, "true");
        runner.setProperty(csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        runner.setProperty(csvReader, CSVUtils.TRAILING_DELIMITER, "false");
        runner.enableControllerService(csvReader);

        final JsonRecordSetWriter validWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", validWriter);
        runner.setProperty(validWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(validWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(ValidateRecord.SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "false");

        // The validationSchema expects 'id' to be int, but CSVReader reads it as 'string'
        // with non-strict type check, the type difference should be accepted, and results should be written as 'int'.
        final String content = "id, firstName, lastName\n"
                + "1, John, Doe\n"
                + "2, Jane, Doe\n"
                + "Three, Jack, Doe\n";

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        /*
        TODO: JsonRecordSetWriter does not coerce value. Should we fix this??
         */
        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).get(0);
        validFlowFile.assertAttributeEquals("record.count", "2");
        final String expectedValidContents = "[" +
                "{\"id\":\"1\",\"firstName\":\"John\",\"lastName\":\"Doe\"}," +
                "{\"id\":\"2\",\"firstName\":\"Jane\",\"lastName\":\"Doe\"}" +
                "]";
        validFlowFile.assertContentEquals(expectedValidContents);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).get(0);
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        final String expectedInvalidContents = "invalid\n"
                + "\"Three\",\"Jack\",\"Doe\"\n";
        invalidFlowFile.assertContentEquals(expectedInvalidContents);
    }

}
