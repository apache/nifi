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
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.avro.AvroReaderWithEmbeddedSchema;
import org.apache.nifi.avro.AvroRecordReader;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestValidateRecord {
    private TestRunner runner;

    @BeforeEach
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
        runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst().assertContentEquals(content);
    }


    @Test
    public void testWriteFailureRoutesToFailure() throws InitializationException {
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
    public void testAppropriateServiceUsedForInvalidRecords() throws InitializationException, IOException {
        final String schema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc"));

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SCHEMA_TEXT, schema);
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

        final String content = """
                1, John Doe
                2, Jane Doe
                Three, Jack Doe
                """;

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();
        validFlowFile.assertAttributeEquals("record.count", "2");
        validFlowFile.assertContentEquals("""
                valid
                1,John Doe
                2,Jane Doe
                """);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        invalidFlowFile.assertContentEquals("invalid\n\"Three\",\"Jack Doe\"\n");
    }

    @Test
    public void testStrictTypeCheck() throws InitializationException, IOException {
        final String validateSchema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string-fields.avsc"));

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, "csv-header-derived");
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
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "true");

        // The validationSchema expects 'id' to be int, but CSVReader reads it as 'string'
        // with strict type check, the type difference is not allowed.
        final String content = """
                id, firstName, lastName
                1, John, Doe
                2, Jane, Doe
                Three, Jack, Doe
                """;

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 0);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "3");
        final String expectedInvalidContents = """
                invalid
                "1","John","Doe"
                "2","Jane","Doe"
                "Three","Jack","Doe"
                """;
        invalidFlowFile.assertContentEquals(expectedInvalidContents);
    }

    @Test
    public void testNonStrictTypeCheckWithAvroWriter() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        final String validateSchema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string-fields.avsc"));

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, "csv-header-derived");
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
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "false");

        // The validationSchema expects 'id' to be int, but CSVReader reads it as 'string'
        // with non-strict type check, the type difference should be accepted, and results should be written as 'int'.
        final String content = """
                id, firstName, lastName
                1, John, Doe
                2, Jane, Doe
                Three, Jack, Doe
                """;

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final AvroReader avroReader = new AvroReader();
        runner.addControllerService("avroReader", avroReader);
        runner.setProperty(avroReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.enableControllerService(avroReader);
        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();
        final byte[] validFlowFileBytes = validFlowFile.toByteArray();
        try (
        final ByteArrayInputStream resultContentStream = new ByteArrayInputStream(validFlowFileBytes);
        final RecordReader recordReader = avroReader.createRecordReader(validFlowFile.getAttributes(), resultContentStream, validFlowFileBytes.length, runner.getLogger())
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

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        final String expectedInvalidContents = """
                invalid
                "Three","Jack","Doe"
                """;
        invalidFlowFile.assertContentEquals(expectedInvalidContents);
    }

    /**
     * This test case demonstrates the limitation on JsonRecordSetWriter type-coercing when strict type check is disabled.
     * Since WriteJsonResult.writeRawRecord doesn't use record schema,
     * type coercing does not happen with JsonWriter even if strict type check is disabled.
     * <p>
     * E.g. When an input "1" as string is given, and output field schema is int:
     * <ul>
     * <li>Expected result: "id": 1 (without quote)</li>
     * <li>Actual result: "id": "1" (with quote)</li>
     * </ul>
     */
    @Test
    public void testNonStrictTypeCheckWithJsonWriter() throws InitializationException, IOException {
        final String validateSchema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string-fields.avsc"));

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, "csv-header-derived");
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
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "false");

        // The validationSchema expects 'id' to be int, but CSVReader reads it as 'string'
        // with non-strict type check, the type difference should be accepted, and results should be written as 'int'.
        final String content = """
                id, firstName, lastName
                1, John, Doe
                2, Jane, Doe
                Three, Jack, Doe
                """;

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        /*
        TODO: JsonRecordSetWriter does not coerce value. Should we fix this??
         */
        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();
        validFlowFile.assertAttributeEquals("record.count", "2");
        final String expectedValidContents = "[" +
                "{\"id\":\"1\",\"firstName\":\"John\",\"lastName\":\"Doe\"}," +
                "{\"id\":\"2\",\"firstName\":\"Jane\",\"lastName\":\"Doe\"}" +
                "]";
        validFlowFile.assertContentEquals(expectedValidContents);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        final String expectedInvalidContents = """
                invalid
                "Three","Jack","Doe"
                """;
        invalidFlowFile.assertContentEquals(expectedInvalidContents);
    }

    @Test
    public void testValidateNestedMap() throws InitializationException, IOException {
        final String validateSchema = Files.readString(Paths.get("src/test/resources/TestValidateRecord/nested-map-schema.avsc"));

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.setProperty(jsonReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, "schema-text-property");
        runner.setProperty(jsonReader, SCHEMA_TEXT, validateSchema);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter validWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", validWriter);
        runner.setProperty(validWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(validWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");

        // Both records should be valid if strict type checking is off
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "false");
        final Path nestedMapINputpath = Paths.get("src/test/resources/TestValidateRecord/nested-map-input.json");
        runner.enqueue(nestedMapINputpath);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 0);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);
        runner.clearTransferState();

        // The second record should be invalid if strict type checking is on
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "true");
        runner.enqueue(nestedMapINputpath);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);
    }

    @Test
    public void testValidateMissingRequiredArray() throws InitializationException, IOException {
        final String validateSchema = Files.readString(Paths.get("src/test/resources/TestValidateRecord/missing-array.avsc"));

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.setProperty(jsonReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, "schema-text-property");
        runner.setProperty(jsonReader, SCHEMA_TEXT, validateSchema);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter validWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", validWriter);
        runner.setProperty(validWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(validWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "true");

        // The record is invalid due to not containing the required array from the schema
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "false");
        runner.enqueue(Paths.get("src/test/resources/TestValidateRecord/missing-array.json"));
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 0);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);
        runner.clearTransferState();
    }

    @Test
    public void testValidateMissingRequiredArrayWithDefault() throws InitializationException, IOException {
        final String validateSchema = Files.readString(Paths.get("src/test/resources/TestValidateRecord/missing-array-with-default.avsc"));

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.setProperty(jsonReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, "schema-text-property");
        runner.setProperty(jsonReader, SCHEMA_TEXT, validateSchema);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter validWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", validWriter);
        runner.setProperty(validWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(validWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "true");

        // The record is invalid due to not containing the required array from the schema
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "false");
        runner.enqueue(Paths.get("src/test/resources/TestValidateRecord/missing-array.json"));
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 0);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);
        runner.clearTransferState();
    }


    @Test
    public void testValidateJsonTimestamp() throws IOException, InitializationException {
        final String validateSchema = Files.readString(Paths.get("src/test/resources/TestValidateRecord/timestamp.avsc"));

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.setProperty(jsonReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, "schema-text-property");
        runner.setProperty(jsonReader, SCHEMA_TEXT, validateSchema);
        runner.setProperty(jsonReader, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss");
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter validWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", validWriter);
        runner.setProperty(validWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.setProperty(validWriter, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss");
        runner.enableControllerService(validWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");

        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "true");
        final Path timestampPath = Paths.get("src/test/resources/TestValidateRecord/timestamp.json");
        runner.enqueue(timestampPath);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();
        validFlowFile.assertContentEquals(new File("src/test/resources/TestValidateRecord/timestamp.json"));

        // Test with a timestamp that has an invalid format.
        runner.clearTransferState();

        runner.disableControllerService(jsonReader);
        runner.setProperty(jsonReader, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss");
        runner.enqueue(timestampPath);
        runner.enableControllerService(jsonReader);

        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertContentEquals(new File("src/test/resources/TestValidateRecord/timestamp.json"));

        // Test with an Inferred Schema.
        runner.disableControllerService(jsonReader);
        runner.setProperty(jsonReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        runner.setProperty(jsonReader, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss");
        runner.enableControllerService(jsonReader);

        runner.clearTransferState();
        runner.enqueue(timestampPath);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        final MockFlowFile validFlowFileInferredSchema = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();
        validFlowFileInferredSchema.assertContentEquals(new File("src/test/resources/TestValidateRecord/timestamp.json"));
    }

    @Test
    public void testValidateCsvTimestampZoneOffset() throws InitializationException {
        final String recordSchema = """
                {
                  "name": "ts",
                  "namespace": "nifi",
                  "type": "record",
                  "fields": [{
                      "name": "created",
                      "type": {
                          "type": "long", "logicalType": "timestamp-millis"
                       }
                   }]
                }
                """;

        final String inputDateTime = "2020-01-01 12:00:00";
        final Timestamp inputTimestamp = Timestamp.valueOf(inputDateTime);
        final LocalDateTime inputLocalDateTime = inputTimestamp.toLocalDateTime();
        final String systemZoneOffsetId = getSystemZoneOffsetId(inputLocalDateTime);

        final String serializedRecord = inputDateTime + systemZoneOffsetId;
        final String timestampFormat = "yyyy-MM-dd HH:mm:ssZZZZZ";

        final String readerServiceId = "reader";
        final String writerServiceId = "writer";

        final CSVReader recordReader = new CSVReader();
        runner.addControllerService(readerServiceId, recordReader);
        runner.setProperty(recordReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(recordReader, SCHEMA_TEXT, recordSchema);
        runner.setProperty(recordReader, DateTimeUtils.TIMESTAMP_FORMAT, timestampFormat);
        runner.enableControllerService(recordReader);

        final CSVRecordSetWriter recordSetWriter = new CSVRecordSetWriter();
        runner.addControllerService(writerServiceId, recordSetWriter);
        runner.setProperty(recordSetWriter, "Schema Write Strategy", JsonRecordSetWriter.AVRO_SCHEMA_ATTRIBUTE.getValue());
        runner.setProperty(recordSetWriter, DateTimeUtils.TIMESTAMP_FORMAT, timestampFormat);
        runner.setProperty(recordSetWriter, CSVUtils.INCLUDE_HEADER_LINE, Boolean.FALSE.toString());
        runner.enableControllerService(recordSetWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, readerServiceId);
        runner.setProperty(ValidateRecord.RECORD_WRITER, writerServiceId);
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, recordSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, writerServiceId);
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, Boolean.TRUE.toString());

        runner.enqueue(serializedRecord);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();
        final String flowFileContent = validFlowFile.getContent().trim();

        assertEquals(serializedRecord, flowFileContent);
    }

    @Test
    public void testValidateMaps() throws IOException, InitializationException, MalformedRecordException {
        final String validateSchema = Files.readString(Paths.get("src/test/resources/TestValidateRecord/int-maps-schema.avsc"));

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.setProperty(jsonReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SCHEMA_TEXT, validateSchema);
        runner.enableControllerService(jsonReader);

        final AvroRecordSetWriter avroWriter = new AvroRecordSetWriter();
        runner.addControllerService("writer", avroWriter);
        runner.enableControllerService(avroWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SCHEMA_TEXT, validateSchema);
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");

        runner.enqueue(Paths.get("src/test/resources/TestValidateRecord/int-maps-data.json"));
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();

        byte[] source = validFlowFile.toByteArray();

        try (final InputStream in = new ByteArrayInputStream(source); final AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in)) {
            final Object[] values = reader.nextRecord().getValues();
            assertEquals("uuid", values[0]);
            assertEquals(2, ((Map<?, ?>) values[1]).size());
            final Object[] data = (Object[]) values[2];
            assertEquals(3, data.length);
            assertEquals(2, ( (Map<?, ?>) ((Record) data[0]).getValue("points")).size());
            assertEquals(2, ( (Map<?, ?>) ((Record) data[1]).getValue("points")).size());
            assertEquals(2, ( (Map<?, ?>) ((Record) data[2]).getValue("points")).size());
        }
    }

    @Test
    public void testValidationsDetailsAttributeForInvalidRecords()  throws InitializationException, IOException {
        final String schema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc"));

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SCHEMA_TEXT, schema);
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
        runner.setProperty(ValidateRecord.MAX_VALIDATION_DETAILS_LENGTH, "150");
        runner.setProperty(ValidateRecord.VALIDATION_DETAILS_ATTRIBUTE_NAME, "valDetails");

        final String content = """
                1, John Doe
                2, Jane Doe
                Three, Jack Doe
                """;

        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        invalidFlowFile.assertContentEquals("invalid\n\"Three\",\"Jack Doe\"\n");
        invalidFlowFile.assertAttributeExists("valDetails");
        invalidFlowFile.assertAttributeEquals("valDetails", "Records in this FlowFile were invalid for the following reasons: ; "
                + "The following 1 fields had values whose type did not match the schema: [/id]");
    }

    @Test
    public void testValidationForNullElementArrayAndMap() throws Exception {
        final AvroReader avroReader = new AvroReader();
        runner.addControllerService("reader", avroReader);
        runner.enableControllerService(avroReader);

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
        runner.setProperty(ValidateRecord.MAX_VALIDATION_DETAILS_LENGTH, "150");
        runner.setProperty(ValidateRecord.VALIDATION_DETAILS_ATTRIBUTE_NAME, "valDetails");

        runner.enqueue(Paths.get("src/test/resources/TestValidateRecord/array-and-map-with-null-element.avro"));
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_INVALID, 0);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);

        final MockFlowFile validFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();
        validFlowFile.assertAttributeEquals("record.count", "1");
        validFlowFile.assertContentEquals("valid\n[text, null],{key=null}\n");
    }

    @Test
    public void testSchemaNameAccess() throws Exception {
        final String schema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc"));

        final MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("record", AvroTypeUtil.createSchema(new Schema.Parser().parse(schema)));
        runner.addControllerService("registry", registry);
        runner.enableControllerService(registry);

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SCHEMA_TEXT, schema);
        runner.enableControllerService(csvReader);

        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("writer", csvWriter);
        runner.setProperty(csvWriter, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvWriter, SCHEMA_TEXT, schema);
        runner.enableControllerService(csvWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY);
        runner.setProperty(SCHEMA_REGISTRY, "registry");
        runner.setProperty(SCHEMA_NAME, "record");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.MAX_VALIDATION_DETAILS_LENGTH, "150");
        runner.setProperty(ValidateRecord.VALIDATION_DETAILS_ATTRIBUTE_NAME, "valDetails");

        final String content = """
                1, John Doe
                2, Jane Doe
                Three, Jack Doe
                """;
        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        invalidFlowFile.assertContentEquals("invalid\n\"Three\",\"Jack Doe\"\n");
        invalidFlowFile.assertAttributeExists("valDetails");
        invalidFlowFile.assertAttributeEquals("valDetails", "Records in this FlowFile were invalid for the following reasons: ; "
                + "The following 1 fields had values whose type did not match the schema: [/id]");
    }

    @Test
    public void testSchemaNameAccessWithBranch() throws Exception {
        final String schema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc"));

        final MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("record", "branch", AvroTypeUtil.createSchema(new Schema.Parser().parse(schema)));
        runner.addControllerService("registry", registry);
        runner.enableControllerService(registry);

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SCHEMA_TEXT, schema);
        runner.enableControllerService(csvReader);

        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("writer", csvWriter);
        runner.setProperty(csvWriter, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvWriter, SCHEMA_TEXT, schema);
        runner.enableControllerService(csvWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY);
        runner.setProperty(SCHEMA_REGISTRY, "registry");
        runner.setProperty(SCHEMA_NAME, "record");
        runner.setProperty(SCHEMA_BRANCH_NAME, "branch");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.MAX_VALIDATION_DETAILS_LENGTH, "150");
        runner.setProperty(ValidateRecord.VALIDATION_DETAILS_ATTRIBUTE_NAME, "valDetails");

        final String content = """
                1, John Doe
                2, Jane Doe
                Three, Jack Doe
                """;
        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        invalidFlowFile.assertContentEquals("invalid\n\"Three\",\"Jack Doe\"\n");
        invalidFlowFile.assertAttributeExists("valDetails");
        invalidFlowFile.assertAttributeEquals("valDetails", "Records in this FlowFile were invalid for the following reasons: ; "
                + "The following 1 fields had values whose type did not match the schema: [/id]");
    }

    @Test
    public void testSchemaNameAccessWithVersion() throws Exception {
        final String schema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc"));

        final MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("record", 1, AvroTypeUtil.createSchema(new Schema.Parser().parse(schema)));
        runner.addControllerService("registry", registry);
        runner.enableControllerService(registry);

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SCHEMA_TEXT, schema);
        runner.enableControllerService(csvReader);

        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("writer", csvWriter);
        runner.setProperty(csvWriter, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvWriter, SCHEMA_TEXT, schema);
        runner.enableControllerService(csvWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY);
        runner.setProperty(SCHEMA_REGISTRY, "registry");
        runner.setProperty(SCHEMA_NAME, "record");
        runner.setProperty(SCHEMA_VERSION, "1");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.MAX_VALIDATION_DETAILS_LENGTH, "150");
        runner.setProperty(ValidateRecord.VALIDATION_DETAILS_ATTRIBUTE_NAME, "valDetails");

        final String content = """
                1, John Doe
                2, Jane Doe
                Three, Jack Doe
                """;
        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        invalidFlowFile.assertContentEquals("invalid\n\"Three\",\"Jack Doe\"\n");
        invalidFlowFile.assertAttributeExists("valDetails");
        invalidFlowFile.assertAttributeEquals("valDetails", "Records in this FlowFile were invalid for the following reasons: ; "
                + "The following 1 fields had values whose type did not match the schema: [/id]");
    }

    @Test
    public void testSchemaNameAccessWithBranchAndVersion() throws Exception {
        final String schema = Files.readString(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc"));

        final MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("record", "branch", 1, AvroTypeUtil.createSchema(new Schema.Parser().parse(schema)));
        runner.addControllerService("registry", registry);
        runner.enableControllerService(registry);

        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("reader", csvReader);
        runner.setProperty(csvReader, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SCHEMA_TEXT, schema);
        runner.enableControllerService(csvReader);

        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("writer", csvWriter);
        runner.setProperty(csvWriter, ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvWriter, SCHEMA_TEXT, schema);
        runner.enableControllerService(csvWriter);

        final MockRecordWriter invalidWriter = new MockRecordWriter("invalid", true);
        runner.addControllerService("invalid-writer", invalidWriter);
        runner.enableControllerService(invalidWriter);

        runner.setProperty(ValidateRecord.RECORD_READER, "reader");
        runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        runner.setProperty(ValidateRecord.INVALID_RECORD_WRITER, "invalid-writer");
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY);
        runner.setProperty(SCHEMA_REGISTRY, "registry");
        runner.setProperty(SCHEMA_NAME, "record");
        runner.setProperty(SCHEMA_BRANCH_NAME, "branch");
        runner.setProperty(SCHEMA_VERSION, "1");
        runner.setProperty(ValidateRecord.ALLOW_EXTRA_FIELDS, "false");
        runner.setProperty(ValidateRecord.MAX_VALIDATION_DETAILS_LENGTH, "150");
        runner.setProperty(ValidateRecord.VALIDATION_DETAILS_ATTRIBUTE_NAME, "valDetails");

        final String content = """
                1, John Doe
                2, Jane Doe
                Three, Jack Doe
                """;
        runner.enqueue(content);
        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);

        final MockFlowFile invalidFlowFile = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalidFlowFile.assertAttributeEquals("record.count", "1");
        invalidFlowFile.assertContentEquals("invalid\n\"Three\",\"Jack Doe\"\n");
        invalidFlowFile.assertAttributeExists("valDetails");
        invalidFlowFile.assertAttributeEquals("valDetails", "Records in this FlowFile were invalid for the following reasons: ; "
                + "The following 1 fields had values whose type did not match the schema: [/id]");
    }

    private String getSystemZoneOffsetId(final LocalDateTime inputLocalDateTime) {
        final ZoneId systemDefaultZoneId = ZoneOffset.systemDefault();
        final ZoneRules zoneRules = systemDefaultZoneId.getRules();

        final ZoneOffset systemZoneOffset = zoneRules.getOffset(inputLocalDateTime);
        return systemZoneOffset.getId();
    }
}
