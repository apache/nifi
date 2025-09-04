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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.avro.NonCachingDatumReader;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.xml.XMLReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.xerial.snappy.SnappyInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Pretty-printing is not portable across operating systems")
public class TestConvertRecord {

    private static final String PERSON_SCHEMA;
    private static final String READER_ID = "reader";
    private static final String WRITER_ID = "writer";
    private TestRunner runner;

    static {
        try {
            PERSON_SCHEMA = Files.readString(Paths.get("src/test/resources/TestConvertRecord/schema/person.avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(ConvertRecord.class);
    }

    @Test
    public void testXMLReaderAttributePrefixWithInferredSchema() throws InitializationException {
        // Configure XML Reader to infer schema, parse attributes, and prefix attribute field names
        final XMLReader xmlReader = new XMLReader();
        final String xmlReaderId = "xml-reader";
        runner.addControllerService(xmlReaderId, xmlReader);
        runner.setProperty(xmlReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        // Use single-record mode to keep element name as a nested field (e.g., "to")
        runner.setProperty(xmlReader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        runner.setProperty(xmlReader, XMLReader.PARSE_XML_ATTRIBUTES, "true");
        runner.setProperty(xmlReader, XMLReader.ATTRIBUTE_PREFIX, "attr_");
        runner.setProperty(xmlReader, XMLReader.CONTENT_FIELD_NAME, "tagval");
        runner.enableControllerService(xmlReader);

        // Configure JSON Writer to inherit schema from reader
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        final String jsonWriterId = "json-writer";
        runner.addControllerService(jsonWriterId, jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.enableControllerService(jsonWriter);

        runner.setProperty(ConvertRecord.RECORD_READER, xmlReaderId);
        runner.setProperty(ConvertRecord.RECORD_WRITER, jsonWriterId);

        final String input = """
                <note>
                  <to alias=\"Toto\">Thomas Mills</to>
                </note>
                """;

        runner.enqueue(input);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();

        // Expected behavior: attribute field uses configured prefix
        final String expected = "[{\"to\":{\"attr_alias\":\"Toto\",\"tagval\":\"Thomas Mills\"}}]";
        assertEquals(expected, new String(flowFile.toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    public void testSuccessfulConversion() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        runner.addControllerService(READER_ID, readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService(WRITER_ID, writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("header\nJohn Doe,48\nJane Doe,47\nJimmy Doe,14\n");
    }

    @Test
    public void testDropEmpty() throws InitializationException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        runner.addControllerService(READER_ID, readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService(WRITER_ID, writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConvertRecord.INCLUDE_ZERO_RECORD_FLOWFILES, "false");
        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 0);
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_FAILURE, 0);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();

        out.assertAttributeEquals("record.count", "3");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertContentEquals("header\nJohn Doe,48\nJane Doe,47\nJimmy Doe,14\n");
    }

    @Test
    public void testReadFailure() throws InitializationException, IOException {
        final MockRecordParser readerService = new MockRecordParser(2);
        final MockRecordWriter writerService = new MockRecordWriter("header", false);

        runner.addControllerService(READER_ID, readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService(WRITER_ID, writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        final MockFlowFile original = runner.enqueue("hello");
        runner.run();

        // Original FlowFile should be routed to 'failure' relationship without modification
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertRecord.REL_FAILURE).getFirst();
        out.assertContentEquals(original.toByteArray());
        out.assertAttributeEquals("record.error.message", "Intentional Unit Test Exception because 2 records have been read");
    }


    @Test
    public void testWriteFailure() throws InitializationException, IOException {
        final MockRecordParser readerService = new MockRecordParser();
        final MockRecordWriter writerService = new MockRecordWriter("header", false, 2);

        runner.addControllerService(READER_ID, readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService(WRITER_ID, writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        final MockFlowFile original = runner.enqueue("hello");
        runner.run();

        // Original FlowFile should be routed to 'failure' relationship without modification
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertRecord.REL_FAILURE).getFirst();
        out.assertContentEquals(original.toByteArray());
        out.assertAttributeEquals("record.error.message", "Unit Test intentionally throwing IOException after 2 records were written");
    }

    @Test
    public void testJSONCompression() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService(READER_ID, jsonReader);

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, PERSON_SCHEMA);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService(WRITER_ID, jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, PERSON_SCHEMA);
        runner.setProperty(jsonWriter, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.setProperty(jsonWriter, JsonRecordSetWriter.COMPRESSION_FORMAT, "snappy");
        runner.enableControllerService(jsonWriter);

        final Path person = Paths.get("src/test/resources/TestConvertRecord/input/person.json");
        runner.enqueue(person);

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final SnappyInputStream sis = new SnappyInputStream(new ByteArrayInputStream(flowFile.toByteArray())); final OutputStream out = baos) {
            final byte[] buffer = new byte[8192]; int len;
            while ((len = sis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            out.flush();
        }

        assertEquals(Files.readString(person), baos.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testCSVFormattingWithEL() throws InitializationException {
        CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.setProperty(csvReader, CSVUtils.VALUE_SEPARATOR, "${csv.in.delimiter}");
        runner.setProperty(csvReader, CSVUtils.QUOTE_CHAR, "${csv.in.quote}");
        runner.setProperty(csvReader, CSVUtils.ESCAPE_CHAR, "${csv.in.escape}");
        runner.setProperty(csvReader, CSVUtils.COMMENT_MARKER, "${csv.in.comment}");
        runner.enableControllerService(csvReader);

        CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("csv-writer", csvWriter);
        runner.setProperty(csvWriter, CSVUtils.VALUE_SEPARATOR, "${csv.out.delimiter}");
        runner.setProperty(csvWriter, CSVUtils.QUOTE_CHAR, "${csv.out.quote}");
        runner.setProperty(csvWriter, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_ALL);
        runner.enableControllerService(csvWriter);

        runner.setProperty(ConvertRecord.RECORD_READER, "csv-reader");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "csv-writer");

        String ffContent = """
                ~ comment
                id|username|password
                123|'John'|^|^'^^
                """;

        Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("csv.in.delimiter", "|");
        ffAttributes.put("csv.in.quote", "'");
        ffAttributes.put("csv.in.escape", "^");
        ffAttributes.put("csv.in.comment", "~");
        ffAttributes.put("csv.out.delimiter", "\t");
        ffAttributes.put("csv.out.quote", "`");

        runner.enqueue(ffContent, ffAttributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();

        String expected = """
                `id`\t`username`\t`password`
                `123`\t`John`\t`|'^`
                """;
        assertEquals(expected, new String(flowFile.toByteArray()));
    }

    @Test
    public void testJSONLongToInt() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService(READER_ID, jsonReader);

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, PERSON_SCHEMA);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService(WRITER_ID, jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, PERSON_SCHEMA);
        runner.setProperty(jsonWriter, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestConvertRecord/input/person_long_id.json"));

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_FAILURE, 1);
    }

    @Test
    public void testEnumBadValue() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService(READER_ID, jsonReader);

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, PERSON_SCHEMA);
        runner.enableControllerService(jsonReader);

        final AvroRecordSetWriter avroWriter = new AvroRecordSetWriter();
        runner.addControllerService(WRITER_ID, avroWriter);
        runner.setProperty(avroWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(avroWriter, SchemaAccessUtils.SCHEMA_TEXT, PERSON_SCHEMA);
        runner.enableControllerService(avroWriter);

        runner.enqueue(Paths.get("src/test/resources/TestConvertRecord/input/person_bad_enum.json"));

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_FAILURE, 1);
    }

    @Test
    public void testEnumUnionString() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService(READER_ID, jsonReader);

        final String personWithUnionEnumStringSchema = Files.readString(Paths.get("src/test/resources/TestConvertRecord/schema/person_with_union_enum_string.avsc"));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, personWithUnionEnumStringSchema);
        runner.enableControllerService(jsonReader);

        final AvroRecordSetWriter avroWriter = new AvroRecordSetWriter();
        runner.addControllerService(WRITER_ID, avroWriter);
        runner.setProperty(avroWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(avroWriter, SchemaAccessUtils.SCHEMA_TEXT, personWithUnionEnumStringSchema);
        runner.enableControllerService(avroWriter);

        runner.enqueue(Paths.get("src/test/resources/TestConvertRecord/input/person_bad_enum.json"));

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testDateConversionWithUTCMinusTimezone() throws Exception {
        final String timezone = System.getProperty("user.timezone");
        System.setProperty("user.timezone", "EST");
        try {
            JsonTreeReader jsonTreeReader = new JsonTreeReader();
            runner.addControllerService("json-reader", jsonTreeReader);
            runner.setProperty(jsonTreeReader, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd");
            runner.enableControllerService(jsonTreeReader);

            AvroRecordSetWriter avroWriter = new AvroRecordSetWriter();
            runner.addControllerService("avro-writer", avroWriter);
            runner.enableControllerService(avroWriter);

            runner.setProperty(ConvertRecord.RECORD_READER, "json-reader");
            runner.setProperty(ConvertRecord.RECORD_WRITER, "avro-writer");

            runner.enqueue("{ \"date\": \"1970-01-02\" }");

            runner.run();

            runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);

            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();
            DataFileStream<GenericRecord> avroStream = new DataFileStream<>(flowFile.getContentStream(), new NonCachingDatumReader<>());

            assertTrue(avroStream.hasNext());
            assertEquals(1, avroStream.next().get("date")); // see https://avro.apache.org/docs/1.10.0/spec.html#Date
        } finally {
            if (timezone != null) {
                System.setProperty("user.timezone", timezone);
            }
        }
    }

    @Test
    public void testJSONDroppingUnknownFields() throws InitializationException, IOException {
        final String personJobSchema =
                Files.readString(Paths.get("src/test/resources/TestConvertRecord/schema/personJob.avsc"));
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService(READER_ID, jsonReader);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, personJobSchema);
        runner.enableControllerService(jsonReader);
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService(WRITER_ID, jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, personJobSchema);
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestConvertRecord/input/personJob_dropfield.json"));

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();

        // This covers all the cases
        // "undefinedKeyInObject", "undefinedKey", "undefinedObjectArray", "undefinedObject", "undefinedScalarArray"
        assertFalse(flowFile.getContent().contains("undefined"));
    }

    @Test
    public void testJSONWithDefinedFieldTypeBytesAndLogicalTypeDecimal() throws Exception {
        final String schema = """
                {
                  "type": "record",
                  "name": "ExampleRecord",
                  "fields": [
                    {
                      "name": "big_decimal_field",
                      "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 10,
                        "scale": 4
                      },
                      "default": "0.0000"
                    }, {
                      "name": "default_field",
                      "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 10,
                        "scale": 4
                      },
                      "default": "0.0001"
                    }
                  ]
                }
                """;

        final String inputContent = """
                [
                  {
                    "big_decimal_field": 14000.5833
                  }
                ]
                """;

        final String expectedContent = """
                [
                  {
                    "big_decimal_field": 14000.5833,
                    "default_field": 0.0001
                  }
                ]
                """.replaceAll("\\s", "");

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService(READER_ID, jsonReader);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, schema);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService(WRITER_ID, jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, schema);
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.setProperty(ConvertRecord.RECORD_READER, READER_ID);
        runner.setProperty(ConvertRecord.RECORD_WRITER, WRITER_ID);
        runner.enqueue(inputContent);

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(expectedContent);
    }
}
