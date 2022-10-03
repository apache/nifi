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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestGenerateFakeRecord {

    private TestRunner testRunner;
    private GenerateFakeRecord processor;

    @BeforeEach
    public void setup() {
        processor = new GenerateFakeRecord();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testGenerateNoNullableFields() throws Exception {
        testRunner.setProperty("address", GenerateFakeRecord.FT_ADDRESS.getValue());
        testRunner.setProperty("aircraft", GenerateFakeRecord.FT_AIRCRAFT.getValue());
        testRunner.setProperty("airport", GenerateFakeRecord.FT_AIRPORT.getValue());
        testRunner.setProperty("god", GenerateFakeRecord.FT_ANCIENT_GOD.getValue());
        testRunner.setProperty("hero", GenerateFakeRecord.FT_ANCIENT_HERO.getValue());
        testRunner.setProperty("primordial", GenerateFakeRecord.FT_ANCIENT_PRIMORDIAL.getValue());
        testRunner.setProperty("titan", GenerateFakeRecord.FT_ANCIENT_TITAN.getValue());
        testRunner.setProperty("animal", GenerateFakeRecord.FT_ANIMAL.getValue());
        testRunner.setProperty("app_author", GenerateFakeRecord.FT_APP_AUTHOR.getValue());
        testRunner.setProperty("app_name", GenerateFakeRecord.FT_APP_NAME.getValue());
        testRunner.setProperty("app_version", GenerateFakeRecord.FT_APP_VERSION.getValue());
        testRunner.setProperty("artist", GenerateFakeRecord.FT_ARTIST.getValue());
        testRunner.setProperty("avatar", GenerateFakeRecord.FT_AVATAR.getValue());
        testRunner.setProperty("hop", GenerateFakeRecord.FT_BEER_HOP.getValue());
        testRunner.setProperty("malt", GenerateFakeRecord.FT_BEER_MALT.getValue());
        testRunner.setProperty("beer_name", GenerateFakeRecord.FT_BEER_NAME.getValue());
        testRunner.setProperty("beer_style", GenerateFakeRecord.FT_BEER_STYLE.getValue());
        testRunner.setProperty("beer_yeast", GenerateFakeRecord.FT_BEER_YEAST.getValue());
        testRunner.setProperty("birthday", GenerateFakeRecord.FT_BIRTHDAY.getValue());
        testRunner.setProperty("book_author", GenerateFakeRecord.FT_BOOK_AUTHOR.getValue());
        testRunner.setProperty("book_genre", GenerateFakeRecord.FT_BOOK_GENRE.getValue());
        testRunner.setProperty("book_publisher", GenerateFakeRecord.FT_BOOK_PUBLISHER.getValue());
        testRunner.setProperty("book_title", GenerateFakeRecord.FT_BOOK_TITLE.getValue());
        testRunner.setProperty("bool", GenerateFakeRecord.FT_BOOL.getValue());
        testRunner.setProperty("BIC", GenerateFakeRecord.FT_BIC.getValue());
        testRunner.setProperty("building_no", GenerateFakeRecord.FT_BUILDING_NUMBER.getValue());

        final Map<String, String> recordFields = processor.getFields(testRunner.getProcessContext());
        final RecordSchema outputSchema = processor.generateRecordSchema(recordFields, false);
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateFakeRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateFakeRecord.NULLABLE_FIELDS, "false");
        testRunner.setProperty(GenerateFakeRecord.NULL_PERCENTAGE, "100"); // This should be ignored
        testRunner.setProperty(GenerateFakeRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateFakeRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateFakeRecord.REL_SUCCESS).get(0);
        final String output = flowFile.getContent();
        for (String line : output.split(System.lineSeparator())) {
            // A null value would not be output so a comma would be the last character on the line
            if (line.endsWith(",")) {
                fail(line + "should not end with a value");
            }
        }
    }

    @Test
    public void testGenerateNullableFieldsZeroNullPercentage() throws Exception {
        testRunner.setProperty("chuck", GenerateFakeRecord.FT_CHUCK_NORRIS_FACT.getValue());
        testRunner.setProperty("cat_breed", GenerateFakeRecord.FT_CAT_BREED.getValue());
        testRunner.setProperty("cat_name", GenerateFakeRecord.FT_CAT_NAME.getValue());
        testRunner.setProperty("cat_registry", GenerateFakeRecord.FT_CAT_REGISTRY.getValue());
        testRunner.setProperty("celsius", GenerateFakeRecord.FT_TEMP_C.getValue());
        testRunner.setProperty("city", GenerateFakeRecord.FT_CITY.getValue());
        testRunner.setProperty("color", GenerateFakeRecord.FT_COLOR.getValue());
        testRunner.setProperty("company", GenerateFakeRecord.FT_COMPANY_NAME.getValue());
        testRunner.setProperty("constellation", GenerateFakeRecord.FT_CONSTELLATION.getValue());
        testRunner.setProperty("country", GenerateFakeRecord.FT_COUNTRY.getValue());
        testRunner.setProperty("country_capitol", GenerateFakeRecord.FT_COUNTRY_CAPITOL.getValue());
        testRunner.setProperty("country_code", GenerateFakeRecord.FT_COUNTRY_CODE.getValue());
        testRunner.setProperty("course", GenerateFakeRecord.FT_COURSE.getValue());
        testRunner.setProperty("cc_number", GenerateFakeRecord.FT_CREDIT_CARD_NUMBER.getValue());
        testRunner.setProperty("demonym", GenerateFakeRecord.FT_DEMONYM.getValue());
        testRunner.setProperty("dept_name", GenerateFakeRecord.FT_DEPARTMENT_NAME.getValue());
        testRunner.setProperty("dog_breed", GenerateFakeRecord.FT_DOG_BREED.getValue());
        testRunner.setProperty("dog_name", GenerateFakeRecord.FT_DOG_NAME.getValue());
        testRunner.setProperty("attain", GenerateFakeRecord.FT_EDUCATIONAL_ATTAINMENT.getValue());
        testRunner.setProperty("email", GenerateFakeRecord.FT_EMAIL_ADDRESS.getValue());
        testRunner.setProperty("fahrenheit", GenerateFakeRecord.FT_TEMP_F.getValue());
        testRunner.setProperty("file_extension", GenerateFakeRecord.FT_FILE_EXTENSION.getValue());
        testRunner.setProperty("filename", GenerateFakeRecord.FT_FILENAME.getValue());
        testRunner.setProperty("firstname", GenerateFakeRecord.FT_FIRST_NAME.getValue());
        testRunner.setProperty("food", GenerateFakeRecord.FT_FOOD.getValue());
        testRunner.setProperty("funny_name", GenerateFakeRecord.FT_FUNNY_NAME.getValue());
        testRunner.setProperty("future_date", GenerateFakeRecord.FT_FUTURE_DATE.getValue());

        final Map<String, String> recordFields = processor.getFields(testRunner.getProcessContext());
        final RecordSchema outputSchema = processor.generateRecordSchema(recordFields, true);
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateFakeRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateFakeRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateFakeRecord.NULL_PERCENTAGE, "0");
        testRunner.setProperty(GenerateFakeRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateFakeRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateFakeRecord.REL_SUCCESS).get(0);
        final String output = flowFile.getContent();
        for (String line : output.split(System.lineSeparator())) {
            // A null value would not be output so a comma would be the last character on the line
            if (line.endsWith(",")) {
                fail(line + "should not end with a value");
            }
        }
    }

    @Test
    public void testGenerateNullableFieldsOneHundredNullPercentage() throws Exception {
        testRunner.setProperty("got", GenerateFakeRecord.FT_GOT.getValue());
        testRunner.setProperty("harry", GenerateFakeRecord.FT_HARRY_POTTER.getValue());
        testRunner.setProperty("IBAN", GenerateFakeRecord.FT_IBAN.getValue());
        testRunner.setProperty("industry", GenerateFakeRecord.FT_INDUSTRY.getValue());
        testRunner.setProperty("ipv4", GenerateFakeRecord.FT_IPV4_ADDRESS.getValue());
        testRunner.setProperty("ipv6", GenerateFakeRecord.FT_IPV6_ADDRESS.getValue());
        testRunner.setProperty("job", GenerateFakeRecord.FT_JOB.getValue());
        testRunner.setProperty("lang", GenerateFakeRecord.FT_LANGUAGE.getValue());
        testRunner.setProperty("lastname", GenerateFakeRecord.FT_LAST_NAME.getValue());
        testRunner.setProperty("lat", GenerateFakeRecord.FT_LATITUDE.getValue());
        testRunner.setProperty("long", GenerateFakeRecord.FT_LONGITUDE.getValue());
        testRunner.setProperty("lorem", GenerateFakeRecord.FT_LOREM.getValue());
        testRunner.setProperty("mac", GenerateFakeRecord.FT_MAC_ADDRESS.getValue());
        testRunner.setProperty("marital_status", GenerateFakeRecord.FT_MARITAL_STATUS.getValue());
        testRunner.setProperty("md5", GenerateFakeRecord.FT_MD5.getValue());
        testRunner.setProperty("METAR", GenerateFakeRecord.FT_METAR.getValue());
        testRunner.setProperty("mime.type", GenerateFakeRecord.FT_MIME_TYPE.getValue());
        testRunner.setProperty("name", GenerateFakeRecord.FT_NAME.getValue());
        testRunner.setProperty("nasdaq", GenerateFakeRecord.FT_NASDAQ_SYMBOL.getValue());
        testRunner.setProperty("nationality", GenerateFakeRecord.FT_NATIONALITY.getValue());
        testRunner.setProperty("number", GenerateFakeRecord.FT_NUMBER.getValue());
        testRunner.setProperty("nyse", GenerateFakeRecord.FT_NYSE_SYMBOL.getValue());

        final Map<String, String> recordFields = processor.getFields(testRunner.getProcessContext());
        final RecordSchema outputSchema = processor.generateRecordSchema(recordFields, true);
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateFakeRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateFakeRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateFakeRecord.NULL_PERCENTAGE, "100");
        testRunner.setProperty(GenerateFakeRecord.NUM_RECORDS, "1");

        testRunner.run();
        testRunner.assertTransferCount(GenerateFakeRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateFakeRecord.REL_SUCCESS).get(0);
        // null values should cause all 22 fields to be empty in the output
        flowFile.assertContentEquals(",,,,,,,,,,,,,,,,,,,,,\n");
    }

    // Tests that the remaining fields are supported by the processor.
    @Test
    public void testFieldsReturnValue() throws Exception {

        List<Field> fieldTypeFields = Arrays.stream(GenerateFakeRecord.class.getFields()).filter((field) -> field.getName().startsWith("FT_")).collect(Collectors.toList());
        for (Field field : fieldTypeFields) {
            testRunner.setProperty(field.getName().toLowerCase(Locale.ROOT), ((AllowableValue) field.get(processor)).getValue());
        }

        final Map<String, String> recordFields = processor.getFields(testRunner.getProcessContext());
        final RecordSchema outputSchema = processor.generateRecordSchema(recordFields, true);
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateFakeRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateFakeRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateFakeRecord.NULL_PERCENTAGE, "100");
        testRunner.setProperty(GenerateFakeRecord.NUM_RECORDS, "1");

        testRunner.run();
        testRunner.assertTransferCount(GenerateFakeRecord.REL_SUCCESS, 1);
    }

    // TODO add Schema Text tests (nullable, non-nullable, null percentage 0 and 100
    @Test
    public void testGenerateNoNullableFieldsSchemaText() throws Exception {

        String schemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGenerateFakeRecord/nested_no_nullable.avsc")));
        final Schema avroSchema = new Schema.Parser().parse(schemaText);
        final RecordSchema outputSchema = AvroTypeUtil.createSchema(avroSchema);

        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateFakeRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateFakeRecord.SCHEMA_TEXT, schemaText);
        testRunner.setProperty(GenerateFakeRecord.NULLABLE_FIELDS, "true"); // Should be ignored
        testRunner.setProperty(GenerateFakeRecord.NULL_PERCENTAGE, "0");
        testRunner.setProperty(GenerateFakeRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateFakeRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateFakeRecord.REL_SUCCESS).get(0);
        final String output = flowFile.getContent();
        for (String line : output.split(System.lineSeparator())) {
            // A null value would not be output so a comma would be the last character on the line
            if (line.contains(",,")) {
                fail(line + "should not contain null values");
            }
        }
    }

    @Test
    public void testGenerateNullableFieldsZeroNullPercentageSchemaText() throws Exception {
        String schemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGenerateFakeRecord/nested_nullable.avsc")));
        final JsonRecordSetWriter recordWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateFakeRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateFakeRecord.SCHEMA_TEXT, schemaText);
        testRunner.setProperty(GenerateFakeRecord.NULLABLE_FIELDS, "false"); // Should be ignored
        testRunner.setProperty(GenerateFakeRecord.NULL_PERCENTAGE, "0");
        testRunner.setProperty(GenerateFakeRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateFakeRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateFakeRecord.REL_SUCCESS).get(0);
        final String output = flowFile.getContent();
        final JsonFactory jsonFactory = new JsonFactory();
        try (JsonParser jsonParser = jsonFactory.createParser(output)) {
            jsonParser.setCodec(new ObjectMapper());
            JsonNode recordArray = jsonParser.readValueAsTree();
            assertTrue(recordArray instanceof ArrayNode);
            JsonNode recordNode = recordArray.get(0);
            JsonNode systemNode = recordNode.get("System");
            assertNotNull(systemNode);
            JsonNode providerNode = systemNode.get("Provider");
            assertNotNull(providerNode);
            JsonNode guidNode = providerNode.get("Guid");
            assertNotNull(guidNode);
            assertNotNull(guidNode.asText());
            JsonNode nameNode = providerNode.get("Name");
            assertNotNull(nameNode);
            assertNotNull(nameNode.asText());
            JsonNode eventIdNode = systemNode.get("EventID");
            assertNotNull(eventIdNode);
            eventIdNode.asInt(); // This would throw a NullPointerException if the value was null
            JsonNode eventDataNode = recordNode.get("EventData");
            assertNotNull(eventDataNode);
            JsonNode dataNode = eventDataNode.get("Data");
            assertNotNull(dataNode);
            assertTrue(dataNode instanceof ArrayNode);
            assertTrue(dataNode.size() <= 10 && dataNode.size() >= 0);
            for (int i = 0; i < dataNode.size(); i++) {
                JsonNode dataElementNode = dataNode.get(i);
                assertNotNull(dataElementNode);
                JsonNode dataElementNameNode = dataElementNode.get("Name");
                assertNotNull(dataElementNameNode);
                assertNotNull(dataElementNameNode.asText());
                JsonNode dataElementDataNode = dataElementNode.get("DataElement");
                assertNotNull(dataElementDataNode);
                assertNotNull(dataElementDataNode.asText());
            }
        }
    }

    @Test
    public void testGenerateNullableFieldsOneHundredNullPercentageSchemaText() throws Exception {
        String schemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGenerateFakeRecord/nested_nullable.avsc")));
        final Schema avroSchema = new Schema.Parser().parse(schemaText);
        final RecordSchema outputSchema = AvroTypeUtil.createSchema(avroSchema);

        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateFakeRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateFakeRecord.SCHEMA_TEXT, schemaText);
        testRunner.setProperty(GenerateFakeRecord.NULLABLE_FIELDS, "false"); // Should be ignored
        testRunner.setProperty(GenerateFakeRecord.NULL_PERCENTAGE, "100");
        testRunner.setProperty(GenerateFakeRecord.NUM_RECORDS, "1");

        testRunner.run();
        testRunner.assertTransferCount(GenerateFakeRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateFakeRecord.REL_SUCCESS).get(0);
        // null values should cause all fields to be empty in the output (2 top-level record fields in this case
        flowFile.assertContentEquals(",\n");
    }
}