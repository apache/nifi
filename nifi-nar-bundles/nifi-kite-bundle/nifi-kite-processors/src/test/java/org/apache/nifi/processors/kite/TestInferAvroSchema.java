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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestInferAvroSchema {

    private TestRunner runner = null;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(InferAvroSchema.class);

        // Prepare the common setup.
        runner.assertNotValid();

        runner.setProperty(InferAvroSchema.INPUT_CONTENT_TYPE, InferAvroSchema.USE_MIME_TYPE);
        runner.setProperty(InferAvroSchema.GET_CSV_HEADER_DEFINITION_FROM_INPUT, "true");
        runner.setProperty(InferAvroSchema.SCHEMA_DESTINATION, InferAvroSchema.DESTINATION_CONTENT);
        runner.setProperty(InferAvroSchema.HEADER_LINE_SKIP_COUNT, "0");
        runner.setProperty(InferAvroSchema.ESCAPE_STRING, "\\");
        runner.setProperty(InferAvroSchema.QUOTE_STRING, "'");
        runner.setProperty(InferAvroSchema.RECORD_NAME, "org.apache.nifi.contact");
        runner.setProperty(InferAvroSchema.CHARSET, "UTF-8");
        runner.setProperty(InferAvroSchema.PRETTY_AVRO_OUTPUT, "true");
    }

    @Test
    public void inferAvroSchemaFromHeaderDefinitionOfCSVFile() throws Exception {

        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue(new File("src/test/resources/Shapes_Header.csv").toPath(), attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_UNSUPPORTED_CONTENT, 0);
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(InferAvroSchema.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(unix2PlatformSpecificLineEndings(new File("src/test/resources/Shapes_header.csv.avro")));
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");
    }

    @Test
    public void inferAvroSchemaFromJSONFile() throws Exception {

        runner.assertValid();

        runner.setProperty(InferAvroSchema.INPUT_CONTENT_TYPE, InferAvroSchema.USE_MIME_TYPE);

        // Purposely set to True to test that none of the JSON file is read which would cause issues.
        runner.setProperty(InferAvroSchema.GET_CSV_HEADER_DEFINITION_FROM_INPUT, "true");
        runner.setProperty(InferAvroSchema.SCHEMA_DESTINATION, InferAvroSchema.DESTINATION_ATTRIBUTE);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        runner.enqueue(new File("src/test/resources/Shapes.json").toPath(), attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_UNSUPPORTED_CONTENT, 0);
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 1);

        MockFlowFile data = runner.getFlowFilesForRelationship(InferAvroSchema.REL_SUCCESS).get(0);
        String avroSchema = data.getAttribute(InferAvroSchema.AVRO_SCHEMA_ATTRIBUTE_NAME);
        String knownSchema = new String(unix2PlatformSpecificLineEndings(new File("src/test/resources/Shapes.json.avro")), StandardCharsets.UTF_8);
        Assert.assertEquals(avroSchema, knownSchema);

        // Since that avro schema is written to an attribute this should be teh same as the original
        data.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
    }

    @Test
    public void inferAvroSchemaFromCSVFile() throws Exception {

        runner.assertValid();

        // Read in the header
        StringWriter writer = new StringWriter();
        IOUtils.copy((Files.newInputStream(Paths.get("src/test/resources/ShapesHeader.csv"), StandardOpenOption.READ)), writer, "UTF-8");
        runner.setProperty(InferAvroSchema.CSV_HEADER_DEFINITION, writer.toString());
        runner.setProperty(InferAvroSchema.GET_CSV_HEADER_DEFINITION_FROM_INPUT, "false");

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue(new File("src/test/resources/Shapes_NoHeader.csv").toPath(), attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_UNSUPPORTED_CONTENT, 0);
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 1);

        MockFlowFile data = runner.getFlowFilesForRelationship(InferAvroSchema.REL_SUCCESS).get(0);
        data.assertContentEquals(unix2PlatformSpecificLineEndings(new File("src/test/resources/Shapes_header.csv.avro")));
        data.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");
    }

    @Test
    public void inferSchemaFormHeaderLinePropertyOfProcessor() throws Exception {

        final String CSV_HEADER_LINE = FileUtils.readFileToString(new File("src/test/resources/ShapesHeader.csv"));

        runner.assertValid();

        runner.setProperty(InferAvroSchema.GET_CSV_HEADER_DEFINITION_FROM_INPUT, "false");
        runner.setProperty(InferAvroSchema.CSV_HEADER_DEFINITION, CSV_HEADER_LINE);
        runner.setProperty(InferAvroSchema.HEADER_LINE_SKIP_COUNT, "1");

        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue((CSV_HEADER_LINE + "\nJane,Doe,29,55555").getBytes(), attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 1);

        MockFlowFile data = runner.getFlowFilesForRelationship(InferAvroSchema.REL_SUCCESS).get(0);
        data.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");
    }

    @Test
    public void inferSchemaFromEmptyContent() throws Exception {
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue("", attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 1);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 0);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 0);
    }

    @Test
    public void inferAvroSchemaFromHeaderDefinitionOfCSVTabDelimitedFile() throws Exception {

        runner.setProperty(InferAvroSchema.DELIMITER, "\\t");
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue(new File("src/test/resources/Shapes_Header_TabDelimited.csv").toPath(), attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_UNSUPPORTED_CONTENT, 0);
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(InferAvroSchema.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(unix2PlatformSpecificLineEndings(new File("src/test/resources/Shapes_header.csv.avro")));
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");
    }

    @Test
    public void inferAvroSchemaFromHeaderDefinitionOfCSVTabDelimitedFileNegativeTest() throws Exception {

        // Inproper InferAvroSchema.DELIMITER > original goes to InferAvroSchema.REL_FAILURE
        runner.setProperty(InferAvroSchema.DELIMITER, ";");
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        runner.enqueue(new File("src/test/resources/Shapes_Header_TabDelimited.csv").toPath(), attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_UNSUPPORTED_CONTENT, 0);
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 1);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 0);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 0);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(InferAvroSchema.REL_FAILURE).get(0);
        flowFile.assertContentEquals(new File("src/test/resources/Shapes_Header_TabDelimited.csv").toPath());
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/csv");
    }


    @Test
    public void specifyCSVparametersInExpressionLanguage() throws Exception {
        runner.setProperty(InferAvroSchema.DELIMITER, "${csv.delimiter}");
        runner.setProperty(InferAvroSchema.ESCAPE_STRING, "${csv.escape}");
        runner.setProperty(InferAvroSchema.QUOTE_STRING, "${csv.quote}");
        runner.setProperty(InferAvroSchema.CHARSET, "${csv.charset}");
        runner.setProperty(InferAvroSchema.GET_CSV_HEADER_DEFINITION_FROM_INPUT, "true");

        runner.assertValid();

        @SuppressWarnings("serial")
        Map<String, String> attributes = new HashMap<String, String>() {
            {
                put("csv.delimiter",",");
                put("csv.escape", "\\");
                put("csv.quote", "\"");
                put("csv.charset", "UTF-8");
                put(CoreAttributes.MIME_TYPE.key(), "text/csv");
            }
        };

        runner.enqueue(new File("src/test/resources/Shapes_Header.csv").toPath(), attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_UNSUPPORTED_CONTENT, 0);
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(InferAvroSchema.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(unix2PlatformSpecificLineEndings(new File("src/test/resources/Shapes_header.csv.avro")));
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");

    }

    @Test
    public void specifyJsonParametersInExpressionLanguage() throws Exception {
        runner.assertValid();
        runner.setProperty(InferAvroSchema.INPUT_CONTENT_TYPE, InferAvroSchema.USE_MIME_TYPE);

        // Purposely set to True to test that none of the JSON file is read which would cause issues.
        runner.setProperty(InferAvroSchema.GET_CSV_HEADER_DEFINITION_FROM_INPUT, "true");
        runner.setProperty(InferAvroSchema.SCHEMA_DESTINATION, InferAvroSchema.DESTINATION_ATTRIBUTE);
        runner.setProperty(InferAvroSchema.RECORD_NAME, "${record.name}");
        runner.setProperty(InferAvroSchema.NUM_RECORDS_TO_ANALYZE, "${records.analyze}");

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        attributes.put("record.name", "myrecord");
        attributes.put("records.analyze", "2");
        runner.enqueue(new File("src/test/resources/Shapes.json").toPath(), attributes);

        runner.run();
        runner.assertTransferCount(InferAvroSchema.REL_UNSUPPORTED_CONTENT, 0);
        runner.assertTransferCount(InferAvroSchema.REL_FAILURE, 0);
        runner.assertTransferCount(InferAvroSchema.REL_ORIGINAL, 1);
        runner.assertTransferCount(InferAvroSchema.REL_SUCCESS, 1);

        MockFlowFile data = runner.getFlowFilesForRelationship(InferAvroSchema.REL_SUCCESS).get(0);
        String avroSchema = data.getAttribute(InferAvroSchema.AVRO_SCHEMA_ATTRIBUTE_NAME);
        Assert.assertTrue(avroSchema.contains("\"name\" : \"myrecord\""));

        // Since that avro schema is written to an attribute this should be teh same as the original
        data.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
    }


    static byte[] unix2PlatformSpecificLineEndings(final File file) throws IOException {
        try (final BufferedInputStream in = new BufferedInputStream(new FileInputStream(file)); final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte eol[] = System.lineSeparator().getBytes(StandardCharsets.UTF_8);
            int justRead;
            while ((justRead = in.read()) != -1) {
                if (justRead == '\n') {
                    out.write(eol);
                } else {
                    out.write(justRead);
                }
            }
            return out.toByteArray();
        }
    }

}
