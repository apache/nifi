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
package org.apache.nifi.processors.sawmill;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSawmillTransformRecord {
    private final TestRunner runner = TestRunners.newTestRunner(new SawmillTransformRecord());

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private JsonRecordSetWriter writer;


    @BeforeEach
    public void setup() throws IOException {
        JsonTreeReader parser = new JsonTreeReader();
        try {
            runner.addControllerService("parser", parser);
            runner.setProperty(parser, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);

        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(parser);
        runner.setProperty(SawmillTransformRecord.RECORD_READER, "parser");
        writer = new JsonRecordSetWriter();
        try {
            runner.addControllerService("writer", writer);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.setProperty(writer, "Schema Write Strategy", "full-schema-attribute");
        runner.setProperty(SawmillTransformRecord.RECORD_WRITER, "writer");
        // Each test must set the Schema Access strategy and Schema, and enable the writer CS
    }

    @Test
    public void testBadInput() {
        final String inputFlowFile = "I am not JSON";
        final String transform = getResource("simpleTransform.json");
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);

        runner.setProperty(SawmillTransformRecord.SAWMILL_TRANSFORM, transform);
        runner.enqueue(inputFlowFile);

        runner.run();

        runner.assertTransferCount(SawmillTransformRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(SawmillTransformRecord.REL_FAILURE, 1);
    }

    @Test
    public void testInvalidSawmillTransform() {
        final TestRunner runner = TestRunners.newTestRunner(new SawmillTransformRecord());
        final String invalidTransform = "invalid";
        runner.setProperty(SawmillTransformRecord.SAWMILL_TRANSFORM, invalidTransform);
        runner.assertNotValid();
    }

    @Test
    public void testTransformFilePath() {
        final URL transformUrl = Objects.requireNonNull(getClass().getResource("/simpleTransform.json"));
        final String transformPath = transformUrl.getPath();

        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);

        runner.setProperty(SawmillTransformRecord.SAWMILL_TRANSFORM, transformPath);

        final String json = getResource("input.json");
        runner.enqueue(json);

        assertRunSuccess();
    }

    @Test
    public void testSimpleSawmill() {
        final String inputFlowFile = getResource("input.json");
        final String transform = getResource("simpleTransform.json");
        final String expectedOutput = "[" + getResource("simpleOutput.json") + "]";
        runner.setProperty(SawmillTransformRecord.SAWMILL_TRANSFORM, transform);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);

        runner.assertValid();
        runner.enqueue(inputFlowFile);
        runner.run();
        runner.assertTransferCount(SawmillTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SawmillTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(SawmillTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertContentEquals(transformed, expectedOutput);
    }

    @Test
    public void testExpressionLanguageTransform() {
        final String inputFlowFile = getResource("input.json");
        final String transform = getResource("expressionLanguageTransform.json");
        runner.setProperty(SawmillTransformRecord.SAWMILL_TRANSFORM, transform);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        runner.assertValid();
        Map<String, String> attrs = new HashMap<>();
        attrs.put("hashing.key", "ThisIsMyHashingKey");
        runner.enqueue(inputFlowFile, attrs);

        final MockFlowFile flowFile = assertRunSuccess();
        // Output will be as array
        final String expectedOutput = "[" + getResource("expressionLanguageOutput.json") + "]";
        assertContentEquals(flowFile, expectedOutput);
    }

    @Test
    public void testSawmillNoOutput() {
        final String input = "{\"a\":1}";
        final String transform = "{\"steps\": [{\"drop\": {\"config\": {}}}]}";

        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        runner.setProperty(SawmillTransformRecord.SAWMILL_TRANSFORM, transform);
        runner.enqueue(input);

        final MockFlowFile flowFile = assertRunSuccess();
        flowFile.assertContentEquals("[ ]");
    }

    // This test verifies transformCache cleanup does not throw an exception
    @Test
    public void testShutdown() {
        runner.stop();
    }


    private void assertContentEquals(final MockFlowFile flowFile, final String expectedJson) {
        try {
            final JsonNode flowFileNode = objectMapper.readTree(flowFile.getContent());
            final JsonNode expectedNode = objectMapper.readTree(expectedJson);
            assertEquals(expectedNode, flowFileNode);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MockFlowFile assertRunSuccess() {
        runner.run();
        runner.assertTransferCount(SawmillTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(SawmillTransformRecord.REL_FAILURE, 0);
        return runner.getFlowFilesForRelationship(SawmillTransformRecord.REL_SUCCESS).iterator().next();
    }

    private String getResource(final String fileName) {
        final String path = String.format("/%s", fileName);
        try (
                final InputStream inputStream = Objects.requireNonNull(getClass().getResourceAsStream(path), "Resource not found");
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        ) {
            StreamUtils.copy(inputStream, outputStream);
            return outputStream.toString();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}