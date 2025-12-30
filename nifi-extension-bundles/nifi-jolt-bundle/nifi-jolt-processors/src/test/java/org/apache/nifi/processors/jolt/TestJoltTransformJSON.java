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
package org.apache.nifi.processors.jolt;

import io.joltcommunity.jolt.Diffy;
import io.joltcommunity.jolt.JsonUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.jolt.util.JoltTransformStrategy;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestJoltTransformJSON {
    static final Path JSON_INPUT = Paths.get("src/test/resources/TestJoltTransformJson/input.json");
    static final Diffy DIFFY = new Diffy();
    static final String CHAINR_SPEC_PATH = "src/test/resources/specs/chainrSpec.json";
    static final String SHIFTR_SPEC_PATH = "src/test/resources/specs/shiftrSpec.json";
    static final String SHIFTR_JSON_OUTPUT = "shiftrOutput.json";
    static final String CHAINR_JSON_OUTPUT = "chainrOutput.json";
    private static final String CUSTOM_CLASS_NAME = CustomTransformJarProvider.getCustomTransformClassName();
    private static final String JSON_SOURCE_ATTR_NAME = "jsonSourceAttr";
    private static String chainrSpecContents;
    private static Path customTransformJar;

    @TempDir
    private static Path tempDir;

    private Processor processor;
    private TestRunner runner;

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        chainrSpecContents = Files.readString(Paths.get(CHAINR_SPEC_PATH));
        customTransformJar = CustomTransformJarProvider.createCustomTransformJar(tempDir);
    }

    @BeforeEach
    void setUp() {
        this.processor = new JoltTransformJSON();
        this.runner = TestRunners.newTestRunner(processor);
    }

    @Test
    void testRelationshipsCreated() throws IOException {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.enqueue(JSON_INPUT);
        Set<Relationship> relationships = processor.getRelationships();
        assertTrue(relationships.contains(JoltTransformJSON.REL_FAILURE));
        assertTrue(relationships.contains(JoltTransformJSON.REL_SUCCESS));
        assertEquals(2, relationships.size());
    }

    @Test
    void testRelationshipsCreatedFromFile() throws IOException {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, CHAINR_SPEC_PATH);
        runner.enqueue(JSON_INPUT);
        Set<Relationship> relationships = processor.getRelationships();
        assertTrue(relationships.contains(JoltTransformJSON.REL_FAILURE));
        assertTrue(relationships.contains(JoltTransformJSON.REL_SUCCESS));
        assertEquals(2, relationships.size());
    }

    @Test
    void testInvalidJOLTSpec() {
        String spec = "[{}]";
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.assertNotValid();

        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.assertValid();
    }

    @Test
    void testIncorrectJOLTSpec() {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    void testIncorrectJOLTSpecFromFile() {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, CHAINR_SPEC_PATH);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    void testSpecIsNotSet() {
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    void testSpecIsEmpty() {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, StringUtils.EMPTY);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    void testSpecNotRequired() {
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SORTR);
        runner.assertValid();
    }

    @Test
    void testNoFlowFileContent() {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(JoltTransformJSON.REL_FAILURE, 0);
        runner.assertTransferCount(JoltTransformJSON.REL_SUCCESS, 0);
    }

    @Test
    void testInvalidFlowFileContentJson() {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.enqueue("invalid json");
        runner.run();
        runner.assertAllFlowFilesTransferred(JoltTransformJSON.REL_FAILURE);
    }

    @Test
    void testInvalidFlowFileContentJsonFromFile() {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, CHAINR_SPEC_PATH);
        runner.enqueue("invalid json");
        runner.run();
        runner.assertAllFlowFilesTransferred(JoltTransformJSON.REL_FAILURE);
    }

    @Test
    void testCustomTransformationWithNoModule() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/customChainrSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.CUSTOM_CLASS, CUSTOM_CLASS_NAME);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(JoltTransformJSON.REL_SUCCESS);
    }

    @Test
    void testCustomTransformationWithMissingClassName() throws IOException {
        final String customJarPath = customTransformJar.toString();
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformJSON.MODULES, customJarPath);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(JSON_INPUT);
        runner.assertNotValid();
    }

    @Test
    void testCustomTransformationWithInvalidClassPath() throws IOException {
        final String customJarPath = "src/test/resources/TestJoltTransformJson/FakeCustomJar.jar";
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformJSON.CUSTOM_CLASS, CUSTOM_CLASS_NAME);
        runner.setProperty(JoltTransformJSON.MODULES, customJarPath);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(JSON_INPUT);
        runner.assertNotValid();
    }

    @Test
    void testCustomTransformationWithInvalidClassName() throws IOException {
        final String customJarPath = customTransformJar.toString();
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformJSON.CUSTOM_CLASS, "org.apache.nifi.processors.jolt.FakeCustomJoltTransform");
        runner.setProperty(JoltTransformJSON.MODULES, customJarPath);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(JSON_INPUT);
        runner.assertNotValid();
    }

    @ParameterizedTest()
    @MethodSource("getChainrArguments")
    void testTransformInputWithChainr(Path specPath) throws IOException {
        final String spec = Files.readString(specPath);
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals(CHAINR_JSON_OUTPUT);
    }

    @Test
    void testTransformInputWithShiftr() throws IOException {
        final String spec = Files.readString(Paths.get(SHIFTR_SPEC_PATH));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals(SHIFTR_JSON_OUTPUT);
    }

    @Test
    void testTransformInputWithShiftrFromFile() throws IOException {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, SHIFTR_SPEC_PATH);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals(SHIFTR_JSON_OUTPUT);
    }

    @Test
    void testTransformInputWithShiftrFromFileExpression() throws IOException {
        final String specFilename = "shiftrSpec.json";
        final String spec = "./src/test/resources/specs/${filename}";
        final Map<String, String> attributes = Map.of(CoreAttributes.FILENAME.key(), specFilename);

        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);

        runner.enqueue(JSON_INPUT, attributes);
        runner.run();

        assertTransformedEquals(SHIFTR_JSON_OUTPUT);
    }

    String addAccentedChars(String input) {
        return input.replace("\"primary\"", "\"primaryÄÖÜ\"");
    }

    @Test
    void testTransformInputWithShiftrAccentedChars() throws IOException {
        final String spec = addAccentedChars(Files.readString(Paths.get(SHIFTR_SPEC_PATH)));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.enqueue(addAccentedChars(Files.readString(JSON_INPUT)));
        runner.run();

        assertTransformedEquals(SHIFTR_JSON_OUTPUT);
    }

    @Test
    void testTransformInputWithDefaultr() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/defaultrSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.DEFAULTR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals("defaultrOutput.json");
    }

    @Test
    void testTransformInputWithRemovr() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/removrSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.REMOVR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals("removrOutput.json");
    }

    @Test
    void testTransformInputWithCardinality() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/cardrSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.CARDINALITY);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals("cardrOutput.json");
    }

    @Test
    void testTransformInputWithSortr() throws IOException {
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SORTR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(JoltTransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformJSON.REL_SUCCESS).getFirst();
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestJoltTransformJson/sortrOutput.json")));
        String transformedJsonString = JsonUtils.toJsonString(transformedJson);
        String compareJsonString = JsonUtils.toJsonString(compareJson);
        assertEquals(compareJsonString, transformedJsonString);
    }

    @Test
    void testTransformInputWithDefaultrExpressionLanguage() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/defaultrELSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.DEFAULTR);
        runner.setEnvironmentVariableValue("quota", "5");
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals("defaultrELOutput.json");
    }

    @Test
    void testTransformInputWithModifierDefault() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/modifierDefaultSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.MODIFIER_DEFAULTR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals("modifierDefaultOutput.json");
    }

    @Test
    void testTransformInputWithModifierDefine() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/modifierDefineSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.MODIFIER_DEFAULTR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals("modifierDefineOutput.json");
    }

    @Test
    void testTransformInputWithModifierOverwrite() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/modifierOverwriteSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.MODIFIER_DEFAULTR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals("modifierOverwriteOutput.json");
    }

    @Test
    void testTransformInputWithSortrPopulatedSpec() throws IOException {
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.SORTR);
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, "abcd");
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(JoltTransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformJSON.REL_SUCCESS).getFirst();
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestJoltTransformJson/sortrOutput.json")));
        String transformedJsonString = JsonUtils.toJsonString(transformedJson);
        String compareJsonString = JsonUtils.toJsonString(compareJson);
        assertEquals(compareJsonString, transformedJsonString);
    }

    @Test
    void testTransformInputWithCustomTransformationWithJar() throws IOException {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformJSON.CUSTOM_CLASS, CUSTOM_CLASS_NAME);
        runner.setProperty(JoltTransformJSON.MODULES, customTransformJar.toString());
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals(CHAINR_JSON_OUTPUT);
    }

    @Test
    void testExpressionLanguageJarFile() throws IOException {
        final String customJoltTransform = CUSTOM_CLASS_NAME;

        Map<String, String> customSpecs = new HashMap<>();
        customSpecs.put("JOLT_SPEC", chainrSpecContents);
        customSpecs.put("CUSTOM_JOLT_CLASS", customJoltTransform);
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, "${JOLT_SPEC}");
        runner.setProperty(JoltTransformJSON.CUSTOM_CLASS, "${CUSTOM_JOLT_CLASS}");
        runner.setProperty(JoltTransformJSON.MODULES, "${CUSTOM_JAR}");
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.setEnvironmentVariableValue("CUSTOM_JAR", customTransformJar.toString());
        runner.enqueue(JSON_INPUT, customSpecs);
        runner.run();

        assertTransformedEquals(CHAINR_JSON_OUTPUT);
    }

    @Test
    void testTransformInputWithCustomTransformationWithDir() throws IOException {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformJSON.CUSTOM_CLASS, CUSTOM_CLASS_NAME);
        runner.setProperty(JoltTransformJSON.MODULES, customTransformJar.getParent().toString());
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals(CHAINR_JSON_OUTPUT);
    }

    @Test
    void testTransformInputWithChainrEmbeddedCustomTransformation() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/customChainrSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.MODULES, customTransformJar.getParent().toString());
        runner.setProperty(JoltTransformJSON.CUSTOM_CLASS, CUSTOM_CLASS_NAME);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals(CHAINR_JSON_OUTPUT);
    }

    @Test
    void testTransformInputCustomTransformationIgnored() throws IOException {
        final String spec = Files.readString(Paths.get("src/test/resources/specs/defaultrSpec.json"));
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.CUSTOM_CLASS, CUSTOM_CLASS_NAME);
        runner.setProperty(JoltTransformJSON.MODULES, customTransformJar.toString());
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.DEFAULTR);
        runner.enqueue(JSON_INPUT);
        runner.run();

        assertTransformedEquals("defaultrOutput.json");
    }

    @Test
    void testJoltSpecEL() throws IOException {
        final String spec = "${joltSpec}";
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, JoltTransformStrategy.DEFAULTR);
        final Map<String, String> attributes = Collections.singletonMap("joltSpec",
                "{\"RatingRange\":5,\"rating\":{\"*\":{\"MaxLabel\":\"High\",\"MinLabel\":\"Low\",\"DisplayType\":\"NORMAL\"}}}");
        runner.enqueue(JSON_INPUT, attributes);
        runner.run();

        assertTransformedEquals("defaultrOutput.json");
    }

    @Test
    void testJoltSpecInvalidEL() throws IOException {
        final String spec = "${joltSpec:nonExistingFunction()}";
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, spec);
        runner.enqueue(JSON_INPUT);
        runner.assertNotValid();
    }

    private static Stream<Arguments> provideJsonSourceAttributeArguments() {
        String INVALID_INPUT_JSON = "{\"rating\":{\"primary\":{\"value\":3},\"series\":{\"value\":[5,4]},\"quality\":{\"value\":}}}";
        String EXPECTED_JSON = "{\"rating\":{\"primary\":{\"value\":3},\"series\":{\"value\":[5,4]},\"quality\":{\"value\":3}}}";

        return Stream.of(
                Arguments.argumentSet("testJsonAttributeNotInitialised", JSON_SOURCE_ATTR_NAME, null,
                        SHIFTR_SPEC_PATH, JoltTransformStrategy.SHIFTR, false, null),
                Arguments.argumentSet("testInvalidJsonAttribute", JSON_SOURCE_ATTR_NAME, Map.of(JSON_SOURCE_ATTR_NAME, INVALID_INPUT_JSON),
                        SHIFTR_SPEC_PATH, JoltTransformStrategy.SHIFTR, false, null),
                Arguments.argumentSet("testValidJsonAttribute", JSON_SOURCE_ATTR_NAME, Map.of(JSON_SOURCE_ATTR_NAME, EXPECTED_JSON),
                        CHAINR_SPEC_PATH, JoltTransformStrategy.CHAINR, true, CHAINR_JSON_OUTPUT)
        );
    }

    @ParameterizedTest
    @MethodSource("provideJsonSourceAttributeArguments")
    void testJsonSourceAttribute(String jsonSourceAttribute,
                                 Map<String, String> flowFileAttributes,
                                 String joltSpec,
                                 JoltTransformStrategy joltStrategy,
                                 boolean expectSuccess,
                                 String expectedOutputFile) throws IOException {
        runner.setProperty(JoltTransformJSON.JOLT_SPEC, joltSpec);
        runner.setProperty(JoltTransformJSON.JOLT_TRANSFORM, joltStrategy);
        runner.setProperty(JoltTransformJSON.JSON_SOURCE, JsonSourceStrategy.ATTRIBUTE);
        runner.setProperty(JoltTransformJSON.JSON_SOURCE_ATTRIBUTE, jsonSourceAttribute);
        runner.enqueue(JSON_INPUT, flowFileAttributes != null ? flowFileAttributes : Collections.emptyMap());
        runner.run();

        if (expectSuccess) {
            assertTransformedJsonAttributeEquals(expectedOutputFile);
        } else {
            runner.assertAllFlowFilesTransferred(JoltTransformJSON.REL_FAILURE);
        }
    }

    private void assertTransformedJsonAttributeEquals(final String expectedOutputContent) throws IOException {
        runner.assertAllFlowFilesTransferred(JoltTransformJSON.REL_SUCCESS);

        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformJSON.REL_SUCCESS).getFirst();
        transformed.assertAttributeExists(JSON_SOURCE_ATTR_NAME);

        final Object transformedJson = JsonUtils.jsonToObject(transformed.getAttribute(JSON_SOURCE_ATTR_NAME));

        final String compareOutputPath = "src/test/resources/TestJoltTransformJson/%s".formatted(expectedOutputContent);
        final Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get(compareOutputPath)));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    private void assertTransformedEquals(final String expectedOutputFilename) throws IOException {
        runner.assertAllFlowFilesTransferred(JoltTransformJSON.REL_SUCCESS);

        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformJSON.REL_SUCCESS).getFirst();
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");

        final Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));

        final String compareOutputPath = "src/test/resources/TestJoltTransformJson/%s".formatted(expectedOutputFilename);
        final Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get(compareOutputPath)));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    private static Stream<Arguments> getChainrArguments() {
        return Stream.of(
                Arguments.argumentSet("has no single line comments", Paths.get(CHAINR_SPEC_PATH)),
                Arguments.argumentSet("has a single line comment", Paths.get("src/test/resources/specs/chainrSpecWithSingleLineComment.json")));
    }
}
