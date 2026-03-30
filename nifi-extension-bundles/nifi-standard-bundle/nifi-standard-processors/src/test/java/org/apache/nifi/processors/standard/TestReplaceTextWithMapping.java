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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestReplaceTextWithMapping {
    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(ReplaceTextWithMapping.class);

        /*
         * we have to disable validation of expression language because the processor will
         * need to evaluate the REGEX field with AND without FlowFiles. If not disabled,
         * the test will throw an error about the evaluation scope
         */
        runner.setValidateExpressionUsage(false);
    }

    @ParameterizedTest
    @MethodSource("simpleTestArgs")
    public void testSimple(String mappingFile) throws IOException {
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, mappingFile);

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors-without-dashes.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = """
                roses are apple
                violets are blueberry
                something else is grape
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    private static Stream<Arguments> simpleTestArgs() throws IOException {
        final Path mappingFile = Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-mapping.txt");
        return Stream.of(
                Arguments.argumentSet("File path", mappingFile.toFile().getAbsolutePath()),
                Arguments.argumentSet("File contents", Files.readString(mappingFile))
        );
    }

    @Test
    public void testExpressionLanguageInText() {
        final String mappingFile = Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-mapping.txt").toFile().getAbsolutePath();
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, mappingFile);

        String text = "${foo} red ${baz}";

        runner.enqueue(text.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = "${foo} apple ${baz}";
        assertEquals(expected, outputString);
    }

    @Test
    public void testExpressionLanguageInText2() {
        final String mappingFile = Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-mapping.txt").toFile().getAbsolutePath();
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, mappingFile);
        runner.setProperty(ReplaceTextWithMapping.REGEX, "\\|(.*?)\\|");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "1");

        String text = "${foo}|red|${baz}";

        runner.enqueue(text.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = "${foo}|apple|${baz}";
        assertEquals(expected, outputString);
    }

    @Test
    public void testExpressionLanguageInText3() {
        final String mappingFile = Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-mapping.txt").toFile().getAbsolutePath();
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, mappingFile);
        runner.setProperty(ReplaceTextWithMapping.REGEX, ".*\\|(.*?)\\|.*");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "1");

        String text = "${foo}|red|${baz}";

        runner.enqueue(text.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = "${foo}|apple|${baz}";
        assertEquals(expected, outputString);
    }

    @Test
    public void testWithMatchingGroupAndContext() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.REGEX, "-(.*?)-");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "1");
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-mapping.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = """
                -roses- are -apple-
                violets are -blueberry-
                something else is -grape-
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testBackReference() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.REGEX, "(\\S+)");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "1");
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-backreference-mapping.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors-without-dashes.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = """
                roses are red apple
                violets are blue blueberry
                something else is green grape
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testRoutesToFailureIfTooLarge() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.REGEX, "[123]");
        runner.setProperty(ReplaceTextWithMapping.MAX_BUFFER_SIZE, "1 b");
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-mapping.txt").toFile().getAbsolutePath());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "Good");
        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors.txt"), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_FAILURE, 1);
    }

    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscaped() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.REGEX, "-(.*?)-");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "1");
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-excessive-backreference-mapping.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = """
                -roses- are -red$2 apple-
                violets are -blue$2 blueberry-
                something else is -green$2 grape-
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testBackReferenceWithTooLargeOfIndexIsEscapedSimple() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE,
                Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-excessive-backreference-mapping-simple.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors-without-dashes.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = """
                roses are red$1 apple
                violets are blue$1 blueberry
                something else is green$1 grape
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testBackReferenceWithInvalidReferenceIsEscaped() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.REGEX, "(\\S+)");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "1");
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-invalid-backreference-mapping.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors-without-dashes.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        String expected = """
                roses are red$d apple
                violets are blue$d blueberry
                something else is green$d grape
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testEscapingDollarSign() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.REGEX, "-(.*?)-");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "1");
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-escaped-dollar-mapping.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = """
                -roses- are -$1 apple-
                violets are -$1 blueberry-
                something else is -$1 grape-
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testEscapingDollarSignSimple() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-escaped-dollar-mapping.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors-without-dashes.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = """
                roses are $1 apple
                violets are $1 blueberry
                something else is $1 grape
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testReplaceWithEmptyString() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-blank-mapping.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors-without-dashes.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        String expected = """
                roses are\s
                violets are\s
                something else is\s
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testReplaceWithSpaceInString() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-space-mapping.txt").toFile().getAbsolutePath());

        runner.enqueue(Paths.get("src/test/resources/TestReplaceTextWithMapping/colors-without-dashes.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        String expected = """
                roses are really red
                violets are super blue
                something else is ultra green
                I'm not good at writing poems""";
        assertEquals(expected, outputString);
    }

    @Test
    public void testWithNoMatch() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.REGEX, "-(.*?)-");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "1");
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-fruit-no-match-mapping.txt").toFile().getAbsolutePath());

        final Path path = Paths.get("src/test/resources/TestReplaceTextWithMapping/colors.txt");
        runner.enqueue(path);
        runner.run();

        runner.assertAllFlowFilesTransferred(ReplaceTextWithMapping.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceTextWithMapping.REL_SUCCESS).getFirst();
        final String outputString = out.getContent();
        final String expected = Files.readString(path);
        assertEquals(expected, outputString);
    }

    @Test
    public void testMatchingGroupForLookupKeyTooLarge() throws IOException {
        runner.setProperty(ReplaceTextWithMapping.REGEX, "-(.*?)-");
        runner.setProperty(ReplaceTextWithMapping.MATCHING_GROUP_FOR_LOOKUP_KEY, "2");
        runner.setProperty(ReplaceTextWithMapping.MAPPING_FILE, Paths.get("src/test/resources/TestReplaceTextWithMapping/color-mapping.txt").toFile().getAbsolutePath());

        final Path path = Paths.get("src/test/resources/TestReplaceTextWithMapping/colors.txt");
        runner.enqueue(path);
        assertThrows(AssertionError.class, runner::run);
    }
}
