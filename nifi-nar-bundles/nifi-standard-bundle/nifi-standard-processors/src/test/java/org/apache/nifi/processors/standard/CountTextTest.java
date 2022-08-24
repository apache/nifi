/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class CountTextTest {
    private static final String TLC = "text.line.count";
    private static final String TLNEC = "text.line.nonempty.count";
    private static final String TWC = "text.word.count";
    private static final String TCC = "text.character.count";
    private TestRunner runner;

    @BeforeEach
    void setupRunner() {
        runner = TestRunners.newTestRunner(CountText.class);
    }


    @Test
    void testShouldCountAllMetrics() throws IOException {
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "true");

        final Path inputPath = Paths.get("src/test/resources/TestCountText/jabberwocky.txt");

        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put(TLC, "34");
        expectedValues.put(TLNEC, "28");
        expectedValues.put(TWC, "166");
        expectedValues.put(TCC, "900");

        runner.enqueue(Files.readAllBytes(inputPath));

        runner.run();

        runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).get(0);
        for (final Map.Entry<String, String> entry: expectedValues.entrySet()) {
            final String attribute = entry.getKey();
            final String expectedValue = entry.getValue();
            flowFile.assertAttributeEquals(attribute, expectedValue);
        }
    }

    @Test
    void testShouldCountEachMetric() throws IOException {
        final Path inputPath = Paths.get("src/test/resources/TestCountText/jabberwocky.txt");

        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put(TLC, "34");
        expectedValues.put(TLNEC, "28");
        expectedValues.put(TWC, "166");
        expectedValues.put(TCC, "900");

        final Map<PropertyDescriptor, String> linesOnly = Collections.singletonMap(CountText.TEXT_LINE_COUNT_PD, "true");
        final Map<PropertyDescriptor, String> linesNonEmptyOnly = Collections.singletonMap(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true");
        final Map<PropertyDescriptor, String> wordsOnly = Collections.singletonMap(CountText.TEXT_WORD_COUNT_PD, "true");
        final Map<PropertyDescriptor, String> charactersOnly = Collections.singletonMap(CountText.TEXT_CHARACTER_COUNT_PD, "true");

        final List<Map<PropertyDescriptor, String>> scenarios = Arrays.asList(linesOnly, linesNonEmptyOnly, wordsOnly, charactersOnly);

        for (final Map<PropertyDescriptor, String> map: scenarios) {
            // Reset the processor properties
            runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "false");
            runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "false");
            runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "false");
            runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "false");

            // Apply the scenario-specific properties
            for (final Map.Entry<PropertyDescriptor, String> entry: map.entrySet()) {
                runner.setProperty(entry.getKey(), entry.getValue());
            }

            runner.clearProvenanceEvents();
            runner.clearTransferState();
            runner.enqueue(Files.readAllBytes(inputPath));

            runner.run();

            runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1);
            MockFlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).get(0);
            for (final Map.Entry<String, String> entry: expectedValues.entrySet()) {
                final String attribute = entry.getKey();
                final String expectedValue = entry.getValue();

                if (flowFile.getAttributes().containsKey(attribute)) {
                    flowFile.assertAttributeEquals(attribute, expectedValue);
                }
            }
        }
    }

    @Test
    void testShouldCountWordsSplitOnSymbol() throws IOException {
        final Path inputPath = Paths.get("src/test/resources/TestCountText/jabberwocky.txt");

        final String EXPECTED_WORD_COUNT = "167";

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "false");
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "false");
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "false");
        runner.setProperty(CountText.SPLIT_WORDS_ON_SYMBOLS_PD, "true");

        runner.clearProvenanceEvents();
        runner.clearTransferState();
        runner.enqueue(Files.readAllBytes(inputPath));

        runner.run();

        runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CountText.TEXT_WORD_COUNT, EXPECTED_WORD_COUNT);
    }

    @Test
    void testShouldCountIndependentlyPerFlowFile() throws IOException {
        final Path inputPath = Paths.get("src/test/resources/TestCountText/jabberwocky.txt");

        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put(TLC, "34");
        expectedValues.put(TLNEC, "28");
        expectedValues.put(TWC, "166");
        expectedValues.put(TCC, "900");

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "true");

        for (int i = 0; i < 2; i++) {
            runner.clearProvenanceEvents();
            runner.clearTransferState();
            runner.enqueue(Files.readAllBytes(inputPath));

            runner.run();

            runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1);
            MockFlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).get(0);
            for (final Map.Entry<String, String> entry: expectedValues.entrySet()) {
                final String attribute = entry.getKey();
                final String expectedValue = entry.getValue();

                flowFile.assertAttributeEquals(attribute, expectedValue);
            }
        }
    }

    @Test
    void testShouldTrackSessionCountersAcrossMultipleFlowfiles() throws IOException, NoSuchFieldException, IllegalAccessException {
        final Path inputPath = Paths.get("src/test/resources/TestCountText/jabberwocky.txt");

        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put(TLC, "34");
        expectedValues.put(TLNEC, "28");
        expectedValues.put(TWC, "166");
        expectedValues.put(TCC, "900");

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "true");

        final int n = 2;
        for (int i = 0; i < n; i++) {
            runner.clearTransferState();
            runner.enqueue(Files.readAllBytes(inputPath));

            runner.run();

            runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1);
            MockFlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).get(0);
            for (final Map.Entry<String, String> entry: expectedValues.entrySet()) {
                final String attribute = entry.getKey();
                final String expectedValue = entry.getValue();

                flowFile.assertAttributeEquals(attribute, expectedValue);
            }
        }

        assertEquals(Long.valueOf(expectedValues.get(TLC)) * n, runner.getCounterValue("Lines Counted"));
        assertEquals(Long.valueOf(expectedValues.get(TLNEC)) * n, runner.getCounterValue("Lines (non-empty) Counted"));
        assertEquals(Long.valueOf(expectedValues.get(TWC)) * n, runner.getCounterValue("Words Counted"));
        assertEquals(Long.valueOf(expectedValues.get(TCC)) * n, runner.getCounterValue("Characters Counted"));
    }

    @Test
    void testShouldHandleInternalError() {
        CountText ct = new CountText() {
            @Override
            int countWordsInLine(String line, boolean splitWordsOnSymbols) throws IOException {
                throw new IOException("Expected exception");
            }
        };

        final TestRunner runner = TestRunners.newTestRunner(ct);
        final String INPUT_TEXT = "This flowfile should throw an error";

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "true");
        runner.setProperty(CountText.CHARACTER_ENCODING_PD, StandardCharsets.US_ASCII.displayName());

        runner.enqueue(INPUT_TEXT.getBytes());

        // Need initialize = true to run #onScheduled()
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(CountText.REL_FAILURE, 1);
    }

    @Test
    void testShouldIgnoreWhitespaceWordsWhenCounting() {
        final String INPUT_TEXT = "a  b  c";

        final String EXPECTED_WORD_COUNT = "3";

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "false");
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "false");
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "false");
        runner.setProperty(CountText.SPLIT_WORDS_ON_SYMBOLS_PD, "true");

        runner.clearProvenanceEvents();
        runner.clearTransferState();
        runner.enqueue(INPUT_TEXT.getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CountText.TEXT_WORD_COUNT, EXPECTED_WORD_COUNT);
    }

    @Test
    void testShouldIgnoreWhitespaceWordsWhenCountingDebugMode() {
        final MockComponentLog componentLogger = spy(new MockComponentLog("processorId", new CountText()));
        doReturn(true).when(componentLogger).isDebugEnabled();
        final TestRunner runner = TestRunners.newTestRunner(CountText.class, componentLogger);
        final String INPUT_TEXT = "a  b  c";

        final String EXPECTED_WORD_COUNT = "3";

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "false");
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "false");
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true");
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "false");
        runner.setProperty(CountText.SPLIT_WORDS_ON_SYMBOLS_PD, "true");

        runner.clearProvenanceEvents();
        runner.clearTransferState();
        runner.enqueue(INPUT_TEXT.getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).get(0);

        flowFile.assertAttributeEquals(CountText.TEXT_WORD_COUNT, EXPECTED_WORD_COUNT);
    }

}
