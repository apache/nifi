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
package org.apache.nifi.processors.standard

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.util.MockProcessSession
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.security.Security

import static org.mockito.Matchers.anyBoolean
import static org.mockito.Matchers.anyString
import static org.mockito.Mockito.when

@RunWith(JUnit4.class)
class CountTextTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CountTextTest.class)

    private static final String TLC = "text.line.count"
    private static final String TLNEC = "text.line.nonempty.count"
    private static final String TWC = "text.word.count"
    private static final String TCC = "text.character.count"


    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @Test
    void testShouldCountAllMetrics() throws Exception {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(CountText.class)

        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "true")

        // This text is the same as in src/test/resources/TestCountText/jabberwocky.txt but is copied here
        // to ensure that reading from a file vs. static text doesn't cause line break issues
        String INPUT_TEXT = """’Twas brillig, and the slithy toves
Did gyre and gimble in the wade;
All mimsy were the borogoves,
And the mome raths outgrabe.

"Beware the Jabberwock, my son!
The jaws that bite, the claws that catch!
Beware the Jubjub bird, and shun
The frumious Bandersnatch!"

He took his vorpal sword in hand:
Long time the manxome foe he sought—
So rested he by the Tumtum tree,
And stood awhile in thought.

And as in uffish thought he stood,
The Jabberwock, with eyes of flame,
Came whiffling through the tulgey wood.
And burbled as it came!

One, two! One, two! And through and through
The vorpal blade went snicker-snack!
He left it dead, and with its head
He went galumphing back.

"And hast thou slain the Jabberwock?
Come to my arms, my beamish boy!
O frabjous day! Callooh! Callay!"
He chortled in his joy.

’Twas brillig, and the slithy toves
Did gyre and gimble in the wabe;
All mimsy were the borogoves,
And the mome raths outgrabe."""

        runner.enqueue(INPUT_TEXT.bytes)

        // Act
        runner.run()

        // Assert
        runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1)
        FlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).first()
        assert flowFile.attributes."$TLC" == 34 as String
        assert flowFile.attributes."$TLNEC" == 28 as String
        assert flowFile.attributes."$TWC" == 166 as String
        assert flowFile.attributes."$TCC" == 900 as String
    }

    @Test
    void testShouldCountEachMetric() throws Exception {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(CountText.class)
        String INPUT_TEXT = new File("src/test/resources/TestCountText/jabberwocky.txt").text

        final def EXPECTED_VALUES = [
                (TLC)  : 34,
                (TLNEC): 28,
                (TWC)  : 166,
                (TCC)  : 900,
        ]

        def linesOnly = [(CountText.TEXT_LINE_COUNT_PD): "true"]
        def linesNonEmptyOnly = [(CountText.TEXT_LINE_NONEMPTY_COUNT_PD): "true"]
        def wordsOnly = [(CountText.TEXT_WORD_COUNT_PD): "true"]
        def charactersOnly = [(CountText.TEXT_CHARACTER_COUNT_PD): "true"]

        final List<Map<PropertyDescriptor, String>> SCENARIOS = [linesOnly, linesNonEmptyOnly, wordsOnly, charactersOnly]

        SCENARIOS.each { map ->
            // Reset the processor properties
            runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "false")
            runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "false")
            runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "false")
            runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "false")

            // Apply the scenario-specific properties
            map.each { key, value ->
                runner.setProperty(key, value)
            }

            runner.clearProvenanceEvents()
            runner.clearTransferState()
            runner.enqueue(INPUT_TEXT.bytes)

            // Act
            runner.run()

            // Assert
            runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1)
            FlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).first()
            logger.info("Generated flowfile: ${flowFile} | ${flowFile.attributes}")
            EXPECTED_VALUES.each { key, value ->
                if (flowFile.attributes.containsKey(key)) {
                    assert flowFile.attributes.get(key) == value as String
                }
            }
        }
    }

    @Test
    void testShouldCountWordsSplitOnSymbol() throws Exception {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(CountText.class)
        String INPUT_TEXT = new File("src/test/resources/TestCountText/jabberwocky.txt").text

        final int EXPECTED_WORD_COUNT = 167

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "false")
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "false")
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "false")
        runner.setProperty(CountText.SPLIT_WORDS_ON_SYMBOLS_PD, "true")

        runner.clearProvenanceEvents()
        runner.clearTransferState()
        runner.enqueue(INPUT_TEXT.bytes)

        // Act
        runner.run()

        // Assert
        runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1)
        FlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).first()
        logger.info("Generated flowfile: ${flowFile} | ${flowFile.attributes}")
        assert flowFile.attributes.get(CountText.TEXT_WORD_COUNT) == EXPECTED_WORD_COUNT as String
    }

    @Test
    void testShouldCountIndependentlyPerFlowFile() throws Exception {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(CountText.class)
        String INPUT_TEXT = new File("src/test/resources/TestCountText/jabberwocky.txt").text

        final def EXPECTED_VALUES = [
                (TLC)  : 34,
                (TLNEC): 28,
                (TWC)  : 166,
                (TCC)  : 900,
        ]

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "true")

        2.times { int i ->
            runner.clearProvenanceEvents()
            runner.clearTransferState()
            runner.enqueue(INPUT_TEXT.bytes)

            // Act
            runner.run()

            // Assert
            runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1)
            FlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).first()
            logger.info("Generated flowfile: ${flowFile} | ${flowFile.attributes}")
            EXPECTED_VALUES.each { key, value ->
                if (flowFile.attributes.containsKey(key)) {
                    assert flowFile.attributes.get(key) == value as String
                }
            }
        }
    }

    @Test
    void testShouldTrackSessionCountersAcrossMultipleFlowfiles() throws Exception {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(CountText.class)
        String INPUT_TEXT = new File("src/test/resources/TestCountText/jabberwocky.txt").text

        final def EXPECTED_VALUES = [
                (TLC)  : 34,
                (TLNEC): 28,
                (TWC)  : 166,
                (TCC)  : 900,
        ]

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "true")

        MockProcessSession mockPS = runner.processSessionFactory.createSession()
        def sessionCounters = mockPS.sharedState.counterMap
        logger.info("Session counters (0): ${sessionCounters}")

        int n = 2

        n.times { int i ->
            runner.clearTransferState()
            runner.enqueue(INPUT_TEXT.bytes)

            // Act
            runner.run()
            logger.info("Session counters (${i + 1}): ${sessionCounters}")

            // Assert
            runner.assertAllFlowFilesTransferred(CountText.REL_SUCCESS, 1)
            FlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_SUCCESS).first()
            logger.info("Generated flowfile: ${flowFile} | ${flowFile.attributes}")
            EXPECTED_VALUES.each { key, value ->
                if (flowFile.attributes.containsKey(key)) {
                    assert flowFile.attributes.get(key) == value as String
                }
            }
        }

        assert sessionCounters.get("Lines Counted").get() == EXPECTED_VALUES[TLC] * n as long
        assert sessionCounters.get("Lines (non-empty) Counted").get() == EXPECTED_VALUES[TLNEC] * n as long
        assert sessionCounters.get("Words Counted").get() == EXPECTED_VALUES[TWC] * n as long
        assert sessionCounters.get("Characters Counted").get() == EXPECTED_VALUES[TCC] * n as long
    }

    @Test
    void testShouldHandleInternalError() throws Exception {
        // Arrange
        CountText ct = new CountText()
        ct.countLines = true
        ct.countLinesNonEmpty = true
        ct.countWords = true
        ct.countCharacters = true

        CountText ctSpy = Mockito.spy(ct)
        when(ctSpy.countWordsInLine(anyString(), anyBoolean())).thenThrow(new IOException("Expected exception"))

        final TestRunner runner = TestRunners.newTestRunner(ctSpy)
        final String INPUT_TEXT = "This flowfile should throw an error"

        // Reset the processor properties
        runner.setProperty(CountText.TEXT_LINE_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_LINE_NONEMPTY_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_WORD_COUNT_PD, "true")
        runner.setProperty(CountText.TEXT_CHARACTER_COUNT_PD, "true")
        runner.setProperty(CountText.CHARACTER_ENCODING_PD, StandardCharsets.US_ASCII.displayName())

        runner.enqueue(INPUT_TEXT.bytes)

        // Act
        // Need initialize = true to run #onScheduled()
        runner.run(1, true, true)

        // Assert
        runner.assertAllFlowFilesTransferred(CountText.REL_FAILURE, 1)
        FlowFile flowFile = runner.getFlowFilesForRelationship(CountText.REL_FAILURE).first()
        logger.info("Generated flowfile: ${flowFile} | ${flowFile.attributes}")
    }
}
