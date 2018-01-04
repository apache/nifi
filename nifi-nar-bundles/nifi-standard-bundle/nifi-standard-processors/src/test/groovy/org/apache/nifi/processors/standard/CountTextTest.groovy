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
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.KeyDerivationFunction
import org.apache.nifi.security.util.crypto.PasswordBasedEncryptor
import org.apache.nifi.util.MockProcessContext
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

@RunWith(JUnit4.class)
class CountTextTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CountTextTest.class)

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
        assert flowFile.attributes."text.line.count" == 34 as String
        assert flowFile.attributes."text.line.nonempty.count" == 28 as String
        assert flowFile.attributes."text.word.count" == 166 as String
        assert flowFile.attributes."text.character.count" == 900 as String
    }

    @Test
    void testShouldCountEachMetric() throws Exception {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(CountText.class)
        String INPUT_TEXT = new File("src/test/resources/TestCountText/jabberwocky.txt").text

        final def EXPECTED_VALUES = [
                "text.line.count"         : 34,
                "text.line.nonempty.count": 28,
                "text.word.count"         : 166,
                "text.character.count"    : 900,
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
                "text.line.count"         : 34,
                "text.line.nonempty.count": 28,
                "text.word.count"         : 166,
                "text.character.count"    : 900,
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

    @Ignore("Not yet implemented")
    @Test
    void testShouldValidateCharacterEncodings() throws Exception {
        // Arrange
        final TestRunner runner = TestRunners.newTestRunner(EncryptContent.class)
        Collection<ValidationResult> results
        MockProcessContext pc

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC

        // Integer.MAX_VALUE or 128, so use 256 or 128
        final int MAX_KEY_LENGTH = [PasswordBasedEncryptor.getMaxAllowedKeyLength(encryptionMethod.algorithm), 256].min()
        final String TOO_LONG_KEY_HEX = "ab" * (MAX_KEY_LENGTH / 8 + 1)
        logger.info("Using key ${TOO_LONG_KEY_HEX} (${TOO_LONG_KEY_HEX.length() * 4} bits)")

        runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE)
        runner.setProperty(EncryptContent.ENCRYPTION_ALGORITHM, encryptionMethod.name())
        runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name())
        runner.setProperty(EncryptContent.RAW_KEY_HEX, TOO_LONG_KEY_HEX)

        runner.enqueue(new byte[0])
        pc = (MockProcessContext) runner.getProcessContext()

        // Act
        results = pc.validate()

        // Assert
        Assert.assertEquals(1, results.size())
        logger.expected(results)
        ValidationResult vr = results.first()

        String expectedResult = "'raw-key-hex' is invalid because Key must be valid length [128, 192, 256]"
        String message = "'" + vr.toString() + "' contains '" + expectedResult + "'"
        Assert.assertTrue(message, vr.toString().contains(expectedResult))
    }

}
