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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.encoding.EncodingMode;
import org.apache.nifi.processors.standard.encoding.EncodingType;
import org.apache.nifi.processors.standard.encoding.LineOutputMode;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestEncodeContent {

    private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

    private static final Path FILE_PATH = Paths.get("src/test/resources/hello.txt");

    private TestRunner testRunner;

    @BeforeEach
    void setUp() {
        testRunner = TestRunners.newTestRunner(EncodeContent.class);
    }

    @Test
    void testFailDecodeNotBase64ButIsAMultipleOfFourBytes() {
        testRunner.setProperty(EncodeContent.MODE, EncodingMode.DECODE);
        testRunner.setProperty(EncodeContent.ENCODING, EncodingType.BASE64);

        testRunner.enqueue("four@@@@multiple".getBytes());
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncodeContent.REL_FAILURE, 1);
    }

    @ParameterizedTest
    @EnumSource(value = EncodingType.class)
    void testRoundTrip(final EncodingType encoding) throws IOException {
        testRunner.setProperty(EncodeContent.MODE, EncodingMode.ENCODE);
        testRunner.setProperty(EncodeContent.ENCODING, encoding);

        testRunner.enqueue(FILE_PATH);
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncodeContent.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncodeContent.REL_SUCCESS).get(0);
        testRunner.assertQueueEmpty();

        testRunner.setProperty(EncodeContent.MODE, EncodingMode.DECODE);
        testRunner.enqueue(flowFile);
        testRunner.clearTransferState();
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(EncodeContent.REL_SUCCESS, 1);

        flowFile = testRunner.getFlowFilesForRelationship(EncodeContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(FILE_PATH);
    }

    @ParameterizedTest
    @EnumSource(value = EncodingType.class)
    void testDecodeFailure(final EncodingType encoding) throws IOException {
        testRunner.setProperty(EncodeContent.MODE, EncodingMode.DECODE);
        testRunner.setProperty(EncodeContent.ENCODING, encoding);

        testRunner.enqueue(FILE_PATH);
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncodeContent.REL_FAILURE, 1);
    }

    @Test
    void testEncodeDecodeSpecialCharsBase64() {
        final String specialChars = "!@#$%^&*()_+{}:\"<>?[];',./~`-=";
        final String expectedOutput = "IUAjJCVeJiooKV8re306Ijw+P1tdOycsLi9+YC09\n";

        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.BASE64, specialChars, expectedOutput);
        testRunner.clearTransferState(); // clear the state for the next test
        executeTestSuccessHelper(EncodingMode.DECODE, EncodingType.BASE64, expectedOutput, specialChars);
    }

    @ParameterizedTest
    @MethodSource("encodeBase32Args")
    void testBasicDecodeBase32(final String input, final String expectedOutput) {
        // use the same args from `encodeBase32Args`, only flip around input and output
        executeTestSuccessHelper(EncodingMode.DECODE, EncodingType.BASE32, expectedOutput, input);
    }

    @ParameterizedTest
    @MethodSource("encodeBase64Args")
    void testBasicDecodeBase64(final String input, final String expectedOutput) {
        // use the same args from `encodeBase64Args`, only flip around input and output
        executeTestSuccessHelper(EncodingMode.DECODE, EncodingType.BASE64, expectedOutput, input);
    }

    @ParameterizedTest
    @MethodSource("encodeHexArgs")
    void testBasicDecodeHex(final String input, final String expectedOutput) {
        // use the same args from `encodeHexArgs`, only flip around input and output
        executeTestSuccessHelper(EncodingMode.DECODE, EncodingType.HEXADECIMAL, expectedOutput, input);
    }

    @ParameterizedTest
    @MethodSource("encodeHexArgs")
    void testBasicEncodeHex(final String input, final String expectedOutput) {
        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.HEXADECIMAL, input, expectedOutput);
    }

    private static Stream<Arguments> encodeHexArgs() {
       return Stream.of(
               Arguments.of("hello", "68656C6C6F"),
               Arguments.of("foo", "666F6F"),
               Arguments.of("你好", "E4BDA0E5A5BD"),
               Arguments.of("Здравствуйте", "D097D0B4D180D0B0D0B2D181D182D0B2D183D0B9D182D0B5")
       );
   }

    @ParameterizedTest
    @MethodSource("encodeBase32Args")
    void testBasicEncodeBase32(final String input, final String expectedOutput) {
        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.BASE32, input, expectedOutput);
    }

    private static Stream<Arguments> encodeBase32Args() {
       return Stream.of(
               Arguments.of("hello", "NBSWY3DP\n"),
               Arguments.of("foo", "MZXW6===\n"),
               Arguments.of("你好", "4S62BZNFXU======\n"),
               Arguments.of("Здравствуйте", "2CL5BNGRQDILBUFS2GA5DAWQWLIYHUFZ2GBNBNI=\n")
       );
   }

    @ParameterizedTest
    @MethodSource("encodeBase64Args")
    void testBasicEncodeBase64(final String input, final String expectedOutput) {
        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.BASE64, input, expectedOutput);
    }

    private static Stream<Arguments> encodeBase64Args() {
       return Stream.of(
               Arguments.of("hello", "aGVsbG8=\n"),
               Arguments.of("foo", "Zm9v\n"),
               Arguments.of("你好", "5L2g5aW9\n"),
               Arguments.of("Здравствуйте", "0JfQtNGA0LDQstGB0YLQstGD0LnRgtC1\n")
       );
   }

    @Test
    void testBlankValueShouldNotFail() {
        executeTestSuccessHelper(EncodingMode.ENCODE,
            EncodingType.BASE64,
            StringUtils.EMPTY,
            StringUtils.EMPTY);
    }

    @Test
    void testEncodeContentMultipleLinesBase64() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdCwg\n"
            + "c2VkIGRvIGVpdXNtb2QgdGVtcG9yIGluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWdu\n"
            + "YSBhbGlxdWEu\n";

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE64,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES,
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testEncodeContentSingleLineBase64() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdCwg"
            + "c2VkIGRvIGVpdXNtb2QgdGVtcG9yIGluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWdu"
            + "YSBhbGlxdWEu";

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE64,
            LOREM_IPSUM,
            LineOutputMode.SINGLE_LINE,
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testEncodeContentSingleLineBase32() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "JRXXEZLNEBUXA43VNUQGI33MN5ZCA43JOQQGC3LFOQWCAY3PNZZWKY3UMV2HK4RAMFSGS4DJONRWS3THEBSWY2LUF"
            + "QQHGZLEEBSG6IDFNF2XG3LPMQQHIZLNOBXXEIDJNZRWSZDJMR2W45BAOV2CA3DBMJXXEZJAMV2CAZDPNRXXEZJANVQWO3TBEBQWY2LROVQS4===";

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE32,
            LOREM_IPSUM,
            LineOutputMode.SINGLE_LINE,
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testEncodeContentMultipleLinesBase32() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "JRXXEZLNEBUXA43VNUQGI33MN5ZCA43JOQQGC3LFOQWCAY3PNZZWKY3UMV2HK4RAMFSGS4DJ\n"
            + "ONRWS3THEBSWY2LUFQQHGZLEEBSG6IDFNF2XG3LPMQQHIZLNOBXXEIDJNZRWSZDJMR2W45BA\n"
            + "OV2CA3DBMJXXEZJAMV2CAZDPNRXXEZJANVQWO3TBEBQWY2LROVQS4===\n";

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE32,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES, // set false to output multiple lines
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testEncodeContentMultipleLinesNonStandardLengthBase32() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "JRXXEZLNEBUXA43VNUQGI33MN5ZCA43JOQQGC3LFOQWCAY3PNZZWKY3UMV2HK4RAMFSGS4DJONRWS3TH\n"
            + "EBSWY2LUFQQHGZLEEBSG6IDFNF2XG3LPMQQHIZLNOBXXEIDJNZRWSZDJMR2W45BAOV2CA3DBMJXXEZJA\n"
            + "MV2CAZDPNRXXEZJANVQWO3TBEBQWY2LROVQS4===\n";

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE32,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES,
            80,
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testThatLineLengthIsIgnoredIfSingleLineOutputTrueBase32() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "JRXXEZLNEBUXA43VNUQGI33MN5ZCA43JOQQGC3LFOQWCAY3PNZZWKY3UMV2HK4RAMFSGS4DJONRWS3THEBSWY2LUFQQHGZLEEB"
            + "SG6IDFNF2XG3LPMQQHIZLNOBXXEIDJNZRWSZDJMR2W45BAOV2CA3DBMJXXEZJAMV2CAZDPNRXXEZJANVQWO3TBEBQWY2LROVQS4===";

        // Setting a low value for `lineLength` but single line true ensures that `lineLength` is ignored
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE32,
            LOREM_IPSUM,
            LineOutputMode.SINGLE_LINE,
            2, // set a low value >= 0
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testEncodeContentMultipleLinesNonStandardLengthBase64() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdCwgc2Vk\n"
            + "IGRvIGVpdXNtb2QgdGVtcG9yIGluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWduYSBhbGlx\n"
            + "dWEu\n";

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE64,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES, // set false to output multiple lines
            80,
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testThatLineLengthIsIgnoredIfSingleLineOutputTrueBase64() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdCwg"
            + "c2VkIGRvIGVpdXNtb2QgdGVtcG9yIGluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWdu"
            + "YSBhbGlxdWEu";

        // Setting a low value for `lineLength` but single line true ensures that `lineLength` is ignored
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE64,
            LOREM_IPSUM,
            LineOutputMode.SINGLE_LINE, // set true to output single line
            2,                                      // set a low value >= 0
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    private void executeTestSuccessHelper(final DescribedValue mode,
        final DescribedValue encodingType,
        final String input,
        final String expectedOutput) {
        executeTestSuccessHelper(mode, encodingType, input, LineOutputMode.MULTIPLE_LINES, expectedOutput);
    }

    private void executeTestSuccessHelper(final DescribedValue mode,
        final DescribedValue encodingType,
        final String input,
        final DescribedValue outputToSingleLine,
        final String expectedOutput) {
        executeTestHelper(mode, encodingType, input, outputToSingleLine, expectedOutput, EncodeContent.REL_SUCCESS);
    }

    private void executeTestHelper(final DescribedValue mode,
        final DescribedValue encodingType,
        final String input,
        final DescribedValue outputToSingleLine,
        final String expectedOutput,
        final Relationship routedTo) {
        executeTestHelper(mode,
            encodingType,
            input,
            outputToSingleLine,
            76,
            expectedOutput,
            routedTo);
    }

    private void executeTestHelper(final DescribedValue mode,
        final DescribedValue encodingType,
        final String input,
        final DescribedValue outputToSingleLine,
        final Integer lineLength,
        final String expectedOutput,
        final Relationship routedTo) {

        testRunner.setProperty(EncodeContent.MODE, mode);
        testRunner.setProperty(EncodeContent.ENCODING, encodingType);
        testRunner.setProperty(EncodeContent.LINE_OUTPUT_MODE, outputToSingleLine);
        testRunner.setProperty(EncodeContent.ENCODED_LINE_LENGTH, Integer.toString(lineLength));

        testRunner.enqueue(input);
        testRunner.run();

        final MockFlowFile result = testRunner.getFlowFilesForRelationship(routedTo).get(0);
        assertEquals(expectedOutput, result.getContent());
        testRunner.assertAllFlowFilesTransferred(routedTo, 1);
    }
}
