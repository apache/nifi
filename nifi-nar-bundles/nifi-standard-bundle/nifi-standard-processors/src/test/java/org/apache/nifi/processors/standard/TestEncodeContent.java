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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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

class TestEncodeContent {

    private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

    private static final Path FILE_PATH = Paths.get("src/test/resources/hello.txt");
    private TestRunner testRunner;

    @BeforeEach
    void setUp() {
        testRunner = TestRunners.newTestRunner(EncodeContent.class);
    }

    @Test
    void testBase64RoundTrip() throws IOException {
        runTestRoundTrip(EncodingType.BASE64_ENCODING.getValue());
    }

    @Test
    void testFailDecodeNotBase64() throws IOException {
        runTestDecodeFailure(EncodingType.BASE64_ENCODING.getValue());
    }

    @Test
    void testFailDecodeNotBase64ButIsAMultipleOfFourBytes() {
        testRunner.setProperty(EncodeContent.MODE, EncodingMode.DECODE.getValue());
        testRunner.setProperty(EncodeContent.ENCODING, EncodingType.BASE64_ENCODING.getValue());

        testRunner.enqueue("four@@@@multiple".getBytes());
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncodeContent.REL_FAILURE, 1);
    }

    @Test
    void testBase32RoundTrip() throws IOException {
        runTestRoundTrip(EncodingType.BASE32_ENCODING.getValue());
    }

    @Test
    void testFailDecodeNotBase32() throws IOException {
        runTestDecodeFailure(EncodingType.BASE32_ENCODING.getValue());
    }

    @Test
    void testHexRoundTrip() throws IOException {
        runTestRoundTrip(EncodingType.HEX_ENCODING.getValue());
    }

    @Test
    void testFailDecodeNotHex() throws IOException {
        runTestDecodeFailure(EncodingType.HEX_ENCODING.getValue());
    }

    private void runTestRoundTrip(String encoding) throws IOException {
        testRunner.setProperty(EncodeContent.MODE, EncodingMode.ENCODE.getValue());
        testRunner.setProperty(EncodeContent.ENCODING, encoding);

        testRunner.enqueue(FILE_PATH);
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncodeContent.REL_SUCCESS, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(EncodeContent.REL_SUCCESS).get(0);
        testRunner.assertQueueEmpty();

        testRunner.setProperty(EncodeContent.MODE, EncodingMode.DECODE.getValue());
        testRunner.enqueue(flowFile);
        testRunner.clearTransferState();
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(EncodeContent.REL_SUCCESS, 1);

        flowFile = testRunner.getFlowFilesForRelationship(EncodeContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(FILE_PATH);
    }

    private void runTestDecodeFailure(String encoding) throws IOException {
        testRunner.setProperty(EncodeContent.MODE, EncodingMode.DECODE.getValue());
        testRunner.setProperty(EncodeContent.ENCODING, encoding);

        testRunner.enqueue(FILE_PATH);
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(EncodeContent.REL_FAILURE, 1);
    }

    @Test
    void testEncodeDecodeSpecialCharsBase64() {
        final String specialChars = "!@#$%^&*()_+{}:\"<>?[];',./~`-=";
        final String expectedOutput = "IUAjJCVeJiooKV8re306Ijw+P1tdOycsLi9+YC09" + System.lineSeparator();

        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.BASE64_ENCODING, specialChars, expectedOutput);
        testRunner.clearTransferState(); // clear the state for the next test
        executeTestSuccessHelper(EncodingMode.DECODE, EncodingType.BASE64_ENCODING, expectedOutput, specialChars);
    }

    @Test
    void testBasicDecodeBase32() {
        executeTestSuccessHelper(EncodingMode.DECODE, EncodingType.BASE32_ENCODING, "NBSWY3DP", "hello");
    }

    @Test
    void testBasicDecodeBase64() {
        executeTestSuccessHelper(EncodingMode.DECODE, EncodingType.BASE64_ENCODING, "Zm9v", "foo");
    }

    @Test
    void testBasicDecodeHex() {
        executeTestSuccessHelper(EncodingMode.DECODE, EncodingType.HEX_ENCODING, "666F6F", "foo");
    }

    @Test
    void testBasicEncodeHex0() {
        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.HEX_ENCODING, "hello", "68656C6C6F");
    }

    @Test
    void testBasicEncodeHex1() {
        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.HEX_ENCODING, "foo", "666F6F");
    }

    @Test
    void testBasicEncodeBase320() {
        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.BASE32_ENCODING, "hello", "NBSWY3DP" + System.lineSeparator());
    }

    @Test
    void testBasicEncodeBase321() {
        executeTestSuccessHelper(EncodingMode.ENCODE, EncodingType.BASE32_ENCODING, "foo", "MZXW6===" + System.lineSeparator());
    }

    @Test
    void testBasicEncodeBase640() {
        executeTestSuccessHelper(EncodingMode.ENCODE,
            EncodingType.BASE64_ENCODING,
            "hello",
            "aGVsbG8=" + System.lineSeparator());
    }

    @Test
    void testBasicEncodeBase641() {
        executeTestSuccessHelper(EncodingMode.ENCODE,
            EncodingType.BASE64_ENCODING,
            "foo",
            "Zm9v" + System.lineSeparator());
    }

    @Test
    void testBlankValueShouldNotFail() {
        executeTestSuccessHelper(EncodingMode.ENCODE,
            EncodingType.BASE64_ENCODING,
            StringUtils.EMPTY,
            StringUtils.EMPTY);
    }

    @Test
    void testEncodeContentMultipleLinesBase64() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdCwg" + System.lineSeparator()
            + "c2VkIGRvIGVpdXNtb2QgdGVtcG9yIGluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWdu" + System.lineSeparator()
            + "YSBhbGlxdWEu" + System.lineSeparator();

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE64_ENCODING,
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
            EncodingType.BASE64_ENCODING,
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
            EncodingType.BASE32_ENCODING,
            LOREM_IPSUM,
            LineOutputMode.SINGLE_LINE,
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testEncodeContentMultipleLinesBase32() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "JRXXEZLNEBUXA43VNUQGI33MN5ZCA43JOQQGC3LFOQWCAY3PNZZWKY3UMV2HK4RAMFSGS4DJ" + System.lineSeparator()
            + "ONRWS3THEBSWY2LUFQQHGZLEEBSG6IDFNF2XG3LPMQQHIZLNOBXXEIDJNZRWSZDJMR2W45BA" + System.lineSeparator()
            + "OV2CA3DBMJXXEZJAMV2CAZDPNRXXEZJANVQWO3TBEBQWY2LROVQS4===" + System.lineSeparator();

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE32_ENCODING,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES, // set false to output multiple lines
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testEncodeContentMultipleLinesNonStandardLengthBase32() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "JRXXEZLNEBUXA43VNUQGI33MN5ZCA43JOQQGC3LFOQWCAY3PNZZWKY3UMV2HK4RAMFSGS4DJONRWS3TH" + System.lineSeparator()
            + "EBSWY2LUFQQHGZLEEBSG6IDFNF2XG3LPMQQHIZLNOBXXEIDJNZRWSZDJMR2W45BAOV2CA3DBMJXXEZJA" + System.lineSeparator()
            + "MV2CAZDPNRXXEZJANVQWO3TBEBQWY2LROVQS4===" + System.lineSeparator();

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE32_ENCODING,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES,
            80,
            System.lineSeparator(),
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
            EncodingType.BASE32_ENCODING,
            LOREM_IPSUM,
            LineOutputMode.SINGLE_LINE,
            2,                          // set a low value >= 0
            System.lineSeparator(),
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testEncodeContentMultipleLinesNonStandardLengthBase64() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdCwgc2Vk" + System.lineSeparator()
            + "IGRvIGVpdXNtb2QgdGVtcG9yIGluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWduYSBhbGlx" + System.lineSeparator()
            + "dWEu" + System.lineSeparator();

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE64_ENCODING,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES, // set false to output multiple lines
            80,
            System.lineSeparator(),
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testOverrideLineSeparatorBase64() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQsIGNvbnNlY3RldHVyIGFkaXBpc2NpbmcgZWxpdCwgc2Vk" + "|"
            + "IGRvIGVpdXNtb2QgdGVtcG9yIGluY2lkaWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWduYSBhbGlx" + "|"
            + "dWEu" + "|";

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE64_ENCODING,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES, // set false to output multiple lines
            80,
            "|",
            expectedOutput,
            EncodeContent.REL_SUCCESS);
    }

    @Test
    void testOverrideLineSeparatorBase32() {
        // this input is greater than 57 bytes, sure to generate multiple lines in base64
        final String expectedOutput = "JRXXEZLNEBUXA43VNUQGI33MN5ZCA43JOQQGC3LFOQWCAY3PNZZWKY3UMV2HK4RAMFSGS4DJONRWS3TH" + "|"
            + "EBSWY2LUFQQHGZLEEBSG6IDFNF2XG3LPMQQHIZLNOBXXEIDJNZRWSZDJMR2W45BAOV2CA3DBMJXXEZJA" + "|"
            + "MV2CAZDPNRXXEZJANVQWO3TBEBQWY2LROVQS4===" + "|";

        // Execute the test using the helper method
        executeTestHelper(EncodingMode.ENCODE,
            EncodingType.BASE32_ENCODING,
            LOREM_IPSUM,
            LineOutputMode.MULTIPLE_LINES, // set false to output multiple lines
            80,
            "|",
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
            EncodingType.BASE64_ENCODING,
            LOREM_IPSUM,
            LineOutputMode.SINGLE_LINE, // set true to output single line
            2,                                      // set a low value >= 0
            System.lineSeparator(),
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
            System.lineSeparator(),
            expectedOutput,
            routedTo);
    }

    private void executeTestHelper(final DescribedValue mode,
        final DescribedValue encodingType,
        final String input,
        final DescribedValue outputToSingleLine,
        final Integer lineLength,
        final String lineSeparator,
        final String expectedOutput,
        final Relationship routedTo) {

        testRunner.setProperty(EncodeContent.MODE, mode.getValue());
        testRunner.setProperty(EncodeContent.ENCODING, encodingType.getValue());
        testRunner.setProperty(EncodeContent.LINE_OUTPUT_MODE, outputToSingleLine.getValue());
        testRunner.setProperty(EncodeContent.ENCODED_LINE_LENGTH, Integer.toString(lineLength));
        testRunner.setProperty(EncodeContent.ENCODED_LINE_SEPARATOR, lineSeparator);

        testRunner.enqueue(input);
        testRunner.run();

        final MockFlowFile result = testRunner.getFlowFilesForRelationship(routedTo).get(0);
        assertEquals(expectedOutput, result.getContent());
        testRunner.assertAllFlowFilesTransferred(routedTo, 1);
    }
}
