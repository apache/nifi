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

package org.apache.nifi.processors.cipher;

import org.apache.nifi.processors.cipher.VerifyContentMAC.Encoding;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.nifi.processors.cipher.VerifyContentMAC.FAILURE;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.HMAC_SHA256;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.HMAC_SHA512;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.MAC_ALGORITHM_ATTRIBUTE;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.MAC_CALCULATED_ATTRIBUTE;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.MAC_ENCODING_ATTRIBUTE;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.SUCCESS;

class VerifyContentMACTest {

    private static final String INVALID = "invalid";
    private static final String FLOW_CONTENT = "content";
    private static final String CONTENT_HMAC_SHA256_HEXADECIMAL = "3c81220b10838ab972fd4f6796304dab1bb3ccdf65a0edc8c5ce7eef30191b6c";
    private static final String CONTENT_HMAC_SHA512_HEXADECIMAL =
        "1e85c66a5ade958b0282632c9e0654c9a9f5985170ee53fbf9b55c536dbab768ddf835fe1afe11c37aeaec5f9751ab3762852fd33ea3c279a21ca98db99b3c62";
    private static final String CONTENT_HMAC_SHA256_BASE64 = "PIEiCxCDirly/U9nljBNqxuzzN9loO3Ixc5+7zAZG2w=";
    private static final String CONTENT_HMAC_SHA512_BASE64 = "HoXGalrelYsCgmMsngZUyan1mFFw7lP7+bVcU226t2jd+DX+Gv4Rw3rq7F+XUas3YoUv0z6jwnmiHKmNuZs8Yg==";
    private static final String TEST_SECRET_KEY_UTF8 = "test";
    private static final String TEST_SECRET_KEY_HEXADECIMAL = "74657374";
    private static final String TEST_SECRET_KEY_BASE64 = "dGVzdA==";

    private TestRunner runner;

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(new VerifyContentMAC());
    }

    @Test
    void testNotValidWithoutMACAlgorithm() {
        runner.setProperty(VerifyContentMAC.SECRET_KEY, TEST_SECRET_KEY_HEXADECIMAL);
        runner.setProperty(VerifyContentMAC.MAC, CONTENT_HMAC_SHA256_HEXADECIMAL);

        runner.assertNotValid();
    }

    @Test
    void testNotValidWithInvalidAlgorithm() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, INVALID);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, TEST_SECRET_KEY_HEXADECIMAL);
        runner.setProperty(VerifyContentMAC.MAC, CONTENT_HMAC_SHA256_HEXADECIMAL);

        runner.assertNotValid();
    }

    @Test
    void testNotValidWithoutSecretKey() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA256);
        runner.setProperty(VerifyContentMAC.MAC, CONTENT_HMAC_SHA256_HEXADECIMAL);

        runner.assertNotValid();
    }

    @Test
    void testNotValidWithoutProvidedMac() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA256);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, TEST_SECRET_KEY_HEXADECIMAL);

        runner.assertNotValid();
    }

    @Test
    void testNotValidWhenSecretKeyEncodingIsHexadecimalButProvidedKeyIsNotValidHexadecimal() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA256);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, "not_hexadecimal");
        runner.setProperty(VerifyContentMAC.SECRET_KEY_ENCODING, Encoding.HEXADECIMAL.name());
        runner.setProperty(VerifyContentMAC.MAC, CONTENT_HMAC_SHA256_HEXADECIMAL);

        runner.assertNotValid();
    }

    @Test
    void testNotValidWhenSecretKeyEncodingIsBase64ButProvidedKeyIsNotValidBase64() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA256);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, "not_base64");
        runner.setProperty(VerifyContentMAC.SECRET_KEY_ENCODING, Encoding.BASE64.name());
        runner.setProperty(VerifyContentMAC.MAC, CONTENT_HMAC_SHA256_HEXADECIMAL);

        runner.assertNotValid();
    }

    @ParameterizedTest(name = "macAlgorithm={0} secretKeyEncoding={1} secretKey={2} inputMac={3}")
    @MethodSource("invalidConstructorArguments")
    void testFlowFileTransferredToSuccessWhenMacMatch(String macAlgorithm, Encoding secretKeyEncoding, String secretKey, String inputMac, Encoding macEncoding) {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, macAlgorithm);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, secretKey);
        runner.setProperty(VerifyContentMAC.SECRET_KEY_ENCODING, secretKeyEncoding.name());
        runner.setProperty(VerifyContentMAC.MAC_ENCODING, macEncoding.name());
        runner.setProperty(VerifyContentMAC.MAC, inputMac);

        runner.enqueue(FLOW_CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(SUCCESS);
        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put(MAC_CALCULATED_ATTRIBUTE, inputMac);
        expectedAttributes.put(MAC_ALGORITHM_ATTRIBUTE, macAlgorithm);
        expectedAttributes.put(MAC_ENCODING_ATTRIBUTE, macEncoding.name());

        runner.assertAttributes(SUCCESS, expectedAttributes.keySet(), Collections.singleton(expectedAttributes));
    }

    private static Stream<Arguments> invalidConstructorArguments() {
        return Stream.of(
            Arguments.of(HMAC_SHA256, Encoding.UTF8, TEST_SECRET_KEY_UTF8, CONTENT_HMAC_SHA256_HEXADECIMAL, Encoding.HEXADECIMAL),
            Arguments.of(HMAC_SHA512, Encoding.UTF8, TEST_SECRET_KEY_UTF8, CONTENT_HMAC_SHA512_HEXADECIMAL, Encoding.HEXADECIMAL),
            Arguments.of(HMAC_SHA256, Encoding.UTF8, TEST_SECRET_KEY_UTF8, CONTENT_HMAC_SHA256_BASE64, Encoding.BASE64),
            Arguments.of(HMAC_SHA512, Encoding.UTF8, TEST_SECRET_KEY_UTF8, CONTENT_HMAC_SHA512_BASE64, Encoding.BASE64),
            Arguments.of(HMAC_SHA256, Encoding.HEXADECIMAL, TEST_SECRET_KEY_HEXADECIMAL, CONTENT_HMAC_SHA256_HEXADECIMAL, Encoding.HEXADECIMAL),
            Arguments.of(HMAC_SHA512, Encoding.HEXADECIMAL, TEST_SECRET_KEY_HEXADECIMAL, CONTENT_HMAC_SHA512_HEXADECIMAL, Encoding.HEXADECIMAL),
            Arguments.of(HMAC_SHA256, Encoding.HEXADECIMAL, TEST_SECRET_KEY_HEXADECIMAL, CONTENT_HMAC_SHA256_BASE64, Encoding.BASE64),
            Arguments.of(HMAC_SHA512, Encoding.HEXADECIMAL, TEST_SECRET_KEY_HEXADECIMAL, CONTENT_HMAC_SHA512_BASE64, Encoding.BASE64),
            Arguments.of(HMAC_SHA256, Encoding.BASE64, TEST_SECRET_KEY_BASE64, CONTENT_HMAC_SHA256_HEXADECIMAL, Encoding.HEXADECIMAL),
            Arguments.of(HMAC_SHA512, Encoding.BASE64, TEST_SECRET_KEY_BASE64, CONTENT_HMAC_SHA512_HEXADECIMAL, Encoding.HEXADECIMAL),
            Arguments.of(HMAC_SHA256, Encoding.BASE64, TEST_SECRET_KEY_BASE64, CONTENT_HMAC_SHA256_BASE64, Encoding.BASE64),
            Arguments.of(HMAC_SHA512, Encoding.BASE64, TEST_SECRET_KEY_BASE64, CONTENT_HMAC_SHA512_BASE64, Encoding.BASE64)
        );
    }

    @Test
    void testFlowFileTransferredToFailureInCaseOfVerificationFailure() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA256);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, TEST_SECRET_KEY_HEXADECIMAL);
        runner.setProperty(VerifyContentMAC.MAC, CONTENT_HMAC_SHA512_HEXADECIMAL);

        runner.enqueue(FLOW_CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(FAILURE);
    }

    @Test
    void testFlowFileTransferredToFailureInCaseOfException() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA256);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, TEST_SECRET_KEY_HEXADECIMAL);
        runner.setProperty(VerifyContentMAC.MAC, INVALID);

        runner.enqueue(FLOW_CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(FAILURE);
    }
}