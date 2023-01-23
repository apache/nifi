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

import static org.apache.nifi.processors.standard.VerifyContentMAC.FAILURE;
import static org.apache.nifi.processors.standard.VerifyContentMAC.SUCCESS;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VerifyContentMACTest {

    private static final String HMAC_SHA_256 = "HmacSHA256";
    private static final String SECRET_KEY = "secretKey";
    private static final String PROVIDED_MAC = "providedMac";
    private static final String INVALID = "invalid";
    private static final String FLOW_CONTENT = "content";

    private TestRunner runner;

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(new VerifyContentMAC());
    }

    @Test
    public void testNotValidWithoutMACAlgorithm() {
        runner.setProperty(VerifyContentMAC.SECRET_KEY, SECRET_KEY);
        runner.setProperty(VerifyContentMAC.MAC, PROVIDED_MAC);

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithInvalidAlgorithm() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, INVALID);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, SECRET_KEY);
        runner.setProperty(VerifyContentMAC.MAC, PROVIDED_MAC);

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithoutSecretKey() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA_256);
        runner.setProperty(VerifyContentMAC.MAC, PROVIDED_MAC);

        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithoutProvidedMac() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA_256);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, SECRET_KEY);

        runner.assertNotValid();
    }

    @Test
    public void testFlowFileTransferredToSuccessWhenMacMatch() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA_256);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, SECRET_KEY);
        runner.setProperty(VerifyContentMAC.MAC, "d698c07902835568e69eec11c08ae53b7502e31acbc84cc210a72172f029ed80");

        runner.enqueue(FLOW_CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(SUCCESS);
    }

    @Test
    public void testFlowFileTransferredToFailureInCaseOfVerificationFailure() {
        runner.setProperty(VerifyContentMAC.MAC_ALGORITHM, HMAC_SHA_256);
        runner.setProperty(VerifyContentMAC.SECRET_KEY, SECRET_KEY);
        runner.setProperty(VerifyContentMAC.MAC, INVALID);

        runner.enqueue(FLOW_CONTENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(FAILURE);
    }
}