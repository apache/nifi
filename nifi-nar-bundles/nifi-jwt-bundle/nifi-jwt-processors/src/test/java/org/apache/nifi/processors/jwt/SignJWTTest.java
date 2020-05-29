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
package org.apache.nifi.processors.jwt;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.junit.Assert;

public class SignJWTTest {

    final String json = "{\"sub\":\"1234567890\",\"name\":\"John Doe\",\"admin\":true,\"iat\":1516239022,\"testComplexStruct\":{\"even more complex\":[\"and a\",\"bit more\"]}}";

    @Test
    public void Test() {
        final String privateKey = "./src/test/resources/4096key2.key";
        final String pubKeyDir = "./src/test/resources/";

        final String result = signAndUnsignJWTSuccess(json, privateKey, pubKeyDir);
        System.out.println(result);
    }

    @Test
    public void TestOpenSSHkey() {
        final String privateKey = "./src/test/resources/openssh_private.key";
        final String pubKeyDir = "./src/test/resources/";

        final String result = signAndUnsignJWTSuccess(json, privateKey, pubKeyDir);
        System.out.println(result);
    }

    @Test
    public void TestNonJSON() {
        final String nonJson = "This is a test string";
        final String privateKey = "./src/test/resources/2048key2.key";

        MockFlowFile file = signJWTFailure(nonJson, privateKey);

        String error = file.getAttribute(SignJWT.FAILURE_REASON_ATTR);

        Assert.assertTrue(error.startsWith(SignJWT.INVALID_JSON));
    }

    @Test
    public void TestinvalidJSON() {
        // this json string is invalid
        final String invalidJson = "{\"sub\": \"1234567890\",\"name\": \"John Doe\",\"admin\": true,\"iat\": 1516239022,\"testComplexStruct\": {\"even more complex\": [\"and a\", \"bit more\"}}";
        final String privateKey = "./src/test/resources/2048key2.key";

        MockFlowFile file = signJWTFailure(invalidJson, privateKey);

        String error = file.getAttribute(SignJWT.FAILURE_REASON_ATTR);

        Assert.assertTrue(error.startsWith(SignJWT.INVALID_JSON));
    }

    @Test
    public void TestInvalidSigningKey() {
        final String privateKeyPath = "./src/test/resources/2048key2.key.pub";

        final InputStream content = new ByteArrayInputStream(json.getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new SignJWT());

        // Add properties
        runner.setProperty(SignJWT.PRIVATE_KEY_PATH, privateKeyPath);

        // Add the content to the runner
        runner.enqueue(content);

        runner.assertNotValid();
    }

    public String signAndUnsignJWTSuccess(final String input, final String privateKeyPath, final String publicKeyDirectoryPath) {
        TestRunner runner = initSignProcessor(input, privateKeyPath);

        Assert.assertEquals(0, runner.getFlowFilesForRelationship(SignJWT.FAILURE_REL).size());
        Assert.assertEquals(1, runner.getFlowFilesForRelationship(SignJWT.SUCCESS_REL).size());

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(SignJWT.SUCCESS_REL);

        Assert.assertEquals(1, results.size());
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));

        TestRunner unsign = TestRunners.newTestRunner(new UnsignJWT());

        unsign.setProperty(UnsignJWT.PUBLIC_KEYS_PATH, publicKeyDirectoryPath);
        unsign.enqueue(result);
        unsign.run();
        unsign.assertQueueEmpty();
        Assert.assertTrue(unsign.isQueueEmpty());
        Assert.assertEquals(0, unsign.getFlowFilesForRelationship(UnsignJWT.FAILURE_REL).size());
        Assert.assertEquals(1, unsign.getFlowFilesForRelationship(UnsignJWT.SUCCESS_REL).size());
        List<MockFlowFile> unsignResults = unsign.getFlowFilesForRelationship(UnsignJWT.SUCCESS_REL);
        Assert.assertEquals(1, unsignResults.size());
        MockFlowFile unsignResult = unsignResults.get(0);
        String outJson = new String(runner.getContentAsByteArray(unsignResult));

        Assert.assertEquals(input, outJson);

        return resultValue;
    }

    public MockFlowFile signJWTFailure(final String input, final String privateKeyPath) {
        TestRunner runner = initSignProcessor(input, privateKeyPath);

        Assert.assertEquals(1, runner.getFlowFilesForRelationship(SignJWT.FAILURE_REL).size());
        Assert.assertEquals(0, runner.getFlowFilesForRelationship(SignJWT.SUCCESS_REL).size());

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(SignJWT.FAILURE_REL);

        Assert.assertEquals(1, results.size());
        MockFlowFile result = results.get(0);

        return result;

    }

    public TestRunner initSignProcessor(final String input, final String privateKeyPath) {
        final InputStream content = new ByteArrayInputStream(input.getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new SignJWT());

        // Add properties
        runner.setProperty(SignJWT.PRIVATE_KEY_PATH, privateKeyPath);

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content
        runner.run();

        // All results were processed with out failure
        runner.assertQueueEmpty();
        Assert.assertTrue(runner.isQueueEmpty());

        return runner;
    }
}