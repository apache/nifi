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
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Ignore;

public class SignJWTTest {

    final static String json = "{\"sub\":\"1234567890\",\"name\":\"John Doe\",\"admin\":true,\"iat\":1516239022,\"testComplexStruct\":{\"even more complex\":[\"and a\",\"bit more\"]}}";
    final static String pubKeyDir = "./src/test/resources/";

    @Test
    public void testKeyPath() {
        final String privateKey = "./src/test/resources/key2";

        signAndVerifyJWTSuccess(json, privateKey, SignJWT.PATH, pubKeyDir);
    }

    @Test
    public void testKeyInProperty() {
        try {
            final File file = new File("./src/test/resources/key1");
            final String asciiKey = new String(Files.readAllBytes(file.toPath()));
            signAndVerifyJWTSuccess(json, asciiKey, SignJWT.ASCII, pubKeyDir);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testInvalidKeyInProperty() {
        final InputStream content = new ByteArrayInputStream(json.getBytes());

        // Generate a test runner to mock a processor in a flow
        final TestRunner runner = TestRunners.newTestRunner(new SignJWT());

        // Add properties
        runner.setProperty(SignJWT.KEY_TYPE, SignJWT.ASCII);
        runner.setProperty(SignJWT.PRIVATE_KEY, "THIS IS NOT A KEY");

        // Add the content to the runner
        runner.enqueue(content);

        runner.assertNotValid();
    }

    @Test
    @Ignore("OpenSSH keys currently not supported")
    public void testOpenSSHkey() {
        final String privateKey = "./src/test/resources/openssh_private.key";

        signAndVerifyJWTSuccess(json, privateKey, SignJWT.PATH, pubKeyDir);
    }

    @Test
    public void testNonJSON() {
        final String nonJson = "This is a test string";
        final String privateKey = "./src/test/resources/key1";

        final MockFlowFile file = signJWTFailure(nonJson, privateKey, SignJWT.PATH);

        final String error = file.getAttribute(SignJWT.FAILURE_REASON_ATTR);

        Assert.assertTrue(error.startsWith(SignJWT.INVALID_JSON));
    }

    @Test
    public void testinvalidJSON() {
        // this json string is invalid
        final String invalidJson = "{\"sub\": \"1234567890\",\"name\": \"John Doe\",\"admin\": true,\"iat\": 1516239022,\"testComplexStruct\": {\"even more complex\": [\"and a\", \"bit more\"}}";
        final String privateKey = "./src/test/resources/key1";

        final MockFlowFile file = signJWTFailure(invalidJson, privateKey, SignJWT.PATH);

        final String error = file.getAttribute(SignJWT.FAILURE_REASON_ATTR);

        Assert.assertTrue(error.startsWith(SignJWT.INVALID_JSON));
    }

    @Test
    public void testInvalidSigningKey() {
        final String privateKeyPath = "";

        final InputStream content = new ByteArrayInputStream(json.getBytes());

        // Generate a test runner to mock a processor in a flow
        final TestRunner runner = TestRunners.newTestRunner(new SignJWT());

        // Add properties
        runner.setProperty(SignJWT.KEY_TYPE, SignJWT.PATH);
        runner.setProperty(SignJWT.PRIVATE_KEY, privateKeyPath);

        // Add the content to the runner
        runner.enqueue(content);

        runner.assertNotValid();
    }

    public void signAndVerifyJWTSuccess(final String input, final String privateKey, final String keyType,
            final String publicKeyDirectoryPath) {
        final TestRunner runner = initSignProcessor(input, privateKey, keyType);

        Assert.assertEquals(0, runner.getFlowFilesForRelationship(SignJWT.FAILURE_REL).size());
        Assert.assertEquals(1, runner.getFlowFilesForRelationship(SignJWT.SUCCESS_REL).size());

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(SignJWT.SUCCESS_REL);

        Assert.assertEquals(1, results.size());
        final MockFlowFile result = results.get(0);

        final TestRunner verify = TestRunners.newTestRunner(new VerifyJWT());

        verify.setProperty(VerifyJWT.PUBLIC_KEYS_PATH, publicKeyDirectoryPath);
        verify.enqueue(result);
        verify.run();
        verify.assertQueueEmpty();
        Assert.assertTrue(verify.isQueueEmpty());
        Assert.assertEquals(0, verify.getFlowFilesForRelationship(VerifyJWT.FAILURE_REL).size());
        Assert.assertEquals(1, verify.getFlowFilesForRelationship(VerifyJWT.SUCCESS_REL).size());
        final List<MockFlowFile> verifyResults = verify.getFlowFilesForRelationship(VerifyJWT.SUCCESS_REL);
        Assert.assertEquals(1, verifyResults.size());
        final MockFlowFile verifyResult = verifyResults.get(0);
        final String outJson = new String(runner.getContentAsByteArray(verifyResult));

        Assert.assertEquals(input, outJson);
    }

    public MockFlowFile signJWTFailure(final String input, final String privateKey, final String keyType) {
        final TestRunner runner = initSignProcessor(input, privateKey, keyType);

        Assert.assertEquals(1, runner.getFlowFilesForRelationship(SignJWT.FAILURE_REL).size());
        Assert.assertEquals(0, runner.getFlowFilesForRelationship(SignJWT.SUCCESS_REL).size());

        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(SignJWT.FAILURE_REL);

        Assert.assertEquals(1, results.size());
        final MockFlowFile result = results.get(0);

        return result;

    }

    public TestRunner initSignProcessor(final String input, final String privateKeyPath, final String keyType) {
        final InputStream content = new ByteArrayInputStream(input.getBytes());

        // Generate a test runner to mock a processor in a flow
        final TestRunner runner = TestRunners.newTestRunner(new SignJWT());

        // Add properties
        runner.setProperty(SignJWT.KEY_TYPE, keyType);
        runner.setProperty(SignJWT.PRIVATE_KEY, privateKeyPath);

        // Add the content to the runner and check its valid
        runner.enqueue(content);
        runner.assertValid();

        // Run the enqueued content
        runner.run();

        // All results were processed with out failure
        runner.assertQueueEmpty();
        Assert.assertTrue(runner.isQueueEmpty());

        return runner;
    }
}