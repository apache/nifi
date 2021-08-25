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
package org.apache.nifi.processors.aws.lambda;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * This test contains both unit and integration test (integration tests are ignored by default)
 */
public class ITPutLambda {

    private TestRunner runner;
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutLambda.class);
        runner.setProperty(PutLambda.ACCESS_KEY, "abcd");
        runner.setProperty(PutLambda.SECRET_KEY, "secret key");
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "functionName");
        runner.assertValid();
    }

    @AfterEach
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testSizeGreaterThan6MB() throws Exception {
        runner = TestRunners.newTestRunner(PutLambda.class);
        runner.setProperty(PutLambda.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "hello");
        runner.assertValid();
        byte [] largeInput = new byte[6000001];
        for (int i = 0; i < 6000001; i++) {
            largeInput[i] = 'a';
        }
        runner.enqueue(largeInput);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    @Disabled
    public void testIntegrationSuccess() throws Exception {
        runner = TestRunners.newTestRunner(PutLambda.class);
        runner.setProperty(PutLambda.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "hello");
        runner.assertValid();

        runner.enqueue("{\"test\":\"hi\"}".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutLambda.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();
        Assertions.assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR), "Function error should be null " + out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR));
        Assertions.assertNotNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_LOG), "log should not be null");
        Assertions.assertEquals("200",out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_STATUS_CODE), "Status should be equal");
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    @Disabled
    public void testIntegrationClientErrorBadMessageBody() throws Exception {
        runner = TestRunners.newTestRunner(PutLambda.class);
        runner.setProperty(PutLambda.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "hello");
        runner.assertValid();

        runner.enqueue("badbod".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutLambda.REL_FAILURE);
        final MockFlowFile out = ffs.iterator().next();
        Assertions.assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR), "Function error should be null since there is exception"
            + out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR));
        Assertions.assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_LOG), "log should not be null");
        Assertions.assertEquals(null,out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_STATUS_CODE), "Status should be equal");
        Assertions.assertEquals("InvalidRequestContentException",out.getAttribute(PutLambda.AWS_LAMBDA_EXCEPTION_ERROR_CODE), "exception error code should be equal");
        Assertions.assertEquals("Client",out.getAttribute(PutLambda.AWS_LAMBDA_EXCEPTION_ERROR_TYPE), "exception exception error type should be equal");
        Assertions.assertEquals("400",out.getAttribute(PutLambda.AWS_LAMBDA_EXCEPTION_STATUS_CODE), "exception exception error code should be equal");
        Assertions.assertTrue(out.getAttribute(PutLambda.AWS_LAMBDA_EXCEPTION_MESSAGE)
               .startsWith("Could not parse request body into json: Unrecognized token 'badbod': was expecting ('true', 'false' or 'null')"), "exception exception error message should be start with");
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    @Disabled
    public void testIntegrationFailedBadStreamName() throws Exception {
        runner = TestRunners.newTestRunner(PutLambda.class);
        runner.setProperty(PutLambda.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "bad-function-name");
        runner.assertValid();

        runner.enqueue("{\"test\":\"hi\"}".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutLambda.REL_FAILURE);
        final MockFlowFile out = ffs.iterator().next();
        Assertions.assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR), "Function error should be null since there is exception"
            + out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR));
        Assertions.assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_LOG), "log should not be null");
        Assertions.assertEquals(null,out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_STATUS_CODE), "Status should be equal");

    }
}
