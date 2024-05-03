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

import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test contains both unit and integration test (integration tests are ignored by default)
 */
public class ITPutLambda {

    private TestRunner runner;
    protected final static String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";

    @BeforeEach
    public void setUp() {
        Assumptions.assumeTrue(new File(CREDENTIALS_FILE).exists());

        runner = TestRunners.newTestRunner(PutLambda.class);
        AuthUtils.enableCredentialsFile(runner, CREDENTIALS_FILE);
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "functionName");
        runner.assertValid();
    }

    @AfterEach
    public void tearDown() {
        runner = null;
    }

    @Test
    public void testSizeGreaterThan6MB() {
        runner = TestRunners.newTestRunner(PutLambda.class);
        AuthUtils.enableCredentialsFile(runner, CREDENTIALS_FILE);
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "hello");
        runner.assertValid();
        byte [] largeInput = new byte[6000001];
        runner.enqueue(largeInput);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testIntegrationSuccess() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "hello");
        runner.assertValid();

        runner.enqueue("{\"test\":\"hi\"}".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutLambda.REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutLambda.REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();
        assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR), "Function error should be null " + out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR));
        assertNotNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_LOG), "log should not be null");
        assertEquals("200",out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_STATUS_CODE), "Status should be equal");
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testIntegrationClientErrorBadMessageBody() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "hello");
        runner.assertValid();

        runner.enqueue("badbod".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutLambda.REL_FAILURE);
        final MockFlowFile out = ffs.iterator().next();
        assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR), "Function error should be null since there is exception"
            + out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR));
        assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_LOG), "log should not be null");
        assertEquals(null,out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_STATUS_CODE), "Status should be equal");
        assertEquals("InvalidRequestContentException",out.getAttribute(PutLambda.AWS_LAMBDA_EXCEPTION_ERROR_CODE), "exception error code should be equal");
        assertEquals("400",out.getAttribute(PutLambda.AWS_LAMBDA_EXCEPTION_STATUS_CODE), "exception exception error code should be equal");
        assertTrue(out.getAttribute(PutLambda.AWS_LAMBDA_EXCEPTION_MESSAGE)
               .startsWith("Could not parse request body into json: Could not parse payload into json: Unrecognized token 'badbod': " +
                       "was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')"), "exception exception error message should be start with");
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testIntegrationFailedBadStreamName() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "bad-function-name");
        runner.assertValid();

        runner.enqueue("{\"test\":\"hi\"}".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(PutLambda.REL_FAILURE);
        final MockFlowFile out = ffs.iterator().next();
        assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR), "Function error should be null since there is exception"
            + out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_FUNCTION_ERROR));
        assertNull(out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_LOG));
        assertEquals(null, out.getAttribute(PutLambda.AWS_LAMBDA_RESULT_STATUS_CODE));

    }
}
