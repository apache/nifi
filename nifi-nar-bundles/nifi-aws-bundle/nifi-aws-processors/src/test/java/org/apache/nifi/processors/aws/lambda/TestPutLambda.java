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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvalidParameterValueException;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.TooManyRequestsException;
import com.amazonaws.util.Base64;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestPutLambda {

    private TestRunner runner = null;
    private PutLambda mockPutLambda = null;
    private AWSLambdaClient actualLambdaClient = null;
    private AWSLambdaClient mockLambdaClient = null;

    @Before
    public void setUp() {
        mockLambdaClient = Mockito.mock(AWSLambdaClient.class);
        mockPutLambda = new PutLambda() {
            protected AWSLambdaClient getClient() {
                actualLambdaClient = client;
                return mockLambdaClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPutLambda);
    }

    @Test
    public void testSizeGreaterThan6MB() throws Exception {
        runner = TestRunners.newTestRunner(PutLambda.class);
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

    @Test
    public void testPutLambdaSimple() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "test-function");
        runner.enqueue("TestContent");

        InvokeResult invokeResult = new InvokeResult();
        invokeResult.setStatusCode(200);
        invokeResult.setLogResult(Base64.encodeAsString("test-log-result".getBytes()));
        invokeResult.setPayload(ByteBuffer.wrap("test-payload".getBytes()));
        Mockito.when(mockLambdaClient.invoke(Mockito.any(InvokeRequest.class))).thenReturn(invokeResult);

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<InvokeRequest> captureRequest = ArgumentCaptor.forClass(InvokeRequest.class);
        Mockito.verify(mockLambdaClient, Mockito.times(1)).invoke(captureRequest.capture());
        InvokeRequest request = captureRequest.getValue();
        assertEquals("test-function", request.getFunctionName());

        runner.assertAllFlowFilesTransferred(PutLambda.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutLambda.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals(PutLambda.AWS_LAMBDA_RESULT_STATUS_CODE, "200");
        ff0.assertAttributeEquals(PutLambda.AWS_LAMBDA_RESULT_LOG, "test-log-result");
        ff0.assertAttributeEquals(PutLambda.AWS_LAMBDA_RESULT_PAYLOAD, "test-payload");
    }

    @Test
    public void testPutLambdaParameterException() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "test-function");
        runner.enqueue("TestContent");
        Mockito.when(mockLambdaClient.invoke(Mockito.any(InvokeRequest.class))).thenThrow(new InvalidParameterValueException("TestFail"));

        runner.assertValid();
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
    }

    @Test
    public void testPutLambdaTooManyRequestsException() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "test-function");
        runner.enqueue("TestContent");
        Mockito.when(mockLambdaClient.invoke(Mockito.any(InvokeRequest.class))).thenThrow(new TooManyRequestsException("TestFail"));

        runner.assertValid();
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutLambda.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.get(0);
        assertTrue(ff0.isPenalized());
    }

    @Test
    public void testPutLambdaAmazonException() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "test-function");
        runner.enqueue("TestContent");
        Mockito.when(mockLambdaClient.invoke(Mockito.any(InvokeRequest.class))).thenThrow(new AmazonServiceException("TestFail"));

        runner.assertValid();
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
    }

}
