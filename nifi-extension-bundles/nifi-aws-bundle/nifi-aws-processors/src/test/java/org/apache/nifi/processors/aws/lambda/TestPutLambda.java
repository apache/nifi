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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.util.Base64;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvalidParameterValueException;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;

import java.nio.charset.Charset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class TestPutLambda {

    private TestRunner runner = null;
    private PutLambda mockPutLambda = null;

    private LambdaClient mockLambdaClient = null;

    @BeforeEach
    public void setUp() {
        mockLambdaClient = Mockito.mock(LambdaClient.class);
        mockPutLambda = new PutLambda() {
            @Override
            protected LambdaClient getClient(final ProcessContext context) {
                return mockLambdaClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPutLambda);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testSizeGreaterThan6MB() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "test-function");
        byte [] largeInput = new byte[6000001];
        runner.enqueue(largeInput);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
    }

    @Test
    public void testPutLambdaSimple() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "test-function");
        runner.enqueue("TestContent");

        final InvokeResponse invokeResult = InvokeResponse.builder()
                .statusCode(200)
                .logResult(Base64.encodeAsString("test-log-result".getBytes()))
                .payload(SdkBytes.fromString("test-payload", Charset.defaultCharset()))
                .build();
        when(mockLambdaClient.invoke(any(InvokeRequest.class))).thenReturn(invokeResult);

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<InvokeRequest> captureRequest = ArgumentCaptor.forClass(InvokeRequest.class);
        Mockito.verify(mockLambdaClient, Mockito.times(1)).invoke(captureRequest.capture());
        InvokeRequest request = captureRequest.getValue();
        assertEquals("test-function", request.functionName());

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
        when(mockLambdaClient.invoke(any(InvokeRequest.class))).thenThrow(InvalidParameterValueException.builder()
                .message("TestFail")
                .awsErrorDetails(AwsErrorDetails.builder().errorMessage("TestFail").errorCode("400").build())
                .build());

        runner.assertValid();
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
    }

    @Test
    public void testPutLambdaTooManyRequestsException() {
        runner.setProperty(PutLambda.AWS_LAMBDA_FUNCTION_NAME, "test-function");
        runner.enqueue("TestContent");
        when(mockLambdaClient.invoke(any(InvokeRequest.class))).thenThrow(TooManyRequestsException.builder()
                .message("TestFail")
                .awsErrorDetails(AwsErrorDetails.builder().errorMessage("TestFail").errorCode("400").build())
                .build());

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
        when(mockLambdaClient.invoke(any(InvokeRequest.class))).thenThrow(new AmazonServiceException("TestFail"));

        runner.assertValid();
        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutLambda.REL_FAILURE, 1);
    }

}
