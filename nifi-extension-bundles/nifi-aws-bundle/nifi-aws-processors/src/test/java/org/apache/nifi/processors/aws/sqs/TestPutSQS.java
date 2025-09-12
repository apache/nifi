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
package org.apache.nifi.processors.aws.sqs;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestPutSQS {

    private TestRunner runner = null;
    private PutSQS mockPutSQS = null;
    private SqsClient mockSQSClient = null;

    @BeforeEach
    public void setUp() {
        mockSQSClient = Mockito.mock(SqsClient.class);
        mockPutSQS = new PutSQS() {
            @Override
            protected SqsClient getClient(ProcessContext context) {
                return mockSQSClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPutSQS);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testSimplePut() {
        runner.setProperty(PutSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue("TestMessageBody", attrs);

        SendMessageBatchResponse batchResult = SendMessageBatchResponse.builder().build();
        Mockito.when(mockSQSClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenReturn(batchResult);

        runner.run(1);

        ArgumentCaptor<SendMessageBatchRequest> captureRequest = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).sendMessageBatch(captureRequest.capture());
        SendMessageBatchRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.queueUrl());
        assertEquals("hello", request.entries().get(0).messageAttributes().get("x-custom-prop").stringValue());
        assertEquals("TestMessageBody", request.entries().get(0).messageBody());

        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 1);
    }

    @Test
    public void testPutException() {
        runner.setProperty(PutSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue("TestMessageBody", attrs);

        Mockito.when(mockSQSClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenThrow(new RuntimeException());

        runner.run(1);

        ArgumentCaptor<SendMessageBatchRequest> captureRequest = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).sendMessageBatch(captureRequest.capture());
        SendMessageBatchRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.queueUrl());
        assertEquals("TestMessageBody", request.entries().get(0).messageBody());

        runner.assertAllFlowFilesTransferred(PutSQS.REL_FAILURE, 1);
    }

    @Test
    public void testFIFOPut() {
        runner.setProperty(PutSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.setProperty(PutSQS.MESSAGEDEDUPLICATIONID, "${myuuid}");
        runner.setProperty(PutSQS.MESSAGEGROUPID, "test1234");
        assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        attrs.put("myuuid", "fb0dfed8-092e-40ee-83ce-5b576cd26236");
        runner.enqueue("TestMessageBody", attrs);

        final SendMessageBatchResponse batchResult = SendMessageBatchResponse.builder().build();
        Mockito.when(mockSQSClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenReturn(batchResult);

        runner.run(1);

        ArgumentCaptor<SendMessageBatchRequest> captureRequest = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).sendMessageBatch(captureRequest.capture());
        SendMessageBatchRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.queueUrl());
        assertEquals("hello", request.entries().get(0).messageAttributes().get("x-custom-prop").stringValue());
        assertEquals("TestMessageBody", request.entries().get(0).messageBody());
        assertEquals("test1234", request.entries().get(0).messageGroupId());
        assertEquals("fb0dfed8-092e-40ee-83ce-5b576cd26236", request.entries().get(0).messageDeduplicationId());

        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 1);
    }
}
