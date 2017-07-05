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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;


public class TestPutSQS {

    private TestRunner runner = null;
    private PutSQS mockPutSQS = null;
    private AmazonSQSClient actualSQSClient = null;
    private AmazonSQSClient mockSQSClient = null;

    @Before
    public void setUp() {
        mockSQSClient = Mockito.mock(AmazonSQSClient.class);
        mockPutSQS = new PutSQS() {
            @Override
            protected AmazonSQSClient getClient() {
                actualSQSClient = client;
                return mockSQSClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPutSQS);
    }

    @Test
    public void testSimplePut() throws IOException {
        runner.setProperty(PutSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue("TestMessageBody", attrs);

        SendMessageBatchResult batchResult = new SendMessageBatchResult();
        Mockito.when(mockSQSClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenReturn(batchResult);

        runner.run(1);

        ArgumentCaptor<SendMessageBatchRequest> captureRequest = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).sendMessageBatch(captureRequest.capture());
        SendMessageBatchRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.getQueueUrl());
        assertEquals("hello", request.getEntries().get(0).getMessageAttributes().get("x-custom-prop").getStringValue());
        assertEquals("TestMessageBody", request.getEntries().get(0).getMessageBody());

        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 1);
    }

    @Test
    public void testPutException() throws IOException {
        runner.setProperty(PutSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        runner.enqueue("TestMessageBody", attrs);

        Mockito.when(mockSQSClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenThrow(new AmazonSQSException("TestFail"));

        runner.run(1);

        ArgumentCaptor<SendMessageBatchRequest> captureRequest = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).sendMessageBatch(captureRequest.capture());
        SendMessageBatchRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.getQueueUrl());
        assertEquals("TestMessageBody", request.getEntries().get(0).getMessageBody());

        runner.assertAllFlowFilesTransferred(PutSQS.REL_FAILURE, 1);
    }

    @Test
    public void testSimplePutBatch() throws IOException {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PutSQS.QUEUE_URL, "${url}");
        runner.setProperty(PutSQS.BATCH_SIZE, "2");
        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        attrs.put("url", "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.enqueue("TestMessageBody1", attrs);

        attrs = new HashMap<>();
        attrs.put("filename", "2.txt");
        attrs.put("url", "https://sqs.us-west-2.amazonaws.com/123456789012/another");
        runner.enqueue("TestMessageBody2", attrs);

        attrs = new HashMap<>();
        attrs.put("filename", "3.txt");
        attrs.put("url", "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.enqueue("TestMessageBody3", attrs);

        attrs = new HashMap<>();
        attrs.put("filename", "4.txt");
        attrs.put("url", "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.enqueue("TestMessageBody4", attrs);

        SendMessageBatchResult batchResult = new SendMessageBatchResult();
        Mockito.when(mockSQSClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenReturn(batchResult);

        runner.run(1);

        ArgumentCaptor<SendMessageBatchRequest> captureRequest = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).sendMessageBatch(captureRequest.capture());
        SendMessageBatchRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.getQueueUrl());

        List<String> messageBodies = new ArrayList<String>();
        assertEquals(request.getEntries().size(), 2);
        messageBodies.add(request.getEntries().get(0).getMessageBody());
        messageBodies.add(request.getEntries().get(1).getMessageBody());

        assertTrue(messageBodies.contains("TestMessageBody1"));
        assertTrue(messageBodies.contains("TestMessageBody3"));

        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 2);
    }

    @Test
    public void testSimplePutBatchWithOneFailure() throws IOException {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PutSQS.QUEUE_URL, "${url}");
        runner.setProperty(PutSQS.BATCH_SIZE, "2");
        Assert.assertTrue(runner.setProperty("x-custom-prop", "hello").isValid());

        Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "1.txt");
        attrs.put("url", "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.enqueue("TestMessageBody1", attrs);

        attrs = new HashMap<>();
        attrs.put("filename", "2.txt");
        attrs.put("url", "https://sqs.us-west-2.amazonaws.com/123456789012/another");
        runner.enqueue("TestMessageBody2", attrs);

        attrs = new HashMap<>();
        attrs.put("filename", "3.txt");
        attrs.put("url", "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.enqueue("TestMessageBody3", attrs);

        attrs = new HashMap<>();
        attrs.put("filename", "4.txt");
        attrs.put("url", "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.enqueue("TestMessageBody4", attrs);

        when(mockSQSClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenAnswer(new Answer<SendMessageBatchResult>() {
            @Override
            public SendMessageBatchResult answer(InvocationOnMock invocation) throws Throwable {
                SendMessageBatchRequest request = (SendMessageBatchRequest) invocation.getArguments()[0];
                SendMessageBatchResult result = new SendMessageBatchResult();
                int i = 0;
                for (SendMessageBatchRequestEntry entry : request.getEntries()) {
                    if(i % 2 == 0) {
                        // we introduce half failures in the batch request
                        BatchResultErrorEntry failure = new BatchResultErrorEntry();
                        failure.setId(entry.getId());
                        failure.setMessage("Error message");
                        result.getFailed().add(failure);
                    }
                    i++;
                }
                return result;
            }
        });

        runner.run(1);

        ArgumentCaptor<SendMessageBatchRequest> captureRequest = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).sendMessageBatch(captureRequest.capture());
        SendMessageBatchRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.getQueueUrl());

        runner.assertTransferCount(PutSQS.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSQS.REL_FAILURE, 1);
    }

}
