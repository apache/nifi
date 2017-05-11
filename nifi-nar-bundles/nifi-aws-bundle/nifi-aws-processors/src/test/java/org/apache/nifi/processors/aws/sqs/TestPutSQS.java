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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;


public class TestPutSQS {

    private TestRunner runner = null;
    private PutSQS mockPutSQS = null;
    private AmazonSQSClient actualSQSClient = null;
    private AmazonSQSClient mockSQSClient = null;

    @Before
    public void setUp() {
        mockSQSClient = Mockito.mock(AmazonSQSClient.class);
        mockPutSQS = new PutSQS() {
            protected AmazonSQSClient getClient() {
                actualSQSClient = client;
                return mockSQSClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPutSQS);
    }

    @Test
    public void testSimplePut() throws IOException {
        runner.setValidateExpressionUsage(false);
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
        runner.setValidateExpressionUsage(false);
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

}
