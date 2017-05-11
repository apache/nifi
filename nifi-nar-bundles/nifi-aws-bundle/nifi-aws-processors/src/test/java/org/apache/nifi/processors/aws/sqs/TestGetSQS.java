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

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;


public class TestGetSQS {

    private TestRunner runner = null;
    private GetSQS mockGetSQS = null;
    private AmazonSQSClient actualSQSClient = null;
    private AmazonSQSClient mockSQSClient = null;

    @Before
    public void setUp() {
        mockSQSClient = Mockito.mock(AmazonSQSClient.class);
        mockGetSQS = new GetSQS() {
            protected AmazonSQSClient getClient() {
                actualSQSClient = client;
                return mockSQSClient;
            }
        };
        runner = TestRunners.newTestRunner(mockGetSQS);
    }

    @Test
    public void testGetMessageNoAutoDelete() {
        runner.setProperty(GetSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.setProperty(GetSQS.AUTO_DELETE, "false");

        Message message1 = new Message();
        message1.setBody("TestMessage1");
        message1.addAttributesEntry("attrib-key-1", "attrib-value-1");
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue("msg-attrib-value-1");
        message1.addMessageAttributesEntry("msg-attrib-key-1", messageAttributeValue);
        message1.setMD5OfBody("test-md5-hash-1");
        message1.setMessageId("test-message-id-1");
        message1.setReceiptHandle("test-receipt-handle-1");
        ReceiveMessageResult receiveMessageResult = new ReceiveMessageResult()
                .withMessages(message1);
        Mockito.when(mockSQSClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);

        runner.run(1);

        ArgumentCaptor<ReceiveMessageRequest> captureRequest = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).receiveMessage(captureRequest.capture());
        ReceiveMessageRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.getQueueUrl());
        Mockito.verify(mockSQSClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));

        runner.assertAllFlowFilesTransferred(GetSQS.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("sqs.attrib-key-1", "attrib-value-1");
        ff0.assertAttributeEquals("sqs.msg-attrib-key-1", "msg-attrib-value-1");
        ff0.assertAttributeEquals("hash.value", "test-md5-hash-1");
        ff0.assertAttributeEquals("hash.algorithm", "md5");
        ff0.assertAttributeEquals("sqs.message.id", "test-message-id-1");
        ff0.assertAttributeEquals("sqs.receipt.handle", "test-receipt-handle-1");
    }

    @Test
    public void testGetNoMessages() {
        runner.setProperty(GetSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        ReceiveMessageResult receiveMessageResult = new ReceiveMessageResult();
        Mockito.when(mockSQSClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);

        runner.run(1);

        ArgumentCaptor<ReceiveMessageRequest> captureRequest = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).receiveMessage(captureRequest.capture());
        ReceiveMessageRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.getQueueUrl());

        runner.assertAllFlowFilesTransferred(GetSQS.REL_SUCCESS, 0);
    }

    @Test
    public void testGetMessageAndAutoDelete() {
        runner.setProperty(GetSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.setProperty(GetSQS.AUTO_DELETE, "true");

        Message message1 = new Message();
        message1.setBody("TestMessage1");
        message1.setMessageId("test-message-id-1");
        message1.setReceiptHandle("test-receipt-handle-1");
        Message message2 = new Message();
        message2.setBody("TestMessage2");
        message2.setMessageId("test-message-id-2");
        message2.setReceiptHandle("test-receipt-handle-2");
        ReceiveMessageResult receiveMessageResult = new ReceiveMessageResult()
                .withMessages(message1, message2);
        Mockito.when(mockSQSClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);

        runner.run(1);

        ArgumentCaptor<ReceiveMessageRequest> captureReceiveRequest = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).receiveMessage(captureReceiveRequest.capture());
        ReceiveMessageRequest receiveRequest = captureReceiveRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", receiveRequest.getQueueUrl());

        ArgumentCaptor<DeleteMessageBatchRequest> captureDeleteRequest = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).deleteMessageBatch(captureDeleteRequest.capture());
        DeleteMessageBatchRequest deleteRequest = captureDeleteRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", deleteRequest.getQueueUrl());
        assertEquals("test-message-id-1", deleteRequest.getEntries().get(0).getId());
        assertEquals("test-message-id-2", deleteRequest.getEntries().get(1).getId());

        runner.assertAllFlowFilesTransferred(GetSQS.REL_SUCCESS, 2);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("sqs.message.id", "test-message-id-1");
        MockFlowFile ff1 = flowFiles.get(1);
        ff1.assertAttributeEquals("sqs.message.id", "test-message-id-2");
    }

}
