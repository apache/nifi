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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestGetSQS {

    private TestRunner runner = null;
    private GetSQS mockGetSQS = null;
    private SqsClient mockSQSClient = null;

    @BeforeEach
    public void setUp() {
        mockSQSClient = Mockito.mock(SqsClient.class);
        mockGetSQS = new GetSQS() {
            @Override
            protected SqsClient getClient(ProcessContext context) {
                return mockSQSClient;
            }
        };
        runner = TestRunners.newTestRunner(mockGetSQS);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testGetMessageNoAutoDelete() {
        runner.setProperty(GetSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.setProperty(GetSQS.AUTO_DELETE, "false");

        final Map<String, String> attributes = new HashMap<>();
        final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
                .stringValue("msg-attrib-value-1").build();
        messageAttributes.put("msg-attrib-key-1", messageAttributeValue);
        attributes.put("attrib-key-1", "attrib-value-1"); // This attribute is no longer valid in SDK v2
        attributes.put(MessageSystemAttributeName.MESSAGE_GROUP_ID.toString(), "attrib-value-1"); // However, this one is allowed
        Message message1 = Message.builder()
                .body("TestMessage1")
                .attributesWithStrings(attributes)
                .messageAttributes(messageAttributes)
                .md5OfBody("test-md5-hash-1")
                .messageId("test-message-id-1")
                .receiptHandle("test-receipt-handle-1")
                .build();
        ReceiveMessageResponse receiveMessageResult = ReceiveMessageResponse.builder()
                .messages(message1)
                .build();
        Mockito.when(mockSQSClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);

        runner.run(1);

        ArgumentCaptor<ReceiveMessageRequest> captureRequest = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).receiveMessage(captureRequest.capture());
        ReceiveMessageRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.queueUrl());
        Mockito.verify(mockSQSClient, Mockito.never()).deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class));

        runner.assertAllFlowFilesTransferred(GetSQS.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeNotExists("sqs.attrib-key-1");
        ff0.assertAttributeEquals("sqs.MessageGroupId", "attrib-value-1");
        ff0.assertAttributeEquals("sqs.msg-attrib-key-1", "msg-attrib-value-1");
        ff0.assertAttributeEquals("hash.value", "test-md5-hash-1");
        ff0.assertAttributeEquals("hash.algorithm", "md5");
        ff0.assertAttributeEquals("sqs.message.id", "test-message-id-1");
        ff0.assertAttributeEquals("sqs.receipt.handle", "test-receipt-handle-1");
    }

    @Test
    public void testGetNoMessages() {
        runner.setProperty(GetSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        ReceiveMessageResponse receiveMessageResult = ReceiveMessageResponse.builder().build();
        Mockito.when(mockSQSClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);

        runner.run(1);

        ArgumentCaptor<ReceiveMessageRequest> captureRequest = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).receiveMessage(captureRequest.capture());
        ReceiveMessageRequest request = captureRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", request.queueUrl());

        runner.assertAllFlowFilesTransferred(GetSQS.REL_SUCCESS, 0);
    }

    @Test
    public void testGetMessageAndAutoDelete() {
        runner.setProperty(GetSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.setProperty(GetSQS.AUTO_DELETE, "true");

        Message message1 = Message.builder()
                .body("TestMessage1")
                .messageId("test-message-id-1")
                .receiptHandle("test-receipt-handle-1")
                .build();
        Message message2 = Message.builder()
                .body("TestMessage2")
                .messageId("test-message-id-2")
                .receiptHandle("test-receipt-handle-2")
                .build();
        ReceiveMessageResponse receiveMessageResult = ReceiveMessageResponse.builder()
                .messages(message1, message2)
                .build();
        Mockito.when(mockSQSClient.receiveMessage(Mockito.any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);

        runner.run(1);

        ArgumentCaptor<ReceiveMessageRequest> captureReceiveRequest = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).receiveMessage(captureReceiveRequest.capture());
        ReceiveMessageRequest receiveRequest = captureReceiveRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", receiveRequest.queueUrl());

        ArgumentCaptor<DeleteMessageBatchRequest> captureDeleteRequest = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).deleteMessageBatch(captureDeleteRequest.capture());
        DeleteMessageBatchRequest deleteRequest = captureDeleteRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", deleteRequest.queueUrl());
        assertEquals("test-message-id-1", deleteRequest.entries().get(0).id());
        assertEquals("test-message-id-2", deleteRequest.entries().get(1).id());

        runner.assertAllFlowFilesTransferred(GetSQS.REL_SUCCESS, 2);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("sqs.message.id", "test-message-id-1");
        MockFlowFile ff1 = flowFiles.get(1);
        ff1.assertAttributeEquals("sqs.message.id", "test-message-id-2");
    }

}
