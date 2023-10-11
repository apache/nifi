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
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestDeleteSQS {

    private TestRunner runner = null;
    private DeleteSQS mockDeleteSQS = null;
    private SqsClient mockSQSClient = null;

    @BeforeEach
    public void setUp() {
        mockSQSClient = Mockito.mock(SqsClient.class);
        DeleteMessageBatchResponse mockResponse = DeleteMessageBatchResponse.builder()
                .failed(Collections.emptyList())
                .build();
        Mockito.when(mockSQSClient.deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class))).thenReturn(mockResponse);
        mockDeleteSQS = new DeleteSQS() {

            @Override
            protected SqsClient getClient(ProcessContext context) {
                return mockSQSClient;
            }
        };
        runner = TestRunners.newTestRunner(mockDeleteSQS);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    public void testDeleteSingleMessage() {
        runner.setProperty(DeleteSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "1.txt");
        ffAttributes.put("sqs.receipt.handle", "test-receipt-handle-1");
        runner.enqueue("TestMessageBody", ffAttributes);

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<DeleteMessageBatchRequest> captureDeleteRequest = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).deleteMessageBatch(captureDeleteRequest.capture());
        DeleteMessageBatchRequest deleteRequest = captureDeleteRequest.getValue();
        assertEquals("https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000", deleteRequest.queueUrl());
        assertEquals("test-receipt-handle-1", deleteRequest.entries().get(0).receiptHandle());

        runner.assertAllFlowFilesTransferred(DeleteSQS.REL_SUCCESS, 1);
    }

    @Test
    public void testDeleteWithCustomReceiptHandle() {
        runner.setProperty(DeleteSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        runner.setProperty(DeleteSQS.RECEIPT_HANDLE, "${custom.receipt.handle}");
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "1.txt");
        ffAttributes.put("custom.receipt.handle", "test-receipt-handle-1");
        runner.enqueue("TestMessageBody", ffAttributes);

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<DeleteMessageBatchRequest> captureDeleteRequest = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).deleteMessageBatch(captureDeleteRequest.capture());
        DeleteMessageBatchRequest deleteRequest = captureDeleteRequest.getValue();
        assertEquals("test-receipt-handle-1", deleteRequest.entries().get(0).receiptHandle());

        runner.assertAllFlowFilesTransferred(DeleteSQS.REL_SUCCESS, 1);
    }

    @Test
    public void testDeleteException() {
        runner.setProperty(DeleteSQS.QUEUE_URL, "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000");
        final Map<String, String> ff1Attributes = new HashMap<>();
        ff1Attributes.put("filename", "1.txt");
        ff1Attributes.put("sqs.receipt.handle", "test-receipt-handle-1");
        runner.enqueue("TestMessageBody1", ff1Attributes);
        Mockito.when(mockSQSClient.deleteMessageBatch(Mockito.any(DeleteMessageBatchRequest.class)))
                .thenThrow(new RuntimeException());

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<DeleteMessageBatchRequest> captureDeleteRequest = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(mockSQSClient, Mockito.times(1)).deleteMessageBatch(captureDeleteRequest.capture());

        runner.assertAllFlowFilesTransferred(DeleteSQS.REL_FAILURE, 1);
    }

}
