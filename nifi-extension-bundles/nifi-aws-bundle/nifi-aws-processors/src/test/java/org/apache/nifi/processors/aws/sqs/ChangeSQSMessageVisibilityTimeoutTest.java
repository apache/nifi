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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChangeSQSMessageVisibilityTimeoutTest {

    private TestRunner runner = null;
    private SqsClient mockSQSClient = null;

    private static final String QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue-000000000";
    private static final String FILENAME = "1.txt";
    private static final String RECEIPT_HANDLE = "test-receipt-handle-1";

    @BeforeEach
    void setUp() {
        mockSQSClient = mock(SqsClient.class);
        ChangeMessageVisibilityBatchResponse mockResponse = ChangeMessageVisibilityBatchResponse.builder()
                .failed(Collections.emptyList())
                .build();
        when(mockSQSClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class))).thenReturn(mockResponse);
        ChangeSQSMessageVisibilityTimeout mockChangeSQSMessageVisibilityTimeout = new ChangeSQSMessageVisibilityTimeout() {

            @Override
            protected SqsClient getClient(ProcessContext context) {
                return mockSQSClient;
            }
        };
        runner = TestRunners.newTestRunner(mockChangeSQSMessageVisibilityTimeout);
        AuthUtils.enableAccessKey(runner, "accessKeyId", "secretKey");
    }

    @Test
    void testChangeVisibilityTimeoutSingleMessage() {
        runner.setProperty(ChangeSQSMessageVisibilityTimeout.QUEUE_URL, QUEUE_URL);
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put(CoreAttributes.FILENAME.key(), FILENAME);
        ffAttributes.put("sqs.receipt.handle", RECEIPT_HANDLE);
        runner.enqueue("TestMessageBody", ffAttributes);

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<ChangeMessageVisibilityBatchRequest> captureChangeMessageVisibilityTimeoutRequest = ArgumentCaptor.forClass(ChangeMessageVisibilityBatchRequest.class);
        verify(mockSQSClient, times(1)).changeMessageVisibilityBatch(captureChangeMessageVisibilityTimeoutRequest.capture());
        ChangeMessageVisibilityBatchRequest changeMessageVisibilityTimeoutRequest = captureChangeMessageVisibilityTimeoutRequest.getValue();
        assertEquals(QUEUE_URL, changeMessageVisibilityTimeoutRequest.queueUrl());
        assertEquals(RECEIPT_HANDLE, changeMessageVisibilityTimeoutRequest.entries().getFirst().receiptHandle());
        assertEquals(15 * 60, changeMessageVisibilityTimeoutRequest.entries().getFirst().visibilityTimeout());

        runner.assertAllFlowFilesTransferred(ChangeSQSMessageVisibilityTimeout.REL_SUCCESS, 1);
    }

    @Test
    void testChangeVisibilityTimeoutWithCustomReceiptHandle() {
        runner.setProperty(ChangeSQSMessageVisibilityTimeout.QUEUE_URL, QUEUE_URL);
        runner.setProperty(ChangeSQSMessageVisibilityTimeout.RECEIPT_HANDLE, "${custom.receipt.handle}");
        runner.setProperty(ChangeSQSMessageVisibilityTimeout.VISIBILITY_TIMEOUT, "${custom.timeout.value}");
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put(CoreAttributes.FILENAME.key(), FILENAME);
        ffAttributes.put("custom.receipt.handle", RECEIPT_HANDLE);
        ffAttributes.put("custom.timeout.value", "1 min");
        runner.enqueue("TestMessageBody", ffAttributes);

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<ChangeMessageVisibilityBatchRequest> captureChangeMessageVisibilityRequest = ArgumentCaptor.forClass(ChangeMessageVisibilityBatchRequest.class);
        verify(mockSQSClient, times(1)).changeMessageVisibilityBatch(captureChangeMessageVisibilityRequest.capture());
        ChangeMessageVisibilityBatchRequest changeMessageVisibilityRequest = captureChangeMessageVisibilityRequest.getValue();
        assertEquals(60, changeMessageVisibilityRequest.entries().getFirst().visibilityTimeout());

        runner.assertAllFlowFilesTransferred(ChangeSQSMessageVisibilityTimeout.REL_SUCCESS, 1);
    }

    @Test
    void testChangeMessageVisibilityTimeoutException() {
        runner.setProperty(ChangeSQSMessageVisibilityTimeout.QUEUE_URL, QUEUE_URL);
        final Map<String, String> ff1Attributes = new HashMap<>();
        ff1Attributes.put(CoreAttributes.FILENAME.key(), FILENAME);
        ff1Attributes.put("sqs.receipt.handle", RECEIPT_HANDLE);
        runner.enqueue("TestMessageBody1", ff1Attributes);
        when(mockSQSClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
                .thenThrow(new RuntimeException());

        runner.assertValid();
        runner.run(1);

        ArgumentCaptor<ChangeMessageVisibilityBatchRequest> captureChangeVisibilityTimeoutRequest = ArgumentCaptor.forClass(ChangeMessageVisibilityBatchRequest.class);
        verify(mockSQSClient, times(1)).changeMessageVisibilityBatch(captureChangeVisibilityTimeoutRequest.capture());

        runner.assertAllFlowFilesTransferred(ChangeSQSMessageVisibilityTimeout.REL_FAILURE, 1);
    }

}
