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
import org.apache.nifi.processors.aws.AbstractAwsProcessor;
import org.apache.nifi.processors.aws.ObsoleteAbstractAwsProcessorProperties;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.util.PropertyMigrationResult;
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
import java.util.Set;

import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPutSQS {

    private TestRunner runner = null;
    private SqsClient mockSQSClient = null;

    @BeforeEach
    public void setUp() {
        mockSQSClient = Mockito.mock(SqsClient.class);
        PutSQS mockPutSQS = new PutSQS() {
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
        assertEquals("hello", request.entries().getFirst().messageAttributes().get("x-custom-prop").stringValue());
        assertEquals("TestMessageBody", request.entries().getFirst().messageBody());

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
        assertEquals("TestMessageBody", request.entries().getFirst().messageBody());

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
        assertEquals("hello", request.entries().getFirst().messageAttributes().get("x-custom-prop").stringValue());
        assertEquals("TestMessageBody", request.entries().getFirst().messageBody());
        assertEquals("test1234", request.entries().getFirst().messageGroupId());
        assertEquals("fb0dfed8-092e-40ee-83ce-5b576cd26236", request.entries().getFirst().messageDeduplicationId());

        runner.assertAllFlowFilesTransferred(PutSQS.REL_SUCCESS, 1);
    }

    @Test
    void testMigration() {
        final Map<String, String> expected = Map.ofEntries(
                Map.entry("aws-region", REGION.getName()),
                Map.entry(AbstractAwsProcessor.OBSOLETE_AWS_CREDENTIALS_PROVIDER_SERVICE_PROPERTY_NAME, AbstractAwsProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE.getName()),
                Map.entry(ProxyConfigurationService.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE.getName()),
                Map.entry("message-group-id", PutSQS.MESSAGEGROUPID.getName()),
                Map.entry("deduplication-message-id", PutSQS.MESSAGEDEDUPLICATIONID.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());

        final Set<String> expectedRemoved = Set.of(
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_ACCESS_KEY.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_SECRET_KEY.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_CREDENTIALS_FILE.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_HOST.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_PORT.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_USERNAME.getValue(),
                ObsoleteAbstractAwsProcessorProperties.OBSOLETE_PROXY_PASSWORD.getValue());

        assertEquals(expectedRemoved,
                propertyMigrationResult.getPropertiesRemoved());
    }
}
