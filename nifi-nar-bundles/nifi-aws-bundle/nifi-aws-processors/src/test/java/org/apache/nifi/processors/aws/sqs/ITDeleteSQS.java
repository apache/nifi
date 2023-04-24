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

import org.apache.nifi.processors.aws.credentials.provider.PropertiesCredentialsProvider;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ITDeleteSQS {

    private final String CREDENTIALS_FILE = System.getProperty("user.home") + "/aws-credentials.properties";
    private final String TEST_QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/123456789012/nifi-test-queue";
    private final String TEST_REGION = "us-west-2";
    SqsClient sqsClient = null;

    @BeforeEach
    public void setUp() throws IOException {
        sqsClient = SqsClient.builder()
                .region(Region.of(TEST_REGION))
                .credentialsProvider(() -> new PropertiesCredentialsProvider(new File(CREDENTIALS_FILE)).resolveCredentials())
                .build();
    }

    @Test
    public void testSimpleDelete() {
        // Setup - put one message in queue
        final SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(TEST_QUEUE_URL)
                .messageBody("Test message")
                .build();
        SendMessageResponse sendMessageResult = sqsClient.sendMessage(sendMessageRequest);
        assertEquals(200, sendMessageResult.sdkHttpResponse().statusCode());

        // Setup - receive message to get receipt handle
        final ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(TEST_QUEUE_URL)
                .build();
        ReceiveMessageResponse receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        assertEquals(200, receiveMessageResult.sdkHttpResponse().statusCode());
        Message deleteMessage = receiveMessageResult.messages().get(0);
        String receiptHandle = deleteMessage.receiptHandle();

        // Test - delete message with DeleteSQS
        final TestRunner runner = TestRunners.newTestRunner(new DeleteSQS());
        runner.setProperty(DeleteSQS.CREDENTIALS_FILE, CREDENTIALS_FILE);
        runner.setProperty(DeleteSQS.QUEUE_URL, TEST_QUEUE_URL);
        runner.setProperty(DeleteSQS.REGION, TEST_REGION);
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "1.txt");
        ffAttributes.put("sqs.receipt.handle", receiptHandle);
        runner.enqueue("TestMessageBody", ffAttributes);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteSQS.REL_SUCCESS, 1);
    }
}