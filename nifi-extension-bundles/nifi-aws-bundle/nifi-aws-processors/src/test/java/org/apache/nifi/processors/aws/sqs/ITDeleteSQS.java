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

import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITDeleteSQS extends AbstractSQSIT {


    @Test
    public void testSimpleDelete() {
        final SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(getQueueUrl())
                .messageBody("Hello World")
                .build();
        final SendMessageResponse response = getClient().sendMessage(request);
        assertTrue(response.sdkHttpResponse().isSuccessful());

        // Setup - receive message to get receipt handle
        final ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQueueUrl())
                .build();
        final ReceiveMessageResponse receiveMessageResult = getClient().receiveMessage(receiveMessageRequest);
        assertEquals(200, receiveMessageResult.sdkHttpResponse().statusCode());
        final Message deleteMessage = receiveMessageResult.messages().get(0);
        final String receiptHandle = deleteMessage.receiptHandle();

        // Test - delete message with DeleteSQS
        final TestRunner runner = initRunner(DeleteSQS.class);
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "1.txt");
        ffAttributes.put("sqs.receipt.handle", receiptHandle);
        runner.enqueue("TestMessageBody", ffAttributes);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(DeleteSQS.REL_SUCCESS, 1);
    }
}