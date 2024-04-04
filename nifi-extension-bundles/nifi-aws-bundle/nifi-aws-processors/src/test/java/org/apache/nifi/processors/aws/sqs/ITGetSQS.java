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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITGetSQS extends AbstractSQSIT {

    @Test
    public void testSimpleGet() {
        final int messageCount = 4;

        for (int i = 0; i < messageCount; i++) {
            final SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(getQueueUrl())
                    .messageBody("Hello World " + i)
                    .messageAttributes(Map.of("i", MessageAttributeValue.builder().stringValue(Integer.toString(i)).dataType("String").build()))
                    .build();
            final SendMessageResponse response = getClient().sendMessage(request);
            assertTrue(response.sdkHttpResponse().isSuccessful());
        }

        final TestRunner runner = initRunner(GetSQS.class);
        runner.setProperty(GetSQS.TIMEOUT, "30 secs");

        runner.run();
        int count = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS).size();
        while (count < messageCount) {
            runner.run(1, false, false);
            count = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS).size();
        }
        runner.run();

        runner.assertAllFlowFilesTransferred(GetSQS.REL_SUCCESS, messageCount);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetSQS.REL_SUCCESS);
        for (int i = 0; i < messageCount; i++) {
            final String attributeValue = Integer.toString(i);
            final String messageBody = "Hello World " + i;

            final long matchingCount = flowFiles.stream()
                    .filter(ff -> attributeValue.equals(ff.getAttribute("sqs.i")))
                    .filter(ff -> messageBody.equals(ff.getContent()))
                    .count();
            assertEquals(1L, matchingCount);
        }
    }
}
