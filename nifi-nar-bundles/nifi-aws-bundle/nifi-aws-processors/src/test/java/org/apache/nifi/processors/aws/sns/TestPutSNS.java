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
package org.apache.nifi.processors.aws.sns;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestPutSNS {

    private TestRunner runner = null;
    private PutSNS mockPutSNS = null;
    private SnsClient mockSNSClient = null;

    @BeforeEach
    public void setUp() {
        mockSNSClient = Mockito.mock(SnsClient.class);
        mockPutSNS = new PutSNS() {
            @Override
            protected SnsClient getClient(ProcessContext context) {
                return mockSNSClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPutSNS);
    }

    @Test
    public void testPublish() throws InitializationException {
        AuthUtils.enableCredentialsFile(runner, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(PutSNS.ARN, "arn:aws:sns:us-west-2:123456789012:test-topic-1");
        runner.setProperty(PutSNS.SUBJECT, "${eval.subject}");
        assertTrue(runner.setProperty("DynamicProperty", "hello!").isValid());
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "1.txt");
        ffAttributes.put("eval.subject", "test-subject");
        runner.enqueue("Test Message Content", ffAttributes);

        final PublishResponse mockPublishResponse = PublishResponse.builder().build();
        Mockito.when(mockSNSClient.publish(Mockito.any(PublishRequest.class))).thenReturn(mockPublishResponse);

        runner.run();

        ArgumentCaptor<PublishRequest> captureRequest = ArgumentCaptor.forClass(PublishRequest.class);
        Mockito.verify(mockSNSClient, Mockito.times(1)).publish(captureRequest.capture());
        PublishRequest request = captureRequest.getValue();
        assertEquals("arn:aws:sns:us-west-2:123456789012:test-topic-1", request.topicArn());
        assertEquals("Test Message Content", request.message());
        assertEquals("test-subject", request.subject());
        assertEquals("hello!", request.messageAttributes().get("DynamicProperty").stringValue());

        runner.assertAllFlowFilesTransferred(PutSNS.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutSNS.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals(CoreAttributes.FILENAME.key(), "1.txt");
    }

    @Test
    public void testPublishFIFO() throws InitializationException {
        AuthUtils.enableCredentialsFile(runner, "src/test/resources/mock-aws-credentials.properties");
        runner.setProperty(PutSNS.ARN, "arn:aws:sns:us-west-2:123456789012:test-topic-1.fifo");
        runner.setProperty(PutSNS.SUBJECT, "${eval.subject}");
        runner.setProperty(PutSNS.MESSAGEDEDUPLICATIONID, "${myuuid}");
        runner.setProperty(PutSNS.MESSAGEGROUPID, "test1234");
        assertTrue(runner.setProperty("DynamicProperty", "hello!").isValid());
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "1.txt");
        ffAttributes.put("eval.subject", "test-subject");
        ffAttributes.put("myuuid", "fb0dfed8-092e-40ee-83ce-5b576cd26236");
        runner.enqueue("Test Message Content", ffAttributes);

        final PublishResponse mockPublishResponse = PublishResponse.builder().build();
        Mockito.when(mockSNSClient.publish(Mockito.any(PublishRequest.class))).thenReturn(mockPublishResponse);

        runner.run();

        ArgumentCaptor<PublishRequest> captureRequest = ArgumentCaptor.forClass(PublishRequest.class);
        Mockito.verify(mockSNSClient, Mockito.times(1)).publish(captureRequest.capture());
        PublishRequest request = captureRequest.getValue();
        assertEquals("arn:aws:sns:us-west-2:123456789012:test-topic-1.fifo", request.topicArn());
        assertEquals("Test Message Content", request.message());
        assertEquals("test-subject", request.subject());
        assertEquals("test1234", request.messageGroupId());
        assertEquals("fb0dfed8-092e-40ee-83ce-5b576cd26236", request.messageDeduplicationId());
        assertEquals("hello!", request.messageAttributes().get("DynamicProperty").stringValue());

        runner.assertAllFlowFilesTransferred(PutSNS.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutSNS.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals(CoreAttributes.FILENAME.key(), "1.txt");
    }

    @Test
    public void testPublishFailure() {
        runner.setProperty(PutSNS.ARN, "arn:aws:sns:us-west-2:123456789012:test-topic-1");
        final Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", "1.txt");
        runner.enqueue("Test Message Content", ffAttributes);
        Mockito.when(mockSNSClient.publish(Mockito.any(PublishRequest.class))).thenThrow(SnsException.builder().build());

        AuthUtils.enableAccessKey(runner, "accessKey", "secretKey");
        runner.run();

        ArgumentCaptor<PublishRequest> captureRequest = ArgumentCaptor.forClass(PublishRequest.class);
        Mockito.verify(mockSNSClient, Mockito.times(1)).publish(captureRequest.capture());
        runner.assertAllFlowFilesTransferred(PutSNS.REL_FAILURE, 1);
    }
}