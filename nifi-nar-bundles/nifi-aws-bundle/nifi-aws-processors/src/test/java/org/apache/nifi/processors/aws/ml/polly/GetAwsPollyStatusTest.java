/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.aws.ml.polly;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.AWS_TASK_OUTPUT_LOCATION;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_RUNNING;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_ORIGINAL;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.TASK_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.polly.AmazonPollyClient;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.SynthesisTask;
import com.amazonaws.services.polly.model.TaskStatus;
import java.util.Collections;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class GetAwsPollyStatusTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private static final String PLACEHOLDER_CONTENT = "content";
    private TestRunner runner;
    @Mock
    private AmazonPollyClient mockPollyClient;
    @Mock
    private AWSCredentialsProviderService mockAwsCredentialsProvider;
    @Captor
    private ArgumentCaptor<GetSpeechSynthesisTaskRequest> requestCaptor;

    @BeforeEach
    public void setUp() throws InitializationException {
        when(mockAwsCredentialsProvider.getIdentifier()).thenReturn("awsCredentialProvider");

        final GetAwsPollyJobStatus mockGetAwsPollyStatus = new GetAwsPollyJobStatus() {
            @Override
            protected AmazonPollyClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
                return mockPollyClient;
            }
        };
        runner = TestRunners.newTestRunner(mockGetAwsPollyStatus);
        runner.addControllerService("awsCredentialProvider", mockAwsCredentialsProvider);
        runner.enableControllerService(mockAwsCredentialsProvider);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialProvider");
    }

    @Test
    public void testPollyTaskInProgress() {
        GetSpeechSynthesisTaskResult taskResult = new GetSpeechSynthesisTaskResult();
        SynthesisTask task = new SynthesisTask().withTaskId(TEST_TASK_ID)
                .withTaskStatus(TaskStatus.InProgress);
        taskResult.setSynthesisTask(task);
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_RUNNING);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getTaskId());
    }

    @Test
    public void testPollyTaskCompleted() {
        GetSpeechSynthesisTaskResult taskResult = new GetSpeechSynthesisTaskResult();
        SynthesisTask task = new SynthesisTask().withTaskId(TEST_TASK_ID)
                .withTaskStatus(TaskStatus.Completed)
                .withOutputUri("outputLocationPath");
        taskResult.setSynthesisTask(task);
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, AWS_TASK_OUTPUT_LOCATION);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getTaskId());
    }


    @Test
    public void testPollyTaskFailed() {
        GetSpeechSynthesisTaskResult taskResult = new GetSpeechSynthesisTaskResult();
        SynthesisTask task = new SynthesisTask().withTaskId(TEST_TASK_ID)
                .withTaskStatus(TaskStatus.Failed)
                .withTaskStatusReason("reasonOfFailure");
        taskResult.setSynthesisTask(task);
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getTaskId());
    }
}