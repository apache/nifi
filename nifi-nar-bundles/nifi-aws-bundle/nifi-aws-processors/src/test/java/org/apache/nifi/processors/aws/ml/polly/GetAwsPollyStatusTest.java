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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.polly.PollyClient;
import software.amazon.awssdk.services.polly.model.GetSpeechSynthesisTaskRequest;
import software.amazon.awssdk.services.polly.model.GetSpeechSynthesisTaskResponse;
import software.amazon.awssdk.services.polly.model.SynthesisTask;
import software.amazon.awssdk.services.polly.model.TaskStatus;

import java.util.Collections;

import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.AWS_TASK_OUTPUT_LOCATION;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_ORIGINAL;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_RUNNING;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.TASK_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetAwsPollyStatusTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private static final String PLACEHOLDER_CONTENT = "content";
    private TestRunner runner;
    @Mock
    private PollyClient mockPollyClient;

    private GetAwsPollyJobStatus processor;

    @Captor
    private ArgumentCaptor<GetSpeechSynthesisTaskRequest> requestCaptor;

    private TestRunner createRunner(final GetAwsPollyJobStatus processor) {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(runner, "abcd", "defg");
        return runner;
    }

    @BeforeEach
    public void setUp() throws InitializationException {
        processor = new GetAwsPollyJobStatus() {
            @Override
            public PollyClient getClient(final ProcessContext context) {
                return mockPollyClient;
            }
        };
        runner = createRunner(processor);
    }

    @Test
    public void testPollyTaskInProgress() {
        GetSpeechSynthesisTaskResponse response = GetSpeechSynthesisTaskResponse.builder()
                .synthesisTask(SynthesisTask.builder().taskId(TEST_TASK_ID).taskStatus(TaskStatus.IN_PROGRESS).build())
                .build();
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(response);
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_RUNNING);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().taskId());
    }

    @Test
    public void testPollyTaskCompleted() {
        final String uri = "https://s3.us-west2.amazonaws.com/bucket/object";
        final GetSpeechSynthesisTaskResponse response = GetSpeechSynthesisTaskResponse.builder()
                .synthesisTask(SynthesisTask.builder()
                        .taskId(TEST_TASK_ID)
                        .taskStatus(TaskStatus.COMPLETED)
                        .outputUri(uri).build())
                .build();
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(response);
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, AWS_TASK_OUTPUT_LOCATION);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().taskId());

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next();
        assertEquals(uri, flowFile.getAttribute(GetAwsPollyJobStatus.AWS_TASK_OUTPUT_LOCATION));
        assertEquals("bucket", flowFile.getAttribute("PollyS3OutputBucket"));
        assertEquals("object", flowFile.getAttribute("filename"));
    }

    @Test
    public void testPollyTaskFailed() {
        final String failureReason = "reasonOfFailure";
        final GetSpeechSynthesisTaskResponse response = GetSpeechSynthesisTaskResponse.builder()
                .synthesisTask(SynthesisTask.builder()
                        .taskId(TEST_TASK_ID)
                        .taskStatus(TaskStatus.FAILED)
                        .taskStatusReason(failureReason).build())
                .build();
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(response);
        runner.enqueue(PLACEHOLDER_CONTENT, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().taskId());

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_FAILURE).iterator().next();
        assertEquals(failureReason, flowFile.getAttribute(GetAwsPollyJobStatus.FAILURE_REASON_ATTRIBUTE));
    }
}