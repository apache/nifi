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

package org.apache.nifi.processors.aws.ml.transcribe;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
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
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.Transcript;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJob;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus;

import java.util.Collections;

import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.AWS_TASK_OUTPUT_LOCATION;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.FAILURE_REASON_ATTRIBUTE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_RUNNING;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.TASK_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetAwsTranscribeJobStatusTest {
    private static final String TEST_TASK_ID = "testJobId";
    private static final String OUTPUT_LOCATION_PATH = "outputLocationPath";
    private static final String REASON_OF_FAILURE = "reasonOfFailure";
    private static final String CONTENT_STRING = "content";
    private TestRunner runner;
    @Mock
    private TranscribeClient mockTranscribeClient;

    private GetAwsTranscribeJobStatus processor;

    @Captor
    private ArgumentCaptor<GetTranscriptionJobRequest> requestCaptor;

    private TestRunner createRunner(final GetAwsTranscribeJobStatus processor) {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(runner, "abcd", "defg");
        return runner;
    }

    @BeforeEach
    public void setUp() throws InitializationException {
        processor = new GetAwsTranscribeJobStatus() {
            @Override
            public TranscribeClient getClient(final ProcessContext context) {
                return mockTranscribeClient;
            }
        };
        runner = createRunner(processor);
    }

    @Test
    public void testTranscribeJobInProgress() {
        final TranscriptionJob job = TranscriptionJob.builder()
                .transcriptionJobName(TEST_TASK_ID)
                .transcriptionJobStatus(TranscriptionJobStatus.IN_PROGRESS)
                .build();
        testTranscribeJob(job, REL_RUNNING);
    }

    @Test
    public void testTranscribeJobQueued() {
        final TranscriptionJob job = TranscriptionJob.builder()
                .transcriptionJobName(TEST_TASK_ID)
                .transcriptionJobStatus(TranscriptionJobStatus.QUEUED)
                .build();
        testTranscribeJob(job, REL_RUNNING);
    }

    @Test
    public void testTranscribeJobCompleted() {
        final TranscriptionJob job = TranscriptionJob.builder()
                .transcriptionJobName(TEST_TASK_ID)
                .transcript(Transcript.builder().transcriptFileUri(OUTPUT_LOCATION_PATH).build())
                .transcriptionJobStatus(TranscriptionJobStatus.COMPLETED)
                .build();
        testTranscribeJob(job, REL_SUCCESS);
        runner.assertAllFlowFilesContainAttribute(AWS_TASK_OUTPUT_LOCATION);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next();
        assertEquals(OUTPUT_LOCATION_PATH, flowFile.getAttribute(AWS_TASK_OUTPUT_LOCATION));
    }

    @Test
    public void testTranscribeJobFailed() {
        final TranscriptionJob job = TranscriptionJob.builder()
                .transcriptionJobName(TEST_TASK_ID)
                .failureReason(REASON_OF_FAILURE)
                .transcriptionJobStatus(TranscriptionJobStatus.FAILED)
                .build();
        testTranscribeJob(job, REL_FAILURE);
        runner.assertAllFlowFilesContainAttribute(FAILURE_REASON_ATTRIBUTE);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_FAILURE).iterator().next();
        assertEquals(REASON_OF_FAILURE, flowFile.getAttribute(FAILURE_REASON_ATTRIBUTE));
    }

    @Test
    public void testTranscribeJobUnrecognized() {
        final TranscriptionJob job = TranscriptionJob.builder()
                .transcriptionJobName(TEST_TASK_ID)
                .failureReason(REASON_OF_FAILURE)
                .transcriptionJobStatus(TranscriptionJobStatus.UNKNOWN_TO_SDK_VERSION)
                .build();
        testTranscribeJob(job, REL_FAILURE);
        runner.assertAllFlowFilesContainAttribute(FAILURE_REASON_ATTRIBUTE);
    }

    private void testTranscribeJob(final TranscriptionJob job, final Relationship expectedRelationship) {
        final GetTranscriptionJobResponse response = GetTranscriptionJobResponse.builder().transcriptionJob(job).build();
        when(mockTranscribeClient.getTranscriptionJob(requestCaptor.capture())).thenReturn(response);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(expectedRelationship);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().transcriptionJobName());
    }
}