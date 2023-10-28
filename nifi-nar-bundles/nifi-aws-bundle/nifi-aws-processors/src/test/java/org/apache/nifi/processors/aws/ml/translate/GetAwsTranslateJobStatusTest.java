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

package org.apache.nifi.processors.aws.ml.translate;

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
import software.amazon.awssdk.services.translate.TranslateClient;
import software.amazon.awssdk.services.translate.model.DescribeTextTranslationJobRequest;
import software.amazon.awssdk.services.translate.model.DescribeTextTranslationJobResponse;
import software.amazon.awssdk.services.translate.model.JobStatus;
import software.amazon.awssdk.services.translate.model.OutputDataConfig;
import software.amazon.awssdk.services.translate.model.TextTranslationJobProperties;

import java.time.Instant;
import java.util.Collections;

import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.AWS_TASK_OUTPUT_LOCATION;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_RUNNING;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.TASK_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetAwsTranslateJobStatusTest {
    private static final String TEST_TASK_ID = "testJobId";
    private static final String OUTPUT_LOCATION_PATH = "outputLocationPath";
    private static final String REASON_OF_FAILURE = "reasonOfFailure";
    private static final String CONTENT_STRING = "content";
    private TestRunner runner;
    @Mock
    private TranslateClient mockTranslateClient;

    private GetAwsTranslateJobStatus processor;

    @Captor
    private ArgumentCaptor<DescribeTextTranslationJobRequest> requestCaptor;

    private TestRunner createRunner(final GetAwsTranslateJobStatus processor) {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(runner, "abcd", "defg");
        return runner;
    }

    @BeforeEach
    public void setUp() throws InitializationException {
        processor = new GetAwsTranslateJobStatus() {
            @Override
            public TranslateClient getClient(final ProcessContext context) {
                return mockTranslateClient;
            }
        };
        runner = createRunner(processor);
    }

    @Test
    public void testTranslateJobInProgress() {
        final TextTranslationJobProperties job = TextTranslationJobProperties.builder()
                .jobId(TEST_TASK_ID)
                .jobStatus(JobStatus.IN_PROGRESS)
                .build();
        testTranslateJob(job, REL_RUNNING);
    }

    @Test
    public void testTranslateSubmitted() {
        final TextTranslationJobProperties job = TextTranslationJobProperties.builder()
                .jobId(TEST_TASK_ID)
                .jobStatus(JobStatus.SUBMITTED)
                .build();
        testTranslateJob(job, REL_RUNNING);
    }

    @Test
    public void testTranslateStopRequested() {
        final TextTranslationJobProperties job = TextTranslationJobProperties.builder()
                .jobId(TEST_TASK_ID)
                .jobStatus(JobStatus.STOP_REQUESTED)
                .build();
        testTranslateJob(job, REL_RUNNING);
    }

    @Test
    public void testTranslateJobCompleted() {
        final TextTranslationJobProperties job = TextTranslationJobProperties.builder()
                .jobStatus(TEST_TASK_ID)
                .outputDataConfig(OutputDataConfig.builder().s3Uri(OUTPUT_LOCATION_PATH).build())
                .submittedTime(Instant.now())
                .jobStatus(JobStatus.COMPLETED)
                .build();
        testTranslateJob(job, REL_SUCCESS);
        runner.assertAllFlowFilesContainAttribute(AWS_TASK_OUTPUT_LOCATION);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).iterator().next();
        assertEquals(OUTPUT_LOCATION_PATH, flowFile.getAttribute(AWS_TASK_OUTPUT_LOCATION));
    }

    @Test
    public void testTranslateJobFailed() {
        final TextTranslationJobProperties job = TextTranslationJobProperties.builder()
                .jobStatus(TEST_TASK_ID)
                .jobStatus(JobStatus.FAILED)
                .build();
        testTranslateJob(job, REL_FAILURE);
    }

    @Test
    public void testTranslateJobStopped() {
        final TextTranslationJobProperties job = TextTranslationJobProperties.builder()
                .jobStatus(TEST_TASK_ID)
                .jobStatus(JobStatus.STOPPED)
                .build();
        testTranslateJob(job, REL_FAILURE);
    }

    @Test
    public void testTranslateJobUnrecognized() {
        final TextTranslationJobProperties job = TextTranslationJobProperties.builder()
                .jobStatus(TEST_TASK_ID)
                .jobStatus(JobStatus.UNKNOWN_TO_SDK_VERSION)
                .build();
        testTranslateJob(job, REL_FAILURE);
    }

    private void testTranslateJob(final TextTranslationJobProperties job, final Relationship expectedRelationship) {
        final DescribeTextTranslationJobResponse response = DescribeTextTranslationJobResponse.builder().textTranslationJobProperties(job).build();
        when(mockTranslateClient.describeTextTranslationJob(requestCaptor.capture())).thenReturn(response);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(expectedRelationship);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().jobId());
    }

}