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

package org.apache.nifi.processors.aws.ml.textract;

import com.google.common.collect.ImmutableMap;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.aws.testutil.AuthUtils;
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
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.GetDocumentAnalysisRequest;
import software.amazon.awssdk.services.textract.model.GetDocumentAnalysisResponse;
import software.amazon.awssdk.services.textract.model.GetDocumentTextDetectionRequest;
import software.amazon.awssdk.services.textract.model.GetDocumentTextDetectionResponse;
import software.amazon.awssdk.services.textract.model.GetExpenseAnalysisRequest;
import software.amazon.awssdk.services.textract.model.GetExpenseAnalysisResponse;
import software.amazon.awssdk.services.textract.model.JobStatus;

import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_RUNNING;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.REL_THROTTLED;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.TASK_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetAwsTextractJobStatusTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private TestRunner runner;
    @Mock
    private TextractClient mockTextractClient;

    private GetAwsTextractJobStatus processor;

    @Captor
    private ArgumentCaptor<GetDocumentAnalysisRequest> documentAnalysisCaptor;
    @Captor
    private ArgumentCaptor<GetExpenseAnalysisRequest> expenseAnalysisRequestCaptor;
    @Captor
    private ArgumentCaptor<GetDocumentTextDetectionRequest> documentTextDetectionCaptor;

    private TestRunner createRunner(final GetAwsTextractJobStatus processor) {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        AuthUtils.enableAccessKey(runner, "abcd", "defg");
        return runner;
    }

    @BeforeEach
    public void setUp() throws InitializationException {
        processor = new GetAwsTextractJobStatus() {
            @Override
            public TextractClient getClient(final ProcessContext context) {
                return mockTextractClient;
            }
        };
        runner = createRunner(processor);
    }

    @Test
    public void testTextractDocAnalysisTaskInProgress() {
        testTextractDocAnalysis(JobStatus.IN_PROGRESS, REL_RUNNING);
    }

    @Test
    public void testTextractDocAnalysisTaskComplete() {
        testTextractDocAnalysis(JobStatus.SUCCEEDED, REL_SUCCESS);
    }

    @Test
    public void testTextractDocAnalysisTaskFailed() {
        testTextractDocAnalysis(JobStatus.FAILED, REL_FAILURE);
    }

    @Test
    public void testTextractDocAnalysisTaskPartialSuccess() {
        testTextractDocAnalysis(JobStatus.PARTIAL_SUCCESS, REL_THROTTLED);
    }

    @Test
    public void testTextractDocAnalysisTaskUnkownStatus() {
        testTextractDocAnalysis(JobStatus.UNKNOWN_TO_SDK_VERSION, REL_FAILURE);
    }

    private void testTextractDocAnalysis(final JobStatus jobStatus, final Relationship expectedRelationship) {
        final GetDocumentAnalysisResponse response = GetDocumentAnalysisResponse.builder()
                .jobStatus(jobStatus).build();
        when(mockTextractClient.getDocumentAnalysis(documentAnalysisCaptor.capture())).thenReturn(response);
        runner.enqueue("content", ImmutableMap.of(
                TASK_ID.getName(), TEST_TASK_ID,
                StartAwsTextractJob.TEXTRACT_TYPE_ATTRIBUTE, TextractType.DOCUMENT_ANALYSIS.getType()));
        runner.run();

        runner.assertAllFlowFilesTransferred(expectedRelationship);
        assertEquals(TEST_TASK_ID, documentAnalysisCaptor.getValue().jobId());
    }

    @Test
    public void testTextractExpenseAnalysisTaskInProgress() {
        testTextractExpenseAnalysis(JobStatus.IN_PROGRESS, REL_RUNNING);
    }

    @Test
    public void testTextractExpenseAnalysisTaskComplete() {
        testTextractExpenseAnalysis(JobStatus.SUCCEEDED, REL_SUCCESS);
    }

    @Test
    public void testTextractExpenseAnalysisTaskFailed() {
        testTextractExpenseAnalysis(JobStatus.FAILED, REL_FAILURE);
    }

    @Test
    public void testTextractExpenseAnalysisTaskPartialSuccess() {
        testTextractExpenseAnalysis(JobStatus.PARTIAL_SUCCESS, REL_THROTTLED);
    }

    @Test
    public void testTextractExpenseAnalysisTaskUnkownStatus() {
        testTextractExpenseAnalysis(JobStatus.UNKNOWN_TO_SDK_VERSION, REL_FAILURE);
    }

    private void testTextractExpenseAnalysis(final JobStatus jobStatus, final Relationship expectedRelationship) {
        runner.setProperty(GetAwsTextractJobStatus.TEXTRACT_TYPE, TextractType.EXPENSE_ANALYSIS.getType());
        final GetExpenseAnalysisResponse response = GetExpenseAnalysisResponse.builder()
                .jobStatus(jobStatus).build();
        when(mockTextractClient.getExpenseAnalysis(expenseAnalysisRequestCaptor.capture())).thenReturn(response);
        runner.enqueue("content", ImmutableMap.of(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(expectedRelationship);
        assertEquals(TEST_TASK_ID, expenseAnalysisRequestCaptor.getValue().jobId());
    }

    @Test
    public void testTextractDocumentTextDetectionTaskInProgress() {
        testTextractDocumentTextDetection(JobStatus.IN_PROGRESS, REL_RUNNING);
    }

    @Test
    public void testTextractDocumentTextDetectionTaskComplete() {
        testTextractDocumentTextDetection(JobStatus.SUCCEEDED, REL_SUCCESS);
    }

    @Test
    public void testTextractDocumentTextDetectionTaskFailed() {
        testTextractDocumentTextDetection(JobStatus.FAILED, REL_FAILURE);
    }

    @Test
    public void testTextractDocumentTextDetectionTaskPartialSuccess() {
        testTextractDocumentTextDetection(JobStatus.PARTIAL_SUCCESS, REL_THROTTLED);
    }

    @Test
    public void testTextractDocumentTextDetectionTaskUnkownStatus() {
        testTextractDocumentTextDetection(JobStatus.UNKNOWN_TO_SDK_VERSION, REL_FAILURE);
    }

    private void testTextractDocumentTextDetection(final JobStatus jobStatus, final Relationship expectedRelationship) {
        runner.setProperty(GetAwsTextractJobStatus.TEXTRACT_TYPE, TextractType.DOCUMENT_TEXT_DETECTION.getType());

        final GetDocumentTextDetectionResponse response = GetDocumentTextDetectionResponse.builder()
                .jobStatus(jobStatus).build();
        when(mockTextractClient.getDocumentTextDetection(documentTextDetectionCaptor.capture())).thenReturn(response);
        runner.enqueue("content", ImmutableMap.of(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(expectedRelationship);
        assertEquals(TEST_TASK_ID, documentTextDetectionCaptor.getValue().jobId());
    }
}