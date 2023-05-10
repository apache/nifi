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

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_RUNNING;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.TASK_ID;
import static org.apache.nifi.processors.aws.ml.textract.StartAwsTextractJob.TEXTRACT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.GetDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.GetDocumentAnalysisResult;
import com.amazonaws.services.textract.model.JobStatus;
import com.google.common.collect.ImmutableMap;
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
public class GetAwsTextractJobStatusTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private TestRunner runner;
    @Mock
    private AmazonTextractClient mockTextractClient;
    @Mock
    private AWSCredentialsProviderService mockAwsCredentialsProvider;
    @Captor
    private ArgumentCaptor<GetDocumentAnalysisRequest> requestCaptor;

    @BeforeEach
    public void setUp() throws InitializationException {
        when(mockAwsCredentialsProvider.getIdentifier()).thenReturn("awsCredentialProvider");
        final GetAwsTextractJobStatus awsTextractJobStatusGetter = new GetAwsTextractJobStatus() {
            @Override
            protected AmazonTextractClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
                return mockTextractClient;
            }
        };
        runner = TestRunners.newTestRunner(awsTextractJobStatusGetter);
        runner.addControllerService("awsCredentialProvider", mockAwsCredentialsProvider);
        runner.enableControllerService(mockAwsCredentialsProvider);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialProvider");
    }

    @Test
    public void testTextractDocAnalysisTaskInProgress() {
        GetDocumentAnalysisResult taskResult = new GetDocumentAnalysisResult()
                .withJobStatus(JobStatus.IN_PROGRESS);
        when(mockTextractClient.getDocumentAnalysis(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", ImmutableMap.of(TASK_ID.getName(), TEST_TASK_ID,
                TEXTRACT_TYPE.getName(), TextractType.DOCUMENT_ANALYSIS.name()));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_RUNNING);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getJobId());
    }

    @Test
    public void testTextractDocAnalysisTaskComplete() {
        GetDocumentAnalysisResult taskResult = new GetDocumentAnalysisResult()
                .withJobStatus(JobStatus.SUCCEEDED);
        when(mockTextractClient.getDocumentAnalysis(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", ImmutableMap.of(TASK_ID.getName(), TEST_TASK_ID,
                TEXTRACT_TYPE.getName(), TextractType.DOCUMENT_ANALYSIS.name()));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getJobId());
    }

    @Test
    public void testTextractDocAnalysisTaskFailed() {
        GetDocumentAnalysisResult taskResult = new GetDocumentAnalysisResult()
                .withJobStatus(JobStatus.FAILED);
        when(mockTextractClient.getDocumentAnalysis(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", ImmutableMap.of(TASK_ID.getName(), TEST_TASK_ID,
                TEXTRACT_TYPE.getName(), TextractType.DOCUMENT_ANALYSIS.type));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getJobId());
    }
}