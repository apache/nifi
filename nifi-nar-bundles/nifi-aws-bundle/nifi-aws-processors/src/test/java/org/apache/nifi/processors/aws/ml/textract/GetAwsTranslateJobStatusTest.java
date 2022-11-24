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
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.AWS_TASK_ID_PROPERTY;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_RUNNING;
import static org.apache.nifi.processors.aws.ml.textract.GetAwsTextractJobStatus.DOCUMENT_ANALYSIS;
import static org.apache.nifi.processors.aws.ml.textract.StartAwsTextractJob.TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
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
public class GetAwsTranslateJobStatusTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private TestRunner runner = null;
    @Mock
    private AmazonTextractClient mockTextractClient = null;
    @Mock
    private AWSCredentialsProviderService mockAwsCredentialsProvider = null;
    @Captor
    private ArgumentCaptor<GetDocumentAnalysisRequest> requestCaptor;

    @BeforeEach
    public void setUp() throws InitializationException {
        when(mockAwsCredentialsProvider.getIdentifier()).thenReturn("awsCredetialProvider");
        final GetAwsTextractJobStatus awsTextractJobStatusGetter = new GetAwsTextractJobStatus() {
            protected AmazonTextractClient getClient() {
                return mockTextractClient;
            }

            @Override
            protected AmazonTextractClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
                return mockTextractClient;
            }
        };
        runner = TestRunners.newTestRunner(awsTextractJobStatusGetter);
        runner.addControllerService("awsCredetialProvider", mockAwsCredentialsProvider);
        runner.enableControllerService(mockAwsCredentialsProvider);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredetialProvider");
    }

    @Test
    public void testTextractDocAnalysisTaskInProgress() {
        GetDocumentAnalysisResult taskResult = new GetDocumentAnalysisResult()
                .withJobStatus(JobStatus.IN_PROGRESS);
        when(mockTextractClient.getDocumentAnalysis(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", ImmutableMap.of(AWS_TASK_ID_PROPERTY, TEST_TASK_ID,
                TYPE.getName(), DOCUMENT_ANALYSIS));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_RUNNING);
        assertEquals(requestCaptor.getValue().getJobId(), TEST_TASK_ID);
    }

    @Test
    public void testTextractDocAnalysisTaskComplete() {
        GetDocumentAnalysisResult taskResult = new GetDocumentAnalysisResult()
                .withJobStatus(JobStatus.SUCCEEDED);
        when(mockTextractClient.getDocumentAnalysis(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", ImmutableMap.of(AWS_TASK_ID_PROPERTY, TEST_TASK_ID,
                TYPE.getName(), DOCUMENT_ANALYSIS));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertEquals(requestCaptor.getValue().getJobId(), TEST_TASK_ID);
    }

    @Test
    public void testTextractDocAnalysisTaskFailed() {
        GetDocumentAnalysisResult taskResult = new GetDocumentAnalysisResult()
                .withJobStatus(JobStatus.FAILED);
        when(mockTextractClient.getDocumentAnalysis(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", ImmutableMap.of(AWS_TASK_ID_PROPERTY, TEST_TASK_ID,
                TYPE.getName(), DOCUMENT_ANALYSIS));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertEquals(requestCaptor.getValue().getJobId(), TEST_TASK_ID);
    }
}