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

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.FAILURE_REASON_ATTRIBUTE;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_RUNNING;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.TASK_ID;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor.REL_FAILURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.transcribe.AmazonTranscribeClient;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.Transcript;
import com.amazonaws.services.transcribe.model.TranscriptionJob;
import com.amazonaws.services.transcribe.model.TranscriptionJobStatus;
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
public class GetAwsTranscribeJobStatusTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private static final String AWS_CREDENTIAL_PROVIDER_NAME = "awsCredetialProvider";
    private static final String OUTPUT_LOCATION_PATH = "outputLocationPath";
    private static final String REASON_OF_FAILURE = "reasonOfFailure";
    private static final String CONTENT_STRING = "content";
    private TestRunner runner;
    @Mock
    private AmazonTranscribeClient mockTranscribeClient;
    @Mock
    private AWSCredentialsProviderService mockAwsCredentialsProvider;
    @Captor
    private ArgumentCaptor<GetTranscriptionJobRequest> requestCaptor;

    @BeforeEach
    public void setUp() throws InitializationException {
        when(mockAwsCredentialsProvider.getIdentifier()).thenReturn(AWS_CREDENTIAL_PROVIDER_NAME);
        final GetAwsTranscribeJobStatus mockPollyFetcher = new GetAwsTranscribeJobStatus() {
            @Override
            protected AmazonTranscribeClient createClient(ProcessContext context, AWSCredentialsProvider credentials, ClientConfiguration config) {
                return mockTranscribeClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPollyFetcher);
        runner.addControllerService(AWS_CREDENTIAL_PROVIDER_NAME, mockAwsCredentialsProvider);
        runner.enableControllerService(mockAwsCredentialsProvider);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, AWS_CREDENTIAL_PROVIDER_NAME);
    }

    @Test
    public void testTranscribeTaskInProgress() {
        TranscriptionJob task = new TranscriptionJob()
                .withTranscriptionJobName(TEST_TASK_ID)
                .withTranscriptionJobStatus(TranscriptionJobStatus.IN_PROGRESS);
        GetTranscriptionJobResult taskResult = new GetTranscriptionJobResult().withTranscriptionJob(task);
        when(mockTranscribeClient.getTranscriptionJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_RUNNING);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getTranscriptionJobName());
    }

    @Test
    public void testTranscribeTaskCompleted() {
        TranscriptionJob task = new TranscriptionJob()
                .withTranscriptionJobName(TEST_TASK_ID)
                .withTranscript(new Transcript().withTranscriptFileUri(OUTPUT_LOCATION_PATH))
                .withTranscriptionJobStatus(TranscriptionJobStatus.COMPLETED);
        GetTranscriptionJobResult taskResult = new GetTranscriptionJobResult().withTranscriptionJob(task);
        when(mockTranscribeClient.getTranscriptionJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getTranscriptionJobName());
    }


    @Test
    public void testPollyTaskFailed() {
        TranscriptionJob task = new TranscriptionJob()
                .withTranscriptionJobName(TEST_TASK_ID)
                .withFailureReason(REASON_OF_FAILURE)
                .withTranscriptionJobStatus(TranscriptionJobStatus.FAILED);
        GetTranscriptionJobResult taskResult = new GetTranscriptionJobResult().withTranscriptionJob(task);
        when(mockTranscribeClient.getTranscriptionJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(TASK_ID.getName(), TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        runner.assertAllFlowFilesContainAttribute(FAILURE_REASON_ATTRIBUTE);
        assertEquals(TEST_TASK_ID, requestCaptor.getValue().getTranscriptionJobName());

    }
}