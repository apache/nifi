package org.apache.nifi.processors.aws.ml.textract;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AwsMLFetcherProcessor.AWS_TASK_ID_PROPERTY;
import static org.apache.nifi.processors.aws.ml.AwsMLFetcherProcessor.REL_IN_PROGRESS;
import static org.apache.nifi.processors.aws.ml.textract.TextractFetcher.DOCUMENT_ANALYSIS;
import static org.apache.nifi.processors.aws.ml.textract.TextractProcessor.TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.GetDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.GetDocumentAnalysisResult;
import com.amazonaws.services.textract.model.JobStatus;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.polly.MockAwsCredentialsProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TextractFetcherTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private TestRunner runner = null;
    private AmazonTextractClient mockTextractClient = null;
    private MockAwsCredentialsProvider mockAwsCredentialsProvider = null;

    @BeforeEach
    public void setUp() throws InitializationException {
        mockTextractClient = Mockito.mock(AmazonTextractClient.class);
        mockAwsCredentialsProvider = new MockAwsCredentialsProvider();
        mockAwsCredentialsProvider.setIdentifier("awsCredetialProvider");
        final TextractFetcher mockPollyFetcher = new TextractFetcher() {
            protected AmazonTextractClient getClient() {
                return mockTextractClient;
            }

            @Override
            protected AmazonTextractClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
                return mockTextractClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPollyFetcher);
        runner.addControllerService("awsCredetialProvider", mockAwsCredentialsProvider);
        runner.enableControllerService(mockAwsCredentialsProvider);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredetialProvider");
    }

    @Test
    public void testTextractDocAnalysisTaskInProgress() {
        ArgumentCaptor<GetDocumentAnalysisRequest> requestCaptor = ArgumentCaptor.forClass(GetDocumentAnalysisRequest.class);
        GetDocumentAnalysisResult taskResult = new GetDocumentAnalysisResult()
                .withJobStatus(JobStatus.IN_PROGRESS);
        when(mockTextractClient.getDocumentAnalysis(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", ImmutableMap.of(AWS_TASK_ID_PROPERTY, TEST_TASK_ID,
                TYPE.getName(), DOCUMENT_ANALYSIS));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_IN_PROGRESS);
        assertEquals(requestCaptor.getValue().getJobId(), TEST_TASK_ID);
    }

    @Test
    public void testTextractDocAnalysisTaskComplete() {
        ArgumentCaptor<GetDocumentAnalysisRequest> requestCaptor = ArgumentCaptor.forClass(GetDocumentAnalysisRequest.class);
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
        ArgumentCaptor<GetDocumentAnalysisRequest> requestCaptor = ArgumentCaptor.forClass(GetDocumentAnalysisRequest.class);
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