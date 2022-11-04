package org.apache.nifi.processors.aws.ml.transcribe;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.AWS_TASK_ID_PROPERTY;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.FAILURE_REASON_ATTRIBUTE;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.REL_IN_PROGRESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.transcribe.AmazonTranscribeClient;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.Transcript;
import com.amazonaws.services.transcribe.model.TranscriptionJob;
import com.amazonaws.services.transcribe.model.TranscriptionJobStatus;
import java.util.Collections;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.polly.MockAwsCredentialsProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TranscribeFetcherTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private static final String AWS_CREDENTIAL_PROVIDER_NAME = "awsCredetialProvider";
    private static final String OUTPUT_LOCATION_PATH = "outputLocationPath";
    private static final String REASON_OF_FAILURE = "reasonOfFailure";
    private static final String CONTENT_STRING = "content";
    private TestRunner runner = null;
    private AmazonTranscribeClient mockTranscribeClient = null;
    private MockAwsCredentialsProvider mockAwsCredentialsProvider = null;

    @BeforeEach
    public void setUp() throws InitializationException {
        mockTranscribeClient = Mockito.mock(AmazonTranscribeClient.class);
        mockAwsCredentialsProvider = new MockAwsCredentialsProvider();
        mockAwsCredentialsProvider.setIdentifier(AWS_CREDENTIAL_PROVIDER_NAME);
        final GetAwsTranscribeJobStatus mockPollyFetcher = new GetAwsTranscribeJobStatus() {
            protected AmazonTranscribeClient getClient() {
                return mockTranscribeClient;
            }

            @Override
            protected AmazonTranscribeClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
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
        ArgumentCaptor<GetTranscriptionJobRequest> requestCaptor = ArgumentCaptor.forClass(GetTranscriptionJobRequest.class);
        TranscriptionJob task = new TranscriptionJob()
                .withTranscriptionJobName(TEST_TASK_ID)
                .withTranscriptionJobStatus(TranscriptionJobStatus.IN_PROGRESS);
        GetTranscriptionJobResult taskResult = new GetTranscriptionJobResult().withTranscriptionJob(task);
        when(mockTranscribeClient.getTranscriptionJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_IN_PROGRESS);
        assertEquals(requestCaptor.getValue().getTranscriptionJobName(), TEST_TASK_ID);
    }

    @Test
    public void testTranscribeTaskCompleted() {
        ArgumentCaptor<GetTranscriptionJobRequest> requestCaptor = ArgumentCaptor.forClass(GetTranscriptionJobRequest.class);
        TranscriptionJob task = new TranscriptionJob()
                .withTranscriptionJobName(TEST_TASK_ID)
                .withTranscript(new Transcript().withTranscriptFileUri(OUTPUT_LOCATION_PATH))
                .withTranscriptionJobStatus(TranscriptionJobStatus.COMPLETED);
        GetTranscriptionJobResult taskResult = new GetTranscriptionJobResult().withTranscriptionJob(task);
        when(mockTranscribeClient.getTranscriptionJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        assertEquals(requestCaptor.getValue().getTranscriptionJobName(), TEST_TASK_ID);
    }


    @Test
    public void testPollyTaskFailed() {
        ArgumentCaptor<GetTranscriptionJobRequest> requestCaptor = ArgumentCaptor.forClass(GetTranscriptionJobRequest.class);
        TranscriptionJob task = new TranscriptionJob()
                .withTranscriptionJobName(TEST_TASK_ID)
                .withFailureReason(REASON_OF_FAILURE)
                .withTranscriptionJobStatus(TranscriptionJobStatus.FAILED);
        GetTranscriptionJobResult taskResult = new GetTranscriptionJobResult().withTranscriptionJob(task);
        when(mockTranscribeClient.getTranscriptionJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        runner.assertAllFlowFilesContainAttribute(FAILURE_REASON_ATTRIBUTE);
        assertEquals(requestCaptor.getValue().getTranscriptionJobName(), TEST_TASK_ID);

    }
}