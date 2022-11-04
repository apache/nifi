package org.apache.nifi.processors.aws.ml.polly;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.AWS_TASK_ID_PROPERTY;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.AWS_TASK_OUTPUT_LOCATION;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.REL_FAILURE;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.REL_IN_PROGRESS;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.REL_ORIGINAL;
import static org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.polly.AmazonPollyClient;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.SynthesisTask;
import com.amazonaws.services.polly.model.TaskStatus;
import java.util.Collections;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class GetAwsPollyStatusTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private TestRunner runner = null;
    private AmazonPollyClient mockPollyClient = null;
    private MockAwsCredentialsProvider mockAwsCredentialsProvider = null;

    @BeforeEach
    public void setUp() throws InitializationException {
        mockPollyClient = Mockito.mock(AmazonPollyClient.class);
        mockAwsCredentialsProvider = new MockAwsCredentialsProvider();
        mockAwsCredentialsProvider.setIdentifier("awsCredetialProvider");
        final GetAwsPollyJobStatus mockGetAwsPollyStatus = new GetAwsPollyJobStatus() {
            protected AmazonPollyClient getClient() {
                return mockPollyClient;
            }

            @Override
            protected AmazonPollyClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
                return mockPollyClient;
            }
        };
        runner = TestRunners.newTestRunner(mockGetAwsPollyStatus);
        runner.addControllerService("awsCredetialProvider", mockAwsCredentialsProvider);
        runner.enableControllerService(mockAwsCredentialsProvider);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredetialProvider");
    }

    @Test
    public void testPollyTaskInProgress() {
        ArgumentCaptor<GetSpeechSynthesisTaskRequest> requestCaptor = ArgumentCaptor.forClass(GetSpeechSynthesisTaskRequest.class);
        GetSpeechSynthesisTaskResult taskResult = new GetSpeechSynthesisTaskResult();
        SynthesisTask task = new SynthesisTask().withTaskId(TEST_TASK_ID)
                .withTaskStatus(TaskStatus.InProgress);
        taskResult.setSynthesisTask(task);
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_IN_PROGRESS);
        assertEquals(requestCaptor.getValue().getTaskId(), TEST_TASK_ID);
    }

    @Test
    public void testPollyTaskCompleted() {
        ArgumentCaptor<GetSpeechSynthesisTaskRequest> requestCaptor = ArgumentCaptor.forClass(GetSpeechSynthesisTaskRequest.class);
        GetSpeechSynthesisTaskResult taskResult = new GetSpeechSynthesisTaskResult();
        SynthesisTask task = new SynthesisTask().withTaskId(TEST_TASK_ID)
                .withTaskStatus(TaskStatus.Completed)
                .withOutputUri("outputLocationPath");
        taskResult.setSynthesisTask(task);
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_ORIGINAL, 1);
        runner.assertAllFlowFilesContainAttribute(REL_SUCCESS, AWS_TASK_OUTPUT_LOCATION);
        assertEquals(requestCaptor.getValue().getTaskId(), TEST_TASK_ID);
    }


    @Test
    public void testPollyTaskFailed() {
        ArgumentCaptor<GetSpeechSynthesisTaskRequest> requestCaptor = ArgumentCaptor.forClass(GetSpeechSynthesisTaskRequest.class);
        GetSpeechSynthesisTaskResult taskResult = new GetSpeechSynthesisTaskResult();
        SynthesisTask task = new SynthesisTask().withTaskId(TEST_TASK_ID)
                .withTaskStatus(TaskStatus.Failed)
                .withTaskStatusReason("reasonOfFailure");
        taskResult.setSynthesisTask(task);
        when(mockPollyClient.getSpeechSynthesisTask(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue("content", Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertEquals(requestCaptor.getValue().getTaskId(), TEST_TASK_ID);
    }
}