package org.apache.nifi.processors.aws.ml.polly;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.polly.AmazonPollyClient;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.TaskStatus;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AwsMLFetcherProcessor;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Polly"})
@CapabilityDescription("Turn text into lifelike speech using deep learning. Checking Polly synthesis job's status.")
public class PollyFetcher extends AwsMLFetcherProcessor<AmazonPollyClient> {

    @Override
    protected AmazonPollyClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonPollyClient) AmazonPollyClientBuilder.standard().withCredentials(credentialsProvider).build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        GetSpeechSynthesisTaskResult speechSynthesisTask = getSynthesisTask(flowFile);

        TaskStatus taskStatus = TaskStatus.fromValue(speechSynthesisTask.getSynthesisTask().getTaskStatus());

        if (taskStatus == TaskStatus.InProgress || taskStatus == TaskStatus.Scheduled) {
            session.penalize(flowFile);
            session.transfer(flowFile, REL_IN_PROGRESS);
        }

        if (taskStatus == TaskStatus.Completed) {
            session.putAttribute(flowFile, AWS_TASK_OUTPUT_LOCATION,  speechSynthesisTask.getSynthesisTask().getOutputUri());
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("Amazon Polly reported that the task completed for {}", flowFile);
            return;
        }

        if (taskStatus == TaskStatus.Failed) {
            final String failureReason =  speechSynthesisTask.getSynthesisTask().getTaskStatusReason();
            session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Amazon Polly reported that the task failed for {}: {}", flowFile, failureReason);
            return;
        }
    }

    private GetSpeechSynthesisTaskResult getSynthesisTask(FlowFile flowFile) {
        String taskId = flowFile.getAttribute(AWS_TASK_ID_PROPERTY);
        GetSpeechSynthesisTaskRequest request = new GetSpeechSynthesisTaskRequest().withTaskId(taskId);
        return getClient().getSpeechSynthesisTask(request);
    }
}
