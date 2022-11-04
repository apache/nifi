package org.apache.nifi.processors.aws.ml.polly;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.polly.AmazonPollyClient;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.TaskStatus;
import com.amazonaws.services.textract.model.ThrottlingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AwsMLJobStatusGetter;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Polly"})
@CapabilityDescription("Retrieves the current status of an AWS Polly job.")
@SeeAlso({StartAwsPollyJob.class})
public class GetAwsPollyJobStatus extends AwsMLJobStatusGetter<AmazonPollyClient> {
    private static final String BUCKET = "bucket";
    private static final String KEY = "key";
    private static final Pattern S3_PATH = Pattern.compile("https://s3.*amazonaws.com/(?<" + BUCKET + ">[^/]+)/(?<" + KEY + ">.*)");
    private static final String AWS_S3_BUCKET = "PollyS3OutputBucket";
    private static final String AWS_S3_KEY = "PollyS3OutputKey";

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
        GetSpeechSynthesisTaskResult speechSynthesisTask;
        try {
            speechSynthesisTask = getSynthesisTask(flowFile);
        } catch (ThrottlingException e) {
            getLogger().info("Aws Client reached rate limit", e);
            session.transfer(flowFile, REL_THROTTLED);
            return;
        } catch (Exception e) {
            getLogger().info("Failed to get Polly Job status", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        TaskStatus taskStatus = TaskStatus.fromValue(speechSynthesisTask.getSynthesisTask().getTaskStatus());

        if (taskStatus == TaskStatus.InProgress || taskStatus == TaskStatus.Scheduled) {
            session.penalize(flowFile);
            session.transfer(flowFile, REL_IN_PROGRESS);
        }

        if (taskStatus == TaskStatus.Completed) {
            String outputUri = speechSynthesisTask.getSynthesisTask().getOutputUri();

            Matcher matcher = S3_PATH.matcher(outputUri);
            if (matcher.find()) {
                session.putAttribute(flowFile, AWS_S3_BUCKET, matcher.group(BUCKET));
                session.putAttribute(flowFile, AWS_S3_KEY, matcher.group(KEY));
            }
            FlowFile childFlowFile = session.create(flowFile);
            writeToFlowFile(session, childFlowFile, speechSynthesisTask);
            session.putAttribute(childFlowFile, AWS_TASK_OUTPUT_LOCATION, outputUri);
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(childFlowFile, REL_SUCCESS);
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
