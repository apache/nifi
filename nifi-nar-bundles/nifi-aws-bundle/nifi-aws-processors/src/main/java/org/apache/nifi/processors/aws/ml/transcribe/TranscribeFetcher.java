package org.apache.nifi.processors.aws.ml.transcribe;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.transcribe.AmazonTranscribeClient;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.TranscriptionJobStatus;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AwsMLFetcherProcessor;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Transcribe"})
@CapabilityDescription("Translate text from one language to another. Check transcribe job's status.")
@SeeAlso({TranscribeProcessor.class})
public class TranscribeFetcher extends AwsMLFetcherProcessor<AmazonTranscribeClient> {
    @Override
    protected AmazonTranscribeClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTranscribeClient) AmazonTranscribeClient.builder().build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        GetTranscriptionJobResult job = getJob(flowFile);
        TranscriptionJobStatus jobStatus = TranscriptionJobStatus.fromValue(job.getTranscriptionJob().getTranscriptionJobStatus());

        if (TranscriptionJobStatus.COMPLETED == jobStatus) {
            writeToFlowFile(session, flowFile, job);
            session.putAttribute(flowFile, AWS_TASK_OUTPUT_LOCATION, job.getTranscriptionJob().getTranscript().getTranscriptFileUri());
            session.transfer(flowFile, REL_SUCCESS);
        }

        if (TranscriptionJobStatus.IN_PROGRESS == jobStatus) {
            session.transfer(flowFile, REL_IN_PROGRESS);
        }

        if (TranscriptionJobStatus.FAILED == jobStatus) {
            final String failureReason = job.getTranscriptionJob().getFailureReason();
            session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Amazon transcribe reported that the task failed for {}: {}", flowFile, failureReason);
            return;
        }
    }

    private GetTranscriptionJobResult getJob(FlowFile flowFile) {
        String taskId = flowFile.getAttribute(AWS_TASK_ID_PROPERTY);
        GetTranscriptionJobRequest request = new GetTranscriptionJobRequest().withTranscriptionJobName(taskId);
        return getClient().getTranscriptionJob(request);
    }
}
