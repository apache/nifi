package org.apache.nifi.processors.aws.ml.polly;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.polly.AmazonPollyClient;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskResult;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.AwsMlProcessor;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Polly"})
@CapabilityDescription("Turn text into lifelike speech using deep learning.")
@SeeAlso({PollyFetcher.class})
public class PollyProcessor extends AwsMlProcessor<AmazonPollyClient, StartSpeechSynthesisTaskRequest, StartSpeechSynthesisTaskResult> {
    @Override
    protected AmazonPollyClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonPollyClient) AmazonPollyClientBuilder.standard().withCredentials(credentialsProvider).build();
    }

    @Override
    protected StartSpeechSynthesisTaskResult sendRequest(StartSpeechSynthesisTaskRequest request, ProcessContext context) {
        return getClient().startSpeechSynthesisTask(request);
    }

    @Override
    protected Class<? extends StartSpeechSynthesisTaskRequest> getAwsRequestClass(ProcessContext context) {
        return StartSpeechSynthesisTaskRequest.class;
    }

    @Override
    protected String getAwsTaskId(ProcessContext context, StartSpeechSynthesisTaskResult startSpeechSynthesisTaskResult) {
        return startSpeechSynthesisTaskResult.getSynthesisTask().getTaskId();
    }
}
