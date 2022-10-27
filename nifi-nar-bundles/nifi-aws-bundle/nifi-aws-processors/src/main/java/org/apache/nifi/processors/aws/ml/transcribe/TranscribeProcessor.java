package org.apache.nifi.processors.aws.ml.transcribe;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.transcribe.AmazonTranscribeClient;
import com.amazonaws.services.transcribe.model.StartTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.StartTranscriptionJobResult;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.AwsMlProcessor;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Transcribe"})
@CapabilityDescription("Automatically convert speech to text")
@SeeAlso({TranscribeFetcher.class})
public class TranscribeProcessor extends AwsMlProcessor<AmazonTranscribeClient, StartTranscriptionJobRequest, StartTranscriptionJobResult> {
    @Override
    protected AmazonTranscribeClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTranscribeClient) AmazonTranscribeClient.builder().build();
    }

    @Override
    protected AmazonTranscribeClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
        return (AmazonTranscribeClient) AmazonTranscribeClient.builder().build();
    }

    @Override
    protected StartTranscriptionJobResult sendRequest(StartTranscriptionJobRequest request, ProcessContext context) {
        return getClient().startTranscriptionJob(request);
    }

    @Override
    protected Class<? extends StartTranscriptionJobRequest> getAwsRequestClass(ProcessContext context) {
        return StartTranscriptionJobRequest.class;
    }

    @Override
    protected String getAwsTaskId(ProcessContext context, StartTranscriptionJobResult startTranscriptionJobResult) {
        return startTranscriptionJobResult.getTranscriptionJob().getTranscriptionJobName();
    }
}
