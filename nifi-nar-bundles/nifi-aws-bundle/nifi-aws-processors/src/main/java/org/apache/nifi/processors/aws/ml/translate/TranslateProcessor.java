package org.apache.nifi.processors.aws.ml.translate;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.translate.AmazonTranslateClient;
import com.amazonaws.services.translate.model.StartTextTranslationJobRequest;
import com.amazonaws.services.translate.model.StartTextTranslationJobResult;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.AwsMlProcessor;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Translate"})
@CapabilityDescription("Translate text from one language to another.")
@SeeAlso({TranslateFetcher.class})
public class TranslateProcessor extends AwsMlProcessor<AmazonTranslateClient, StartTextTranslationJobRequest, StartTextTranslationJobResult> {
    @Override
    protected AmazonTranslateClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTranslateClient) AmazonTranslateClient.builder().build();
    }

    @Override
    protected StartTextTranslationJobResult sendRequest(StartTextTranslationJobRequest request, ProcessContext context) {
        return getClient().startTextTranslationJob(request);
    }

    @Override
    protected Class<StartTextTranslationJobRequest> getAwsRequestClass(ProcessContext context) {
        return StartTextTranslationJobRequest.class;
    }

    protected String getAwsTaskId(ProcessContext context, StartTextTranslationJobResult startTextTranslationJobResult) {
        return startTextTranslationJobResult.getJobId();
    }
}
