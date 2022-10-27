package org.apache.nifi.processors.aws.ml.polly;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;

public class MockAwsCredentialsProvider extends AbstractControllerService implements AWSCredentialsProviderService {
    private String identifier;

    @Override
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider() throws ProcessException {
        return null;
    }
}
