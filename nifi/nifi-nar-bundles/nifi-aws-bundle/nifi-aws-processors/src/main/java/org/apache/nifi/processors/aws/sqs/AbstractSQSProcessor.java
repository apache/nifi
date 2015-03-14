package org.apache.nifi.processors.aws.sqs;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSProcessor;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;

public abstract class AbstractSQSProcessor extends AbstractAWSProcessor<AmazonSQSClient> {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The maximum number of messages to send in a single network request")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("25")
        .build();

    public static final PropertyDescriptor QUEUE_URL = new PropertyDescriptor.Builder()
        .name("Queue URL")
        .description("The URL of the queue to act upon")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(true)
        .build();

    @Override
    protected AmazonSQSClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        return new AmazonSQSClient(credentials, config);
    }

}
