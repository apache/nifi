package org.apache.nifi.processors.aws.sns;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSProcessor;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sns.AmazonSNSClient;

public abstract class AbstractSNSProcessor extends AbstractAWSProcessor<AmazonSNSClient> {

    protected static final AllowableValue ARN_TYPE_TOPIC = 
        new AllowableValue("Topic ARN", "Topic ARN", "The ARN is the name of a topic");
    protected static final AllowableValue ARN_TYPE_TARGET = 
        new AllowableValue("Target ARN", "Target ARN", "The ARN is the name of a particular Target, used to notify a specific subscriber");
    
    public static final PropertyDescriptor ARN = new PropertyDescriptor.Builder()
        .name("Amazon Resource Name (ARN)")
        .description("The name of the resource to which notifications should be published")
        .expressionLanguageSupported(true)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    
    public static final PropertyDescriptor ARN_TYPE = new PropertyDescriptor.Builder()
        .name("ARN Type")
        .description("The type of Amazon Resource Name that is being used.")
        .expressionLanguageSupported(false)
        .required(true)
        .allowableValues(ARN_TYPE_TOPIC, ARN_TYPE_TARGET)
        .defaultValue(ARN_TYPE_TOPIC.getValue())
        .build();
    
    
    
    @Override
    protected AmazonSNSClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        return new AmazonSNSClient(credentials, config);
    }

}
