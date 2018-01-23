package org.apache.nifi.processors.standard;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

public class LogarithmicallyPenalizeFlowFileProcessor extends AbstractProcessor {

    private static final PropertyDescriptor INITIAL_RECONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("initialReconnectTimeout")
            .displayName("InitialReconnectTimeoutMs")
            .description("First delay length after first FlowFile arrival in millis")
            .defaultValue("50")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor MAXIMAL_RECONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("maximalReconnectTimeout")
            .displayName("MaximalReconnectTimeoutMs")
            .description("Maximal delay length after first FlowFile arrival in millis")
            .defaultValue(""+10*60*1000)//10 minutes
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor MAXIMAL_NUMBER_OF_RETRIES = new PropertyDescriptor.Builder()
            .name("maximalNumberOfRetries")
            .displayName("maximalNumberOfRetries")
            .description("Maximal number of retries to allow")
            .defaultValue("10")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final PropertyDescriptor RETRY_COUNT_PROPERTY = new PropertyDescriptor.Builder()
            .name("retryCountProperty")
            .displayName("RetryCountProperty")
            .description("Name of attribute in which number of retries should be stored.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(new Validator() {
                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                    return new ValidationResult.Builder().valid(true).build();
                }
            })
            .build();

    private final Relationship REL_RETRY = new Relationship.Builder().name("retry").description("FlowFiles that were successfully processed are routed here").build();

    private final Relationship REL_MAX_RETRIES_REACHED = new Relationship.Builder().name("maxRetriesReached").description("FlowFiles that failed more than allowed times, given FlowFile will be routed here.").build();

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_RETRY, REL_MAX_RETRIES_REACHED));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(RETRY_COUNT_PROPERTY,
                INITIAL_RECONNECT_TIMEOUT,
                MAXIMAL_RECONNECT_TIMEOUT,
                MAXIMAL_NUMBER_OF_RETRIES);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String retryCountAttributeName = context.getProperty(RETRY_COUNT_PROPERTY).getValue();
        int maximalNumberOfRetries = context.getProperty(MAXIMAL_NUMBER_OF_RETRIES).asInteger();


        String retriesCountAsString = flowFile.getAttribute(retryCountAttributeName);
        int retriesCount = retriesCountAsString == null ? 1 : Integer.parseInt(retriesCountAsString) + 1;
        session.putAttribute(flowFile, retryCountAttributeName, Integer.toString(retriesCount));

        if (retriesCount > maximalNumberOfRetries) {
           session.transfer(flowFile, REL_MAX_RETRIES_REACHED);
           return;
        }

        int initialReconnectTimeout = context.getProperty(INITIAL_RECONNECT_TIMEOUT).asInteger();
        int maximalReconnectTimeout = context.getProperty(MAXIMAL_RECONNECT_TIMEOUT).asInteger();

        double delay = getBackoffDelay(initialReconnectTimeout, maximalReconnectTimeout, retriesCount);
        session.penalize(flowFile, (long)Math.ceil(delay));
        session.transfer(flowFile, REL_RETRY);
    }

    // Exponential Backoff Formula by Prof. Douglas Thain
    // http://dthain.blogspot.co.uk/2009/02/exponential-backoff-in-distributed.html
    private double getBackoffDelay(int initialReconnectTimeout, int maxReconnectTimeout, int retryAttempts) {
        double R = Math.random() + 1;
        int T = initialReconnectTimeout;
        int F = 2;
        int N = retryAttempts;
        int M = maxReconnectTimeout;

        return Math.floor(Math.min(R * T * Math.pow(F, N), M));
    };

}