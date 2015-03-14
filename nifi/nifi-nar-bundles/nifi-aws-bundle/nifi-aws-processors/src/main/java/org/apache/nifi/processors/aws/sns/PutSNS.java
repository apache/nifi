package org.apache.nifi.processors.aws.sns;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;


// TODO: Allow user to choose 'content strategy'
//       1. Content from message body or attributes? If attributes, allow new property for configuring EL expression. Otherwise, no property.
//       2. Same content to all subscribers or different content per subscription type? 
//              If same, just use single property.
//              If different, must use Attribute values for each and have a separate property for each type of subscription (HTTP, HTTPS, E-mail, SMS, etc.)
@Tags({"amazon", "aws", "sns", "topic", "put", "publish", "pubsub"})
@CapabilityDescription("Sends the content of a FlowFile as a notification to the Amazon Simple Notification Service")
public class PutSNS extends AbstractSNSProcessor {

    public static final PropertyDescriptor CHARACTER_ENCODING = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The character set in which the FlowFile's content is encoded")
        .defaultValue("UTF-8")
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .required(true)
        .build();
    public static final PropertyDescriptor USE_JSON_STRUCTURE = new PropertyDescriptor.Builder()
        .name("Use JSON Structure")
        .description("If true, the contents of the FlowFile must be JSON with a top-level element named 'default'. Additional elements can be used to send different messages to different protocols. See the Amazon SNS Documentation for more information.")
        .defaultValue("false")
        .allowableValues("true", "false")
        .required(true)
        .build();
    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
        .name("E-mail Subject")
        .description("The optional subject to use for any subscribers that are subscribed via E-mail")
        .expressionLanguageSupported(true)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
        
    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(ARN, ARN_TYPE, SUBJECT, REGION, ACCESS_KEY, SECRET_KEY, CREDENTAILS_FILE, TIMEOUT, 
                    USE_JSON_STRUCTURE, CHARACTER_ENCODING) );

    public static final int MAX_SIZE = 256 * 1024;
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(false)
            .dynamic(true)
            .build();
    }
    
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        if ( flowFile.getSize() > MAX_SIZE ) {
            getLogger().error("Cannot publish {} to SNS because its size exceeds Amazon SNS's limit of 256KB; routing to failure", new Object[] {flowFile});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_ENCODING).evaluateAttributeExpressions(flowFile).getValue());
        
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        final String message = new String(baos.toByteArray(), charset);
        
        final AmazonSNSClient client = getClient();
        final PublishRequest request = new PublishRequest();
        request.setMessage(message);
        
        if ( context.getProperty(USE_JSON_STRUCTURE).asBoolean() ) {
            request.setMessageStructure("json");
        }
        
        final String arn = context.getProperty(ARN).evaluateAttributeExpressions(flowFile).getValue();
        final String arnType = context.getProperty(ARN_TYPE).getValue();
        if ( arnType.equalsIgnoreCase(ARN_TYPE_TOPIC.getValue()) ) {
            request.setTopicArn(arn);
        } else {
            request.setTargetArn(arn);
        }
        
        final String subject = context.getProperty(SUBJECT).evaluateAttributeExpressions(flowFile).getValue();
        if ( subject != null ) {
            request.setSubject(subject);
        }

        for ( final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet() ) {
            if ( entry.getKey().isDynamic() && !isEmpty(entry.getValue()) ) {
                final MessageAttributeValue value = new MessageAttributeValue();
                value.setStringValue(context.getProperty(entry.getKey()).evaluateAttributeExpressions(flowFile).getValue());
                value.setDataType("String");
                request.addMessageAttributesEntry(entry.getKey().getName(), value);
            }
        }
        
        try {
            client.publish(request);
            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, arn);
            getLogger().info("Successfully published notification for {}", new Object[] {flowFile});
        } catch (final Exception e) {
            getLogger().error("Failed to publish Amazon SNS message for {} due to {}", new Object[] {flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
    }

}
