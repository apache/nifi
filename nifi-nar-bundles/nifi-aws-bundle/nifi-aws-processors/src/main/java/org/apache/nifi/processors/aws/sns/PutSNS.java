/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.aws.sns;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.sqs.GetSQS;
import org.apache.nifi.processors.aws.sqs.PutSQS;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;

@SupportsBatching
@SeeAlso({GetSQS.class, PutSQS.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "sns", "topic", "put", "publish", "pubsub"})
@CapabilityDescription("Sends the content of a FlowFile as a notification to the Amazon Simple Notification Service")
public class PutSNS extends AbstractSNSProcessor {

    public static final PropertyDescriptor CHARACTER_ENCODING = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The character set in which the FlowFile's content is encoded")
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor USE_JSON_STRUCTURE = new PropertyDescriptor.Builder()
            .name("Use JSON Structure")
            .description("If true, the contents of the FlowFile must be JSON with a top-level element named 'default'."
                    + " Additional elements can be used to send different messages to different protocols. See the Amazon"
                    + " SNS Documentation for more information.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .required(true)
            .build();
    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
            .name("E-mail Subject")
            .description("The optional subject to use for any subscribers that are subscribed via E-mail")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(ARN, ARN_TYPE, SUBJECT, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT,
                    USE_JSON_STRUCTURE, CHARACTER_ENCODING, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD));

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
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (flowFile.getSize() > MAX_SIZE) {
            getLogger().error("Cannot publish {} to SNS because its size exceeds Amazon SNS's limit of 256KB; routing to failure", new Object[]{flowFile});
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

        if (context.getProperty(USE_JSON_STRUCTURE).asBoolean()) {
            request.setMessageStructure("json");
        }

        final String arn = context.getProperty(ARN).evaluateAttributeExpressions(flowFile).getValue();
        final String arnType = context.getProperty(ARN_TYPE).getValue();
        if (arnType.equalsIgnoreCase(ARN_TYPE_TOPIC.getValue())) {
            request.setTopicArn(arn);
        } else {
            request.setTargetArn(arn);
        }

        final String subject = context.getProperty(SUBJECT).evaluateAttributeExpressions(flowFile).getValue();
        if (subject != null) {
            request.setSubject(subject);
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic() && !StringUtils.isEmpty(entry.getValue())) {
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
            getLogger().info("Successfully published notification for {}", new Object[]{flowFile});
        } catch (final Exception e) {
            getLogger().error("Failed to publish Amazon SNS message for {} due to {}", new Object[]{flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
