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
package org.apache.nifi.processors.aws.sqs;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

@SupportsBatching
@SeeAlso({ GetSQS.class, DeleteSQS.class })
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "AWS", "SQS", "Queue", "Put", "Publish"})
@CapabilityDescription("Publishes a message to an Amazon Simple Queuing Service Queue")
@DynamicProperty(name = "The name of a Message Attribute to add to the message", value = "The value of the Message Attribute",
        description = "Allows the user to add key/value pairs as Message Attributes by adding a property whose name will become the name of "
        + "the Message Attribute and value will become the value of the Message Attribute", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
public class PutSQS extends AbstractSQSProcessor {

    public static final PropertyDescriptor DELAY = new PropertyDescriptor.Builder()
            .name("Delay")
            .description("The amount of time to delay the message before it becomes available to consumers")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 secs")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(QUEUE_URL, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE,
                    REGION, DELAY, TIMEOUT, ENDPOINT_OVERRIDE, PROXY_HOST, PROXY_HOST_PORT));

    private volatile List<PropertyDescriptor> userDefinedProperties = Collections.emptyList();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        userDefinedProperties = new ArrayList<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                userDefinedProperties.add(descriptor);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final AmazonSQSClient client = getClient();
        final SendMessageBatchRequest request = new SendMessageBatchRequest();
        final String queueUrl = context.getProperty(QUEUE_URL).evaluateAttributeExpressions(flowFile).getValue();
        request.setQueueUrl(queueUrl);

        final Set<SendMessageBatchRequestEntry> entries = new HashSet<>();

        final SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();
        entry.setId(flowFile.getAttribute("uuid"));
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        final String flowFileContent = baos.toString();
        entry.setMessageBody(flowFileContent);

        final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

        for (final PropertyDescriptor descriptor : userDefinedProperties) {
            final MessageAttributeValue mav = new MessageAttributeValue();
            mav.setDataType("String");
            mav.setStringValue(context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue());
            messageAttributes.put(descriptor.getName(), mav);
        }

        entry.setMessageAttributes(messageAttributes);
        entry.setDelaySeconds(context.getProperty(DELAY).asTimePeriod(TimeUnit.SECONDS).intValue());
        entries.add(entry);

        request.setEntries(entries);

        try {
            client.sendMessageBatch(request);
        } catch (final Exception e) {
            getLogger().error("Failed to send messages to Amazon SQS due to {}; routing to failure", new Object[]{e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        getLogger().info("Successfully published message to Amazon SQS for {}", new Object[]{flowFile});
        session.transfer(flowFile, REL_SUCCESS);
        final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, queueUrl, transmissionMillis);
    }

}
