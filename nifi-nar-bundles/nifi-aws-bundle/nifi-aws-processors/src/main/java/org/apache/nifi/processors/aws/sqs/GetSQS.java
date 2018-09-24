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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

@SupportsBatching
@SeeAlso({ PutSQS.class, DeleteSQS.class })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"Amazon", "AWS", "SQS", "Queue", "Get", "Fetch", "Poll"})
@CapabilityDescription("Fetches messages from an Amazon Simple Queuing Service Queue")
@WritesAttributes({
    @WritesAttribute(attribute = "hash.value", description = "The MD5 sum of the message"),
    @WritesAttribute(attribute = "hash.algorithm", description = "MD5"),
    @WritesAttribute(attribute = "sqs.message.id", description = "The unique identifier of the SQS message"),
    @WritesAttribute(attribute = "sqs.receipt.handle", description = "The SQS Receipt Handle that is to be used to delete the message from the queue")
})
public class GetSQS extends AbstractSQSProcessor {

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set that should be used to encode the textual content of the SQS message")
            .required(true)
            .defaultValue("UTF-8")
            .allowableValues(Charset.availableCharsets().keySet().toArray(new String[0]))
            .build();

    public static final PropertyDescriptor AUTO_DELETE = new PropertyDescriptor.Builder()
            .name("Auto Delete Messages")
            .description("Specifies whether the messages should be automatically deleted by the processors once they have been received.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor VISIBILITY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Visibility Timeout")
            .description("The amount of time after a message is received but not deleted that the message is hidden from other consumers")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("15 mins")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of messages to send in a single network request")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1L, 10L, true))
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor DYNAMIC_QUEUE_URL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(QUEUE_URL)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .build();

    public static final PropertyDescriptor RECEIVE_MSG_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Receive Message Wait Time")
            .description("The maximum amount of time to wait on a long polling receive call. Setting this to a value of 1 second or greater will "
                + "reduce the number of SQS requests and decrease fetch latency at the cost of a constantly active thread.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("0 sec")
            .addValidator(StandardValidators.createTimePeriodValidator(0, TimeUnit.SECONDS, 20, TimeUnit.SECONDS))  // 20 seconds is the maximum allowed by SQS
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(DYNAMIC_QUEUE_URL, AUTO_DELETE, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE,
                    AWS_CREDENTIALS_PROVIDER_SERVICE, REGION, BATCH_SIZE, TIMEOUT, ENDPOINT_OVERRIDE,
                    CHARSET, VISIBILITY_TIMEOUT, RECEIVE_MSG_WAIT_TIME, PROXY_HOST, PROXY_HOST_PORT));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final String queueUrl = context.getProperty(DYNAMIC_QUEUE_URL).evaluateAttributeExpressions()
                .getValue();

        final AmazonSQSClient client = getClient();

        final ReceiveMessageRequest request = new ReceiveMessageRequest();
        request.setAttributeNames(Collections.singleton("All"));
        request.setMessageAttributeNames(Collections.singleton("All"));
        request.setMaxNumberOfMessages(context.getProperty(BATCH_SIZE).asInteger());
        request.setVisibilityTimeout(context.getProperty(VISIBILITY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue());
        request.setQueueUrl(queueUrl);
        request.setWaitTimeSeconds(context.getProperty(RECEIVE_MSG_WAIT_TIME).asTimePeriod(TimeUnit.SECONDS).intValue());

        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());

        final ReceiveMessageResult result;
        try {
            result = client.receiveMessage(request);
        } catch (final Exception e) {
            getLogger().error("Failed to receive messages from Amazon SQS due to {}", new Object[]{e});
            context.yield();
            return;
        }

        final List<Message> messages = result.getMessages();
        if (messages.isEmpty()) {
            context.yield();
            return;
        }

        final boolean autoDelete = context.getProperty(AUTO_DELETE).asBoolean();

        for (final Message message : messages) {
            FlowFile flowFile = session.create();

            final Map<String, String> attributes = new HashMap<>();
            for (final Map.Entry<String, String> entry : message.getAttributes().entrySet()) {
                attributes.put("sqs." + entry.getKey(), entry.getValue());
            }

            for (final Map.Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                attributes.put("sqs." + entry.getKey(), entry.getValue().getStringValue());
            }

            attributes.put("hash.value", message.getMD5OfBody());
            attributes.put("hash.algorithm", "md5");
            attributes.put("sqs.message.id", message.getMessageId());
            attributes.put("sqs.receipt.handle", message.getReceiptHandle());

            flowFile = session.putAllAttributes(flowFile, attributes);
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(message.getBody().getBytes(charset));
                }
            });

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().receive(flowFile, queueUrl);

            getLogger().info("Successfully received {} from Amazon SQS", new Object[]{flowFile});
        }

        if (autoDelete) {
            // If we want to auto-delete messages, we must fist commit the session to ensure that the data
            // is persisted in NiFi's repositories.
            session.commit();

            final DeleteMessageBatchRequest deleteRequest = new DeleteMessageBatchRequest();
            deleteRequest.setQueueUrl(queueUrl);
            final List<DeleteMessageBatchRequestEntry> deleteRequestEntries = new ArrayList<>();
            for (final Message message : messages) {
                final DeleteMessageBatchRequestEntry entry = new DeleteMessageBatchRequestEntry();
                entry.setId(message.getMessageId());
                entry.setReceiptHandle(message.getReceiptHandle());
                deleteRequestEntries.add(entry);
            }

            deleteRequest.setEntries(deleteRequestEntries);

            try {
                client.deleteMessageBatch(deleteRequest);
            } catch (final Exception e) {
                getLogger().error("Received {} messages from Amazon SQS but failed to delete the messages; these messages"
                        + " may be duplicated. Reason for deletion failure: {}", new Object[]{messages.size(), e});
            }
        }

    }

}
