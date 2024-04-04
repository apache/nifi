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
package org.apache.nifi.processors.gcp.pubsub.lite;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.pubsub.AbstractGCPubSubProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.DYNAMIC_ATTRIBUTES_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.DYNAMIC_ATTRIBUTES_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_ATTRIBUTES_COUNT_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_ATTRIBUTES_COUNT_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_PUBLISH_TIME_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_PUBLISH_TIME_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.ORDERING_KEY_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.ORDERING_KEY_DESCRIPTION;
import static org.apache.nifi.processors.gcp.util.GoogleUtils.GOOGLE_CLOUD_PLATFORM_SCOPE;

@SeeAlso({PublishGCPubSubLite.class})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"google", "google-cloud", "gcp", "message", "pubsub", "consume", "lite"})
@CapabilityDescription("Consumes message from the configured Google Cloud PubSub Lite subscription.")
@WritesAttributes({
        @WritesAttribute(attribute = MESSAGE_ID_ATTRIBUTE, description = MESSAGE_ID_DESCRIPTION),
        @WritesAttribute(attribute = ORDERING_KEY_ATTRIBUTE, description = ORDERING_KEY_DESCRIPTION),
        @WritesAttribute(attribute = MSG_ATTRIBUTES_COUNT_ATTRIBUTE, description = MSG_ATTRIBUTES_COUNT_DESCRIPTION),
        @WritesAttribute(attribute = MSG_PUBLISH_TIME_ATTRIBUTE, description = MSG_PUBLISH_TIME_DESCRIPTION),
        @WritesAttribute(attribute = DYNAMIC_ATTRIBUTES_ATTRIBUTE, description = DYNAMIC_ATTRIBUTES_DESCRIPTION)
})
public class ConsumeGCPubSubLite extends AbstractGCPubSubProcessor implements VerifiableProcessor {

    public static final PropertyDescriptor SUBSCRIPTION = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-subscription")
            .displayName("Subscription")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .description("Name of the Google Cloud Pub/Sub Subscription. Example: projects/8476107443/locations/europe-west1-d/subscriptions/my-lite-subscription")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor BYTES_OUTSTANDING = new PropertyDescriptor
            .Builder().name("gcp-bytes-outstanding")
            .displayName("Bytes Outstanding")
            .description("The number of quota bytes that may be outstanding to the client.")
            .required(true)
            .defaultValue("10 MB")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MESSAGES_OUTSTANDING = new PropertyDescriptor
            .Builder().name("gcp-messages-outstanding")
            .displayName("Messages Outstanding")
            .description("The number of messages that may be outstanding to the client.")
            .required(true)
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            GCP_CREDENTIALS_PROVIDER_SERVICE,
            SUBSCRIPTION,
            BYTES_OUTSTANDING,
            MESSAGES_OUTSTANDING
    );

    public static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private Subscriber subscriber = null;
    private BlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<ValidationResult>(1);
        final String subscription = validationContext.getProperty(SUBSCRIPTION).evaluateAttributeExpressions().getValue();

        try {
            SubscriptionPath.parse(subscription);
        } catch (final ApiException e) {
            results.add(new ValidationResult.Builder()
                    .subject(SUBSCRIPTION.getName())
                    .input(subscription)
                    .valid(false)
                    .explanation("The Suscription does not have a valid format.")
                    .build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            if (subscriber == null) {
                subscriber = getSubscriber(context);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to create Google Cloud PubSub Lite Subscriber", e);
            throw new ProcessException(e);
        }
        try {
            subscriber.startAsync().awaitRunning();
        } catch (final Exception e) {
            getLogger().error("Failed to create Google Cloud PubSub Lite Subscriber", subscriber.failureCause());
            throw new ProcessException(e);
        }
    }

    @OnStopped
    public void onStopped() {
        try {
            if (subscriber != null) {
                subscriber.stopAsync().awaitTerminated();
                subscriber = null;
            }
        } catch (final Exception e) {
            getLogger().warn("Failed to gracefully shutdown the Google Cloud PubSub Lite Subscriber", e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (subscriber == null) {
            getLogger().error("Google Cloud PubSub Lite Subscriber was not properly created. Yielding the processor...");
            context.yield();
            return;
        }

        if (!subscriber.isRunning()) {
            getLogger().error("Google Cloud PubSub Lite Subscriber is not running. Yielding the processor...", subscriber.failureCause());
            throw new ProcessException(subscriber.failureCause());
        }

        final Message message = messages.poll();
        if (message == null) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(MESSAGE_ID_ATTRIBUTE, message.getMessage().getMessageId());
        attributes.put(ORDERING_KEY_ATTRIBUTE, message.getMessage().getOrderingKey());
        attributes.put(MSG_ATTRIBUTES_COUNT_ATTRIBUTE, String.valueOf(message.getMessage().getAttributesCount()));
        attributes.put(MSG_PUBLISH_TIME_ATTRIBUTE, String.valueOf(message.getMessage().getPublishTime().getSeconds()));
        attributes.putAll(message.getMessage().getAttributesMap());

        flowFile = session.putAllAttributes(flowFile, attributes);
        flowFile = session.write(flowFile, out -> out.write(message.getMessage().getData().toStringUtf8().getBytes()));

        session.transfer(flowFile, REL_SUCCESS);
        session.getProvenanceReporter().receive(flowFile, context.getProperty(SUBSCRIPTION).evaluateAttributeExpressions().getValue());

        message.getConsumer().ack();
    }

    private Subscriber getSubscriber(final ProcessContext context) {

        final SubscriptionPath subscriptionPath = SubscriptionPath.parse(context.getProperty(SUBSCRIPTION).evaluateAttributeExpressions().getValue());

        final FlowControlSettings flowControlSettings = FlowControlSettings.builder()
                .setBytesOutstanding(context.getProperty(BYTES_OUTSTANDING).evaluateAttributeExpressions().asDataSize(DataUnit.B).longValue())
                .setMessagesOutstanding(context.getProperty(MESSAGES_OUTSTANDING).evaluateAttributeExpressions().asLong())
                .build();

        final MessageReceiver receiver =
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    try {
                        messages.put(new Message(message, consumer));
                    } catch (final InterruptedException e) {
                        getLogger().error("Could not save the message inside the internal queue of the processor", e);
                    }
                };

        final SubscriberSettings subscriberSettings = SubscriberSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                .setSubscriptionPath(subscriptionPath)
                .setReceiver(receiver)
                .setPerPartitionFlowControlSettings(flowControlSettings)
                .build();

        return Subscriber.create(subscriberSettings);
    }

    private class Message {
        private PubsubMessage message;
        private AckReplyConsumer consumer;

        public Message(final PubsubMessage message, final AckReplyConsumer consumer) {
            this.message = message;
            this.consumer = consumer;
        }

        public PubsubMessage getMessage() {
            return message;
        }

        public AckReplyConsumer getConsumer() {
            return consumer;
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> verificationResults = new ArrayList<>();
        try {
            getSubscriber(context);
            verificationResults.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create the Subscriber")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully created the Google Cloud PubSub Lite Subscriber")
                    .build());
        } catch (final Exception e) {
            verificationLogger.error("Failed to create Google Cloud PubSub Lite Subscriber", e);

            verificationResults.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create the Subscriber")
                    .outcome(Outcome.FAILED)
                    .explanation("Failed to create Google Cloud PubSub Lite Subscriber: " + e.getLocalizedMessage())
                    .build());
        }
        return verificationResults;
    }

    @Override
    protected GoogleCredentials getGoogleCredentials(final ProcessContext context) {
        return super.getGoogleCredentials(context).createScoped(GOOGLE_CLOUD_PLATFORM_SCOPE);
    }
}
