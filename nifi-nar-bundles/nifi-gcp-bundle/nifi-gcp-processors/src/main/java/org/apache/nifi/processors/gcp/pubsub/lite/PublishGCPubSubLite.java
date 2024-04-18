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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.Publisher;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
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
import org.threeten.bp.Duration;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.TOPIC_NAME_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.TOPIC_NAME_DESCRIPTION;
import static org.apache.nifi.processors.gcp.util.GoogleUtils.GOOGLE_CLOUD_PLATFORM_SCOPE;

@SeeAlso({ConsumeGCPubSubLite.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"google", "google-cloud", "gcp", "message", "pubsub", "publish", "lite"})
@CapabilityDescription("Publishes the content of the incoming FlowFile to the configured Google Cloud PubSub Lite topic. The processor supports dynamic properties." +
        " If any dynamic properties are present, they will be sent along with the message in the form of 'attributes'.")
@DynamicProperty(name = "Attribute name", value = "Value to be set to the attribute",
        description = "Attributes to be set for the outgoing Google Cloud PubSub Lite message", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttributes({
        @WritesAttribute(attribute = MESSAGE_ID_ATTRIBUTE, description = MESSAGE_ID_DESCRIPTION),
        @WritesAttribute(attribute = TOPIC_NAME_ATTRIBUTE, description = TOPIC_NAME_DESCRIPTION)
})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content "
        + "will be read into memory to be sent as a PubSub message.")
public class PublishGCPubSubLite extends AbstractGCPubSubProcessor implements VerifiableProcessor {

    public static final PropertyDescriptor TOPIC_NAME = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-topic")
            .displayName("Topic Name")
            .description("Name of the Google Cloud PubSub Topic. Example: projects/8476107443/locations/europe-west1-d/topics/my-lite-topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor ORDERING_KEY = new PropertyDescriptor
            .Builder().name("gcp-ordering-key")
            .displayName("Ordering Key")
            .description("Messages with the same ordering key will always get published to the same partition. When this property is not "
                    + "set, messages can get published to different partitions if more than one partition exists for the topic.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            GCP_CREDENTIALS_PROVIDER_SERVICE,
            TOPIC_NAME,
            ORDERING_KEY,
            BATCH_SIZE_THRESHOLD,
            BATCH_BYTES_THRESHOLD,
            BATCH_DELAY_THRESHOLD
    );

    public static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    private Publisher publisher = null;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<ValidationResult>(1);
        final String topic = validationContext.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();

        try {
            TopicPath.parse(topic);
        } catch (final ApiException e) {
            results.add(new ValidationResult.Builder()
                    .subject(TOPIC_NAME.getName())
                    .input(topic)
                    .valid(false)
                    .explanation("The Topic does not have a valid format.")
                    .build());
        }

        return results;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            if (publisher == null) {
                publisher = getPublisher(context);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to create Google Cloud PubSub Lite Publisher", e);
            throw new ProcessException(e);
        }
        try {
            publisher.startAsync().awaitRunning();
        } catch (final Exception e) {
            getLogger().error("Failed to create Google Cloud PubSub Lite Publisher", publisher.failureCause());
            throw new ProcessException(e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int flowFileCount = context.getProperty(BATCH_SIZE_THRESHOLD).asInteger();
        final List<FlowFile> flowFiles = session.get(flowFileCount);

        if (flowFiles.isEmpty()) {
            context.yield();
            return;
        }

        if (publisher == null) {
            getLogger().error("Google Cloud PubSub Lite Publisher was not properly created. Yielding the processor...");
            context.yield();
            return;
        }

        if(!publisher.isRunning()) {
            getLogger().error("Google Cloud PubSub Lite Publisher is not running. Yielding the processor...", publisher.failureCause());
            throw new ProcessException(publisher.failureCause());
        }

        final long startNanos = System.nanoTime();
        final List<FlowFile> successfulFlowFiles = new ArrayList<>();
        final String topicName = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();
        final List<ApiFuture<String>> futures = new ArrayList<>();

        try {
            for (FlowFile flowFile : flowFiles) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                session.exportTo(flowFile, baos);
                final ByteString flowFileContent = ByteString.copyFrom(baos.toByteArray());
                final String orderingKey = context.getProperty(ORDERING_KEY).evaluateAttributeExpressions(flowFile).getValue();

                final PubsubMessage.Builder message = PubsubMessage.newBuilder().setData(flowFileContent)
                        .setPublishTime(Timestamp.newBuilder().build())
                        .putAllAttributes(getDynamicAttributesMap(context, flowFile));

                if (orderingKey != null) {
                    message.setOrderingKey(orderingKey);
                }

                final ApiFuture<String> messageIdFuture = publisher.publish(message.build());
                futures.add(messageIdFuture);

                flowFile = session.putAttribute(flowFile, TOPIC_NAME_ATTRIBUTE, topicName);
            }

            try {
                ApiFutures.allAsList(futures).get();
                successfulFlowFiles.addAll(flowFiles);
            } catch (InterruptedException | ExecutionException e) {
                getLogger().error("Failed to publish the messages to Google Cloud PubSub Lite topic '{}' due to {}, "
                        + "routing all messages from the batch to failure", topicName, e.getLocalizedMessage(), e);
                session.transfer(flowFiles, REL_FAILURE);
                context.yield();
            }
        } finally {
            if (!successfulFlowFiles.isEmpty()) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                for (FlowFile flowFile : successfulFlowFiles) {
                    final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    session.getProvenanceReporter().send(flowFile, topicName, transmissionMillis);
                }
            }
        }
    }

    @OnStopped
    public void onStopped() {
        try {
            if (publisher != null) {
                publisher.stopAsync().awaitTerminated();
                publisher = null;
            }
        } catch (final Exception e) {
            getLogger().warn("Failed to gracefully shutdown the Google Cloud PubSub Lite Publisher", e);
        }
    }

    private Map<String, String> getDynamicAttributesMap(final ProcessContext context, final FlowFile flowFile) {
        final Map<String, String> attributes = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                final String value = context.getProperty(entry.getKey()).evaluateAttributeExpressions(flowFile).getValue();
                attributes.put(entry.getKey().getName(), value);
            }
        }

        return attributes;
    }

    private Publisher getPublisher(final ProcessContext context) {
        final TopicPath topicPath = TopicPath.parse(context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue());

        final PublisherSettings publisherSettings =
                PublisherSettings.newBuilder()
                    .setTopicPath(topicPath)
                    .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                    .setBatchingSettings(BatchingSettings.newBuilder()
                            .setElementCountThreshold(context.getProperty(BATCH_SIZE_THRESHOLD).asLong())
                            .setRequestByteThreshold(context.getProperty(BATCH_BYTES_THRESHOLD).asDataSize(DataUnit.B).longValue())
                            .setDelayThreshold(Duration.ofMillis(context.getProperty(BATCH_DELAY_THRESHOLD).asTimePeriod(TimeUnit.MILLISECONDS)))
                            .setIsEnabled(true)
                            .build())
                    .build();

        return Publisher.create(publisherSettings);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> verificationResults = new ArrayList<>();
        try {
            getPublisher(context);
            verificationResults.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create the Publisher")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully created the Google Cloud PubSub Lite Publisher")
                    .build());
        } catch (final Exception e) {
            verificationLogger.error("Failed to create Google Cloud PubSub Lite Publisher", e);

            verificationResults.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create the Publisher")
                    .outcome(Outcome.FAILED)
                    .explanation("Failed to create Google Cloud PubSub Lite Publisher: " + e.getLocalizedMessage())
                    .build());
        }
        return verificationResults;
    }

    @Override
    protected GoogleCredentials getGoogleCredentials(final ProcessContext context) {
        return super.getGoogleCredentials(context).createScoped(GOOGLE_CLOUD_PLATFORM_SCOPE);
    }
}
